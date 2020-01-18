use crate::batch_storage::{BatchStorage, BinaryBatch, BatchFactory};
use crate::chained_error::ChainedError;
use crate::batch_records::{RecordsBuilder, RecordsBuilderFactory};
use std::time::{Duration, SystemTime};
use std::{io, thread, error, mem};
use crate::batch_sender::BatchSender;
use std::ops::Deref;
use std::thread::JoinHandle;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, MutexGuard};
use std::io::{Error, ErrorKind};
use log::*;


const DEFAULT_MAX_BATCH_RECORDS: u32 = 10000;
const DEFAULT_MAX_BATCH_BYTES: usize = 1024 * 1024;

pub trait Batcher<T> {
    fn start(&self) -> bool;
    fn stop(self) -> io::Result<()>;
    fn hard_stop(self) -> io::Result<()>;
    fn soft_stop(self)-> io::Result<()>;
    fn is_stopped(&self) -> bool;

    fn put(&self, record: T) -> io::Result<()>;
    fn put_all(&self, records: impl Iterator<Item=T>) -> io::Result<()>;
    fn flush(&self) -> io::Result<()>;
    fn flush_if_needed(&self) -> io::Result<bool>;
}


pub struct BatcherSharedState<T, Records, Builder: RecordsBuilder<T, Records>> {
    stopped: bool,
    hard_stop: bool,
    soft_stop: bool,
    last_flush_time: SystemTime,
    records_builder: Builder,
    upload_thread: Option<JoinHandle<()>>,
    last_upload_result: Arc<io::Result<()>>,

    phantom_t: PhantomData<T>,
    phantom_r: PhantomData<Records>,
}

#[derive(Clone)]
pub struct BatcherImpl<T, Records, Builder, BuilderFactory, Batch, Factory, Storage, Sender>
    where
        Builder: RecordsBuilder<T, Records>,
        BuilderFactory: RecordsBuilderFactory<T, Records, Builder>,
        Batch: Deref<Target=BinaryBatch> + Clone,
        Factory: BatchFactory<Records>,
        Storage: BatchStorage<Batch>,
        Sender: BatchSender
{
    builder_factory: BuilderFactory,
    batch_factory: Factory,
    batch_storage: Storage,
    batch_sender: Sender,

    pub retry_batch_upload: bool,
    pub clock: fn() -> SystemTime,
    pub max_batch_records: u32,
    pub max_batch_bytes: usize,
    pub flush_period: Duration,
    pub read_retry_timeout: Duration,
    pub failed_upload_timeout: Duration,

    pub shared_state: Arc<Mutex<BatcherSharedState<T, Records, Builder>>>,

    phantom_t: PhantomData<T>,
    phantom_r: PhantomData<Records>,
    phantom_b: PhantomData<Batch>,
}

impl<T: Clone + Send + 'static, Records: Clone + Send + 'static, Builder, BuilderFactory, Batch, Factory, Storage, Sender>
BatcherImpl<T, Records, Builder, BuilderFactory, Batch, Factory, Storage, Sender>
    where
        Builder: RecordsBuilder<T, Records>,
        BuilderFactory: RecordsBuilderFactory<T, Records, Builder>,
        Batch: Deref<Target=BinaryBatch> + Clone + Send + 'static,
        Factory: BatchFactory<Records>,
        Storage: BatchStorage<Batch>,
        Sender: BatchSender
{
    pub fn new(builder_factory: BuilderFactory, batch_factory: Factory, batch_storage: Storage, batch_sender: Sender) -> Self {
        let records_builder = builder_factory.create_builder();

        BatcherImpl {
            builder_factory,
            batch_factory,
            batch_storage,
            batch_sender,
            retry_batch_upload: true,
            clock: || SystemTime::now(),
            max_batch_records: DEFAULT_MAX_BATCH_RECORDS,
            max_batch_bytes: DEFAULT_MAX_BATCH_BYTES,
            flush_period: Duration::from_secs(60),
            read_retry_timeout: Duration::from_secs(3),
            failed_upload_timeout: Duration::from_secs(1),

            shared_state: Arc::new(Mutex::new(BatcherSharedState {
                stopped: true,
                hard_stop: false,
                soft_stop: false,
                last_flush_time: SystemTime::UNIX_EPOCH,
                records_builder,
                upload_thread: None,
                last_upload_result: Arc::new(Ok(())),

                phantom_t: PhantomData,
                phantom_r: PhantomData,
            })),

           phantom_t: PhantomData,
           phantom_r: PhantomData,
           phantom_b: PhantomData,
        }
    }

    fn upload(self) {
        trace!("Upload starting...");

        let mut uploaded_batch_counter = 0;
        let mut all_batch_counter = 0;
        let mut send_batches_bytes = 0;
        'outer: loop {
            let batch_read_start = (self.clock)();
            let batch_result = self.batch_storage.get();
            if let Err(e) = batch_result {
                if e.kind() == ErrorKind::Interrupted {
                    if self.should_interrupt(self.shared_state.lock().unwrap()) {
                        break;
                    } else {
                        continue;
                    }
                } else {
                    error!("Error while reading batch: {}", e);
                    thread::sleep(self.read_retry_timeout);
                    continue;
                }
            }

            all_batch_counter += 1;
            let batch = batch_result.unwrap();

            trace!("{} read time: {:?}", *batch, self.since(batch_read_start));

            loop {
                let batch_upload_start = (self.clock)();
                let send_result = self.batch_sender.send_batch(&batch.bytes);
                if let Err(e) = send_result {
                    error!("Unexpected exception while sending the {}: {}", *batch, e);
                    self.shared_state.lock().unwrap() .last_upload_result = Arc::new(Err(e));
                    self.stop().unwrap();
                    return;
                }

                trace!("{} send time: {:?}", *batch, self.since(batch_upload_start));

                let success = if let Ok(None) = send_result {
                    trace!("{} successfully uploaded", *batch);
                    uploaded_batch_counter += 1;
                    send_batches_bytes += batch.bytes.len();

                    self.try_remove(&batch);
                    true
                } else {
                    warn!("Error while sending {}: {}", *batch, send_result.unwrap().unwrap());

                    thread::sleep(self.failed_upload_timeout);

                    if !self.retry_batch_upload {
                        self.try_remove(&batch);
                    }
                    false
                };

                let mutex_guard = self.shared_state.lock().unwrap();
                if mutex_guard.stopped && self.should_interrupt(mutex_guard) {
                    debug!("Upload stopped");
                    break 'outer;
                }

                if success || !self.retry_batch_upload {
                    break;
                }

                info!("Retrying the {}", *batch);
            }
        }

        info!("{} from {} batches uploaded. Send {} bytes", uploaded_batch_counter, all_batch_counter, send_batches_bytes);
    }

    fn since(&self, since: SystemTime) -> Duration {
        (self.clock)().duration_since(since).unwrap_or(Duration::from_secs(0))
    }

    fn try_remove(&self, batch: &Batch) {
        if let Err(e) = self.batch_storage.remove() {
            error!("Error while removing uploaded {}: {}", **batch, e);
        }
    }

    fn should_interrupt(&self, mutex_guard: MutexGuard<BatcherSharedState<T, Records, Builder>>) -> bool {
        mutex_guard.hard_stop || (self.batch_storage.is_persistent() && !mutex_guard.soft_stop) || self.batch_storage.is_empty()
    }

    fn stop_inner(self, hard: bool, soft: bool) -> io::Result<()> {
        {
            let mut mutex_guard = self.shared_state.lock().unwrap();
            if mutex_guard.stopped {
                return Ok(());
            }

            mutex_guard.stopped = true;
            mutex_guard.hard_stop = hard;
            mutex_guard.soft_stop = soft;

            self.flush_inner(&mut mutex_guard)?;

            self.batch_storage.shutdown();
            mutex_guard.upload_thread.take()
        }
            .map(|upload_thread| upload_thread.join())
            .map(|r| r.map_err(|e|
                if let Ok(error) = e.downcast::<Error>() {
                    Error::new(ErrorKind::Other,
                               ChainedError::new("The upload thread panicked with the reason", error))
                } else {
                    Error::new(ErrorKind::Other, "The upload thread panicked with unknown reason")
                }))
            .unwrap_or(Ok(()))
    }

    fn check_state(&self, mutex_guard: &MutexGuard<BatcherSharedState<T, Records, Builder>>) -> io::Result<()> {
        if mutex_guard.stopped {
            return Err(Error::new(ErrorKind::Interrupted,
                                  ChainedError::monad(
                                      "The batcher has been stopped",
                                      Box::new(mutex_guard.last_upload_result.clone()),
                                      Box::new(|any|
                                          any.downcast_ref::<Arc<io::Result<()>>>()
                                              .map(|arc| (**arc).as_ref().err())
                                              .flatten()
                                              .map(|e| e as &(dyn error::Error))))));
        }
        Ok(())
    }

    fn need_flush(&self, mutex_guard: &MutexGuard<BatcherSharedState<T, Records, Builder>>) -> bool {
        (self.max_batch_bytes > 0 && mutex_guard.records_builder.size() >= self.max_batch_bytes)
            || (self.max_batch_records > 0 && mutex_guard.records_builder.len() >= self.max_batch_records)
            || self.since(mutex_guard.last_flush_time) >= self.flush_period
    }

    fn flush_inner(&self, mutex_guard: &mut MutexGuard<BatcherSharedState<T, Records, Builder>>) -> io::Result<()> {
        let records_builder = mem::replace(&mut mutex_guard.records_builder,
                                           self.builder_factory.create_builder());
        let len = records_builder.len();
        let size = records_builder.size();
        trace!("Flushing {} bytes in {} records", size, len);

        if len == 0 {
            debug!("Nothing to flush");
            return Ok(());
        }

        let result = self.batch_storage.store(records_builder.build(), &self.batch_factory);
        if result.is_ok() {
            debug!("Flushing completed for {} bytes in {} records", size, len);
            mutex_guard.last_flush_time = (self.clock)();
        } else {
            warn!("Flushing failed for {} bytes in {} records with error: {}", size, len, result.as_ref().unwrap_err());
        }

        result
    }

    fn put_inner(&self, record: T, mutex_guard: &mut MutexGuard<BatcherSharedState<T, Records, Builder>>) -> io::Result<()> {
        if self.need_flush(mutex_guard) {
            self.flush_inner(mutex_guard)?
        }

        mutex_guard.records_builder.add(record);

        Ok(())
    }
}

impl<T: Clone + Send + 'static, Records: Clone + Send + 'static, Builder, BuilderFactory, Batch, Factory, Storage, Sender>
Batcher<T> for BatcherImpl<T, Records, Builder, BuilderFactory, Batch, Factory, Storage, Sender>
    where
        Builder: RecordsBuilder<T, Records>,
        BuilderFactory: RecordsBuilderFactory<T, Records, Builder>,
        Batch: Deref<Target=BinaryBatch> + Clone + Send + 'static,
        Factory: BatchFactory<Records>,
        Storage: BatchStorage<Batch>,
        Sender: BatchSender
{
    fn start(&self) -> bool {
        let mut mutex_guard = self.shared_state.lock().unwrap();
        if mutex_guard.upload_thread.is_some() {
            return false;
        }

        mutex_guard.stopped = false;
        mutex_guard.last_flush_time = (self.clock)();

        let cloned_batcher = self.clone();
        mutex_guard.upload_thread = Some(thread::Builder::new().name("batcher-upload".to_string()).spawn(move || {
            cloned_batcher.upload();
        }).unwrap());

        true
    }

    fn stop(self) -> io::Result<()> {
        self.stop_inner(false, false)
    }

    fn hard_stop(self) -> io::Result<()> {
        self.stop_inner(true, false)
    }

    fn soft_stop(self) -> io::Result<()> {
        self.stop_inner(false, true)
    }

    fn is_stopped(&self) -> bool {
        self.shared_state.lock().unwrap().stopped
    }

    fn put(&self, record: T) -> Result<(), Error> {
        let mut mutex_guard = self.shared_state.lock().unwrap();
        self.check_state(&mutex_guard)?;
        self.put_inner(record, &mut mutex_guard)
    }

    fn put_all(&self, records: impl Iterator<Item=T>) -> Result<(), Error> {
        let mut mutex_guard = self.shared_state.lock().unwrap();
        self.check_state(&mutex_guard)?;

        for record in records {
            self.put_inner(record, &mut mutex_guard)?
        }
        
        Ok(())
    }

    fn flush(&self) -> Result<(), Error> {
        self.flush_inner(&mut self.shared_state.lock().unwrap())
    }

    fn flush_if_needed(&self) -> Result<bool, Error> {
        let mut mutex_guard = self.shared_state.lock().unwrap();
        self.check_state(&mutex_guard)?;

        if self.need_flush(&mutex_guard) {
            self.flush_inner(&mut mutex_guard)?;
            return Ok(true)
        }

        return Ok(false)
    }
}

#[cfg(test)]
mod test {
    use crate::batch_sender::BatchSender;
    use std::io::{Error, ErrorKind};
    use crate::batcher::{BatcherImpl, Batcher};
    use crate::batch_storage::{GzippedJsonDisplayBatchFactory, BinaryBatch, BatchStorage, BatchFactory};
    use crate::memory_storage::{MemoryStorage, NonBlockingMemoryStorage};
    use crate::batch_records::{RECORDS_BUILDER_FACTORY, JsonArrayRecordsBuilder, JsonArrayRecordsBuilderFactory};
    use std::{thread, io};
    use std::time::{Duration, SystemTime};
    use std::sync::{Once, Arc, Mutex};
    use env_logger::{Builder, Env};
    use miniz_oxide::inflate::decompress_to_vec;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::collections::VecDeque;

    #[derive(Clone)]
    struct MockBatchSender {
        batches: Arc<Mutex<Vec<Vec<u8>>>>,
        result_supplier: Arc<dyn Fn() -> io::Result<Option<Error>> + Send + Sync>
    }

    impl MockBatchSender {
        fn new() -> MockBatchSender {
            MockBatchSender::with_result(Arc::new(|| Ok(None)))
        }

        fn with_result(result_supplier: Arc<dyn Fn() -> io::Result<Option<Error>> + Send + Sync>) -> MockBatchSender {
            MockBatchSender {
                batches: Arc::new(Mutex::new(Vec::new())),
                result_supplier
            }
        }
    }

    impl BatchSender for MockBatchSender {
        fn send_batch(&self, batch: &[u8]) -> io::Result<Option<Error>> {
            self.batches.lock().unwrap().push(batch.to_owned());
            thread::sleep(Duration::from_millis(1));
            (self.result_supplier)()
        }
    }

    #[derive(Clone)]
    struct NothingBatchSender(Arc<AtomicBool>);

    impl NothingBatchSender {
        fn new() -> NothingBatchSender {
            NothingBatchSender(Arc::new(AtomicBool::new(false)))
        }
    }

    impl BatchSender for NothingBatchSender {
        fn send_batch(&self, batch: &[u8]) -> Result<Option<Error>, Error> {
            while !self.0.load(Ordering::Relaxed) {
                thread::yield_now();
            }
            Ok(None)
        }
    }

    #[derive(Clone)]
    pub struct PersistentMemoryStorage(MemoryStorage);

    impl<'a> BatchStorage<Arc<BinaryBatch>> for PersistentMemoryStorage {
        fn store<T>(&self, records: T, batch_factory: &impl BatchFactory<T>) -> io::Result<()> {
            self.0.store(records, batch_factory)
        }

        fn get(&self) -> io::Result<Arc<BinaryBatch>> {
            self.0.get()
        }
        fn remove(&self) -> io::Result<()> {
            self.0.remove()
        }
        fn is_persistent(&self) -> bool {
            true
        }
        fn is_empty(&self) -> bool {
            self.0.is_empty()
        }
        fn shutdown(self) {
            self.0.shutdown()
        }
    }

    const BATCH1: &str = r#"{"serverId":s1,"batchId":1,"batch":[test1]}"#;
    const BATCH2: &str = r#"{"serverId":s1,"batchId":2,"batch":[test2]}"#;

    static INIT: Once = Once::new();

    fn init() {
        INIT.call_once(|| {
            Builder::from_env(Env::default().default_filter_or("trace")).is_test(true).init();
        });
    }

    fn memory_storage() -> MemoryStorage {
        let mut storage = MemoryStorage::new();
        storage.0.clock = || 1;
        storage
    }

    #[test]
    fn do_not_start() {
        let batcher = BatcherImpl::new(
            RECORDS_BUILDER_FACTORY,
            GzippedJsonDisplayBatchFactory::new("s1"),
            MemoryStorage::new(),
            MockBatchSender::new());

        let result = batcher.put("test1");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Interrupted);
        assert_eq!(error.get_ref().unwrap().to_string(), "The batcher has been stopped");
    }

    #[test]
    fn send_manually() {
        init();
        let batch_sender = MockBatchSender::new();
        let memory_storage = memory_storage();

        let batcher = BatcherImpl::new(
            RECORDS_BUILDER_FACTORY,
            GzippedJsonDisplayBatchFactory::new("s1"),
            memory_storage,
            batch_sender.clone());

        assert!(batcher.start());
        batcher.put("test1").unwrap();
        batcher.flush().unwrap();

        thread::sleep(Duration::from_millis(1));

        batcher.hard_stop().unwrap();

        let batches_guard = batch_sender.batches.lock().unwrap();
        assert_eq!(batches_guard.len(), 1);
        validate_batch(&batches_guard[0], BATCH1);
    }

    #[test]
    fn store_by_batch_records() {
        validate_stored_by(|batcher| batcher.max_batch_records = 1);
    }

    #[test]
    fn store_by_batch_bytes() {
        validate_stored_by(|batcher| batcher.max_batch_bytes = 1);
    }

    #[test]
    fn store_by_time() {
        init();
        let batch_sender = NothingBatchSender::new();
        let memory_storage = memory_storage();

        let mut batcher = BatcherImpl::new(
            RECORDS_BUILDER_FACTORY,
            GzippedJsonDisplayBatchFactory::new("s1"),
            memory_storage.clone(),
            batch_sender.clone());

        batcher.flush_period = Duration::from_secs(1);
        batcher.clock = || SystemTime::UNIX_EPOCH;

        assert!(batcher.start());
        batcher.put("test1").unwrap();

        validate_batches_queue0(&memory_storage);

        batcher.clock = || SystemTime::UNIX_EPOCH + Duration::from_secs(1);
        batcher.put("test2").unwrap();

        validate_batches_queue1(&memory_storage);

        let cloned_batcher = batcher.clone();
        let stop_thread = thread::spawn(move || {
            cloned_batcher.hard_stop().unwrap()
        });

        thread::sleep(Duration::from_millis(1));
        validate_batches_queue2(&memory_storage);

        batch_sender.0.store(true, Ordering::Relaxed);
        stop_thread.join().unwrap();
    }

    #[test]
    fn store_discarded() {
        init();
        let batch_sender = NothingBatchSender::new();
        let memory_storage = NonBlockingMemoryStorage::with_max_batch_bytes(0);

        let mut batcher = BatcherImpl::new(
            RECORDS_BUILDER_FACTORY,
            GzippedJsonDisplayBatchFactory::new("s1"),
            memory_storage.clone(),
            batch_sender);

        batcher.max_batch_records = 1;
        assert!(batcher.start());
        batcher.put("test1").unwrap();

        let result = batcher.put("test2");

        assert!(result.unwrap_err().to_string().starts_with("Storage capacity exceeded"));
        assert!(memory_storage.is_empty());
    }

    #[test]
    fn sender_success() {
        init();
        let batch_sender = MockBatchSender::new();
        let memory_storage = memory_storage();

        let batcher = BatcherImpl::new(
            RECORDS_BUILDER_FACTORY,
            GzippedJsonDisplayBatchFactory::new("s1"),
            memory_storage.clone(),
            batch_sender.clone());

        assert!(batcher.start());
        batcher.put("test1").unwrap();
        batcher.flush().unwrap();

        thread::sleep(Duration::from_millis(1));

        let batches_guard = batch_sender.batches.lock().unwrap();
        assert_eq!(batches_guard.len(), 1);
        validate_batch(&batches_guard[0], BATCH1);

        batcher.stop().unwrap();

        validate_batches_queue0(&memory_storage);
    }

    #[test]
    fn sender_success_several_batches() {
        init();
        let batch_sender = MockBatchSender::new();
        let memory_storage = memory_storage();

        let batcher = BatcherImpl::new(
            RECORDS_BUILDER_FACTORY,
            GzippedJsonDisplayBatchFactory::new("s1"),
            memory_storage.clone(),
            batch_sender.clone());

        assert!(batcher.start());
        batcher.put("test1").unwrap();
        batcher.flush().unwrap();
        batcher.put("test2").unwrap();

        batcher.stop().unwrap();

        let batches_guard = batch_sender.batches.lock().unwrap();
        assert_eq!(batches_guard.len(), 2);
        validate_batch(&batches_guard[0], BATCH1);
        validate_batch(&batches_guard[1], BATCH2);

        validate_batches_queue0(&memory_storage);
    }

    #[test]
    fn persistent_storage_and_stop() {
        init();
        let batch_sender = MockBatchSender::new();
        let persistent_memory_storage = PersistentMemoryStorage(memory_storage());

        let batcher = BatcherImpl::new(
            RECORDS_BUILDER_FACTORY,
            GzippedJsonDisplayBatchFactory::new("s1"),
            persistent_memory_storage.clone(),
            batch_sender.clone());

        assert!(batcher.start());
        batcher.put("test1").unwrap();
        batcher.flush().unwrap();
        batcher.put("test2").unwrap();

        batcher.stop().unwrap();

        let batches_guard = batch_sender.batches.lock().unwrap();
        assert_eq!(batches_guard.len(), 1);
        validate_batch(&batches_guard[0], BATCH1);

        validate_batches_queue(&persistent_memory_storage.0, |batches| {
            assert_eq!(batches.len(), 1);
            validate_batch(&batches[0].bytes, BATCH2);
        });
    }

    #[test]
    fn persistent_storage_and_soft_stop() {
        init();
        let batch_sender = MockBatchSender::new();
        let persistent_memory_storage = PersistentMemoryStorage(memory_storage());

        let batcher = BatcherImpl::new(
            RECORDS_BUILDER_FACTORY,
            GzippedJsonDisplayBatchFactory::new("s1"),
            persistent_memory_storage.clone(),
            batch_sender.clone());

        assert!(batcher.start());
        batcher.put("test1").unwrap();
        batcher.flush().unwrap();
        batcher.put("test2").unwrap();

        batcher.soft_stop().unwrap();

        let batches_guard = batch_sender.batches.lock().unwrap();
        assert_eq!(batches_guard.len(), 2);
        validate_batch(&batches_guard[0], BATCH1);
        validate_batch(&batches_guard[1], BATCH2);

        validate_batches_queue0(&persistent_memory_storage.0);
    }

    #[test]
    fn sender_fail() {
        init();
        let batch_sender = MockBatchSender::with_result(Arc::new(
            || Ok(Some(Error::new(ErrorKind::Other, "Test error")))));
        let memory_storage = memory_storage();

        let mut  batcher = BatcherImpl::new(
            RECORDS_BUILDER_FACTORY,
            GzippedJsonDisplayBatchFactory::new("s1"),
            memory_storage.clone(),
            batch_sender.clone());

        batcher.failed_upload_timeout = Duration::from_millis(1);
        assert!(batcher.start());

        batcher.put("test1").unwrap();
        batcher.flush().unwrap();

        thread::sleep(Duration::from_millis(5));

        batcher.hard_stop().unwrap();

        let batches_guard = batch_sender.batches.lock().unwrap();
        assert!(batches_guard.len() >= 2);
        validate_batch(&batches_guard[0], BATCH1);
        validate_batch(&batches_guard[1], BATCH1);

        validate_batches_queue1(&memory_storage);
    }

    #[test]
    fn sender_fail_do_not_retry_upload() {
        init();
        let batch_sender = MockBatchSender::with_result(Arc::new(
            || Ok(Some(Error::new(ErrorKind::Other, "Test error")))));
        let memory_storage = memory_storage();

        let mut  batcher = BatcherImpl::new(
            RECORDS_BUILDER_FACTORY,
            GzippedJsonDisplayBatchFactory::new("s1"),
            memory_storage.clone(),
            batch_sender.clone());

        batcher.failed_upload_timeout = Duration::from_millis(1);
        batcher.retry_batch_upload = false;
        assert!(batcher.start());

        batcher.put("test1").unwrap();
        batcher.flush().unwrap();

        thread::sleep(Duration::from_millis(5));

        batcher.hard_stop().unwrap();

        let batches_guard = batch_sender.batches.lock().unwrap();
        assert_eq!(batches_guard.len(), 1);
        validate_batch(&batches_guard[0], BATCH1);

        validate_batches_queue0(&memory_storage);
    }

    #[test]
    fn sender_exception() {
        init();
        let batch_sender = MockBatchSender::with_result(Arc::new(
            || Err(Error::new(ErrorKind::Other, "Test error"))));
        let memory_storage = memory_storage();

        let mut  batcher = BatcherImpl::new(
            RECORDS_BUILDER_FACTORY,
            GzippedJsonDisplayBatchFactory::new("s1"),
            memory_storage.clone(),
            batch_sender.clone());

        batcher.failed_upload_timeout = Duration::from_millis(1);
        assert!(batcher.start());
        batcher.put("test1").unwrap();
        batcher.flush().unwrap();

        thread::sleep(Duration::from_millis(5));

        let result = batcher.put("test2");
        assert!(result.is_err());

        batcher.hard_stop().unwrap();

        validate_batches_queue1(&memory_storage);
    }

    type BatchImplType<'a> = BatcherImpl<&'a str, String, JsonArrayRecordsBuilder,
        JsonArrayRecordsBuilderFactory, Arc<BinaryBatch>, GzippedJsonDisplayBatchFactory<String>, MemoryStorage, NothingBatchSender>;

    fn validate_stored_by(batcher_consumer: impl Fn(&mut BatchImplType<'_>)) {
        init();
        let batch_sender = NothingBatchSender::new();
        let memory_storage = memory_storage();

        let mut batcher = BatcherImpl::new(
            RECORDS_BUILDER_FACTORY,
            GzippedJsonDisplayBatchFactory::new("s1"),
            memory_storage.clone(),
            batch_sender.clone());

        batcher_consumer(&mut batcher);
        assert!(batcher.start());
        batcher.put("test1").unwrap();
        batcher.put("test2").unwrap();

        validate_batches_queue1(&memory_storage);

        let cloned_batcher = batcher.clone();
        let stop_thread = thread::spawn(move || {
            cloned_batcher.hard_stop().unwrap()
        });

        thread::sleep(Duration::from_millis(1));
        validate_batches_queue2(&memory_storage);

        batch_sender.0.store(true, Ordering::Relaxed);
        stop_thread.join().unwrap();
    }

    fn validate_batches_queue(memory_storage: &MemoryStorage, f: impl Fn(&VecDeque<Arc<BinaryBatch>>)) {
        let mutex_guard = memory_storage.0.shared_state.lock();
        let batches = &mutex_guard.batches_queue;
        f(batches);
    }

    fn validate_batches_queue0(memory_storage: &MemoryStorage) {
        validate_batches_queue(&memory_storage, |batches| {
            assert_eq!(batches.len(), 0);
        });
    }

    fn validate_batches_queue1(memory_storage: &MemoryStorage) {
        validate_batches_queue(&memory_storage, |batches| {
            assert_eq!(batches.len(), 1);
            validate_batch(&batches[0].bytes, BATCH1);
        });
    }

    fn validate_batches_queue2(memory_storage: &MemoryStorage) {
        validate_batches_queue(&memory_storage, |batches| {
            assert_eq!(batches.len(), 2);
            validate_batch(&batches[1].bytes, BATCH2);
        });
    }

    fn validate_batch(bytes: &[u8], batch: &str) {
        assert_eq!(String::from_utf8(decompress_to_vec(bytes).unwrap()).unwrap(), batch);
    }
}

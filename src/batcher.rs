use crate::batch_storage::{BatchStorage, BinaryBatch, BatchFactory};
use std::time::{Duration, SystemTime};
use std::{io, thread, error, mem};
use crate::batch_sender::BatchSender;
use std::ops::Deref;
use std::thread::JoinHandle;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, MutexGuard};
use std::io::{Error, ErrorKind};
use log::*;
use crate::chained_error::ChainedError;


const DEFAULT_MAX_BATCH_RECORDS: u32 = 10000;
const DEFAULT_MAX_BATCH_BYTES: usize = 1024 * 1024;

pub trait RecordsBuilder<T, R>: Clone + Send + 'static {
    fn add(&mut self, record: T);
    fn len(&self) -> u32;
    fn size(&self) -> usize;
    fn build(self) -> R;
}

pub trait RecordsBuilderFactory<T, R, Builder: RecordsBuilder<T, R>>: Clone + Send + 'static {
    fn create_builder(&self) -> Builder;
}

impl<T, R, Builder> RecordsBuilderFactory<T, R, Builder> for fn() -> Builder
    where Builder: RecordsBuilder<T, R>
{
    fn create_builder(&self) -> Builder {
        self()
    }
}

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

    fn upload(mut self) {
        trace!("Upload starting...");

        let mut uploaded_batch_counter = 0;
        let mut all_batch_counter = 0;
        let mut send_batches_bytes = 0;
        loop {
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

                debug!("{} finished. Took {:?}", *batch, self.since(batch_upload_start));

                if let Ok(None) = send_result {
                    trace!("{} successfully uploaded", *batch);
                    uploaded_batch_counter += 1;
                    send_batches_bytes += batch.bytes.len();

                    self.try_remove(&batch);
                    break;
                }

                warn!("Error while uploading {}: {}", *batch, send_result.unwrap().unwrap());

                thread::sleep(self.failed_upload_timeout);

                if !self.retry_batch_upload {
                    self.try_remove(&batch);
                    break;
                }

                info!("Retrying the {}", *batch);
            }

            let mutex_guard = self.shared_state.lock().unwrap();
            if mutex_guard.stopped && self.should_interrupt(mutex_guard) {
                break;
            }
        }

        info!("{} from {} batches uploaded. Send {} bytes", uploaded_batch_counter, all_batch_counter, send_batches_bytes);
    }

    fn since(&self, since: SystemTime) -> Duration {
        (self.clock)().duration_since(since).unwrap_or(Duration::from_secs(0))
    }

    fn try_remove(&mut self, batch: &Batch) {
        if let Err(e) = self.batch_storage.remove() {
            error!("Error while removing uploaded {}: {}", **batch, e);
        }
    }

    fn should_interrupt(&self, mutex_guard: MutexGuard<BatcherSharedState<T, Records, Builder>>) -> bool {
        mutex_guard.hard_stop || (self.batch_storage.is_persistent() && !mutex_guard.soft_stop) || self.batch_storage.is_empty()
    }

    fn stop_inner(self, hard: bool, soft: bool) -> io::Result<()> {
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
            .map(|upload_thread| upload_thread.join())
            .map(|r| r.map_err(|e|
                if let Ok(error) = e.downcast::<Error>() {
                    Error::new(ErrorKind::Other,
                               ChainedError::new("The upload thread panicked with the reason", error))
                } else {
                    Error::new(ErrorKind::Other, "The upload thread panicked with unknown reason".to_string())
                }))
            .unwrap_or(Ok(()))
    }

    fn check_state(&self, mutex_guard: &MutexGuard<BatcherSharedState<T, Records, Builder>>) -> io::Result<()> {
        if mutex_guard.stopped {
            return Err(Error::new(ErrorKind::Interrupted,
                                  ChainedError::monad(
                                      "The batcher has been stopped".to_string(),
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
            || self.since(mutex_guard.last_flush_time) > self.flush_period
    }

    fn flush_inner(&self, mutex_guard: &mut MutexGuard<BatcherSharedState<T, Records, Builder>>) -> io::Result<()> {
        let records_builder = mem::replace(&mut mutex_guard.records_builder, self.builder_factory.create_builder());
        let len = records_builder.len();
        let size = records_builder.size();
        trace!("Flushing {} bytes in {} actions", size, len);

        if len == 0 {
            debug!("Nothing to flush");
            return Ok(());
        }

        let result = self.batch_storage.store(records_builder.build(), &self.batch_factory);
        if result.is_ok() {
            debug!("Flushing completed for {} bytes in {} actions", size, len);
            mutex_guard.last_flush_time = (self.clock)();
        } else {
            warn!("Flushing failed for {} bytes in {} actions with error: {}", size, len, result.as_ref().unwrap_err());
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

use crate::batch_storage::{BatchStorage, BinaryBatch, BatchFactory};
use std::time::{Duration, SystemTime};
use std::{io, thread, error};
use crate::batch_sender::BatchSender;
use std::ops::Deref;
use std::thread::JoinHandle;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, MutexGuard};
use std::io::{Error, ErrorKind};
use log::*;


const DEFAULT_MAX_BATCH_RECORDS: u32 = 10000;
const DEFAULT_MAX_BATCH_BYTES: usize = 1024 * 1024;

pub fn duration_since(now: SystemTime, since: SystemTime) -> Duration {
    now.duration_since(since).unwrap_or(Duration::from_secs(0))
}

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
    fn start(&mut self) -> bool;
    fn stop(self) -> io::Result<()>;
    fn hard_stop(self) -> io::Result<()>;
    fn soft_stop(self)-> io::Result<()>;
    fn is_stopped(&self) -> bool;

    fn put(&mut self, record: T) -> io::Result<()>;
    fn put_all(&mut self, records: impl Iterator<Item=T>) -> io::Result<()>;
    fn flush(&mut self) -> io::Result<()>;
    fn flush_if_needed(&mut self) -> io::Result<bool>;
}


pub struct BatcherSharedState<T, Records, Builder: RecordsBuilder<T, Records>> {
    stopped: bool,
    hard_stop: bool,
    soft_stop: bool,
    last_flush_time: SystemTime,
    records_builder: Builder,
    upload_thread: Option<JoinHandle<()>>,
    last_upload_result: io::Result<()>,

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
                last_upload_result: Ok(()),

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
                    self.shared_state.lock().unwrap() .last_upload_result = Err(e);
                    self.stop();
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

    fn stop_inner(mut self, hard: bool, soft: bool) -> io::Result<()> {
        {
            let mut guard = self.shared_state.lock().unwrap();
            if guard.stopped {
                return Ok(());
            }
            guard.stopped = true;
            guard.hard_stop = hard;
            guard.soft_stop = soft;
        }
        self.flush()?;
        self.batch_storage.shutdown();
        self.shared_state.lock().unwrap().upload_thread.take()
            .map(|upload_thread| upload_thread.join())
            .map(|r| r.map_err(|e|
                if let Ok(error) = e.downcast::<Error>() {
                    Error::new(error.kind(), error)
                } else {
                    Error::new(ErrorKind::Other, "The upload thread panicked with unknown reason".to_string())
                }))
            .unwrap_or(Ok(()))
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
    fn start(&mut self) -> bool {
        let mut guard = self.shared_state.lock().unwrap();
        if guard.upload_thread.is_some() {
            return false;
        }

        guard.stopped = false;
        guard.last_flush_time = (self.clock)();

        let mut cloned_batcher = self.clone();
        guard.upload_thread = Some(thread::Builder::new().name("batcher-upload".to_string()).spawn(move || {
            cloned_batcher.upload();
        }).unwrap());

        true
    }

    fn stop(mut self) -> io::Result<()> {
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

    fn put(&mut self, record: T) -> Result<(), Error> {
        unimplemented!()
    }

    fn put_all(&mut self, records: impl Iterator<Item=T>) -> Result<(), Error> {
        unimplemented!()
    }

    fn flush(&mut self) -> Result<(), Error> {
        unimplemented!()
    }

    fn flush_if_needed(&mut self) -> Result<bool, Error> {
        unimplemented!()
    }
}

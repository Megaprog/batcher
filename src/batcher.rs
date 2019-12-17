use crate::batch_storage::{BatchStorage, BinaryBatch, BatchFactory};
use std::time::{Duration, SystemTime};
use std::{io, thread};
use crate::batch_sender::BatchSender;
use std::ops::Deref;
use std::thread::JoinHandle;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::io::Error;


const DEFAULT_MAX_BATCH_RECORDS: u32 = 10000;
const DEFAULT_MAX_BATCH_BYTES: usize = 1024 * 1024;


pub trait RecordsBuilder<T, R>: Clone {
    fn add(&mut self, record: T);
    fn len(&self) -> u32;
    fn size(&self) -> usize;
    fn build(self) -> R;
}

pub trait RecordsBuilderFactory<T, R, Builder: RecordsBuilder<T, R>>: Clone {
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
    fn stop(self);
    fn hard_stop(self);
    fn soft_stop(self);
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

    phantom_t: PhantomData<*const T>,
    phantom_r: PhantomData<*const Records>,
}

#[derive(Clone)]
pub struct BatcherImpl<T, Records, Builder, BuilderFactory, Batch, Factory, Storage, Sender>
    where
        Builder: RecordsBuilder<T, Records>,
        BuilderFactory: RecordsBuilderFactory<T, Records, Builder>,
        Batch: Deref<Target=BinaryBatch>,
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

    phantom_t: PhantomData<*const T>,
    phantom_r: PhantomData<*const Records>,
    phantom_b: PhantomData<*const Batch>,
}

impl<T, Records, Builder, BuilderFactory, Batch, Factory, Storage, Sender>
BatcherImpl<T, Records, Builder, BuilderFactory, Batch, Factory, Storage, Sender>
    where
        Builder: RecordsBuilder<T, Records>,
        BuilderFactory: RecordsBuilderFactory<T, Records, Builder>,
        Batch: Deref<Target=BinaryBatch>,
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
                stopped: false,
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

    fn upload(&mut self) {

    }
}

impl<T, Records, Builder, BuilderFactory, Batch, Factory, Storage, Sender> Batcher<T> for
BatcherImpl<T, Records, Builder, BuilderFactory, Batch, Factory, Storage, Sender>
    where
        Builder: RecordsBuilder<T, Records>,
        BuilderFactory: RecordsBuilderFactory<T, Records, Builder>,
        Batch: Deref<Target=BinaryBatch>,
        Factory: BatchFactory<Records>,
        Storage: BatchStorage<Batch>,
        Sender: BatchSender
{
    fn start(&mut self) -> bool {
        let guard = self.shared_state.lock().unwrap();
        if guard.upload_thread.is_some() {
            return false;
        }

        guard.last_flush_time = (self.clock)();

        let mut cloned_batcher = self.clone();
        guard.upload_thread = Some(thread::Builder::new().name("batcher-upload".to_string()).spawn(move || {
            cloned_batcher.upload();
        }).unwrap());

        true
    }

    fn stop(self) {
        unimplemented!()
    }

    fn hard_stop(self) {
        unimplemented!()
    }

    fn soft_stop(self) {
        unimplemented!()
    }

    fn is_stopped(&self) -> bool {
        unimplemented!()
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

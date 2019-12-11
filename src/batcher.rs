use crate::batch_storage::{BatchStorage, BinaryBatch, BatchFactory};
use std::time::{Duration, SystemTime};
use std::io;
use crate::batch_sender::BatchSender;
use std::ops::Deref;
use std::thread::JoinHandle;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};


const DEFAULT_MAX_BATCH_RECORDS: u32 = 10000;
const DEFAULT_MAX_BATCH_BYTES: usize = 1024 * 1024;


pub trait RecordsBuilder<T, R> {
    fn add(&mut self, record: T);
    fn len(&self) -> u32;
    fn size(&self) -> usize;
    fn build(self) -> R;
}

pub trait RecordsBuilderFactory<T, R, Builder: RecordsBuilder<T, R>> {
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
    fn start(&mut self);
    fn stop(self);
    fn hard_stop(self);
    fn soft_stop(self);

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
    pub clock: fn() -> i64,
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
            clock: crate::batch_storage::time_from_epoch_millis,
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
}


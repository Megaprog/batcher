use std::sync::Arc;
use crate::batch_storage::{BinaryBatch, BatchStorage, BatchFactory, BatchId};
use crate::waiter::Lock;
use std::{io, fmt, thread};
use log::*;
use std::io::{Error, ErrorKind};
use std::fmt::{Formatter, Debug};
use std::ops::Deref;
use std::thread::JoinHandle;
use crate::file_storage::FileStorage;
use std::marker::PhantomData;
use crate::chained_error::ChainedError;
use crate::memory_storage::{NonBlockingMemoryStorage, MemoryStorage};


pub trait BatchIdGetter: Clone + Send + 'static {
    fn next_batch_id(&self) -> io::Result<BatchId>;
}

impl BatchIdGetter for fn() -> io::Result<BatchId> {
    fn next_batch_id(&self) -> io::Result<BatchId> {
        self()
    }
}

impl BatchIdGetter for FileStorage {
    fn next_batch_id(&self) -> io::Result<BatchId> {
        Ok(self.shared_state.lock().next_batch_id)
    }
}

pub trait StoreBatchMethod: Clone + Send + 'static {
    fn store_batch(&self, batch: BinaryBatch) -> io::Result<()>;
}

pub trait StoreBatchRefMethod: Clone + Send + 'static {
    fn store_batch_ref(&self, batch: &BinaryBatch) -> io::Result<()>;
}

pub trait BatchStorageBuffer<B: Deref<Target=BinaryBatch>>: StoreBatchMethod + Debug + Clone + Send + 'static {
    fn get(&self) -> io::Result<B>;
    fn remove(&self) -> io::Result<()>;
    fn is_empty(&self) -> bool;
}

impl<B, T> BatchStorageBuffer<B> for T
    where
        B: Deref<Target=BinaryBatch>,
        T: BatchStorage<B> + StoreBatchMethod
{
    fn get(&self) -> io::Result<B> {
        BatchStorage::get(self)
    }

    fn remove(&self) -> io::Result<()> {
        BatchStorage::remove(self)
    }

    fn is_empty(&self) -> bool {
        BatchStorage::is_empty(self)
    }
}

pub trait BatchStoragePersistent<B: Deref<Target=BinaryBatch>>: StoreBatchRefMethod + Debug + Clone + Send + 'static {
    fn get(&self) -> io::Result<B>;
    fn remove(&self) -> io::Result<()>;
    fn is_persistent(&self) -> bool;
    fn is_empty(&self) -> bool;
}

impl<B, T> BatchStoragePersistent<B> for T
    where
        B: Deref<Target=BinaryBatch>,
        T: BatchStorage<B> + StoreBatchRefMethod
{
    fn get(&self) -> io::Result<B> {
        BatchStorage::get(self)
    }

    fn remove(&self) -> io::Result<()> {
        BatchStorage::remove(self)
    }

    fn is_persistent(&self) -> bool {
        BatchStorage::is_persistent(self)
    }

    fn is_empty(&self) -> bool {
        BatchStorage::is_empty(self)
    }
}

impl StoreBatchRefMethod for FileStorage {
    fn store_batch_ref(&self, batch: &BinaryBatch) -> io::Result<()> {
        self.store_batch_inner(batch, &mut self.get_waiter()?)
    }
}

impl StoreBatchMethod for NonBlockingMemoryStorage {
    fn store_batch(&self, batch: BinaryBatch) -> io::Result<()> {
        self.this.store_batch(self, batch, self.shared_state.lock())
    }
}

impl StoreBatchMethod for MemoryStorage {
    fn store_batch(&self, batch: BinaryBatch) -> io::Result<()> {
        self.0.this.store_batch(&self.0, batch, self.0.shared_state.lock())
    }
}

pub struct CombinedStorageSharedState {
    next_batch_id: i64,
    stopped: bool,
    saving_thread: Option<JoinHandle<()>>,
    last_result: Arc<io::Result<()>>,
}

#[derive(Clone)]
pub struct CombinedStorage<BufferBatch, BufferStorage, PersistentBatch, PersistentStorage>
    where
        BufferBatch: Deref<Target=BinaryBatch> + Clone + Send + 'static,
        BufferStorage: BatchStorageBuffer<BufferBatch>,
        PersistentBatch: Deref<Target=BinaryBatch> + Clone + Send + 'static,
        PersistentStorage: BatchStoragePersistent<PersistentBatch> + BatchIdGetter
{
    buffer: BufferStorage,
    persistent: PersistentStorage,
    pub(crate) shared_state: Arc<Lock<CombinedStorageSharedState>>,
    phantom_buffer: PhantomData<BufferBatch>,
    phantom_persistent: PhantomData<PersistentBatch>
}

impl<BufferBatch, BufferStorage, PersistentBatch, PersistentStorage> Debug for
CombinedStorage<BufferBatch, BufferStorage, PersistentBatch, PersistentStorage>
    where
        BufferBatch: Deref<Target=BinaryBatch> + Clone + Send + 'static,
        BufferStorage: BatchStorageBuffer<BufferBatch>,
        PersistentBatch: Deref<Target=BinaryBatch> + Clone + Send + 'static,
        PersistentStorage: BatchStoragePersistent<PersistentBatch> + BatchIdGetter
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("CombinedStorage")
            .field("buffer", &self.buffer)
            .field("persistent", &self.persistent)
            .finish()
    }
}

impl<BufferBatch, BufferStorage, PersistentBatch, PersistentStorage>
CombinedStorage<BufferBatch, BufferStorage, PersistentBatch, PersistentStorage>
    where
        BufferBatch: Deref<Target=BinaryBatch> + Clone + Send + 'static,
        BufferStorage: BatchStorageBuffer<BufferBatch>,
        PersistentBatch: Deref<Target=BinaryBatch> + Clone + Send + 'static,
        PersistentStorage: BatchStoragePersistent<PersistentBatch> + BatchIdGetter
{
    pub fn new(buffer: BufferStorage, persistent: PersistentStorage) -> io::Result<CombinedStorage<BufferBatch, BufferStorage, PersistentBatch, PersistentStorage>> {
        let next_batch_id = persistent.next_batch_id()?;

        let mut storage = CombinedStorage {
            buffer,
            persistent,
            shared_state: Arc::new(Lock::new(CombinedStorageSharedState {
                next_batch_id,
                stopped: false,
                saving_thread: None,
                last_result: Arc::new(Ok(()))
            })),
            phantom_buffer: PhantomData,
            phantom_persistent: PhantomData
        };

        let cloned_storage = storage.clone();
        storage.shared_state.lock().saving_thread = Some(thread::Builder::new().name("batcher-combined-save".to_string()).spawn(move || {
            cloned_storage.save();
        }).unwrap());

        Ok(storage)
    }

    fn save(self) {
        loop {
            let get_result = self.buffer.get();
            if let Err(e) = get_result {
                if e.kind() == ErrorKind::Interrupted {
                    debug!("The saving thread has been interrupted");
                } else {
                    error!("Error while getting batch from the buffer storage: {}", e);
                    self.shared_state.lock().last_result = Arc::new(Err(e));
                    self.shutdown();
                }
                break
            }

            if let Err(e) = self.persistent.store_batch_ref(&get_result.unwrap()) {
                error!("Error while store batch to the persistent storage: {}", e);
                self.shared_state.lock().last_result = Arc::new(Err(e));
                self.shutdown();
                break
            }

            if let Err(e) = self.buffer.remove() {
                error!("Error while removing uploaded batch from buffer storage: {}", e);
                self.shared_state.lock().last_result = Arc::new(Err(e));
                self.shutdown();
                break
            }

            if self.shared_state.lock().stopped && self.buffer.is_empty() {
                break
            }
        }
    }
}

impl<BufferBatch, BufferStorage, PersistentBatch, PersistentStorage> BatchStorage<PersistentBatch> for
CombinedStorage<BufferBatch, BufferStorage, PersistentBatch, PersistentStorage>
    where
        BufferBatch: Deref<Target=BinaryBatch> + Clone + Send + 'static,
        BufferStorage: BatchStorageBuffer<BufferBatch>,
        PersistentBatch: Deref<Target=BinaryBatch> + Clone + Send + 'static,
        PersistentStorage: BatchStoragePersistent<PersistentBatch> + BatchIdGetter
{
    fn store<R>(&self, records: R, batch_factory: &impl BatchFactory<R>) -> io::Result<()> {
        let mut waiter = self.shared_state.lock();
        if waiter.stopped {
            return Err(Error::new(ErrorKind::Interrupted, ChainedError::arc_result(
                format!("The storage {:?} has been shut down", self),
                waiter.last_result.clone())))
        }

        let result = self.buffer.store_batch(batch_factory.create_batch(records, waiter.next_batch_id)?);
        if result.is_ok() {
            waiter.next_batch_id += 1;
        }

        result
    }

    fn get(&self) -> io::Result<PersistentBatch> {
        self.persistent.get()
    }

    fn remove(&self) -> Result<(), Error> {
        self.persistent.remove()
    }

    fn is_persistent(&self) -> bool {
        self.persistent.is_persistent()
    }

    fn is_empty(&self) -> bool {
        self.persistent.is_empty()
    }

    fn shutdown(&self) {
        {
            let mut waiter = self.shared_state.lock();
            waiter.stopped = true;
            waiter.interrupt();
            waiter.saving_thread.take()
        }
            .map(|upload_thread| upload_thread.join().unwrap());
    }
}

#[cfg(test)]
mod test_file_storage {
    use crate::batch_storage::{BinaryBatch, BatchStorage, BatchFactory};
    use crate::memory_storage::MemoryStorage;
    use std::thread;
    use std::time::Duration;
    use crate::file_storage::FileStorage;
    use tempfile::tempdir;
    use crate::memory_storage::test::BATCH_FACTORY;
    use std::io::Write;
    use std::sync::{Once, Arc};
    use env_logger::{Builder, Env};
    use std::sync::atomic::{AtomicBool, Ordering};
    use crate::combined_storage::CombinedStorage;

    static INIT: Once = Once::new();

    fn init_log() {
        INIT.call_once(|| {
            Builder::from_env(Env::default().default_filter_or("trace")).is_test(true)
                .format_timestamp_millis()
                .init();
        });
    }

    #[test]
    fn scenario() {
        init_log();
        let dir = tempdir().unwrap();
        let mut f1 = crate::file_storage::test::open_next_batch_id_file(&dir, format!(concat!(batch_file!(), "-", file_id_pattern!()), 1));
        f1.write_all(&BATCH_FACTORY.create_batch(String::new(), 1).unwrap().bytes).unwrap();
        let mut f2 = crate::file_storage::test::open_next_batch_id_file(&dir, format!(concat!(batch_file!(), "-", file_id_pattern!()), 2));
        f2.write_all(&BATCH_FACTORY.create_batch(String::new(), 1).unwrap().bytes).unwrap();
        let file_storage = FileStorage::init(dir.path()).unwrap();

        let batch = file_storage.get().unwrap();
        assert_eq!(1, batch.batch_id);
        assert_eq!(vec![1, 2], batch.bytes);

        let memory_storage = MemoryStorage::new();
        let combined_storage = CombinedStorage::new(memory_storage, file_storage.clone()).unwrap();

        combined_storage.store(String::new(), &BATCH_FACTORY).unwrap();
        let batch = combined_storage.get().unwrap();
        //the same batch
        assert_eq!(1, batch.batch_id);
        assert_eq!(vec![1, 2], batch.bytes);
        //again
        let batch = combined_storage.get().unwrap();
        assert_eq!(1, batch.batch_id);
        assert_eq!(vec![1, 2], batch.bytes);

        combined_storage.remove();
        let batch = combined_storage.get().unwrap();
        assert_eq!(2, batch.batch_id);
        assert_eq!(vec![1, 2], batch.bytes);

        combined_storage.remove();
        let batch = combined_storage.get().unwrap();
        assert_eq!(3, batch.batch_id);
        assert_eq!(vec![1, 2], batch.bytes);

        combined_storage.remove();
        assert!(combined_storage.is_empty());
        assert!(file_storage.shared_state.lock().file_ids.is_empty());
    }

    #[test]
    fn blocks_forever() {
        let dir = tempdir().unwrap();
        let file_storage = FileStorage::init(dir.path()).unwrap();

        let working = Arc::new(AtomicBool::new(true));

        let cloned_storage = file_storage.clone();
        let cloned_working = working.clone();
        let join_handle = thread::spawn(move || {
            cloned_storage.get().unwrap();
            cloned_working.store(false, Ordering::Relaxed);
        });

        thread::sleep(Duration::from_millis(10));

        assert!(working.load(Ordering::Relaxed));
    }

    #[test]
    fn producer_first() {
        let dir = tempdir().unwrap();
        let file_storage = FileStorage::init(dir.path()).unwrap();
        crate::memory_storage::test::producer_first(file_storage);
    }

    #[test]
    fn consumer_first() {
        let dir = tempdir().unwrap();
        let file_storage = FileStorage::init(dir.path()).unwrap();
        crate::memory_storage::test::consumer_first(file_storage);
    }

    #[test]
    fn shutdown() {
        let dir = tempdir().unwrap();
        let file_storage = FileStorage::init(dir.path()).unwrap();
        crate::memory_storage::test::shutdown(file_storage);
    }
}

use std::collections::VecDeque;
use std::sync::Arc;
use crate::batch_storage::{BinaryBatch, BatchStorage, BatchFactory, BatchId};
use crate::waiter::{Lock, Waiter};
use std::path::{Path, PathBuf};
use std::fs::{File, OpenOptions};
use std::{io, fs, fmt, thread};
use log::*;
use std::io::{Error, ErrorKind, BufRead, Write, Seek, SeekFrom, Read};
use std::fmt::{Display, Formatter, Debug};
use std::ops::Deref;
use std::thread::JoinHandle;
use crate::file_storage::FileStorage;
use std::marker::PhantomData;


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
    fn store_batch(&self, batch: &BinaryBatch) -> io::Result<()>;
}

pub trait BatchStorageForCombination<B: Deref<Target=BinaryBatch>>: StoreBatchMethod + Clone + Send + 'static {
    fn get(&self) -> io::Result<B>;
    fn remove(&self) -> io::Result<()>;
    fn is_persistent(&self) -> bool;
    fn is_empty(&self) -> bool;
    fn shutdown(&self);
}

impl<B, T> BatchStorageForCombination<B> for T
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

    fn is_persistent(&self) -> bool {
        BatchStorage::is_persistent(self)
    }

    fn is_empty(&self) -> bool {
        BatchStorage::is_empty(self)
    }

    fn shutdown(&self) {
        BatchStorage::shutdown(self)
    }
}

impl StoreBatchMethod for FileStorage {
    fn store_batch(&self, batch: &BinaryBatch) -> io::Result<()> {
        self.store_batch_inner(batch, &mut self.get_waiter()?)
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
        BufferStorage: BatchStorageForCombination<BufferBatch>,
        PersistentBatch: Deref<Target=BinaryBatch> + Clone + Send + 'static,
        PersistentStorage: BatchStorageForCombination<PersistentBatch> + BatchIdGetter
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
        BufferStorage: BatchStorageForCombination<BufferBatch> + Debug,
        PersistentBatch: Deref<Target=BinaryBatch> + Clone + Send + 'static,
        PersistentStorage: BatchStorageForCombination<PersistentBatch> + BatchIdGetter + Debug
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
        BufferStorage: BatchStorageForCombination<BufferBatch>,
        PersistentBatch: Deref<Target=BinaryBatch> + Clone + Send + 'static,
        PersistentStorage: BatchStorageForCombination<PersistentBatch> + BatchIdGetter
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("CombinedStorage")
            .finish()
    }
}

impl<BufferBatch, BufferStorage, PersistentBatch, PersistentStorage>
CombinedStorage<BufferBatch, BufferStorage, PersistentBatch, PersistentStorage>
    where
        BufferBatch: Deref<Target=BinaryBatch> + Clone + Send + 'static,
        BufferStorage: BatchStorageForCombination<BufferBatch>,
        PersistentBatch: Deref<Target=BinaryBatch> + Clone + Send + 'static,
        PersistentStorage: BatchStorageForCombination<PersistentBatch> + BatchIdGetter + Debug
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("CombinedStorage")
            .field("persistent", &self.persistent)
            .finish()
    }
}

impl<BufferBatch, BufferStorage, PersistentBatch, PersistentStorage>
CombinedStorage<BufferBatch, BufferStorage, PersistentBatch, PersistentStorage>
    where
        BufferBatch: Deref<Target=BinaryBatch> + Clone + Send + 'static,
        BufferStorage: BatchStorageForCombination<BufferBatch>,
        PersistentBatch: Deref<Target=BinaryBatch> + Clone + Send + 'static,
        PersistentStorage: BatchStorageForCombination<PersistentBatch> + BatchIdGetter
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

            if let Err(e) = self.persistent.store_batch(&get_result.unwrap()) {
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
        BufferStorage: BatchStorageForCombination<BufferBatch>,
        PersistentBatch: Deref<Target=BinaryBatch> + Clone + Send + 'static,
        PersistentStorage: BatchStorageForCombination<PersistentBatch> + BatchIdGetter
{
    fn store<R>(&self, records: R, batch_factory: &impl BatchFactory<R>) -> io::Result<()> {
        let mut waiter = self.shared_state.lock();
        if waiter.stopped {
            return Err(Error::new(ErrorKind::Interrupted, format!("The storage {:?} has been shut down", self)))
        }

        (*waiter.last_result)?;

        let result = self.buffer.store_batch(&batch_factory.create_batch(records, waiter.next_batch_id)?);
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
        let mut waiter = self.shared_state.lock();
        waiter.stopped = true;
        waiter.interrupt();
    }
}

#[cfg(test)]
mod test_file_storage {
    use crate::batch_storage::{BinaryBatch, BatchStorage, BatchFactory};
    use crate::memory_storage::MemoryStorage;
    use std::{io, thread};
    use std::time::Duration;
    use crate::file_storage::FileStorage;
    use tempfile::tempdir;
    use crate::memory_storage::test::BATCH_FACTORY;
    use std::fs::{File, OpenOptions};
    use std::io::Write;
    use std::path::Path;
    use std::sync::{Once, Arc};
    use env_logger::{Builder, Env};
    use std::sync::atomic::{AtomicBool, Ordering};

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
        let mut f1 = open_next_batch_id_file(&dir, format!(concat!(batch_file!(), "-", file_id_pattern!()), 1));
        f1.write_all(&BATCH_FACTORY.create_batch(String::new(), 1).unwrap().bytes).unwrap();
        let mut f2 = open_next_batch_id_file(&dir, format!(concat!(batch_file!(), "-", file_id_pattern!()), 2));
        f2.write_all(&BATCH_FACTORY.create_batch(String::new(), 1).unwrap().bytes).unwrap();
        let file_storage = FileStorage::init(dir.path()).unwrap();

        let batch = file_storage.get().unwrap();
        assert_eq!(1, batch.batch_id);
        assert_eq!(vec![1, 2], batch.bytes);

        file_storage.store(String::new(), &BATCH_FACTORY).unwrap();
        let batch = file_storage.get().unwrap();
        //the same batch
        assert_eq!(1, batch.batch_id);
        assert_eq!(vec![1, 2], batch.bytes);
        //again
        let batch = file_storage.get().unwrap();
        assert_eq!(1, batch.batch_id);
        assert_eq!(vec![1, 2], batch.bytes);

        file_storage.remove();
        let batch = file_storage.get().unwrap();
        assert_eq!(2, batch.batch_id);
        assert_eq!(vec![1, 2], batch.bytes);

        file_storage.remove();
        let batch = file_storage.get().unwrap();
        assert_eq!(3, batch.batch_id);
        assert_eq!(vec![1, 2], batch.bytes);

        file_storage.remove();
        assert!(file_storage.shared_state.lock().file_ids.is_empty());
    }

    fn open_next_batch_id_file(dir: impl AsRef<Path>, path: impl AsRef<Path>) -> File {
        OpenOptions::new().write(true).create(true)
            .open(dir.as_ref().join(path)).unwrap()
    }

    #[test]
    fn capacity_exceeded() {
        let dir = tempdir();
        let file_storage = FileStorage::init_with_max_bytes(dir.as_ref().unwrap().path(), 1).unwrap();
        assert!(file_storage.store(String::new(), &BATCH_FACTORY).is_err());
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

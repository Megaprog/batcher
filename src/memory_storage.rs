use std::collections::VecDeque;
use crate::batch_storage::{BinaryBatch, BatchStorage, BatchFactory};
use std::io;
use std::sync::Arc;
use std::io::{Error, ErrorKind};
use crate::waiter::{Waiter, Lock};
use std::fmt::{Debug, Formatter};
use core::fmt;

const DEFAULT_MAX_BYTES_IN_QUEUE: usize = 64 * 1024 * 1024;

pub struct MemoryStorageSharedState {
    pub(crate) batches_queue: VecDeque<Arc<BinaryBatch>>,
    occupied_bytes: usize,
    previous_batch_id: i64,
    stopped: bool,
}

pub(crate) trait MemoryStorageThis {
    fn is_capacity_exceeded(&self, memory_storage: &NonBlockingMemoryStorage, batch: &BinaryBatch,
                            waiter: &mut Waiter<MemoryStorageSharedState>) -> io::Result<bool>;
    fn store_batch(&self, memory_storage: &NonBlockingMemoryStorage, batch: BinaryBatch,
                   waiter: Waiter<MemoryStorageSharedState>) -> io::Result<()>;
    fn notify_after_remove(&self, memory_storage: &NonBlockingMemoryStorage,
                           waiter: &Waiter<MemoryStorageSharedState>) {}
}

#[derive(Clone)]
pub struct NonBlockingMemoryStorage {
    pub max_bytes: usize,
    pub clock: fn() -> i64,
    pub(crate) this: Arc<dyn MemoryStorageThis + Send + Sync>,
    pub(crate) shared_state: Arc<Lock<MemoryStorageSharedState>>
}

impl NonBlockingMemoryStorage {
    pub fn new() -> NonBlockingMemoryStorage {
        NonBlockingMemoryStorage::with_max_bytes(DEFAULT_MAX_BYTES_IN_QUEUE)
    }

    pub fn with_max_bytes(max_bytes: usize) -> NonBlockingMemoryStorage {
        NonBlockingMemoryStorage {
            max_bytes,
            clock: crate::batch_storage::time_from_epoch_millis,
            this: Arc::new(Base),
            shared_state: Arc::new(Lock::new(MemoryStorageSharedState {
                batches_queue: VecDeque::new(),
                occupied_bytes: 0,
                previous_batch_id: 0,
                stopped: false,
            })),
        }
    }
}


struct Base;

impl MemoryStorageThis for Base {
    fn is_capacity_exceeded(&self, memory_storage: &NonBlockingMemoryStorage, batch: &BinaryBatch,
                            waiter: &mut Waiter<MemoryStorageSharedState>) -> io::Result<bool> {
        Ok(waiter.occupied_bytes + batch.bytes.len() > memory_storage.max_bytes)
    }

    fn store_batch(&self, memory_storage: &NonBlockingMemoryStorage, batch: BinaryBatch,
                   mut waiter: Waiter<MemoryStorageSharedState>) -> io::Result<()> {
        if waiter.stopped {
            return Err(Error::new(ErrorKind::Interrupted, "The storage has been shut down"))
        }

        if memory_storage.this.is_capacity_exceeded(memory_storage, &batch, &mut waiter)? {
            return Err(Error::new(ErrorKind::Other,
                                  format!("Storage capacity exceeded: needed {} bytes, remains {} of {}",
                                          batch.bytes.len(), (memory_storage.max_bytes - waiter.occupied_bytes), memory_storage.max_bytes)));
        }

        waiter.previous_batch_id = batch.batch_id;
        waiter.occupied_bytes += batch.bytes.len();
        waiter.batches_queue.push_back(Arc::new(batch));

        waiter.notify_one();

        return Ok(());
    }
}

impl Debug for NonBlockingMemoryStorage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("NonBlockingMemoryStorage")
            .field("max_bytes", &self.max_bytes)
            .finish()
    }
}

impl BatchStorage<Arc<BinaryBatch>> for NonBlockingMemoryStorage {
    fn store<T>(&self, actions: T, batch_factory: &impl BatchFactory<T>) -> io::Result<()> {
        let waiter = self.shared_state.lock();

        let epoch = (self.clock)();
        let batch_id = if epoch > waiter.previous_batch_id {
            epoch
        } else {
            waiter.previous_batch_id + 1
        };

        self.this.store_batch(self, batch_factory.create_batch(actions, batch_id)?, waiter)
    }

    fn get(&self) -> io::Result<Arc<BinaryBatch>> {
        let mut waiter = self.shared_state.lock();
        loop {
            if let Some(batch) = waiter.batches_queue.front() {
                return Ok(Arc::clone(batch));
            }
            waiter.wait()?;
        }
    }

    fn remove(&self) -> io::Result<()> {
        let mut waiter = self.shared_state.lock();
        let batch_opt = waiter.batches_queue.pop_front();
        if let Some(batch) = batch_opt {
            waiter.occupied_bytes -= batch.bytes.len();
        }

        self.this.notify_after_remove(self, &waiter);
        Ok(())
    }

    fn is_persistent(&self) -> bool {
        false
    }

    fn is_empty(&self) -> bool {
        self.shared_state.lock().batches_queue.is_empty()
    }

    fn shutdown(&self) {
        let mut guard = self.shared_state.lock();
        guard.stopped = true;
        guard.interrupt();
    }
}


///-------------------------------------------------------------------------------------------------

struct Blocking(Box<dyn MemoryStorageThis + Send + Sync>);

impl MemoryStorageThis for Blocking {
    fn is_capacity_exceeded(&self, memory_storage: &NonBlockingMemoryStorage, batch: &BinaryBatch,
                            waiter: &mut Waiter<MemoryStorageSharedState>) -> io::Result<bool> {
        loop {
            if !self.0.is_capacity_exceeded(memory_storage, batch, waiter)? {
                return Ok(false)
            }
            waiter.wait()?;
        }
    }

    fn store_batch(&self, memory_storage: &NonBlockingMemoryStorage, batch: BinaryBatch,
                   waiter: Waiter<MemoryStorageSharedState>) -> Result<(), Error> {
        self.0.store_batch(memory_storage, batch, waiter)
    }

    fn notify_after_remove(&self, memory_storage: &NonBlockingMemoryStorage,
                           waiter: &Waiter<MemoryStorageSharedState>) {
        waiter.notify_one();
    }
}

#[derive(Clone)]
pub struct MemoryStorage(pub(crate) NonBlockingMemoryStorage);

impl MemoryStorage {
    pub fn new() -> MemoryStorage {
        MemoryStorage::with_max_batch_bytes(DEFAULT_MAX_BYTES_IN_QUEUE)
    }

    pub fn with_max_batch_bytes(max_bytes: usize) -> MemoryStorage {
        let mut non_blocking_memory_storage = NonBlockingMemoryStorage::with_max_bytes(max_bytes);
        non_blocking_memory_storage.this = Arc::new(Blocking(Box::new(Base)));
        MemoryStorage(non_blocking_memory_storage)
    }
}

impl Debug for MemoryStorage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryStorage")
            .field("max_bytes", &self.0.max_bytes)
            .finish()
    }
}

impl<'a> BatchStorage<Arc<BinaryBatch>> for MemoryStorage {
    fn store<T>(&self, actions: T, batch_factory: &impl BatchFactory<T>) -> io::Result<()> {
        self.0.store(actions, batch_factory)
    }

    fn get(&self) -> io::Result<Arc<BinaryBatch>> {
        self.0.get()
    }

    fn remove(&self) -> io::Result<()> {
        self.0.remove()
    }

    fn is_persistent(&self) -> bool {
        self.0.is_persistent()
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn shutdown(&self) {
        self.0.shutdown();
    }
}


#[cfg(test)]
mod test_non_blocking_memory_storage {
    use crate::batch_storage::{BinaryBatch, BatchStorage};
    use crate::memory_storage::NonBlockingMemoryStorage;
    use std::io;

    static BATCH_FACTORY: fn(String, i64) -> io::Result<BinaryBatch> = |actions, batch_id| Ok(BinaryBatch { batch_id, bytes: vec![1, 2]});

    #[test]
    fn drop_if_out_of_capacity() {
        let non_blocking_memory_storage = NonBlockingMemoryStorage::with_max_bytes(0);
        let result = non_blocking_memory_storage.store("Test".to_string(), &BATCH_FACTORY);

        assert!(result.unwrap_err().to_string().starts_with("Storage capacity exceeded"));
        assert!(non_blocking_memory_storage.is_empty());
    }

    #[test]
    fn producer_first() {
        let mut non_blocking_memory_storage = NonBlockingMemoryStorage::new();
        non_blocking_memory_storage.clock = || 1;
        crate::memory_storage::test::producer_first(non_blocking_memory_storage);
    }

    #[test]
    fn consumer_first() {
        let mut non_blocking_memory_storage = NonBlockingMemoryStorage::new();
        non_blocking_memory_storage.clock = || 1;
        crate::memory_storage::test::consumer_first(non_blocking_memory_storage);
    }

    #[test]
    fn shutdown() {
        let non_blocking_memory_storage = NonBlockingMemoryStorage::new();
        crate::memory_storage::test::shutdown(non_blocking_memory_storage);
    }
}

#[cfg(test)]
mod test_memory_storage {
    use crate::batch_storage::{BinaryBatch, BatchStorage};
    use crate::memory_storage::MemoryStorage;
    use std::{io, thread};
    use std::time::Duration;

    static BATCH_FACTORY: fn(String, i64) -> io::Result<BinaryBatch> = |actions, batch_id| Ok(BinaryBatch { batch_id, bytes: vec![1]});

    #[test]
    fn blocks_forever() {
        let memory_storage = MemoryStorage::with_max_batch_bytes(1);
        assert!(memory_storage.store("Test1".to_string(), &BATCH_FACTORY).is_ok());

        let cloned_storage = memory_storage.clone();
        let join_handle = thread::spawn(move || {
            assert!(cloned_storage.store("Test2".to_string(), &BATCH_FACTORY).is_ok());
        });

        thread::sleep(Duration::from_millis(10));

        assert_eq!(1, memory_storage.0.shared_state.lock().batches_queue.len());
    }

    #[test]
    fn producer_first() {
        let mut memory_storage = MemoryStorage::new();
        memory_storage.0.clock = || 1;
        crate::memory_storage::test::producer_first(memory_storage);
    }

    #[test]
    fn consumer_first() {
        let mut memory_storage = MemoryStorage::new();
        memory_storage.0.clock = || 1;
        crate::memory_storage::test::consumer_first(memory_storage);
    }

    #[test]
    fn shutdown() {
        let memory_storage = MemoryStorage::new();
        crate::memory_storage::test::shutdown(memory_storage);
    }
}

#[cfg(test)]
pub(crate) mod test {
    use crate::batch_storage::{BatchStorage, BinaryBatch};
    use std::thread::JoinHandle;
    use std::{thread, io};
    use std::time::Duration;
    use std::ops::Deref;
    use std::io::ErrorKind;

    pub(crate) static BATCH_FACTORY: fn(String, i64) -> io::Result<BinaryBatch> = |actions, batch_id| Ok(BinaryBatch { batch_id, bytes: vec![1, 2]});

    pub(crate) fn producer_first<B: Deref<Target=BinaryBatch>, T: BatchStorage<B> + Clone + Send + 'static>(storage: T) {
        assert!(storage.store("Test".to_string(), &BATCH_FACTORY).is_ok());
        thread::sleep(Duration::from_millis(5));
        assert!(!storage.is_empty());

        let consumer_thread = start_consumer_thread(storage.clone());
        consumer_thread.join().unwrap();

        assert!(storage.is_empty());
    }

    pub(crate) fn consumer_first<B: Deref<Target=BinaryBatch>, T: BatchStorage<B> + Clone + Send + 'static>(storage: T) {
        let consumer_thread = start_consumer_thread(storage.clone());
        thread::sleep(Duration::from_millis(1));

        assert!(storage.store("Test".to_string(), &BATCH_FACTORY).is_ok());

        consumer_thread.join().unwrap();

        assert!(storage.is_empty());
    }

    pub(crate) fn shutdown<B: Deref<Target=BinaryBatch> + Send + 'static, T: BatchStorage<B> + Clone + Send + 'static>(storage: T) {
        let cloned_storage = storage.clone();
        let consumer_thread = thread::spawn(move || {
            let get_result = cloned_storage.get();
            cloned_storage.remove().unwrap();
            get_result
        });

        thread::sleep(Duration::from_millis(1));
        storage.clone().shutdown();

        let thread_result = consumer_thread.join().unwrap();
        assert!(thread_result.is_err());
        assert_eq!(ErrorKind::Interrupted, thread_result.err().unwrap().kind());

        let store_result = storage.store("Test".to_string(), &BATCH_FACTORY);
        assert!(store_result.is_err());
        assert_eq!(ErrorKind::Interrupted, store_result.err().unwrap().kind());
        assert!(storage.is_empty());
    }

    pub(crate) fn start_consumer_thread<B: Deref<Target=BinaryBatch>, T: BatchStorage<B> + Clone + Send + 'static>(cloned_storage: T) -> JoinHandle<()> {
        thread::spawn(move || {
            {
                let batch = cloned_storage.get().unwrap();
                assert_eq!(1, batch.batch_id);
                assert_eq!(vec![1, 2], batch.bytes);
            }
            cloned_storage.remove().unwrap();
        })
    }
}

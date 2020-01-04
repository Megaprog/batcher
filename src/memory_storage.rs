use std::collections::VecDeque;
use crate::batch_storage::{BinaryBatch, BatchStorage, BatchFactory};
use std::io;
use std::sync::{Mutex, Arc, Condvar, MutexGuard};
use std::io::{Error, ErrorKind};
use std::ops::{Deref, DerefMut};

const DEFAULT_MAX_BYTES_IN_QUEUE: usize = 64 * 1024 * 1024;

pub struct Interruption {
    waiting: i32,
    interrupted: i32,
}

pub struct MemoryStorageSharedState {
    pub(crate) batches_queue: VecDeque<Arc<BinaryBatch>>,
    occupied_bytes: usize,
    previous_batch_id: i64,
    interruption: Interruption,
    stopped: bool,
}

pub struct MemoryStorageSync {
    pub(crate) mutex: Mutex<MemoryStorageSharedState>,
    condvar: Condvar,
}

pub struct MemoryStorageGuard<'a> {
    mutex_guard: Option<MutexGuard<'a, MemoryStorageSharedState>>,
    condvar: &'a Condvar,
}

impl<'a> MemoryStorageGuard<'a> {
    fn from(storage_sync: &'a MemoryStorageSync) -> MemoryStorageGuard<'a> {
        let MemoryStorageSync { mutex, condvar, ..} = storage_sync;
        MemoryStorageGuard { mutex_guard: Some(mutex.lock().unwrap()), condvar}
    }

    fn wait(&mut self) -> io::Result<()> {
        self.interruption.waiting += 1;
        self.mutex_guard = Some(self.condvar.wait(self.mutex_guard.take().unwrap()).unwrap());
        self.interruption.waiting -= 1;

        if self.interruption.interrupted > 0 {
            self.interruption.interrupted -= 1;
            return Err(Error::new(ErrorKind::Interrupted, "Interrupted from another thread"))
        }

        Ok(())
    }

    fn interrupt(&mut self) {
        self.interruption.interrupted = self.interruption.waiting;
        self.condvar.notify_all();
    }
}

impl<'a> Deref for MemoryStorageGuard<'a> {
    type Target = MutexGuard<'a, MemoryStorageSharedState>;

    fn deref(&self) -> &Self::Target {
        self.mutex_guard.as_ref().unwrap()
    }
}

impl<'a> DerefMut for MemoryStorageGuard<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.mutex_guard.as_mut().unwrap()
    }
}

trait MemoryStorageThis {
    fn is_capacity_exceeded(&self, memory_storage: &NonBlockingMemoryStorage, batch: &BinaryBatch, guard: &mut MemoryStorageGuard) -> io::Result<bool>;
    fn store_batch(&self, memory_storage: &NonBlockingMemoryStorage, batch: BinaryBatch, guard: MemoryStorageGuard) -> io::Result<()>;
    fn notify_after_remove(&self, memory_storage: &NonBlockingMemoryStorage, condvar: &Condvar) {}
}

#[derive(Clone)]
pub struct NonBlockingMemoryStorage {
    pub max_bytes: usize,
    pub clock: fn() -> i64,
    this: Arc<dyn MemoryStorageThis + Send + Sync>,
    pub(crate) shared_state: Arc<MemoryStorageSync>
}

impl NonBlockingMemoryStorage {

    pub fn new() -> NonBlockingMemoryStorage {
        NonBlockingMemoryStorage::with_max_batch_bytes(DEFAULT_MAX_BYTES_IN_QUEUE)
    }

    pub fn with_max_batch_bytes(max_bytes: usize) -> NonBlockingMemoryStorage {
        NonBlockingMemoryStorage {
            max_bytes,
            clock: crate::batch_storage::time_from_epoch_millis,
            this: Arc::new(Base),
            shared_state: Arc::new(MemoryStorageSync {
                mutex: Mutex::new(MemoryStorageSharedState {
                    batches_queue: VecDeque::new(),
                    occupied_bytes: 0,
                    previous_batch_id: 0,
                    interruption: Interruption { waiting: 0, interrupted: 0 },
                    stopped: false,
                }),
                condvar: Condvar::new(),
            }),
        }
    }
}


struct Base;

impl MemoryStorageThis for Base {
    fn is_capacity_exceeded(&self, memory_storage: &NonBlockingMemoryStorage, batch: &BinaryBatch, guard: &mut MemoryStorageGuard) -> io::Result<bool> {
        Ok(guard.occupied_bytes + batch.bytes.len() > memory_storage.max_bytes)
    }

    fn store_batch(&self, memory_storage: &NonBlockingMemoryStorage, batch: BinaryBatch, mut guard: MemoryStorageGuard) -> io::Result<()> {
        if guard.stopped {
            return Err(Error::new(ErrorKind::Interrupted, "The storage has been stopped"))
        }

        if memory_storage.this.is_capacity_exceeded(memory_storage, &batch, &mut guard)? {
            return Err(Error::new(ErrorKind::Other, format!("Storage capacity {} exceeded by {}", memory_storage.max_bytes, batch.bytes.len())));
        }

        guard.previous_batch_id = batch.batch_id;
        guard.occupied_bytes += batch.bytes.len();
        guard.batches_queue.push_back(Arc::new(batch));

        guard.condvar.notify_one();

        return Ok(());
    }
}

impl BatchStorage<Arc<BinaryBatch>> for NonBlockingMemoryStorage {
    fn store<T>(&self, actions: T, batch_factory: &impl BatchFactory<T>) -> io::Result<()> {
        let guard = MemoryStorageGuard::from(&self.shared_state);

        let epoch = (self.clock)();
        let batch_id = if epoch > guard.previous_batch_id {
            epoch
        } else {
            guard.previous_batch_id + 1
        };

        self.this.store_batch(self, batch_factory.create_batch(actions, batch_id)?, guard)
    }

    fn get(&self) -> io::Result<Arc<BinaryBatch>> {
        let mut guard = MemoryStorageGuard::from(&self.shared_state);
        loop {
            if let Some(batch) = guard.batches_queue.front() {
                return Ok(batch.clone());
            }
            guard.wait()?;
        }
    }

    fn remove(&self) -> io::Result<()> {
        let mut mutex_guard = self.shared_state.mutex.lock().unwrap();
        let batch_opt = mutex_guard.batches_queue.pop_front();
        if let Some(batch) = batch_opt {
            mutex_guard.occupied_bytes -= batch.bytes.len();
        }

        self.this.notify_after_remove(self, &self.shared_state.condvar);
        Ok(())
    }

    fn is_persistent(&self) -> bool {
        false
    }

    fn is_empty(&self) -> bool {
        self.shared_state.mutex.lock().unwrap().batches_queue.is_empty()
    }

    fn shutdown(self) {
        let mut guard = MemoryStorageGuard::from(&self.shared_state);
        guard.stopped = true;
        guard.interrupt();
    }
}


///-------------------------------------------------------------------------------------------------

struct Blocking(Box<dyn MemoryStorageThis + Send + Sync>);

impl MemoryStorageThis for Blocking {
    fn is_capacity_exceeded(&self, memory_storage: &NonBlockingMemoryStorage, batch: &BinaryBatch, guard: &mut MemoryStorageGuard) -> io::Result<bool> {
        loop {
            if !self.0.is_capacity_exceeded(memory_storage, batch, guard)? {
                return Ok(false)
            }
            guard.wait()?;
        }
    }

    fn store_batch(&self, memory_storage: &NonBlockingMemoryStorage, batch: BinaryBatch, guard: MemoryStorageGuard) -> Result<(), Error> {
        self.0.store_batch(memory_storage, batch, guard)
    }

    fn notify_after_remove(&self, memory_storage: &NonBlockingMemoryStorage, condvar: &Condvar) {
        condvar.notify_one();
    }
}

#[derive(Clone)]
pub struct MemoryStorage(pub NonBlockingMemoryStorage);

impl MemoryStorage {
    pub fn new() -> MemoryStorage {
        MemoryStorage::with_max_batch_bytes(DEFAULT_MAX_BYTES_IN_QUEUE)
    }

    pub fn with_max_batch_bytes(max_bytes: usize) -> MemoryStorage {
        let mut non_blocking_memory_storage = NonBlockingMemoryStorage::with_max_batch_bytes(max_bytes);
        non_blocking_memory_storage.this = Arc::new(Blocking(Box::new(Base)));
        MemoryStorage(non_blocking_memory_storage)
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

    fn shutdown(self) {
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
        let non_blocking_memory_storage = NonBlockingMemoryStorage::with_max_batch_bytes(0);
        let result = non_blocking_memory_storage.store("Test".to_string(), &BATCH_FACTORY);

        assert_eq!("Storage capacity 0 exceeded by 2".to_string(), result.unwrap_err().to_string());
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

        assert_eq!(1, memory_storage.0.shared_state.mutex.lock().unwrap().batches_queue.len());
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
mod test {
    use crate::batch_storage::{BatchStorage, BinaryBatch};
    use std::thread::JoinHandle;
    use std::{thread, io};
    use std::time::Duration;
    use std::ops::Deref;
    use std::io::ErrorKind;

    pub(crate) static BATCH_FACTORY: fn(String, i64) -> io::Result<BinaryBatch> = |actions, batch_id| Ok(BinaryBatch { batch_id, bytes: vec![1, 2]});

    pub(crate) fn producer_first<B: Deref<Target=BinaryBatch>, T: BatchStorage<B> + Clone + Send + 'static>(memory_storage: T) {
        assert!(memory_storage.store("Test".to_string(), &BATCH_FACTORY).is_ok());
        assert!(!memory_storage.is_empty());

        let consumer_thread = start_consumer_thread(memory_storage.clone());
        consumer_thread.join().unwrap();

        assert!(memory_storage.is_empty());
    }

    pub(crate) fn consumer_first<B: Deref<Target=BinaryBatch>, T: BatchStorage<B> + Clone + Send + 'static>(memory_storage: T) {
        let consumer_thread = start_consumer_thread(memory_storage.clone());
        thread::sleep(Duration::from_millis(1));

        assert!(memory_storage.store("Test".to_string(), &BATCH_FACTORY).is_ok());

        consumer_thread.join().unwrap();

        assert!(memory_storage.is_empty());
    }

    pub(crate) fn shutdown<B: Deref<Target=BinaryBatch> + Send + 'static, T: BatchStorage<B> + Clone + Send + 'static>(memory_storage: T) {
        let cloned_storage = memory_storage.clone();
        let consumer_thread = thread::spawn(move || {
            let get_result = cloned_storage.get();
            cloned_storage.remove();
            get_result
        });

        thread::sleep(Duration::from_millis(1));
        memory_storage.clone().shutdown();

        let thread_result = consumer_thread.join().unwrap();
        assert!(thread_result.is_err());
        assert_eq!(ErrorKind::Interrupted, thread_result.err().unwrap().kind());

        let store_result = memory_storage.store("Test".to_string(), &BATCH_FACTORY);
        assert!(store_result.is_err());
        assert_eq!(ErrorKind::Interrupted, store_result.err().unwrap().kind());
        assert!(memory_storage.is_empty());
    }

    pub(crate) fn start_consumer_thread<B: Deref<Target=BinaryBatch>, T: BatchStorage<B> + Clone + Send + 'static>(cloned_storage: T) -> JoinHandle<()> {
        thread::spawn(move || {
            {
                let batch = cloned_storage.get().unwrap();
                assert_eq!(1, batch.batch_id);
                assert_eq!(vec![1, 2], batch.bytes);
            }
            cloned_storage.remove();
        })
    }
}

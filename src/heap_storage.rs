use std::collections::VecDeque;
use crate::batch_storage::{BinaryBatch, BatchStorage, BatchFactory};
use std::io;
use std::sync::{Mutex, Arc, Condvar, MutexGuard};
use std::io::{Error, ErrorKind};
use std::ops::{Deref, DerefMut};

const DEFAULT_MAX_BYTES_IN_QUEUE: usize = 64 * 1024 * 1024;

pub struct HeapStorageSharedState {
    batches_queue: VecDeque<Arc<BinaryBatch>>,
    occupied_bytes: usize,
    previous_batch_id: i64,
}

pub struct HeapStorageSync {
    mutex: Mutex<HeapStorageSharedState>,
    condvar: Condvar
}

pub struct HeapStorageGuard<'a> {
    mutex_guard: Option<MutexGuard<'a, HeapStorageSharedState>>,
    condvar: &'a Condvar
}

impl<'a> HeapStorageGuard<'a> {
    fn from(storage_sync: &'a HeapStorageSync) -> HeapStorageGuard<'a> {
        let HeapStorageSync { mutex, condvar} = storage_sync;
        HeapStorageGuard { mutex_guard: Some(mutex.lock().unwrap()), condvar}
    }

    fn wait(&mut self) {
        self.mutex_guard = Some(self.condvar.wait(self.mutex_guard.take().unwrap()).unwrap());
    }
}

impl<'a> Deref for HeapStorageGuard<'a> {
    type Target = MutexGuard<'a, HeapStorageSharedState>;

    fn deref(&self) -> &Self::Target {
        self.mutex_guard.as_ref().unwrap()
    }
}

impl<'a> DerefMut for HeapStorageGuard<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.mutex_guard.as_mut().unwrap()
    }
}

trait HeapStorageThis {
    fn is_capacity_exceeded(&self, heap_storage: &NonBlockingHeapStorage, batch: &BinaryBatch, guard: &mut HeapStorageGuard) -> bool;
    fn store_batch(&self, heap_storage: &NonBlockingHeapStorage, batch: BinaryBatch, guard: HeapStorageGuard) -> io::Result<()>;
    fn notify_after_remove(&self, heap_storage: &NonBlockingHeapStorage, condvar: &Condvar) {}
}

#[derive(Clone)]
pub struct NonBlockingHeapStorage {
    max_bytes: usize,
    clock: fn() -> i64,
    this: Arc<dyn HeapStorageThis + Send + Sync>,
    shared_state: Arc<HeapStorageSync>
}

impl NonBlockingHeapStorage {

    pub fn new() -> NonBlockingHeapStorage {
        NonBlockingHeapStorage::with_max_batch_bytes(DEFAULT_MAX_BYTES_IN_QUEUE)
    }

    pub fn with_max_batch_bytes(max_bytes: usize) -> NonBlockingHeapStorage {
        NonBlockingHeapStorage {
            max_bytes,
            clock: crate::batch_storage::time_from_epoch_millis,
            this: Arc::new(Base),
            shared_state: Arc::new(HeapStorageSync {
                mutex: Mutex::new(HeapStorageSharedState {
                    batches_queue: VecDeque::new(),
                    occupied_bytes: 0,
                    previous_batch_id: 0,
                }),
                condvar: Condvar::new(),
            }),
        }
    }
}


struct Base;

impl HeapStorageThis for Base {
    fn is_capacity_exceeded(&self, heap_storage: &NonBlockingHeapStorage, batch: &BinaryBatch, guard: &mut HeapStorageGuard) -> bool {
        guard.occupied_bytes + batch.bytes.len() > heap_storage.max_bytes
    }

    fn store_batch(&self, heap_storage: &NonBlockingHeapStorage, batch: BinaryBatch, mut guard: HeapStorageGuard) -> Result<(), Error> {
        if heap_storage.this.is_capacity_exceeded(heap_storage, &batch, &mut guard) {
            return Err(Error::new(ErrorKind::Other, format!("Storage capacity {} exceeded by {}", heap_storage.max_bytes, batch.bytes.len())));
        }

        guard.previous_batch_id = batch.batch_id;
        guard.occupied_bytes += batch.bytes.len();
        guard.batches_queue.push_back(Arc::new(batch));

        guard.condvar.notify_one();

        return Ok(());
    }
}

impl BatchStorage<Arc<BinaryBatch>> for NonBlockingHeapStorage {
    fn store<T>(&mut self, actions: T, batch_factory: impl BatchFactory<T>) -> io::Result<()> {
        let guard = HeapStorageGuard::from(&self.shared_state);

        let epoch = (self.clock)();
        let batch_id = if epoch > guard.previous_batch_id {
            epoch
        } else {
            guard.previous_batch_id + 1
        };

        self.this.store_batch(self, batch_factory.create_batch(actions, batch_id)?, guard)
    }

    fn get(&self) -> io::Result<Arc<BinaryBatch>> {
        let mut guard = HeapStorageGuard::from(&self.shared_state);
        loop {
            if let Some(batch) = guard.batches_queue.front() {
                return Ok(batch.clone());
            }
            guard.wait();
        }
    }

    fn remove(&mut self) -> io::Result<()> {
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
}


///-------------------------------------------------------------------------------------------------

struct Blocking(Box<dyn HeapStorageThis + Send + Sync>);

impl HeapStorageThis for Blocking {
    fn is_capacity_exceeded(&self, heap_storage: &NonBlockingHeapStorage, batch: &BinaryBatch, guard: &mut HeapStorageGuard) -> bool {
        loop {
            let is_capacity_exceeded = self.0.is_capacity_exceeded(heap_storage, batch, guard);
            if !is_capacity_exceeded {
                return false
            }
            guard.wait();
        }
    }

    fn store_batch(&self, heap_storage: &NonBlockingHeapStorage, batch: BinaryBatch, guard: HeapStorageGuard) -> Result<(), Error> {
        self.0.store_batch(heap_storage, batch, guard)
    }

    fn notify_after_remove(&self, heap_storage: &NonBlockingHeapStorage, condvar: &Condvar) {
        condvar.notify_one();
    }
}

#[derive(Clone)]
struct HeapStorage(NonBlockingHeapStorage);

impl HeapStorage {
    pub fn new() -> HeapStorage {
        HeapStorage::with_max_batch_bytes(DEFAULT_MAX_BYTES_IN_QUEUE)
    }

    pub fn with_max_batch_bytes(max_bytes: usize) -> HeapStorage {
        let mut non_blocking_heap_storage = NonBlockingHeapStorage::with_max_batch_bytes(max_bytes);
        non_blocking_heap_storage.this = Arc::new(Blocking(Box::new(Base)));
        HeapStorage(non_blocking_heap_storage)
    }
}

impl<'a> BatchStorage<Arc<BinaryBatch>> for HeapStorage {
    fn store<T>(&mut self, actions: T, batch_factory: impl BatchFactory<T>) -> io::Result<()> {
        self.0.store(actions, batch_factory)
    }

    fn get(&self) -> io::Result<Arc<BinaryBatch>> {
        self.0.get()
    }

    fn remove(&mut self) -> io::Result<()> {
        self.0.remove()
    }

    fn is_persistent(&self) -> bool {
        self.0.is_persistent()
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}


#[cfg(test)]
mod test_non_blocking_heap_storage {
    use crate::batch_storage::{BinaryBatch, BatchStorage};
    use crate::heap_storage::NonBlockingHeapStorage;
    use std::io;

    static BATCH_FACTORY: fn(String, i64) -> io::Result<BinaryBatch> = |actions, batch_id| Ok(BinaryBatch { batch_id, bytes: vec![1, 2]});

    #[test]
    fn drop_if_out_of_capacity() {
        let mut non_blocking_heap_storage = NonBlockingHeapStorage::with_max_batch_bytes(0);
        let result = non_blocking_heap_storage.store("Test".to_string(), BATCH_FACTORY);

        assert_eq!("Storage capacity 0 exceeded by 2".to_string(), result.unwrap_err().to_string());
        assert!(non_blocking_heap_storage.is_empty());
    }

    #[test]
    fn producer_first() {
        let mut non_blocking_heap_storage = NonBlockingHeapStorage::new();
        non_blocking_heap_storage.clock = || 1;
        crate::heap_storage::test::producer_first(non_blocking_heap_storage);
    }

    #[test]
    fn consumer_first() {
        let mut non_blocking_heap_storage = NonBlockingHeapStorage::new();
        non_blocking_heap_storage.clock = || 1;
        crate::heap_storage::test::consumer_first(non_blocking_heap_storage);
    }
}

#[cfg(test)]
mod test_heap_storage {
    use crate::batch_storage::{BinaryBatch, BatchStorage};
    use crate::heap_storage::HeapStorage;
    use std::{io, thread};
    use std::time::Duration;

    static BATCH_FACTORY: fn(String, i64) -> io::Result<BinaryBatch> = |actions, batch_id| Ok(BinaryBatch { batch_id, bytes: vec![1]});

    #[test]
    fn blocks_forever() {
        let mut heap_storage = HeapStorage::with_max_batch_bytes(1);
        assert!(heap_storage.store("Test1".to_string(), BATCH_FACTORY).is_ok());

        let mut cloned_storage = heap_storage.clone();
        let join_handle = thread::spawn(move || {
            assert!(cloned_storage.store("Test2".to_string(), BATCH_FACTORY).is_ok());
        });

        thread::sleep(Duration::from_millis(10));

        assert_eq!(1, heap_storage.0.shared_state.mutex.lock().unwrap().batches_queue.len());
    }

    #[test]
    fn producer_first() {
        let mut heap_storage = HeapStorage::new();
        heap_storage.0.clock = || 1;
        crate::heap_storage::test::producer_first(heap_storage);
    }

    #[test]
    fn consumer_first() {
        let mut heap_storage = HeapStorage::new();
        heap_storage.0.clock = || 1;
        crate::heap_storage::test::consumer_first(heap_storage);
    }
}

#[cfg(test)]
mod test {
    use crate::batch_storage::{BatchStorage, BinaryBatch};
    use std::thread::JoinHandle;
    use std::{thread, io};
    use std::time::Duration;
    use std::ops::Deref;

    pub(crate) static BATCH_FACTORY: fn(String, i64) -> io::Result<BinaryBatch> = |actions, batch_id| Ok(BinaryBatch { batch_id, bytes: vec![1, 2]});

    pub(crate) fn producer_first<B: Deref<Target=BinaryBatch>, T: BatchStorage<B> + Clone + Send + 'static>(mut heap_storage: T) {
        assert!(heap_storage.store("Test".to_string(), BATCH_FACTORY).is_ok());
        assert!(!heap_storage.is_empty());

        let consumer_thread = start_consumer_thread(heap_storage.clone());
        consumer_thread.join().unwrap();

        assert!(heap_storage.is_empty());
    }

    pub(crate) fn consumer_first<B: Deref<Target=BinaryBatch>, T: BatchStorage<B> + Clone + Send + 'static>(mut heap_storage: T) {
        let consumer_thread = start_consumer_thread(heap_storage.clone());
        thread::sleep(Duration::from_millis(1));

        assert!(heap_storage.store("Test".to_string(), BATCH_FACTORY).is_ok());

        consumer_thread.join().unwrap();

        assert!(heap_storage.is_empty());
    }

    pub(crate) fn start_consumer_thread<B: Deref<Target=BinaryBatch>, T: BatchStorage<B> + Clone + Send + 'static>(mut cloned_storage: T) -> JoinHandle<()> {
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

use std::sync::{MutexGuard, Condvar, Mutex, TryLockResult, TryLockError, PoisonError, WaitTimeoutResult, LockResult};
use std::ops::{Deref, DerefMut};
use std::{io, thread};
use std::io::{Error, ErrorKind};
use std::thread::ThreadId;
use std::time::Duration;

#[derive(Debug)]
struct Interruption {
    waiting: i32,
    interrupted: i32,
    thread: Option<(ThreadId, Option<String>)>,
}

#[derive(Debug)]
struct Interruptable<T> {
    value: T,
    interruption: Interruption,
}

#[derive(Debug)]
pub struct Lock<T> {
    mutex: Mutex<Interruptable<T>>,
    condvar: Condvar,
}

impl<T> Lock<T> {
    pub fn new(value: T) -> Lock<T> {
        Lock::with_condvar(value, Condvar::new())
    }

    pub fn from_mutex(mutex: Mutex<T>) -> Lock<T> {
        Lock::new(mutex.into_inner().unwrap())
    }

    pub fn from_mutex_and_condvar(mutex: Mutex<T>, condvar: Condvar) -> Lock<T> {
        Lock::with_condvar(mutex.into_inner().unwrap(), condvar)
    }

    pub fn with_condvar(value: T, condvar: Condvar) -> Lock<T> {
        Lock {
            mutex: Mutex::new(Interruptable {
                value,
                interruption: Interruption { waiting: 0, interrupted: 0, thread: None }}),
            condvar
        }
    }

    pub fn lock(&self) -> Waiter<'_, T> {
        Waiter::new(self.mutex.lock().unwrap(), &self.condvar)
    }

    pub fn lock_safe(&self) -> LockResult<Waiter<'_, T>> {
        self.mutex.lock()
            .map(|mutex_guard| Waiter::new(mutex_guard, &self.condvar))
            .map_err(|p|PoisonError::new(Waiter::new(p.into_inner(), &self.condvar)))
    }

    pub fn try_lock(&self) -> TryLockResult<Waiter<'_, T>> {
        self.mutex.try_lock()
            .map(|mutex_guard| Waiter::new(mutex_guard, &self.condvar))
            .map_err(|e| match e {
                TryLockError::Poisoned(poison) =>
                    TryLockError::Poisoned(PoisonError::new(Waiter::new(poison.into_inner(), &self.condvar))),
                TryLockError::WouldBlock => TryLockError::WouldBlock,
            })
    }

    pub fn is_poisoned(&self) -> bool {
        self.mutex.is_poisoned()
    }

    pub fn into_inner(self) -> LockResult<T> where T: Sized {
        self.mutex.into_inner()
            .map(|interruptable| interruptable.value)
            .map_err(|p|PoisonError::new(p.into_inner().value))
    }

    pub fn get_mut(&mut self) -> LockResult<&mut T> {
        self.mutex.get_mut()
            .map(|interruptable| &mut interruptable.value)
            .map_err(|p|PoisonError::new(&mut p.into_inner().value))
    }
}

pub struct Waiter<'a, T: 'a> {
    mutex_guard: Option<MutexGuard<'a, Interruptable<T>>>,
    condvar: &'a Condvar,
}

impl<'a, T> Waiter<'a, T> {
    fn new(mutex_guard: MutexGuard<'a, Interruptable<T>>, condvar: &'a Condvar) -> Waiter<'a, T> {
        Waiter {
            mutex_guard: Some(mutex_guard),
            condvar
        }
    }

    pub fn wait(&mut self) -> io::Result<()> {
        let condvar = self.condvar;
        self.wait_safe().unwrap()
    }

    pub fn wait_safe(&mut self) -> LockResult<io::Result<()>> {
        let condvar = self.condvar;
        self.wait_inner(|mutex_guard| condvar.wait(mutex_guard)
            .map(|r|(r, ()))
            .map_err(|p|PoisonError::new((p.into_inner(), ()))))
    }

    pub fn wait_timeout(&mut self, dur: Duration) -> io::Result<WaitTimeoutResult> {
        let condvar = self.condvar;
        self.wait_timeout_safe(dur).unwrap()
    }

    pub fn wait_timeout_safe(&mut self, dur: Duration) -> LockResult<io::Result<WaitTimeoutResult>> {
        let condvar = self.condvar;
        self.wait_inner(|mutex_guard| condvar.wait_timeout(mutex_guard, dur))
    }

    fn wait_inner<V, F>(&mut self, f: F) -> LockResult<io::Result<V>>
        where F: FnOnce(MutexGuard<'a, Interruptable<T>>) -> LockResult<(MutexGuard<'a, Interruptable<T>>, V)>
    {
        self.mutex_guard.as_mut().unwrap().interruption.waiting += 1;
        let wait_result = f(self.mutex_guard.take().unwrap());
        if let Err(p) = wait_result {
            let (mutex_guard, value ) = p.into_inner();
            self.mutex_guard = Some(mutex_guard);
            return Err(PoisonError::new(Ok(value)))
        }

        let (mutex_guard, value) = wait_result.unwrap();
        self.mutex_guard = Some(mutex_guard);
        let guard = self.mutex_guard.as_mut().unwrap();
        guard.interruption.waiting -= 1;

        if guard.interruption.interrupted > 0 {
            guard.interruption.interrupted -= 1;
            let (thread_id, thread_name) = guard.interruption.thread.as_ref().unwrap();
            return Ok(Err(Error::new(ErrorKind::Interrupted,
                                  format!("Interrupted from another Thread {{ id: {:?}, name: {:?} }}",
                                          thread_id, thread_name))))
        }

        Ok(Ok(value))
    }

    pub fn notify_one(&self) {
        self.condvar.notify_one()
    }

    pub fn notify_all(&self) {
        self.condvar.notify_all()
    }

    pub fn interrupt(&mut self) {
        let guard = self.mutex_guard.as_mut().unwrap();
        guard.interruption.interrupted = guard.interruption.waiting;
        let thread = thread::current();
        guard.interruption.thread = Some((thread.id(), thread.name().map(|s| s.to_owned())));
        self.condvar.notify_all();
    }
}

impl<'a, T> Deref for Waiter<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.mutex_guard.as_ref().unwrap().value
    }
}

impl<'a, T> DerefMut for Waiter<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.mutex_guard.as_mut().unwrap().value
    }
}

impl<'a, T> AsRef<T> for Waiter<'a, T> {
    fn as_ref(&self) -> &T {
        Deref::deref(self)
    }
}

impl<'a, T> AsMut<T> for Waiter<'a, T> {
    fn as_mut(&mut self) -> &mut T {
        DerefMut::deref_mut(self)
    }
}


#[cfg(test)]
mod test {
    use std::sync::{Mutex, Arc, TryLockError, LockResult};
    use crate::waiter::Lock;
    use std::thread;
    use std::time::{Duration, SystemTime, Instant};

    #[derive(Eq, PartialEq, Debug)]
    struct NonCopy(i32);

    #[test]
    fn unlock() {
        let lock = Lock::new(());
        drop(lock.lock());
        drop(lock.lock());
    }

    #[test]
    fn lock() {
        let lock = Arc::new(Lock::new(()));
        let waiter = lock.lock();

        let moved_lock = lock.clone();
        let join_handle = thread::spawn(move|| {
            let start = Instant::now();
            moved_lock.lock();
            Instant::now().duration_since(start)
        });

        thread::sleep(Duration::from_millis(6));
        drop(waiter);

        let duration = join_handle.join().unwrap();
        assert!(duration >= Duration::from_millis(5));
    }

    #[test]
    fn try_lock() {
        let lock = Arc::new(Lock::new(()));
        let waiter = lock.lock();

        let moved_lock = lock.clone();
        let join_handle = thread::spawn(move|| {
            let result = moved_lock.try_lock();
            assert!(result.is_err());
            match result.err().unwrap() {
                TryLockError::Poisoned(p) => panic!("Poisoned {}", p),
                TryLockError::WouldBlock => true
            }
        });

        assert!(join_handle.join().unwrap());
    }

    #[test]
    fn test_into_inner() {
        let lock = Lock::new(NonCopy(10));
        assert_eq!(lock.into_inner().unwrap(), NonCopy(10));
    }


    #[test]
    fn is_poisoned() {
        let lock = Arc::new(Lock::new(NonCopy(10)));
        let cloned_lock = lock.clone();
        let _ = thread::spawn(move || {
            let w = cloned_lock.lock();
            panic!("test panic in inner thread to poison mutex");
        }).join();

        assert!(lock.is_poisoned());
        match Arc::try_unwrap(lock).unwrap().into_inner() {
            Err(e) => assert_eq!(e.into_inner(), NonCopy(10)),
            Ok(x) => panic!("into_inner of poisoned Mutex is Ok: {:?}", x),
        }
    }


    #[test]
    fn test_get_mut() {
        let mut lock = Lock::new(NonCopy(10));
        *lock.get_mut().unwrap() = NonCopy(20);
        assert_eq!(lock.into_inner().unwrap(), NonCopy(20));
    }

    #[test]
    fn test_get_mut_poison() {
        let lock = Arc::new(Lock::new(NonCopy(10)));
        let cloned_lock = lock.clone();
        let _ = thread::spawn(move || {
            let _lock = cloned_lock.lock();
            panic!("test panic in inner thread to poison mutex");
        }).join();

        assert!(lock.is_poisoned());
        match Arc::try_unwrap(lock).unwrap().get_mut() {
            Err(e) => assert_eq!(*e.into_inner(), NonCopy(10)),
            Ok(x) => panic!("get_mut of poisoned Mutex is Ok: {:?}", x),
        }
    }

    #[test]
    fn arc_poison() {
        let arc = Arc::new(Lock::new(1));
        assert!(!arc.is_poisoned());

        let cloned_arc = arc.clone();
        let _ = thread::spawn(move|| {
            let lock = cloned_arc.lock();
            assert_eq!(*lock, 2);
        }).join();

        assert!(arc.lock_safe().is_err());
        assert!(arc.is_poisoned());
    }

    #[test]
    fn notify_one() {
        let lock = Arc::new(Lock::new(()));
        let cloned_lock = lock.clone();

        let mut w = lock.lock();
        let _t = thread::spawn(move|| {
            let n = cloned_lock.lock();
            n.notify_one();
        });
        assert!(w.wait().is_ok());
        drop(w);
    }

    #[test]
    fn wait_timeout_wait() {
        let lock = Arc::new(Lock::new(()));

        loop {
            let mut w = lock.lock();
            let no_timeout = w.wait_timeout(Duration::from_millis(1)).unwrap();
            // spurious wakeups mean this isn't necessarily true
            // so execute test again, if not timeout
            if !no_timeout.timed_out() {
                continue;
            }

            break;
        }
    }
}

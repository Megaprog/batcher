use std::sync::{MutexGuard, Condvar, Mutex, TryLockResult, TryLockError, PoisonError};
use std::ops::{Deref, DerefMut};
use std::{io, thread};
use std::io::{Error, ErrorKind};
use std::thread::ThreadId;

struct Interruption {
    waiting: i32,
    interrupted: i32,
    thread: Option<(ThreadId, Option<String>)>,
}

struct Interruptable<T> {
    value: T,
    interruption: Interruption,
}

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

    pub fn into_inner(self) -> T where T: Sized {
        self.mutex.into_inner().unwrap().value
    }

    pub fn get_mut(&mut self) -> &mut T {
        &mut self.mutex.get_mut().unwrap().value
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

    fn wait(&mut self) -> io::Result<()> {
        self.mutex_guard.as_mut().unwrap().interruption.waiting += 1;
        self.mutex_guard = Some(self.condvar.wait(self.mutex_guard.take().unwrap()).unwrap());
        let guard = self.mutex_guard.as_mut().unwrap();
        guard.interruption.waiting -= 1;

        if guard.interruption.interrupted > 0 {
            guard.interruption.interrupted -= 1;
            let (thread_id, thread_name) = guard.interruption.thread.as_ref().unwrap();
            return Err(Error::new(ErrorKind::Interrupted,
                                  format!("Interrupted from another Thread {{ id: {:?}, name: {:?} }}",
                                          thread_id, thread_name)))
        }

        Ok(())
    }

    fn interrupt(&mut self) {
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

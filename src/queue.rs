use intmap::IntMap;
use parking_lot::{Condvar, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
struct Inner<T> {
    num: usize,
    queue: IntMap<usize, T>,
    next_index: usize,
}

// TODO we could just put num, queue and next_index in a mutex
#[derive(Debug)]
pub struct NetworkQueue<T> {
    inner: Mutex<Inner<T>>,
    cvar: Condvar,
}

impl<T> NetworkQueue<T> {
    pub fn new(items: Vec<T>) -> Self {
        let mut queue = IntMap::new();
        for (id, item) in items.into_iter().enumerate() {
            queue.insert(id, item);
        }
        Self {
            inner: Mutex::new(Inner {
                num: queue.len(),
                queue,
                next_index: 0,
            }),
            cvar: Condvar::new(),
        }
    }

    pub fn pop(&self) -> (usize, T) {
        let mut inner = self.inner.lock();
        let index = inner.next_index % inner.num;
        inner.next_index += 1;

        // we can get woken up if another item was added back,
        // so we loop and check if it was the one we are wating for
        while inner.queue.get(index).is_none() {
            self.cvar.wait(&mut inner);
        }

        // we got woken up, item must be present now
        // only main thread can call pop, so no other thread can be here
        let item = inner.queue.remove(index).expect("must exist");
        (index, item)
    }

    pub fn push(&self, index: usize, item: T) {
        let mut inner = self.inner.lock();

        // add item back and notfiy main thread if it was wating on the condvar
        inner.queue.insert(index, item);
        self.cvar.notify_one();
    }

    pub fn remove(&self) -> Option<T> {
        let mut inner = self.inner.lock();

        if inner.queue.is_empty() {
            return None;
        }

        let index = inner.queue.len() - 1;
        inner.num -= 1;
        inner.queue.remove(index)
    }

    pub fn insert(&self, item: T) {
        let mut inner = self.inner.lock();

        let index = inner.queue.len();
        inner.num += 1;
        inner.queue.insert(index, item);
    }
}

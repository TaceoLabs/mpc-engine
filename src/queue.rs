use intmap::IntMap;
use parking_lot::{Condvar, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
pub struct NetworkQueue<T> {
    len: usize,
    queue: Mutex<IntMap<usize, T>>,
    cvar: Condvar,
    next_index: AtomicUsize,
}

impl<T> NetworkQueue<T> {
    pub fn new(items: Vec<T>) -> Self {
        let mut queue = IntMap::new();
        for (id, item) in items.into_iter().enumerate() {
            queue.insert(id, item);
        }
        Self {
            len: queue.len(),
            queue: Mutex::new(queue),
            cvar: Condvar::new(),
            next_index: AtomicUsize::default(),
        }
    }

    pub fn pop(&self) -> (usize, T) {
        let mut queue = self.queue.lock();
        let index = self.next_index.fetch_add(1, Ordering::Relaxed) % self.len;

        // we can get woken up if another item was added back,
        // so we loop and check if it was the one we are wating for
        while queue.get(index).is_none() {
            self.cvar.wait(&mut queue);
        }

        // we got woken up, item must be present now
        // only main thread can call pop, so no other thread can be here
        let item = queue.remove(index).expect("must exist");
        (index, item)
    }

    pub fn push(&self, index: usize, item: T) {
        let mut queue = self.queue.lock();

        // add item back and notfiy main thread if it was wating on the condvar
        queue.insert(index, item);
        self.cvar.notify_one();
    }
}

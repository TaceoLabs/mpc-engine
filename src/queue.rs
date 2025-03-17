use intmap::IntMap;
use parking_lot::{Condvar, Mutex};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Debug)]
enum Entry<T> {
    Present(T),
    Vacant(Arc<Condvar>),
}

#[derive(Debug)]
pub struct NetworkQueue<T> {
    len: usize,
    queue: Mutex<IntMap<usize, Entry<T>>>,
    next_index: AtomicUsize,
}

impl<T> NetworkQueue<T> {
    pub fn new(items: Vec<T>) -> Self {
        let mut queue = IntMap::new();
        for (id, item) in items.into_iter().enumerate() {
            queue.insert(id, Entry::Present(item));
        }
        Self {
            len: queue.len(),
            queue: Mutex::new(queue),
            next_index: AtomicUsize::default(),
        }
    }

    pub fn pop(&self) -> (usize, T) {
        let mut queue = self.queue.lock();
        let index = self.next_index.fetch_add(1, Ordering::Relaxed) % self.len;

        // check if vacant and clone cond var if it is
        // we cant borrow the condvar entry and the queue while calling wait at the same time, instead we clone the arc
        let cvar = if let Entry::Vacant(cvar) = queue.get(index).unwrap() {
            Some(Arc::clone(cvar))
        } else {
            None
        };

        if let Some(cvar) = cvar {
            cvar.wait(&mut queue);
        }

        // we got woken up, entry must be present now
        // only main thread can call pop, so noo other thread can be here
        // or even wait in the condvar above
        if let Entry::Present(entry) = queue
            .insert(index, Entry::Vacant(Arc::new(Condvar::new())))
            .expect("must exist")
        {
            (index, entry)
        } else {
            panic!("got woken up but entry is vacant")
        }
    }

    pub fn push(&self, index: usize, item: T) {
        let mut queue = self.queue.lock();

        // add enrt back and notfiy main thread if it was wating on the condvar
        if let Entry::Vacant(cvar) = queue
            .insert(index, Entry::Present(item))
            .expect("must exist")
        {
            cvar.notify_one();
        } else {
            panic!("entry was already present");
        }
    }
}

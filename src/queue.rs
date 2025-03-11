use parking_lot::{Condvar, Mutex};
use std::collections::VecDeque;
use std::sync::atomic::AtomicUsize;

#[derive(Debug)]
pub struct NetworkQueue<T> {
    size: usize,
    // queue of (net_id, net) and queue of waker ids
    pub queue: Mutex<(VecDeque<(usize, T)>, VecDeque<usize>)>,
    pub buffer: Mutex<VecDeque<(usize, T)>>,
    cv: Condvar,
    cv_waker_id: AtomicUsize,
}

impl<T> NetworkQueue<T> {
    pub fn new(items: Vec<T>) -> Self {
        Self {
            size: items.len(),
            queue: Mutex::new((
                VecDeque::from(items.into_iter().enumerate().collect::<Vec<_>>()),
                VecDeque::new(),
            )),
            buffer: Mutex::new(VecDeque::new()),
            cv: Condvar::new(),
            cv_waker_id: AtomicUsize::default(),
        }
    }

    pub fn pop(&self) -> (usize, T) {
        let mut queue = self.queue.lock();
        // get shared increementing waker id to determine wake order
        let id = self
            .cv_waker_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        queue.1.push_back(id);
        // if not empty and my id is next, continue
        while queue.0.is_empty() || queue.1.front().unwrap() != &id {
            tracing::warn!("wating for a free netwok, this should not happen!");
            self.cv.wait(&mut queue);
        }
        // remove waker id
        queue.1.pop_front().unwrap();
        // get item, unwrap because queue guaranteed not empty
        queue.0.pop_front().unwrap()
    }

    pub fn push(&self, index: usize, item: T) {
        let mut buffer = self.buffer.lock();
        buffer.push_back((index, item));
        buffer.make_contiguous().sort_by_key(|&(i, _)| i);

        let mut queue = self.queue.lock();
        while let Some(&(i, _)) = buffer.front() {
            if queue.0.is_empty() && i == 0
                || (!queue.0.is_empty()
                    && ((i == 0 && queue.0.back().unwrap().0 == self.size - 1)
                        || queue.0.back().unwrap().0 == i - 1))
            {
                let item = buffer.pop_front().unwrap();
                queue.0.push_back(item);
            } else {
                break;
            }
        }

        // notify all because we need to wake alll threads and let them check if they are next
        self.cv.notify_all();
    }
}

use rayon::{ThreadPool, ThreadPoolBuilder};
use std::sync::Arc;

use crate::{net::Network, queue::NetworkQueue};

pub const NUM_THREADS_NET: usize = 8;
pub const NUM_THREADS_CPU: usize = 0;

#[derive(Debug)]
pub struct MpcEngine<N: Network> {
    id: usize,
    queue: Arc<NetworkQueue<N>>,
    net_pool: ThreadPool,
    cpu_pool: ThreadPool,
}

impl<N: Network + Send + 'static> MpcEngine<N> {
    pub fn new(id: usize, num_threads_net: usize, num_threads_cpu: usize, nets: Vec<N>) -> Self {
        let net_pool = ThreadPoolBuilder::new()
            .num_threads(num_threads_net)
            .use_current_thread()
            .build()
            .unwrap();
        let cpu_pool = ThreadPoolBuilder::new()
            .num_threads(num_threads_cpu)
            .build()
            .unwrap();
        Self {
            id,
            queue: Arc::new(NetworkQueue::new(nets)),
            net_pool,
            cpu_pool,
        }
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn spawn_net<T: Send + 'static>(
        &self,
        f: impl FnOnce(&N) -> T + Send + 'static,
    ) -> Handle<T> {
        let (id, net) = self.queue.pop();
        let queue = Arc::clone(&self.queue);
        let (tx, rx) = oneshot::channel();
        self.net_pool.spawn(move || {
            tx.send(f(&net)).unwrap();
            queue.push(id, net);
        });

        Handle { sender: rx }
    }

    pub fn spawn_cpu<T: Send + 'static>(
        &self,
        f: impl FnOnce() -> T + Send + 'static,
    ) -> Handle<T> {
        let (tx, rx) = oneshot::channel();
        self.cpu_pool.spawn(move || {
            tx.send(f()).unwrap();
        });

        Handle { sender: rx }
    }

    pub fn install_net<T: Send>(&self, f: impl FnOnce(&N) -> T + Send) -> T {
        let (id, net) = self.queue.pop();
        self.net_pool.install(|| {
            let res = f(&net);
            self.queue.push(id, net);
            res
        })
    }

    pub fn install_cpu<T: Send>(&self, f: impl FnOnce() -> T + Send) -> T {
        self.cpu_pool.install(f)
    }

    pub fn join_net<R0: Send, R1: Send>(
        &self,
        f0: impl FnOnce(&N) -> R0 + Send,
        f1: impl FnOnce(&N) -> R1 + Send,
    ) -> (R0, R1) {
        let (id0, net0) = self.queue.pop();
        let (id1, net1) = self.queue.pop();
        let res = self.net_pool.join(|| f0(&net0), || f1(&net1));
        self.queue.push(id0, net0);
        self.queue.push(id1, net1);
        res
    }

    pub fn join3_net<R0: Send, R1: Send, R2: Send>(
        &self,
        f0: impl FnOnce(&N) -> R0 + Send,
        f1: impl FnOnce(&N) -> R1 + Send,
        f2: impl FnOnce(&N) -> R2 + Send,
    ) -> (R0, R1, R2) {
        let (id0, net0) = self.queue.pop();
        let (id1, net1) = self.queue.pop();
        let (id2, net2) = self.queue.pop();
        let (r0, (r1, r2)) = self
            .net_pool
            .join(|| f0(&net0), || rayon::join(|| f1(&net1), || f2(&net2)));
        self.queue.push(id0, net0);
        self.queue.push(id1, net1);
        self.queue.push(id2, net2);
        (r0, r1, r2)
    }

    pub fn join4_net<R0: Send, R1: Send, R2: Send, R3: Send>(
        &self,
        f0: impl FnOnce(&N) -> R0 + Send,
        f1: impl FnOnce(&N) -> R1 + Send,
        f2: impl FnOnce(&N) -> R2 + Send,
        f3: impl FnOnce(&N) -> R3 + Send,
    ) -> (R0, R1, R2, R3) {
        let (id0, net0) = self.queue.pop();
        let (id1, net1) = self.queue.pop();
        let (id2, net2) = self.queue.pop();
        let (id3, net3) = self.queue.pop();
        let (r0, (r1, (r2, r3))) = self.net_pool.join(
            || f0(&net0),
            || rayon::join(|| f1(&net1), || rayon::join(|| f2(&net2), || f3(&net3))),
        );
        self.queue.push(id0, net0);
        self.queue.push(id1, net1);
        self.queue.push(id2, net2);
        self.queue.push(id3, net3);
        (r0, r1, r2, r3)
    }

    pub fn join5_net<R0: Send, R1: Send, R2: Send, R3: Send, R4: Send>(
        &self,
        f0: impl FnOnce(&N) -> R0 + Send,
        f1: impl FnOnce(&N) -> R1 + Send,
        f2: impl FnOnce(&N) -> R2 + Send,
        f3: impl FnOnce(&N) -> R3 + Send,
        f4: impl FnOnce(&N) -> R4 + Send,
    ) -> (R0, R1, R2, R3, R4) {
        let (id0, net0) = self.queue.pop();
        let (id1, net1) = self.queue.pop();
        let (id2, net2) = self.queue.pop();
        let (id3, net3) = self.queue.pop();
        let (id4, net4) = self.queue.pop();
        let (r0, (r1, (r2, (r3, r4)))) = self.net_pool.join(
            || f0(&net0),
            || {
                rayon::join(
                    || f1(&net1),
                    || rayon::join(|| f2(&net2), || rayon::join(|| f3(&net3), || f4(&net4))),
                )
            },
        );
        self.queue.push(id0, net0);
        self.queue.push(id1, net1);
        self.queue.push(id2, net2);
        self.queue.push(id3, net3);
        self.queue.push(id4, net4);
        (r0, r1, r2, r3, r4)
    }

    pub fn join8_net<
        R0: Send,
        R1: Send,
        R2: Send,
        R3: Send,
        R4: Send,
        R5: Send,
        R6: Send,
        R7: Send,
    >(
        &self,
        f0: impl FnOnce(&N) -> R0 + Send,
        f1: impl FnOnce(&N) -> R1 + Send,
        f2: impl FnOnce(&N) -> R2 + Send,
        f3: impl FnOnce(&N) -> R3 + Send,
        f4: impl FnOnce(&N) -> R4 + Send,
        f5: impl FnOnce(&N) -> R5 + Send,
        f6: impl FnOnce(&N) -> R6 + Send,
        f7: impl FnOnce(&N) -> R7 + Send,
    ) -> (R0, R1, R2, R3, R4, R5, R6, R7) {
        let (id0, net0) = self.queue.pop();
        let (id1, net1) = self.queue.pop();
        let (id2, net2) = self.queue.pop();
        let (id3, net3) = self.queue.pop();
        let (id4, net4) = self.queue.pop();
        let (id5, net5) = self.queue.pop();
        let (id6, net6) = self.queue.pop();
        let (id7, net7) = self.queue.pop();
        let (r0, (r1, (r2, (r3, (r4, (r5, (r6, r7))))))) = self.net_pool.join(
            || f0(&net0),
            || {
                rayon::join(
                    || f1(&net1),
                    || {
                        rayon::join(
                            || f2(&net2),
                            || {
                                rayon::join(
                                    || f3(&net3),
                                    || {
                                        rayon::join(
                                            || f4(&net4),
                                            || {
                                                rayon::join(
                                                    || f5(&net5),
                                                    || rayon::join(|| f6(&net6), || f7(&net7)),
                                                )
                                            },
                                        )
                                    },
                                )
                            },
                        )
                    },
                )
            },
        );
        self.queue.push(id0, net0);
        self.queue.push(id1, net1);
        self.queue.push(id2, net2);
        self.queue.push(id3, net3);
        self.queue.push(id4, net4);
        self.queue.push(id5, net5);
        self.queue.push(id6, net6);
        self.queue.push(id7, net7);
        (r0, r1, r2, r3, r4, r5, r6, r7)
    }

    pub fn join_cpu<R0: Send, R1: Send>(
        &self,
        f0: impl FnOnce() -> R0 + Send,
        f1: impl FnOnce() -> R1 + Send,
    ) -> (R0, R1) {
        self.cpu_pool.join(f0, f1)
    }

    pub fn join3_cpu<R0: Send, R1: Send, R2: Send>(
        &self,
        f0: impl FnOnce() -> R0 + Send,
        f1: impl FnOnce() -> R1 + Send,
        f2: impl FnOnce() -> R2 + Send,
    ) -> (R0, R1, R2) {
        let (r0, (r1, r2)) = self.cpu_pool.join(f0, || rayon::join(f1, f2));
        (r0, r1, r2)
    }

    pub fn join4_cpu<R0: Send, R1: Send, R2: Send, R3: Send>(
        &self,
        f0: impl FnOnce() -> R0 + Send,
        f1: impl FnOnce() -> R1 + Send,
        f2: impl FnOnce() -> R2 + Send,
        f3: impl FnOnce() -> R3 + Send,
    ) -> (R0, R1, R2, R3) {
        let (r0, (r1, (r2, r3))) = self
            .cpu_pool
            .join(f0, || rayon::join(f1, || rayon::join(f2, f3)));
        (r0, r1, r2, r3)
    }

    pub fn join5_cpu<R0: Send, R1: Send, R2: Send, R3: Send, R4: Send>(
        &self,
        f0: impl FnOnce() -> R0 + Send,
        f1: impl FnOnce() -> R1 + Send,
        f2: impl FnOnce() -> R2 + Send,
        f3: impl FnOnce() -> R3 + Send,
        f4: impl FnOnce() -> R4 + Send,
    ) -> (R0, R1, R2, R3, R4) {
        let (r0, (r1, (r2, (r3, r4)))) = self.cpu_pool.join(f0, || {
            rayon::join(f1, || rayon::join(f2, || rayon::join(f3, f4)))
        });
        (r0, r1, r2, r3, r4)
    }
}

#[derive(Debug)]
pub struct Handle<T> {
    sender: oneshot::Receiver<T>,
}

impl<T> Handle<T> {
    pub fn join(self) -> T {
        self.sender.recv().unwrap()
    }
}

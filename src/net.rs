use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use eyre::ContextCompat;
use intmap::IntMap;
use parking_lot::Mutex;
use std::{
    cmp::Ordering,
    io::{Read, Write},
    net::{TcpListener, TcpStream, ToSocketAddrs},
    sync::mpsc,
    time::Duration,
};

const TIMEOUT: Duration = Duration::from_secs(30);

pub trait Network: Send + Sync {
    fn id(&self) -> usize;
    fn send(&self, to: usize, data: &[u8]) -> eyre::Result<()>;
    fn recv(&self, from: usize) -> eyre::Result<Vec<u8>>;
}

// TODO add TLS network

#[derive(Debug)]
pub struct TcpNetwork {
    id: usize,
    send: IntMap<usize, Mutex<TcpStream>>,
    recv: IntMap<usize, Mutex<TcpStream>>,
}

impl TcpNetwork {
    pub fn networks<A: ToSocketAddrs>(
        id: usize,
        bind_addr: A,
        addrs: &[A],
        num: usize,
    ) -> eyre::Result<Vec<Self>> {
        tracing::debug!("creating new network");
        let listener = TcpListener::bind(bind_addr)?;

        let mut nets = Vec::with_capacity(num);
        for _ in 0..num {
            nets.push(Self {
                id,
                send: IntMap::default(),
                recv: IntMap::default(),
            });
        }

        for i in 0..num {
            for (other_id, addr) in addrs.iter().enumerate() {
                match id.cmp(&other_id) {
                    Ordering::Less => {
                        let mut stream = loop {
                            if let Ok(stream) = TcpStream::connect(addr) {
                                break stream;
                            }
                            std::thread::sleep(Duration::from_millis(50));
                        };
                        stream.set_write_timeout(Some(TIMEOUT))?;
                        stream.set_nodelay(true)?;
                        stream.write_u64::<BigEndian>(i as u64)?;
                        stream.write_u64::<BigEndian>(id as u64)?;
                        nets[i]
                            .send
                            .insert(other_id, Mutex::new(stream.try_clone().unwrap()));
                        nets[i].recv.insert(other_id, Mutex::new(stream));
                    }
                    Ordering::Greater => {
                        let (mut stream, _) = listener.accept()?;
                        stream.set_write_timeout(Some(TIMEOUT))?;
                        stream.set_nodelay(true)?;
                        let i = stream.read_u64::<BigEndian>()? as usize;
                        let other_id = stream.read_u64::<BigEndian>()? as usize;
                        nets[i]
                            .send
                            .insert(other_id, Mutex::new(stream.try_clone().unwrap()));
                        nets[i].recv.insert(other_id, Mutex::new(stream));
                    }
                    Ordering::Equal => continue,
                }
            }
        }

        Ok(nets)
    }
}

impl Network for TcpNetwork {
    fn id(&self) -> usize {
        self.id
    }

    fn send(&self, to: usize, data: &[u8]) -> eyre::Result<()> {
        let mut stream = self
            .send
            .get(to)
            .context("while get stream in send")?
            .lock();
        stream.write_u32::<BigEndian>(data.len() as u32)?;
        stream.write_all(data)?;
        Ok(())
    }

    fn recv(&self, from: usize) -> eyre::Result<Vec<u8>> {
        let mut stream = self
            .recv
            .get(from)
            .context("while get stream in recv")?
            .lock();
        let len = stream.read_u32::<BigEndian>()? as usize;
        let mut data = vec![0; len];
        stream.read_exact(&mut data)?;
        Ok(data)
    }
}

#[derive(Debug)]
pub struct TestNetwork {
    id: usize,
    send: IntMap<usize, mpsc::Sender<Vec<u8>>>,
    recv: IntMap<usize, Mutex<mpsc::Receiver<Vec<u8>>>>,
}

impl TestNetwork {
    pub fn party_networks(num_parties: usize) -> Vec<Self> {
        let mut networks = Vec::with_capacity(num_parties);
        let mut senders = Vec::new();
        let mut receivers = Vec::new();

        for _ in 0..num_parties {
            senders.push(IntMap::new());
            receivers.push(IntMap::new());
        }

        #[allow(clippy::needless_range_loop)]
        for i in 0..num_parties {
            for j in 0..num_parties {
                if i != j {
                    let (tx, rx) = mpsc::channel();
                    senders[i].insert(j, tx);
                    receivers[j].insert(i, Mutex::new(rx));
                }
            }
        }

        for (id, (send, recv)) in senders.into_iter().zip(receivers).enumerate() {
            networks.push(TestNetwork { id, send, recv });
        }

        networks
    }

    pub fn networks(num_parties: usize, num: usize) -> Vec<Vec<Self>> {
        let mut nets = (0..num_parties)
            .map(|_| Vec::with_capacity(num))
            .collect::<Vec<Vec<_>>>();
        for _ in 0..num {
            for (party_id, net) in TestNetwork::party_networks(num_parties)
                .into_iter()
                .enumerate()
            {
                nets[party_id].push(net);
            }
        }
        nets
    }
}

impl Network for TestNetwork {
    fn id(&self) -> usize {
        self.id
    }

    fn send(&self, to: usize, data: &[u8]) -> eyre::Result<()> {
        self.send
            .get(to)
            .context("while get stream in send")?
            .send(data.to_owned())?;
        Ok(())
    }

    fn recv(&self, from: usize) -> eyre::Result<Vec<u8>> {
        Ok(self
            .recv
            .get(from)
            .context("while get stream in recv")?
            .lock()
            .recv_timeout(TIMEOUT)?)
    }
}

#[derive(Clone, Copy)]
pub struct DummyNetwork;

impl DummyNetwork {
    pub fn networks(num: usize) -> Vec<Self> {
        vec![DummyNetwork; num]
    }
}

impl Network for DummyNetwork {
    fn id(&self) -> usize {
        0
    }

    fn send(&self, _to: usize, _data: &[u8]) -> eyre::Result<()> {
        Ok(())
    }

    fn recv(&self, _from: usize) -> eyre::Result<Vec<u8>> {
        Ok(vec![])
    }
}

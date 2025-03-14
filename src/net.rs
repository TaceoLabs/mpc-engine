use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use eyre::ContextCompat;
use intmap::IntMap;
use parking_lot::Mutex;
use rustls::{
    ClientConfig, ClientConnection, RootCertStore, ServerConfig, ServerConnection, StreamOwned,
    pki_types::{CertificateDer, PrivateKeyDer, ServerName},
};
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    fmt::Formatter,
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs},
    num::ParseIntError,
    str::FromStr,
    sync::{Arc, mpsc},
    time::Duration,
};

const TIMEOUT: Duration = Duration::from_secs(30);

/// A network address wrapper.
#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub struct Address {
    /// The hostname of the address, will be DNS resolved.
    pub hostname: String,
    /// The port of the address.
    pub port: u16,
}

impl Address {
    /// Construct a new [`Address`] type.
    pub fn new(hostname: String, port: u16) -> Self {
        Self { hostname, port }
    }
}

impl std::fmt::Display for Address {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.hostname, self.port)
    }
}

/// An error for parsing [`Address`]es.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseAddressError {
    /// Must be hostname:port
    InvalidFormat,
    /// Invalid port
    InvalidPort(ParseIntError),
}

impl std::error::Error for ParseAddressError {}

impl std::fmt::Display for ParseAddressError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseAddressError::InvalidFormat => {
                write!(f, "invalid format, expected hostname:port")
            }
            ParseAddressError::InvalidPort(e) => write!(f, "cannot parse port: {e}"),
        }
    }
}

impl FromStr for Address {
    type Err = ParseAddressError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 2 {
            return Err(ParseAddressError::InvalidFormat);
        }
        let hostname = parts[0].to_string();
        let port = parts[1].parse().map_err(ParseAddressError::InvalidPort)?;
        Ok(Address { hostname, port })
    }
}

impl ToSocketAddrs for Address {
    type Iter = std::vec::IntoIter<SocketAddr>;
    fn to_socket_addrs(&self) -> std::io::Result<Self::Iter> {
        format!("{}:{}", self.hostname, self.port).to_socket_addrs()
    }
}

impl Serialize for Address {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format!("{}:{}", self.hostname, self.port))
    }
}

impl<'de> Deserialize<'de> for Address {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Address::from_str(&s).map_err(serde::de::Error::custom)
    }
}

pub trait Network: Send + Sync {
    fn id(&self) -> usize;
    fn send(&self, to: usize, data: &[u8]) -> eyre::Result<()>;
    fn recv(&self, from: usize) -> eyre::Result<Vec<u8>>;
}

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
        addrs: &[Address],
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

/// A wrapper type for client and server TLS streams
#[derive(Debug)]
pub enum TlsStream {
    /// A Stream with a client connection
    Client(StreamOwned<ClientConnection, TcpStream>),
    /// A Stream with a sever connection
    Server(StreamOwned<ServerConnection, TcpStream>),
}

impl From<StreamOwned<ClientConnection, TcpStream>> for TlsStream {
    fn from(value: StreamOwned<ClientConnection, TcpStream>) -> Self {
        Self::Client(value)
    }
}

impl From<StreamOwned<ServerConnection, TcpStream>> for TlsStream {
    fn from(value: StreamOwned<ServerConnection, TcpStream>) -> Self {
        Self::Server(value)
    }
}

impl Read for TlsStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            TlsStream::Client(stream) => stream.read(buf),
            TlsStream::Server(stream) => stream.read(buf),
        }
    }
}

impl Write for TlsStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            TlsStream::Client(stream) => stream.write(buf),
            TlsStream::Server(stream) => stream.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            TlsStream::Client(stream) => stream.flush(),
            TlsStream::Server(stream) => stream.flush(),
        }
    }
}

#[derive(Debug)]
pub struct TlsNetwork {
    id: usize,
    send: IntMap<usize, Mutex<TlsStream>>,
    recv: IntMap<usize, Mutex<TlsStream>>,
}

impl TlsNetwork {
    pub fn networks<A: ToSocketAddrs>(
        id: usize,
        bind_addr: A,
        addrs: &[Address],
        certs: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
        num: usize,
    ) -> eyre::Result<Vec<Self>> {
        tracing::debug!("creating new network");

        let mut root_store = RootCertStore::empty();
        for cert in &certs {
            root_store.add(cert.clone())?;
        }
        let client_config = ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        let server_config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![certs[id].clone()], key)?;

        let client_config = Arc::new(client_config);
        let server_config = Arc::new(server_config);

        let listener = TcpListener::bind(bind_addr)?;

        let mut nets = Vec::with_capacity(num);
        for _ in 0..num {
            nets.push(Self {
                id,
                send: IntMap::default(),
                recv: IntMap::default(),
            });
        }

        const STREAM_0: u8 = 0;
        const STREAM_1: u8 = 1;

        for i in 0..num {
            for s in [STREAM_0, STREAM_1] {
                for (other_id, addr) in addrs.iter().enumerate() {
                    match id.cmp(&other_id) {
                        Ordering::Less => {
                            let stream = loop {
                                if let Ok(stream) = TcpStream::connect(addr) {
                                    break stream;
                                }
                                std::thread::sleep(Duration::from_millis(50));
                            };
                            stream.set_write_timeout(Some(TIMEOUT))?;
                            stream.set_nodelay(true)?;

                            let name = ServerName::try_from(addr.hostname.clone())?.to_owned();
                            let conn = ClientConnection::new(client_config.clone(), name.clone())?;
                            let mut stream = StreamOwned::new(conn, stream);

                            stream.write_u64::<BigEndian>(i as u64)?;
                            stream.write_u64::<BigEndian>(id as u64)?;
                            stream.write_u8(s)?;

                            if s == STREAM_0 {
                                nets[i]
                                    .send
                                    .insert(other_id, Mutex::new(TlsStream::Client(stream)));
                            } else {
                                nets[i]
                                    .recv
                                    .insert(other_id, Mutex::new(TlsStream::Client(stream)));
                            }
                        }
                        Ordering::Greater => {
                            let (stream, _) = listener.accept()?;
                            stream.set_write_timeout(Some(TIMEOUT))?;
                            stream.set_nodelay(true)?;

                            let conn = ServerConnection::new(server_config.clone())?;
                            let mut stream = StreamOwned::new(conn, stream);

                            let i = stream.read_u64::<BigEndian>()? as usize;
                            let other_id = stream.read_u64::<BigEndian>()? as usize;
                            let s_ = stream.read_u8()?;

                            if s_ == STREAM_0 {
                                nets[i]
                                    .recv
                                    .insert(other_id, Mutex::new(TlsStream::Server(stream)));
                            } else {
                                nets[i]
                                    .send
                                    .insert(other_id, Mutex::new(TlsStream::Server(stream)));
                            }
                        }
                        Ordering::Equal => continue,
                    }
                }
            }
        }

        Ok(nets)
    }
}

impl Network for TlsNetwork {
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

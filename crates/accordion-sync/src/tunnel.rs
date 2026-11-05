//! The tunnel inside the app (decision 23 in the `accordion` repo's
//! `docs/decisions`).
//!
//! Each account gets its own WireGuard tunnel to its server, run here in
//! user space with GotaTun, and its own small TCP/IP stack, built on
//! smoltcp. The tunnel carries only the app's own requests, so it's not a
//! system VPN: it needs no permission, no admin rights, and nothing to turn
//! on, and it works next to a work VPN or another VPN. Two accounts on two
//! servers have two tunnels, even when both servers have the same address
//! on their own networks, since each request picks its tunnel by account.
//!
//! The app makes its key pair on the device and registers only the public
//! half, so the private key never leaves it. Connections through the tunnel
//! go to the server's own address on the tunnel, which only the server
//! answers.

use std::collections::VecDeque;
use std::fmt;
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::{Buf, BytesMut};
use gotatun::device::{self, Device, Peer};
use gotatun::packet::{Ip, Packet, PacketBufPool};
use gotatun::tun::{IpRecv, IpSend, MtuWatcher};
use gotatun::udp::socket::UdpSocketFactory;
use gotatun::x25519;
use ipnetwork::{IpNetwork, Ipv4Network};
use serde::{Deserialize, Serialize};
use smoltcp::iface::{Config, Interface, SocketHandle, SocketSet};
use smoltcp::phy::{self, DeviceCapabilities, Medium};
use smoltcp::socket::tcp;
use smoltcp::wire::{HardwareAddress, IpAddress, IpCidr};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{Notify, mpsc, oneshot};

/// The largest packet inside the tunnel, as the server's end uses.
const MTU: u16 = 1420;

/// Packets waiting between WireGuard and the stack, each way.
const QUEUE: usize = 1024;

/// How often the tunnel sends a packet while it's idle, so the network the
/// device is on keeps its way back open, as the WireGuard apps do.
const KEEPALIVE: u16 = 25;

/// smoltcp's buffers for each connection, each way.
const SOCKET_BUFFER: usize = 256 * 1024;

/// Data waiting between smoltcp and the HTTP client, each way.
const STREAM_BUFFER: usize = 256 * 1024;

/// An idle connection is checked this often, and one that doesn't answer
/// for `GONE_AFTER` has gone, such as when the server restarted.
const KEEP_ALIVE: Duration = Duration::from_secs(60);
const GONE_AFTER: Duration = Duration::from_secs(120);

/// The local ports for connections through the tunnel.
const FIRST_PORT: u16 = 49152;
const PORTS: u16 = u16::MAX - FIRST_PORT;

/// What an account's tunnel needs, as the server gave it when the app
/// registered its key, plus the app's own private key. It's kept with the
/// account, in a file only its person can read.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TunnelSetup {
    /// The app's private key, in base64.
    pub private_key: String,
    /// The server's public key, in base64.
    pub server_key: String,
    /// Where the app finds home, such as `k3x9q2wm7f.ourhome.dedyn.io:51820`.
    pub endpoint: String,
    /// The app's own address on the tunnel.
    pub address: Ipv4Addr,
    /// The server's address on the tunnel, which the app connects to.
    pub gateway: Ipv4Addr,
}

impl fmt::Debug for TunnelSetup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TunnelSetup")
            .field("server_key", &self.server_key)
            .field("endpoint", &self.endpoint)
            .field("address", &self.address)
            .field("gateway", &self.gateway)
            .finish_non_exhaustive()
    }
}

/// A new key pair: the private key, to keep, and the public key, to
/// register with the server. Both in base64, as WireGuard writes them.
pub fn new_key() -> (String, String) {
    use ring::rand::{SecureRandom, SystemRandom};
    let mut bytes = [0; 32];
    SystemRandom::new()
        .fill(&mut bytes)
        .expect("the operating system's random number generator works");
    let secret = x25519::StaticSecret::from(bytes);
    let public = x25519::PublicKey::from(&secret);
    (
        BASE64.encode(secret.to_bytes()),
        BASE64.encode(public.as_bytes()),
    )
}

/// The public half of a private key in base64, or `None` if it isn't one.
pub fn public_key(private_key: &str) -> Option<String> {
    let secret = x25519::StaticSecret::from(decode(private_key)?);
    Some(BASE64.encode(x25519::PublicKey::from(&secret).as_bytes()))
}

fn decode(text: &str) -> Option<[u8; 32]> {
    BASE64
        .decode(text.trim())
        .ok()
        .and_then(|bytes| bytes.try_into().ok())
}

fn bad_setup(what: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("the tunnel's setup has {what} that isn't right"),
    )
}

type Transports = (UdpSocketFactory, ToStack, FromStack);

/// A running tunnel to one server. Dropping it stops it.
pub struct Tunnel {
    open: mpsc::Sender<Open>,
    /// Kept so the tunnel runs as long as this does.
    _device: Device<Transports>,
    stack: tokio::task::JoinHandle<()>,
    /// The endpoint's address when the tunnel started.
    endpoint: SocketAddr,
}

/// A request to open a connection to the server's `port`.
struct Open {
    port: u16,
    reply: oneshot::Sender<io::Result<TunnelStream>>,
}

impl Tunnel {
    /// Looks up where home is, then starts the tunnel. WireGuard only
    /// sends its handshake when the first connection needs it.
    pub async fn start(setup: &TunnelSetup) -> io::Result<Self> {
        let key = decode(&setup.private_key).ok_or_else(|| bad_setup("a private key"))?;
        let server = decode(&setup.server_key).ok_or_else(|| bad_setup("a server key"))?;
        let endpoint = tokio::net::lookup_host(&setup.endpoint)
            .await?
            // IPv4 first: home routers forward the tunnel's port on it.
            .min_by_key(SocketAddr::is_ipv6)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("{} has no address", setup.endpoint),
                )
            })?;

        let (to_stack, inbound) = mpsc::channel(QUEUE);
        let (outbound, from_stack) = mpsc::channel(QUEUE);
        let mut peer = Peer::new(x25519::PublicKey::from(server));
        peer.endpoint = Some(endpoint);
        peer.keepalive = Some(KEEPALIVE);
        // Only the server's own address on the tunnel, so nothing else at
        // home can be reached, and nothing else on this device goes in it.
        peer.allowed_ips = vec![IpNetwork::V4(
            Ipv4Network::new(setup.gateway, 32).expect("32 is a valid prefix"),
        )];
        let device = device::build()
            .with_default_udp()
            .with_ip_pair(ToStack(to_stack), FromStack(from_stack))
            .with_private_key(x25519::StaticSecret::from(key))
            .with_peer(peer)
            .build()
            .await
            .map_err(|err| io::Error::other(format!("couldn't start the tunnel: {err}")))?;
        let (open, requests) = mpsc::channel(64);
        let stack = tokio::spawn(run_stack(
            setup.address,
            setup.gateway,
            inbound,
            outbound,
            requests,
        ));
        Ok(Self {
            open,
            _device: device,
            stack,
            endpoint,
        })
    }

    /// Where the tunnel sends its packets.
    pub fn endpoint(&self) -> SocketAddr {
        self.endpoint
    }

    /// Opens a TCP connection through the tunnel to the server's `port`.
    /// It completes once the server answers, so a tunnel that can't reach
    /// home takes as long as the caller lets it.
    pub async fn connect(&self, port: u16) -> io::Result<TunnelStream> {
        let (reply, answer) = oneshot::channel();
        self.open
            .send(Open { port, reply })
            .await
            .map_err(|_| io::Error::other("the tunnel stopped"))?;
        answer
            .await
            .map_err(|_| io::Error::other("the tunnel stopped"))?
    }
}

impl Drop for Tunnel {
    fn drop(&mut self) {
        self.stack.abort();
    }
}

/// Packets WireGuard opened, on their way to the stack.
struct ToStack(mpsc::Sender<Vec<u8>>);

impl IpSend for ToStack {
    async fn send(&mut self, packet: Packet<Ip>) -> io::Result<()> {
        // A full queue drops the packet, as a busy network card would, and
        // TCP sends it again.
        let _ = self.0.try_send(packet.into_bytes().to_vec());
        Ok(())
    }
}

/// Packets from the stack, on their way to WireGuard to be sealed.
struct FromStack(mpsc::Receiver<Vec<u8>>);

impl IpRecv for FromStack {
    async fn recv<'a>(
        &'a mut self,
        _pool: &mut PacketBufPool,
    ) -> io::Result<impl Iterator<Item = Packet<Ip>> + Send + 'a> {
        let Some(first) = self.0.recv().await else {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "the tunnel's network stack stopped",
            ));
        };
        let mut packets = vec![first];
        while packets.len() < 64
            && let Ok(packet) = self.0.try_recv()
        {
            packets.push(packet);
        }
        Ok(packets.into_iter().filter_map(|packet| {
            Packet::from_bytes(BytesMut::from(&packet[..]))
                .try_into_ip()
                .ok()
        }))
    }

    fn mtu(&self) -> MtuWatcher {
        MtuWatcher::new(MTU)
    }
}

/// smoltcp's view of the tunnel: packets in from WireGuard, and packets
/// out to it.
#[derive(Default)]
struct Wire {
    received: VecDeque<Vec<u8>>,
    sent: VecDeque<Vec<u8>>,
}

impl phy::Device for Wire {
    type RxToken<'a> = Received;
    type TxToken<'a> = Sending<'a>;

    fn receive(
        &mut self,
        _: smoltcp::time::Instant,
    ) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        let packet = self.received.pop_front()?;
        Some((Received(packet), Sending(&mut self.sent)))
    }

    fn transmit(&mut self, _: smoltcp::time::Instant) -> Option<Self::TxToken<'_>> {
        Some(Sending(&mut self.sent))
    }

    fn capabilities(&self) -> DeviceCapabilities {
        let mut capabilities = DeviceCapabilities::default();
        capabilities.medium = Medium::Ip;
        capabilities.max_transmission_unit = MTU.into();
        capabilities
    }
}

struct Received(Vec<u8>);

impl phy::RxToken for Received {
    fn consume<R, F: FnOnce(&[u8]) -> R>(self, f: F) -> R {
        f(&self.0)
    }
}

struct Sending<'a>(&'a mut VecDeque<Vec<u8>>);

impl phy::TxToken for Sending<'_> {
    fn consume<R, F: FnOnce(&mut [u8]) -> R>(self, len: usize, f: F) -> R {
        let mut packet = vec![0; len];
        let result = f(&mut packet);
        self.0.push_back(packet);
        result
    }
}

/// One TCP connection, from the stack's side.
struct Connection {
    socket: SocketHandle,
    local_port: u16,
    shared: Arc<Shared>,
    /// Waiting for the server to answer, to hand the connection over.
    reply: Option<oneshot::Sender<io::Result<TunnelStream>>>,
    /// smoltcp was told to close it.
    closing: bool,
}

/// Moves packets and connections until the tunnel is dropped.
async fn run_stack(
    own: Ipv4Addr,
    gateway: Ipv4Addr,
    mut inbound: mpsc::Receiver<Vec<u8>>,
    outbound: mpsc::Sender<Vec<u8>>,
    mut requests: mpsc::Receiver<Open>,
) {
    let mut wire = Wire::default();
    let mut iface = Interface::new(
        Config::new(HardwareAddress::Ip),
        &mut wire,
        smoltcp::time::Instant::now(),
    );
    // A /0 puts every address on the link, so the stack needs no route.
    // WireGuard only lets through what's for the server anyway.
    iface.update_ip_addrs(|list| {
        let _ = list.push(IpCidr::new(IpAddress::Ipv4(own), 0));
    });
    let mut sockets = SocketSet::new(Vec::new());
    let mut connections: Vec<Connection> = Vec::new();
    let wake = Arc::new(Notify::new());
    // From a random place, so a new tunnel with the same key, such as after
    // the app restarts, doesn't reuse a connection the server still holds
    // open from before, which would ignore its first packet.
    let mut next_port = {
        use ring::rand::{SecureRandom, SystemRandom};
        let mut bytes = [0; 2];
        SystemRandom::new()
            .fill(&mut bytes)
            .expect("the operating system's random number generator works");
        FIRST_PORT + u16::from_be_bytes(bytes) % PORTS
    };
    let mut requests_open = true;

    loop {
        let now = smoltcp::time::Instant::now();
        iface.poll(now, &mut wire, &mut sockets);
        connections.retain_mut(|connection| {
            let open = connection.move_data(&mut sockets);
            if !open {
                sockets.remove(connection.socket);
            }
            open
        });
        // Moving data can leave packets to send, such as a window update.
        iface.poll(now, &mut wire, &mut sockets);
        for packet in wire.sent.drain(..) {
            let _ = outbound.try_send(packet);
        }
        if !requests_open && connections.is_empty() {
            return;
        }

        let delay = iface
            .poll_delay(now, &sockets)
            .map_or(Duration::from_secs(60), Duration::from);
        tokio::select! {
            request = requests.recv(), if requests_open => {
                let Some(Open { port, reply }) = request else {
                    requests_open = false;
                    continue;
                };
                let local_port = free_port(&mut next_port, &connections);
                let buffer = || tcp::SocketBuffer::new(vec![0; SOCKET_BUFFER]);
                let mut socket = tcp::Socket::new(buffer(), buffer());
                socket.set_keep_alive(Some(KEEP_ALIVE.into()));
                socket.set_timeout(Some(GONE_AFTER.into()));
                let handle = sockets.add(socket);
                let connected = sockets.get_mut::<tcp::Socket>(handle).connect(
                    iface.context(),
                    (IpAddress::Ipv4(gateway), port),
                    local_port,
                );
                if let Err(err) = connected {
                    sockets.remove(handle);
                    let _ = reply.send(Err(io::Error::other(format!(
                        "couldn't open a connection through the tunnel: {err}"
                    ))));
                    continue;
                }
                connections.push(Connection {
                    socket: handle,
                    local_port,
                    shared: Arc::new(Shared::new(Arc::clone(&wake))),
                    reply: Some(reply),
                    closing: false,
                });
            }
            packet = inbound.recv() => {
                let Some(packet) = packet else { return };
                wire.received.push_back(packet);
                while wire.received.len() < 64
                    && let Ok(packet) = inbound.try_recv()
                {
                    wire.received.push_back(packet);
                }
            }
            () = wake.notified() => {}
            () = tokio::time::sleep(delay) => {}
        }
    }
}

/// The next local port that no connection uses.
fn free_port(next: &mut u16, connections: &[Connection]) -> u16 {
    loop {
        let port = *next;
        *next = if *next == u16::MAX {
            FIRST_PORT
        } else {
            *next + 1
        };
        if !connections
            .iter()
            .any(|connection| connection.local_port == port)
        {
            return port;
        }
    }
}

impl Connection {
    /// Moves data between smoltcp and the HTTP client. Returns `false`
    /// once the connection is over.
    fn move_data(&mut self, sockets: &mut SocketSet<'_>) -> bool {
        let socket = sockets.get_mut::<tcp::Socket>(self.socket);
        if let Some(reply) = self.reply.take() {
            if reply.is_closed() {
                // The caller gave up waiting.
                socket.abort();
            } else if socket.may_send() {
                let _ = reply.send(Ok(TunnelStream {
                    shared: Arc::clone(&self.shared),
                }));
            } else if socket.state() == tcp::State::Closed {
                let _ = reply.send(Err(io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    "the server didn't take the connection through the tunnel",
                )));
                return false;
            } else {
                self.reply = Some(reply);
            }
        }

        let mut state = self.shared.lock();
        if state.dropped {
            // Nobody will read it, so take it and let the server finish.
            while socket.can_recv() && socket.recv(|data| (data.len(), ())).is_ok() {}
        } else {
            let mut received = false;
            while socket.can_recv() && state.received.len() < STREAM_BUFFER {
                let room = STREAM_BUFFER - state.received.len();
                let taken = socket.recv(|data| {
                    let taken = data.len().min(room);
                    state.received.extend_from_slice(&data[..taken]);
                    (taken, taken)
                });
                match taken {
                    Ok(0) | Err(_) => break,
                    Ok(_) => received = true,
                }
            }
            // The server sent everything it's going to.
            let opening = matches!(
                socket.state(),
                tcp::State::Closed | tcp::State::SynSent | tcp::State::SynReceived
            ) && self.reply.is_some();
            if !opening && !socket.may_recv() && !socket.can_recv() && !state.finished {
                state.finished = true;
                received = true;
            }
            if received {
                state.wake_reader();
            }
        }
        if !state.to_send.is_empty() && socket.can_send() {
            let sent = socket.send_slice(&state.to_send).unwrap_or(0);
            if sent > 0 {
                state.to_send.advance(sent);
                state.wake_writer();
            }
        }
        if state.done_writing && state.to_send.is_empty() && !self.closing {
            socket.close();
            self.closing = true;
        }
        if socket.state() == tcp::State::Closed && self.reply.is_none() {
            let reset = !state.finished;
            state.end(reset);
            return false;
        }
        true
    }
}

/// What the stack and the HTTP client share about one connection.
struct Shared {
    state: Mutex<StreamState>,
    /// Wakes the stack when the client reads, writes, or closes.
    wake: Arc<Notify>,
}

#[derive(Default)]
struct StreamState {
    /// From the server, waiting for the client to read it.
    received: BytesMut,
    /// From the client, waiting for room in smoltcp's buffer.
    to_send: BytesMut,
    /// The server finished sending, so reads end once `received` is empty.
    finished: bool,
    /// The connection is gone.
    ended: bool,
    /// It ended before the server finished sending, so reads fail.
    reset: bool,
    /// The client shut down its side, or dropped the stream.
    done_writing: bool,
    /// The client dropped the stream.
    dropped: bool,
    reader: Option<Waker>,
    writer: Option<Waker>,
}

impl Shared {
    fn new(wake: Arc<Notify>) -> Self {
        Self {
            state: Mutex::default(),
            wake,
        }
    }

    fn lock(&self) -> MutexGuard<'_, StreamState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl StreamState {
    fn wake_reader(&mut self) {
        if let Some(waker) = self.reader.take() {
            waker.wake();
        }
    }

    fn wake_writer(&mut self) {
        if let Some(waker) = self.writer.take() {
            waker.wake();
        }
    }

    fn end(&mut self, reset: bool) {
        self.ended = true;
        self.reset = reset;
        self.wake_reader();
        self.wake_writer();
    }
}

/// A TCP connection to the server through the tunnel.
pub struct TunnelStream {
    shared: Arc<Shared>,
}

impl AsyncRead for TunnelStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut state = self.shared.lock();
        if !state.received.is_empty() {
            let taken = state.received.len().min(buf.remaining());
            buf.put_slice(&state.received.split_to(taken));
            drop(state);
            self.shared.wake.notify_one();
            return Poll::Ready(Ok(()));
        }
        if state.finished {
            return Poll::Ready(Ok(()));
        }
        if state.ended {
            return Poll::Ready(if state.reset {
                Err(io::ErrorKind::ConnectionReset.into())
            } else {
                Ok(())
            });
        }
        state.reader = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl AsyncWrite for TunnelStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut state = self.shared.lock();
        if state.ended || state.done_writing {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        let room = STREAM_BUFFER.saturating_sub(state.to_send.len());
        if room == 0 {
            state.writer = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let taken = room.min(buf.len());
        state.to_send.extend_from_slice(&buf[..taken]);
        drop(state);
        self.shared.wake.notify_one();
        Poll::Ready(Ok(taken))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut state = self.shared.lock();
        if state.to_send.is_empty() {
            return Poll::Ready(Ok(()));
        }
        if state.ended {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        state.writer = Some(cx.waker().clone());
        Poll::Pending
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.shared.lock().done_writing = true;
        self.shared.wake.notify_one();
        Poll::Ready(Ok(()))
    }
}

impl Drop for TunnelStream {
    fn drop(&mut self) {
        let mut state = self.shared.lock();
        state.done_writing = true;
        state.dropped = true;
        drop(state);
        self.shared.wake.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn makes_keys_in_wireguards_format() {
        let (private, public) = new_key();
        assert_eq!(private.len(), 44);
        assert_eq!(public.len(), 44);
        assert_eq!(public_key(&private), Some(public));
        assert_eq!(public_key("not a key"), None);
    }

    #[test]
    fn keeps_the_private_key_out_of_logs() {
        let (private, _) = new_key();
        let setup = TunnelSetup {
            private_key: private.clone(),
            server_key: "server".into(),
            endpoint: "k3x9q2wm7f.ourhome.dedyn.io:51820".into(),
            address: Ipv4Addr::new(10, 42, 7, 3),
            gateway: Ipv4Addr::new(10, 42, 7, 1),
        };
        assert!(!format!("{setup:?}").contains(&private));
    }
}

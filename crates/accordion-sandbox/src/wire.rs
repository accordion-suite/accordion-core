//! What the server and the worker service say to each other over their
//! socket, and how open files go with it.
//!
//! The socket is a local sequenced-packet socket, so each message arrives
//! whole, with any open files it carries.

use std::ffi::OsString;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};

/// The longest message, which is far more than a job's arguments need.
pub const MAX_MESSAGE: usize = 64 * 1024;
/// The most open files one message carries: a job's input, output, and
/// error output.
pub const MAX_FILES: usize = 3;

/// From the server to the worker service.
#[derive(Debug, PartialEq, Eq)]
pub enum Request {
    /// The folder this connection's jobs work in, which the worker service
    /// empties when the connection closes.
    Folder(PathBuf),
    /// Runs a job, with its input, output, and error output open.
    Run(Run),
    /// Stops the job that's running, and everything it started.
    Stop,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Run {
    pub program: PathBuf,
    pub args: Vec<OsString>,
    pub env: Vec<(OsString, OsString)>,
    pub read: Vec<PathBuf>,
    pub write: Vec<PathBuf>,
    pub sockets: Option<PathBuf>,
    pub memory: u64,
    pub cpu: u64,
}

/// From the worker service to the server.
#[derive(Debug, PartialEq, Eq)]
pub enum Reply {
    /// The user ID this connection's jobs run as.
    Hello { uid: u32 },
    /// The job finished, with this wait status, and nothing it started is
    /// running any more.
    Exited { status: i32 },
    /// The worker service didn't run the job, and why.
    Refused(String),
}

const FOLDER: u8 = 1;
const RUN: u8 = 2;
const STOP: u8 = 3;
const HELLO: u8 = 10;
const EXITED: u8 = 11;
const REFUSED: u8 = 12;

impl Request {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Encoder::default();
        match self {
            Self::Folder(path) => {
                out.tag(FOLDER);
                out.path(path);
            }
            Self::Run(run) => {
                out.tag(RUN);
                out.path(&run.program);
                out.count(run.args.len());
                for arg in &run.args {
                    out.bytes(arg.as_bytes());
                }
                out.count(run.env.len());
                for (name, value) in &run.env {
                    out.bytes(name.as_bytes());
                    out.bytes(value.as_bytes());
                }
                for paths in [&run.read, &run.write] {
                    out.count(paths.len());
                    for path in paths {
                        out.path(path);
                    }
                }
                out.count(usize::from(run.sockets.is_some()));
                if let Some(path) = &run.sockets {
                    out.path(path);
                }
                out.number(run.memory);
                out.number(run.cpu);
            }
            Self::Stop => out.tag(STOP),
        }
        out.0
    }

    pub fn decode(bytes: &[u8]) -> io::Result<Self> {
        let mut input = Decoder(bytes);
        let request = match input.tag()? {
            FOLDER => Self::Folder(input.path()?),
            RUN => {
                let program = input.path()?;
                let args = (0..input.count()?)
                    .map(|_| input.os_string())
                    .collect::<io::Result<_>>()?;
                let env = (0..input.count()?)
                    .map(|_| Ok((input.os_string()?, input.os_string()?)))
                    .collect::<io::Result<_>>()?;
                let read = (0..input.count()?)
                    .map(|_| input.path())
                    .collect::<io::Result<_>>()?;
                let write = (0..input.count()?)
                    .map(|_| input.path())
                    .collect::<io::Result<_>>()?;
                let sockets = match input.count()? {
                    0 => None,
                    _ => Some(input.path()?),
                };
                Self::Run(Run {
                    program,
                    args,
                    env,
                    read,
                    write,
                    sockets,
                    memory: input.number()?,
                    cpu: input.number()?,
                })
            }
            STOP => Self::Stop,
            _ => return Err(invalid()),
        };
        input.end()?;
        Ok(request)
    }
}

impl Reply {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Encoder::default();
        match self {
            Self::Hello { uid } => {
                out.tag(HELLO);
                out.number(u64::from(*uid));
            }
            Self::Exited { status } => {
                out.tag(EXITED);
                out.number(*status as u32 as u64);
            }
            Self::Refused(message) => {
                out.tag(REFUSED);
                out.bytes(message.as_bytes());
            }
        }
        out.0
    }

    pub fn decode(bytes: &[u8]) -> io::Result<Self> {
        let mut input = Decoder(bytes);
        let reply = match input.tag()? {
            HELLO => Self::Hello {
                uid: u32::try_from(input.number()?).map_err(|_| invalid())?,
            },
            EXITED => Self::Exited {
                status: u32::try_from(input.number()?).map_err(|_| invalid())? as i32,
            },
            REFUSED => Self::Refused(String::from_utf8_lossy(&input.bytes()?).into_owned()),
            _ => return Err(invalid()),
        };
        input.end()?;
        Ok(reply)
    }
}

#[derive(Default)]
struct Encoder(Vec<u8>);

impl Encoder {
    fn tag(&mut self, tag: u8) {
        self.0.push(tag);
    }
    fn number(&mut self, number: u64) {
        self.0.extend_from_slice(&number.to_le_bytes());
    }
    fn count(&mut self, count: usize) {
        self.number(count as u64);
    }
    fn bytes(&mut self, bytes: &[u8]) {
        self.count(bytes.len());
        self.0.extend_from_slice(bytes);
    }
    fn path(&mut self, path: &Path) {
        self.bytes(path.as_os_str().as_bytes());
    }
}

struct Decoder<'a>(&'a [u8]);

impl Decoder<'_> {
    fn take(&mut self, len: usize) -> io::Result<&[u8]> {
        if self.0.len() < len {
            return Err(invalid());
        }
        let (taken, rest) = self.0.split_at(len);
        self.0 = rest;
        Ok(taken)
    }
    fn tag(&mut self) -> io::Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn number(&mut self) -> io::Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn count(&mut self) -> io::Result<usize> {
        let count = usize::try_from(self.number()?).map_err(|_| invalid())?;
        // Each item takes at least eight bytes, so a bigger count is a lie.
        if count > self.0.len() / 8 + 1 {
            return Err(invalid());
        }
        Ok(count)
    }
    fn bytes(&mut self) -> io::Result<Vec<u8>> {
        let len = usize::try_from(self.number()?).map_err(|_| invalid())?;
        Ok(self.take(len)?.to_vec())
    }
    fn os_string(&mut self) -> io::Result<OsString> {
        Ok(OsString::from_vec(self.bytes()?))
    }
    fn path(&mut self) -> io::Result<PathBuf> {
        Ok(PathBuf::from(self.os_string()?))
    }
    fn end(&self) -> io::Result<()> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(invalid())
        }
    }
}

fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "the message isn't one the worker service knows",
    )
}

/// Sends one message with `files`, without waiting if the socket is
/// non-blocking and full.
pub fn send(socket: RawFd, message: &[u8], files: &[RawFd]) -> io::Result<()> {
    let mut iov = libc::iovec {
        iov_base: message.as_ptr() as *mut libc::c_void,
        iov_len: message.len(),
    };
    let space = unsafe { libc::CMSG_SPACE(size_of_val(files) as u32) } as usize;
    let mut control = vec![0u8; space];
    let mut header: libc::msghdr = unsafe { std::mem::zeroed() };
    header.msg_iov = &mut iov;
    header.msg_iovlen = 1;
    if !files.is_empty() {
        header.msg_control = control.as_mut_ptr().cast();
        header.msg_controllen = space as _;
        unsafe {
            let cmsg = libc::CMSG_FIRSTHDR(&header);
            (*cmsg).cmsg_level = libc::SOL_SOCKET;
            (*cmsg).cmsg_type = libc::SCM_RIGHTS;
            (*cmsg).cmsg_len = libc::CMSG_LEN(size_of_val(files) as u32) as _;
            std::ptr::copy_nonoverlapping(
                files.as_ptr(),
                libc::CMSG_DATA(cmsg).cast::<RawFd>(),
                files.len(),
            );
        }
    }
    let sent = unsafe { libc::sendmsg(socket, &header, libc::MSG_NOSIGNAL) };
    if sent < 0 {
        return Err(io::Error::last_os_error());
    }
    if sent as usize != message.len() {
        return Err(io::Error::other("the message was cut short"));
    }
    Ok(())
}

/// Receives one message and the files that came with it. `None` means the
/// other side closed the connection.
pub fn receive(socket: RawFd) -> io::Result<Option<(Vec<u8>, Vec<OwnedFd>)>> {
    let mut buffer = vec![0u8; MAX_MESSAGE];
    let mut iov = libc::iovec {
        iov_base: buffer.as_mut_ptr().cast(),
        iov_len: buffer.len(),
    };
    let space = unsafe { libc::CMSG_SPACE((MAX_FILES * size_of::<RawFd>()) as u32) } as usize;
    let mut control = vec![0u8; space];
    let mut header: libc::msghdr = unsafe { std::mem::zeroed() };
    header.msg_iov = &mut iov;
    header.msg_iovlen = 1;
    header.msg_control = control.as_mut_ptr().cast();
    header.msg_controllen = space as _;
    let received = unsafe { libc::recvmsg(socket, &mut header, libc::MSG_CMSG_CLOEXEC) };
    if received < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut files = Vec::new();
    unsafe {
        let mut cmsg = libc::CMSG_FIRSTHDR(&header);
        while !cmsg.is_null() {
            if (*cmsg).cmsg_level == libc::SOL_SOCKET && (*cmsg).cmsg_type == libc::SCM_RIGHTS {
                let data = libc::CMSG_DATA(cmsg).cast::<RawFd>();
                let len =
                    ((*cmsg).cmsg_len as usize - libc::CMSG_LEN(0) as usize) / size_of::<RawFd>();
                for index in 0..len {
                    files.push(OwnedFd::from_raw_fd(data.add(index).read_unaligned()));
                }
            }
            cmsg = libc::CMSG_NXTHDR(&header, cmsg);
        }
    }
    if header.msg_flags & (libc::MSG_TRUNC | libc::MSG_CTRUNC) != 0 {
        return Err(io::Error::other("the message was too long"));
    }
    if received == 0 && files.is_empty() {
        return Ok(None);
    }
    buffer.truncate(received as usize);
    Ok(Some((buffer, files)))
}

/// A new local sequenced-packet socket.
pub fn socket() -> io::Result<OwnedFd> {
    let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_SEQPACKET | libc::SOCK_CLOEXEC, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// The socket address for `path`.
pub fn address(path: &Path) -> io::Result<(libc::sockaddr_un, libc::socklen_t)> {
    let mut address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    address.sun_family = libc::AF_UNIX as libc::sa_family_t;
    let bytes = path.as_os_str().as_bytes();
    if bytes.len() >= address.sun_path.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "the worker service's socket path is too long",
        ));
    }
    for (to, &from) in address.sun_path.iter_mut().zip(bytes) {
        *to = from as libc::c_char;
    }
    let len = std::mem::offset_of!(libc::sockaddr_un, sun_path) + bytes.len() + 1;
    Ok((address, len as libc::socklen_t))
}

/// The user ID of the process at the other end of `socket`.
pub fn peer_uid(socket: &impl AsRawFd) -> io::Result<u32> {
    let mut credentials: libc::ucred = unsafe { std::mem::zeroed() };
    let mut len = size_of::<libc::ucred>() as libc::socklen_t;
    let done = unsafe {
        libc::getsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            (&raw mut credentials).cast(),
            &mut len,
        )
    };
    if done != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(credentials.uid)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn messages_come_back_as_they_went() {
        let run = Request::Run(Run {
            program: "/usr/bin/vips".into(),
            args: vec!["thumbnail_source".into(), "[descriptor=0]".into()],
            env: vec![("VIPS_BLOCK_UNTRUSTED".into(), "1".into())],
            read: vec!["/usr".into()],
            write: vec!["/var/lib/accordion/jobs/a".into()],
            sockets: Some("/tmp".into()),
            memory: 1 << 30,
            cpu: 20,
        });
        for request in [run, Request::Folder("/x".into()), Request::Stop] {
            assert_eq!(Request::decode(&request.encode()).unwrap(), request);
        }
        for reply in [
            Reply::Hello { uid: 2_000_000_001 },
            Reply::Exited { status: 9 },
            Reply::Refused("no".into()),
        ] {
            assert_eq!(Reply::decode(&reply.encode()).unwrap(), reply);
        }
        assert!(Request::decode(&[RUN, 1]).is_err());
    }

    #[test]
    fn open_files_go_with_a_message() {
        let mut pair = [0; 2];
        assert_eq!(
            unsafe {
                libc::socketpair(
                    libc::AF_UNIX,
                    libc::SOCK_SEQPACKET | libc::SOCK_CLOEXEC,
                    0,
                    pair.as_mut_ptr(),
                )
            },
            0
        );
        let (one, two) = unsafe { (OwnedFd::from_raw_fd(pair[0]), OwnedFd::from_raw_fd(pair[1])) };
        let file = std::fs::File::open("/dev/null").unwrap();
        send(one.as_raw_fd(), b"hello", &[file.as_raw_fd()]).unwrap();
        let (message, files) = receive(two.as_raw_fd()).unwrap().unwrap();
        assert_eq!(message, b"hello");
        assert_eq!(files.len(), 1);
        drop(one);
        assert!(receive(two.as_raw_fd()).unwrap().is_none());
    }
}

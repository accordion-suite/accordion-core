//! The server's side: starts a job, feeds it the file, and stops it if it
//! runs too long or writes too much.
//!
//! Without the worker service, the server starts the launcher itself, as
//! its own user, so the launcher requires Landlock. With it (decision 29),
//! the server sends each job to the worker service, which runs it as a
//! user of its own, in a folder of its own.

use std::ffi::OsString;
use std::fs::File;
use std::io;
use std::os::fd::{AsRawFd, OwnedFd};
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::net::unix::pipe;
use tokio::process::Command;

use crate::acl;
use crate::wire::{self, Reply, Request};

/// The folders every worker can read: programs, their libraries, the list
/// of where libraries are, and random numbers. Ones a system doesn't have
/// are left out.
///
/// Without `/dev/urandom`, ffmpeg 6 seeds its random numbers from a CPU
/// counter instead, which ARM doesn't let programs read, so it stops with
/// SIGILL. It does that to name a WebM file's tracks and to draw text.
const SYSTEM: [&str; 6] = [
    "/usr",
    "/lib",
    "/lib64",
    "/etc/ld.so.cache",
    "/etc/ld.so.conf",
    "/dev/urandom",
];

/// How much of a worker's error output the server keeps, for its log.
const MESSAGE_LIMIT: u64 = 4096;

/// How long the worker service gets to stop a job, or to tidy up after a
/// connection.
const TIDY_TIME: Duration = Duration::from_secs(30);

/// Starts workers through a launcher, the server's own program, which
/// answers `sandbox`, or through the worker service.
#[derive(Clone, Debug)]
pub struct Sandbox {
    launcher: Arc<Path>,
    service: Option<Arc<Service>>,
}

#[derive(Debug)]
struct Service {
    socket: PathBuf,
    /// Where jobs' folders go: `jobs/` in the data folder.
    jobs: PathBuf,
    /// The data folder, which the worker service can't see beyond `jobs/`.
    data: PathBuf,
}

/// Where the worker service listens, and where its jobs' folders go:
/// `jobs/` in the data folder.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Workers {
    pub socket: PathBuf,
    pub jobs: PathBuf,
}

/// What a worker may use.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Writable memory, in bytes: the heap and other memory the worker
    /// can write to, not libraries or space it reserves without using.
    pub memory: u64,
    /// CPU time, across all the worker's threads.
    pub cpu: Duration,
    /// Time from start to finish, including waiting.
    pub time: Duration,
    /// The most the worker may write to its standard output, in bytes.
    pub output: u64,
}

/// One run of a worker program.
pub struct Job<'a> {
    /// The program's full path.
    pub program: &'a Path,
    pub args: &'a [OsString],
    /// The program's whole environment.
    pub env: &'a [(&'a str, &'a str)],
    /// What the program reads on its standard input, such as a stored file.
    pub input: File,
    /// Folders and files the program may read besides the system's own,
    /// such as a stored file it reads by name. With the worker service, a
    /// file in the data folder is linked into the job's folder, and an
    /// argument that's the file's path becomes the link's.
    pub read: &'a [PathBuf],
    /// The folder the program may write in. It can't run what it writes.
    pub folder: Option<&'a Folder>,
    /// A folder the program may make a local socket in, to listen on. It
    /// still can't connect to any socket, or read the folder's files.
    pub sockets: Option<&'a Path>,
    pub limits: Limits,
}

#[derive(Debug, thiserror::Error)]
pub enum RunError {
    #[error("the worker couldn't start: {0}")]
    Start(#[source] std::io::Error),
    #[error("the worker took longer than {0:?}, so it was stopped")]
    TimedOut(Duration),
    #[error("the worker wrote more than {0} bytes, so it was stopped")]
    TooLarge(u64),
    #[error("the worker stopped with {status}: {message}")]
    Failed { status: ExitStatus, message: String },
}

/// A folder for one piece of work, which its jobs can write in. It's
/// removed, with everything in it, when it's dropped.
///
/// With the worker service, it's a folder in `jobs/`, and its jobs run as
/// one user of their own, which only they and the server can use.
pub struct Folder {
    path: PathBuf,
    session: Option<Arc<Session>>,
    /// Stored files linked into the folder, whose ACLs name its user.
    linked: Mutex<Vec<PathBuf>>,
    /// Without the worker service, the folder removes itself.
    _local: Option<tempfile::TempDir>,
}

impl Sandbox {
    /// `launcher` is the program that answers `sandbox`, usually
    /// `std::env::current_exe()`.
    pub fn new(launcher: impl Into<PathBuf>) -> Self {
        Self {
            launcher: Arc::from(launcher.into()),
            service: None,
        }
    }

    /// Sends jobs to the worker service listening on `socket`, with their
    /// folders in `jobs`, which is `jobs/` in the data folder.
    pub fn through_service(mut self, socket: impl Into<PathBuf>, jobs: impl Into<PathBuf>) -> Self {
        let jobs = jobs.into();
        let data = jobs.parent().map(Path::to_owned).unwrap_or_default();
        self.service = Some(Arc::new(Service {
            socket: socket.into(),
            jobs,
            data,
        }));
        self
    }

    /// Sends jobs to the worker service, if there's one.
    pub fn with_workers(self, workers: Option<&Workers>) -> Self {
        match workers {
            Some(workers) => self.through_service(&workers.socket, &workers.jobs),
            None => self,
        }
    }

    /// Whether jobs go to the worker service.
    pub fn uses_service(&self) -> bool {
        self.service.is_some()
    }

    /// Whether workers can run here.
    pub fn check(&self) -> Result<(), crate::Unavailable> {
        let Some(service) = &self.service else {
            return crate::check();
        };
        let reach = || -> io::Result<()> {
            let fd = connect(&service.socket)?;
            match receive_blocking(&fd)? {
                Reply::Hello { .. } => Ok(()),
                Reply::Refused(message) => Err(io::Error::other(message)),
                Reply::Exited { .. } => Err(io::Error::other("it answered out of turn")),
            }
        };
        reach().map_err(|err| crate::Unavailable::NoService(err.to_string()))?;
        let dir = tempfile::Builder::new()
            .prefix("check-")
            .tempdir_in(&service.jobs)
            .map_err(|err| crate::Unavailable::NoService(err.to_string()))?;
        match acl::inherit(dir.path(), &[unsafe { libc::getuid() }]) {
            Ok(()) => Ok(()),
            Err(err) if err.raw_os_error() == Some(libc::EOPNOTSUPP) => {
                Err(crate::Unavailable::NoAcls)
            }
            Err(err) => Err(crate::Unavailable::NoService(err.to_string())),
        }
    }

    /// A new folder for one piece of work. Without the worker service, it's
    /// made in `local`.
    pub async fn folder(&self, local: &Path) -> io::Result<Folder> {
        let Some(service) = &self.service else {
            let dir = tempfile::tempdir_in(local)?;
            return Ok(Folder {
                path: dir.path().to_owned(),
                session: None,
                linked: Mutex::new(Vec::new()),
                _local: Some(dir),
            });
        };
        let session = Arc::new(Session::connect(&service.socket).await?);
        let path = tempfile::Builder::new()
            .prefix("job-")
            .tempdir_in(&service.jobs)?
            .keep();
        let folder = Folder {
            path,
            session: Some(Arc::clone(&session)),
            linked: Mutex::new(Vec::new()),
            _local: None,
        };
        let me = unsafe { libc::getuid() };
        acl::grant(
            &folder.path,
            session.uid,
            acl::READ | acl::WRITE | acl::SEARCH,
        )?;
        acl::inherit(&folder.path, &[session.uid, me])?;
        session
            .send(&Request::Folder(folder.path.clone()).encode(), &[])
            .await?;
        Ok(folder)
    }

    /// Runs `job`, and returns what it wrote to its standard output.
    pub async fn run(&self, job: Job<'_>) -> Result<Vec<u8>, RunError> {
        self.run_with_messages(job).await.map(|(output, _)| output)
    }

    /// Runs `job`, and returns what it wrote to its standard output, and to
    /// its error output, for programs that explain a problem there but still
    /// finish as if they'd succeeded.
    pub async fn run_with_messages(&self, job: Job<'_>) -> Result<(Vec<u8>, String), RunError> {
        self.start(job, false).await
    }

    /// Runs `job`, and returns what it wrote to its standard output, up to
    /// the output limit. A worker that writes more is stopped there rather
    /// than failing, for output that's useful in part, such as a long
    /// document's text.
    pub async fn run_up_to(&self, job: Job<'_>) -> Result<Vec<u8>, RunError> {
        self.start(job, true).await.map(|(output, _)| output)
    }

    async fn start(&self, job: Job<'_>, cut_off: bool) -> Result<(Vec<u8>, String), RunError> {
        match &self.service {
            None => self.start_here(job, cut_off).await,
            Some(service) => {
                let made;
                let folder = match job.folder {
                    Some(folder) => folder,
                    None => {
                        made = self.folder(Path::new("/")).await.map_err(RunError::Start)?;
                        &made
                    }
                };
                start_in_service(service, folder, job, cut_off).await
            }
        }
    }

    /// Starts the launcher as the server's own user.
    async fn start_here(&self, job: Job<'_>, cut_off: bool) -> Result<(Vec<u8>, String), RunError> {
        let limits = job.limits;
        let mut command = Command::new(&*self.launcher);
        command
            .arg("sandbox")
            .arg("--memory")
            .arg(limits.memory.to_string())
            .arg("--cpu")
            .arg(limits.cpu.as_secs().max(1).to_string());
        for path in readable(job.read) {
            command.arg("--read").arg(path);
        }
        if let Some(folder) = job.folder {
            command.arg("--write").arg(&folder.path);
        }
        if let Some(folder) = job.sockets {
            command.arg("--sockets").arg(folder);
        }
        command
            .arg("--")
            .arg(job.program)
            .args(job.args)
            .env_clear()
            .envs(job.env.iter().copied())
            .stdin(Stdio::from(job.input))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0)
            .kill_on_drop(true);
        let mut child = command.spawn().map_err(RunError::Start)?;
        let group = child.id();
        let stdout = child.stdout.take().expect("stdout is piped");
        let stderr = child.stderr.take().expect("stderr is piped");

        let work = async {
            let collected = collect(stdout, stderr, limits, cut_off, async || stop(group)).await?;
            let status = child.wait().await.map_err(RunError::Start)?;
            collected.finish(status)
        };
        let result = match tokio::time::timeout(limits.time, work).await {
            Ok(result) => result,
            Err(_) => Err(RunError::TimedOut(limits.time)),
        };
        if result.is_err() {
            stop(group);
        }
        result
    }
}

/// The system's folders, then `read`, leaving out ones that don't exist.
fn readable(read: &[PathBuf]) -> impl Iterator<Item = &Path> {
    SYSTEM
        .iter()
        .map(Path::new)
        .chain(read.iter().map(PathBuf::as_path))
        .filter(|path| path.exists())
}

/// Sends `job` to the worker service, to run in `folder`.
async fn start_in_service(
    service: &Service,
    folder: &Folder,
    job: Job<'_>,
    cut_off: bool,
) -> Result<(Vec<u8>, String), RunError> {
    let session = folder.session.as_ref().ok_or_else(|| {
        RunError::Start(io::Error::other("the folder isn't the worker service's"))
    })?;
    let _turn = session.turn.lock().await;
    let limits = job.limits;

    // Files in the data folder go into the job's folder, the only part of
    // the data folder its user can reach.
    let mut args = job.args.to_vec();
    let mut read = Vec::new();
    for path in readable(job.read) {
        if !path.starts_with(&service.data) || path.starts_with(&folder.path) {
            read.push(path.to_owned());
            continue;
        }
        let linked = folder.hand_over(path, None).map_err(RunError::Start)?;
        for arg in &mut args {
            if Path::new(arg.as_os_str()) == path {
                *arg = linked.clone().into();
            }
        }
        read.push(linked);
    }
    let run = wire::Run {
        program: job.program.to_owned(),
        args,
        env: job
            .env
            .iter()
            .map(|(name, value)| ((*name).into(), (*value).into()))
            .collect(),
        read,
        write: vec![folder.path.clone()],
        sockets: job.sockets.map(Path::to_owned),
        memory: limits.memory,
        cpu: limits.cpu.as_secs().max(1),
    };

    let (stdout, stdout_end) = std::io::pipe().map_err(RunError::Start)?;
    let (stderr, stderr_end) = std::io::pipe().map_err(RunError::Start)?;
    session
        .send(
            &Request::Run(run).encode(),
            &[
                job.input.as_raw_fd(),
                stdout_end.as_raw_fd(),
                stderr_end.as_raw_fd(),
            ],
        )
        .await
        .map_err(RunError::Start)?;
    // Only the worker has them open now, so reading ends when it does.
    drop((job.input, stdout_end, stderr_end));
    let stdout = pipe::Receiver::from_owned_fd(OwnedFd::from(stdout)).map_err(RunError::Start)?;
    let stderr = pipe::Receiver::from_owned_fd(OwnedFd::from(stderr)).map_err(RunError::Start)?;

    let ask_to_stop = async || {
        let _ = session.send(&Request::Stop.encode(), &[]).await;
    };
    let work = async {
        // The worker service replies once the job and everything it started
        // have stopped, which is after its output ends.
        let (collected, reply) = tokio::join!(
            collect(stdout, stderr, limits, cut_off, ask_to_stop),
            session.receive()
        );
        let collected = collected?;
        let status = match reply.map_err(RunError::Start)? {
            Reply::Exited { status } => ExitStatus::from_raw(status),
            Reply::Refused(message) => return Err(RunError::Start(io::Error::other(message))),
            Reply::Hello { .. } => {
                return Err(RunError::Start(io::Error::other(
                    "the worker service answered out of turn",
                )));
            }
        };
        collected.finish(status)
    };
    let result = tokio::select! {
        result = work => result,
        () = tokio::time::sleep(limits.time) => Err(RunError::TimedOut(limits.time)),
    };
    // A job that took too long is still running. The worker service stops
    // it and says so, which keeps the connection in step for the folder's
    // next job.
    if matches!(result, Err(RunError::TimedOut(_))) {
        session.stop().await;
    }
    result
}

/// What a worker wrote.
struct Collected {
    output: Vec<u8>,
    message: Vec<u8>,
    cut: bool,
}

/// Reads a worker's output and error output, up to their limits. When the
/// output goes past its limit and `cut_off` allows it, `stop` stops the
/// worker, which is waiting to write the rest and would never finish.
async fn collect(
    stdout: impl AsyncRead + Unpin,
    stderr: impl AsyncRead + Unpin,
    limits: Limits,
    cut_off: bool,
    stop: impl AsyncFnOnce(),
) -> Result<Collected, RunError> {
    let read_output = async {
        let output = read_up_to(stdout, limits.output + 1).await;
        if cut_off
            && output
                .as_ref()
                .is_ok_and(|output| output.len() as u64 > limits.output)
        {
            stop().await;
        }
        output
    };
    let (output, message) = tokio::join!(read_output, read_up_to(stderr, MESSAGE_LIMIT));
    let mut output = output.map_err(RunError::Start)?;
    let cut = output.len() as u64 > limits.output;
    if cut && !cut_off {
        return Err(RunError::TooLarge(limits.output));
    }
    output.truncate(limits.output as usize);
    Ok(Collected {
        output,
        message: message.unwrap_or_default(),
        cut,
    })
}

impl Collected {
    fn finish(self, status: ExitStatus) -> Result<(Vec<u8>, String), RunError> {
        // A worker cut off at its limit was stopped on purpose.
        if self.cut {
            return Ok((self.output, String::new()));
        }
        let message = String::from_utf8_lossy(&self.message).trim().to_owned();
        if status.success() {
            return Ok((self.output, message));
        }
        Err(RunError::Failed { status, message })
    }
}

/// Reads at most `limit` bytes, then stops reading.
async fn read_up_to(reader: impl AsyncRead + Unpin, limit: u64) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader.take(limit).read_to_end(&mut bytes).await?;
    Ok(bytes)
}

/// Stops the worker's whole process group, including anything it started.
fn stop(group: Option<u32>) {
    if let Some(group) = group.and_then(|group| libc::pid_t::try_from(group).ok()) {
        // The group may be gone already, which is fine.
        unsafe { libc::kill(-group, libc::SIGKILL) };
    }
}

impl Folder {
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Puts `file` in the folder as `name`, for a program that reads it by
    /// name, and returns its path there. Only this folder's jobs can read it.
    pub fn link(&self, file: &Path, name: &str) -> io::Result<PathBuf> {
        self.hand_over(file, Some(name))
    }

    /// Puts `file` in the folder, under `name` or its own name. A hard link
    /// costs nothing, even for a large video. A file the server can't give
    /// the job's user, such as one an earlier job made, is copied.
    fn hand_over(&self, file: &Path, name: Option<&str>) -> io::Result<PathBuf> {
        let name = match name {
            Some(name) => name.into(),
            None => file
                .file_name()
                .ok_or_else(|| io::Error::other("the file has no name"))?
                .to_owned(),
        };
        let mut target = self.path.join(&name);
        let mut count = 1;
        while target.symlink_metadata().is_ok() {
            count += 1;
            let mut numbered = OsString::from(format!("{count}-"));
            numbered.push(&name);
            target = self.path.join(numbered);
        }
        let Some(session) = &self.session else {
            // Without the worker service, Landlock lets the job read the
            // file itself, so a symbolic link does when a hard one can't.
            if std::fs::hard_link(file, &target).is_err() {
                std::os::unix::fs::symlink(file, &target)?;
            }
            return Ok(target);
        };
        let linked = std::fs::hard_link(file, &target)
            .and_then(|()| acl::grant(&target, session.uid, acl::READ));
        match linked {
            Ok(()) => {
                self.linked
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .push(file.to_owned());
            }
            Err(_) => {
                let _ = std::fs::remove_file(&target);
                // The copy is the server's, and the folder gives its user
                // access to everything made in it.
                std::fs::copy(file, &target)?;
            }
        }
        Ok(target)
    }
}

impl Drop for Folder {
    fn drop(&mut self) {
        let Some(session) = self.session.take() else {
            return;
        };
        let path = std::mem::take(&mut self.path);
        let linked = std::mem::take(
            &mut *self
                .linked
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        );
        let uid = session.uid;
        let closer = session.closer.try_clone();
        drop(session);
        // Waiting for the worker service to tidy up isn't for an async
        // task, and takes milliseconds, so a thread does it.
        std::thread::spawn(move || {
            if let Ok(closer) = closer {
                close_blocking(&closer);
            }
            for file in linked {
                if let Err(err) = acl::revoke(&file, uid) {
                    warn(&format!(
                        "couldn't take back a job's access to {}: {err}",
                        file.display()
                    ));
                }
            }
            let _ = std::fs::remove_dir_all(&path);
        });
    }
}

/// The sandbox crate doesn't log through the server's logger, so it
/// writes to the error output, which ends up in the same journal.
fn warn(message: &str) {
    eprintln!("sandbox: {message}");
}

/// One connection to the worker service: one user ID, one job at a time.
struct Session {
    fd: AsyncFd<OwnedFd>,
    /// The same socket, for closing it from a thread.
    closer: OwnedFd,
    uid: u32,
    turn: tokio::sync::Mutex<()>,
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session").field("uid", &self.uid).finish()
    }
}

impl Session {
    async fn connect(socket: &Path) -> io::Result<Self> {
        let socket = socket.to_owned();
        // The worker service answers once it has a user ID free, which can
        // take a moment when every one is in use.
        let (fd, uid) = tokio::task::spawn_blocking(move || {
            let fd = connect(&socket)?;
            match receive_blocking(&fd)? {
                Reply::Hello { uid } => Ok((fd, uid)),
                Reply::Refused(message) => Err(io::Error::other(message)),
                Reply::Exited { .. } => {
                    Err(io::Error::other("the worker service answered out of turn"))
                }
            }
        })
        .await
        .map_err(io::Error::other)??;
        let closer = fd.try_clone()?;
        set_nonblocking(&fd)?;
        Ok(Self {
            // SAFETY: the OwnedFd is a valid socket, and AsyncFd owns it
            // until it's dropped. `closer` is a separate copy.
            fd: unsafe { AsyncFd::register(fd)? },
            closer,
            uid,
            turn: tokio::sync::Mutex::new(()),
        })
    }

    async fn send(&self, message: &[u8], files: &[std::os::fd::RawFd]) -> io::Result<()> {
        loop {
            let mut guard = self.fd.writable().await?;
            match guard.try_io(|fd| wire::send(fd.as_raw_fd(), message, files)) {
                Ok(result) => return result,
                Err(_would_block) => continue,
            }
        }
    }

    async fn receive(&self) -> io::Result<Reply> {
        loop {
            let mut guard = self.fd.readable().await?;
            match guard.try_io(|fd| wire::receive(fd.as_raw_fd())) {
                Ok(Ok(Some((message, _)))) => return Reply::decode(&message),
                Ok(Ok(None)) => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "the worker service closed the connection",
                    ));
                }
                Ok(Err(err)) => return Err(err),
                Err(_would_block) => continue,
            }
        }
    }

    /// Asks the worker service to stop the running job, and waits until it
    /// has.
    async fn stop(&self) {
        if self.send(&Request::Stop.encode(), &[]).await.is_err() {
            return;
        }
        let _ = tokio::time::timeout(TIDY_TIME, async {
            while let Ok(reply) = self.receive().await {
                if matches!(reply, Reply::Exited { .. }) {
                    break;
                }
            }
        })
        .await;
    }
}

fn connect(socket: &Path) -> io::Result<OwnedFd> {
    let fd = wire::socket()?;
    let (address, len) = wire::address(socket)?;
    let done = unsafe { libc::connect(fd.as_raw_fd(), (&raw const address).cast(), len) };
    if done != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(fd)
}

fn receive_blocking(fd: &OwnedFd) -> io::Result<Reply> {
    match wire::receive(fd.as_raw_fd())? {
        Some((message, _)) => Reply::decode(&message),
        None => Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "the worker service closed the connection",
        )),
    }
}

fn set_nonblocking(fd: &OwnedFd) -> io::Result<()> {
    let flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFL) };
    if flags < 0
        || unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Tells the worker service the server is done with a connection, and waits
/// until it has stopped the connection's jobs and emptied its folder.
fn close_blocking(fd: &OwnedFd) {
    unsafe { libc::shutdown(fd.as_raw_fd(), libc::SHUT_WR) };
    let flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFL) };
    if flags >= 0 {
        unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFL, flags & !libc::O_NONBLOCK) };
    }
    let timeout = libc::timeval {
        tv_sec: TIDY_TIME.as_secs() as libc::time_t,
        tv_usec: 0,
    };
    unsafe {
        libc::setsockopt(
            fd.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_RCVTIMEO,
            (&raw const timeout).cast(),
            size_of::<libc::timeval>() as libc::socklen_t,
        )
    };
    // Replies still on their way, then the end.
    while let Ok(Some(_)) = wire::receive(fd.as_raw_fd()) {}
}

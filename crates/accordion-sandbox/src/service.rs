//! The worker service (decision 29): a small process of its own that runs
//! jobs for the server, each connection's jobs as a user ID of their own.
//!
//! It can't see the data folder, only its `jobs/` folder, and has no
//! network. It starts with two privileges, to switch to each job's user,
//! and uses nothing else. For each connection it:
//!
//! 1. Takes a user ID from its range, and tells the server which.
//! 2. Runs each job through the launcher, as that user, with the input,
//!    output, and error output the server sent.
//! 3. When a job ends, stops everything else running as that user, then
//!    gives the server access to what the job wrote, before saying so. So
//!    nothing changes a result after the server checks it.
//! 4. When the connection closes, stops everything again, empties the
//!    connection's folder as that user, and puts the user ID back.
//!
//! In development and tests, it runs jobs as its own user instead, and the
//! launcher requires Landlock, as it does without the worker service.

use std::collections::BTreeSet;
use std::ffi::OsString;
use std::io;
use std::ops::Range;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::fs::MetadataExt;
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::acl;
use crate::wire::{self, Reply, Request};

/// The user IDs jobs run as, which nothing else on a system uses: above
/// the ones for people and systemd's own, and below 2³¹, which some
/// programs can't handle.
pub const JOB_UIDS: Range<u32> = 2_000_000_000..2_000_001_024;

/// How the worker service runs.
pub struct Config {
    /// Where the server connects, already listening.
    pub listener: OwnedFd,
    /// The folder jobs work in, which only the server can write.
    pub jobs: PathBuf,
    /// The user IDs jobs run as. `None` runs them as the worker service's
    /// own user, for development and tests.
    pub uids: Option<Range<u32>>,
    /// The program that answers `sandbox`, usually the worker service's own.
    pub launcher: PathBuf,
    /// The folders jobs' programs can come from, such as `/usr`, and the
    /// launcher itself.
    pub programs: Vec<PathBuf>,
}

struct Shared {
    config: Config,
    /// The only user that may connect: the server's, which owns `jobs/`.
    server: u32,
    free: Mutex<BTreeSet<u32>>,
    returned: Condvar,
}

/// Listens on the socket at `path`, which anyone may connect to. The
/// worker service checks who connected.
pub fn listen(path: &Path) -> io::Result<OwnedFd> {
    let socket = wire::socket()?;
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }
    let (address, len) = wire::address(path)?;
    let bound = unsafe { libc::bind(socket.as_raw_fd(), (&raw const address).cast(), len) };
    if bound != 0 {
        return Err(io::Error::last_os_error());
    }
    std::fs::set_permissions(path, std::os::unix::fs::PermissionsExt::from_mode(0o666))?;
    if unsafe { libc::listen(socket.as_raw_fd(), 64) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(socket)
}

/// The socket systemd opened for the worker service, if it did.
pub fn from_systemd() -> Option<OwnedFd> {
    let pid = std::env::var("LISTEN_PID").ok()?.parse::<u32>().ok()?;
    let count = std::env::var("LISTEN_FDS").ok()?.parse::<u32>().ok()?;
    if pid != std::process::id() || count != 1 {
        return None;
    }
    // systemd passes its sockets from 3 on.
    Some(unsafe { OwnedFd::from_raw_fd(3) })
}

/// Serves connections until the listener fails.
pub fn serve(config: Config) -> io::Result<()> {
    let server = std::fs::metadata(&config.jobs)?.uid();
    let free = config
        .uids
        .clone()
        .map_or_else(BTreeSet::new, |uids| uids.collect());
    let shared = Arc::new(Shared {
        config,
        server,
        free: Mutex::new(free),
        returned: Condvar::new(),
    });
    loop {
        let fd = unsafe {
            libc::accept4(
                shared.config.listener.as_raw_fd(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                libc::SOCK_CLOEXEC,
            )
        };
        if fd < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(err);
        }
        let connection = unsafe { OwnedFd::from_raw_fd(fd) };
        let shared = Arc::clone(&shared);
        std::thread::spawn(move || {
            if let Err(err) = session(&connection, &shared) {
                eprintln!("workers: a connection ended early: {err}");
            }
        });
    }
}

/// What a connection's jobs run as.
#[derive(Clone, Copy)]
enum Identity {
    /// A user ID of their own.
    Own(u32),
    /// The worker service's user, for development and tests.
    Shared(u32),
}

impl Identity {
    fn uid(self) -> u32 {
        match self {
            Self::Own(uid) | Self::Shared(uid) => uid,
        }
    }
}

struct Running {
    /// The launcher's process ID, which is also its process group's.
    pid: u32,
    waiter: std::thread::JoinHandle<()>,
}

fn session(connection: &OwnedFd, shared: &Arc<Shared>) -> io::Result<()> {
    let peer = wire::peer_uid(connection)?;
    if peer != shared.server {
        let message = format!("only user {} may use the worker service", shared.server);
        let _ = wire::send(
            connection.as_raw_fd(),
            &Reply::Refused(message).encode(),
            &[],
        );
        return Ok(());
    }
    let identity = match shared.config.uids {
        Some(_) => Identity::Own(take_uid(shared)),
        None => Identity::Shared(unsafe { libc::getuid() }),
    };
    let result = serve_session(connection, shared, identity);
    if let Identity::Own(uid) = identity {
        shared
            .free
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(uid);
        shared.returned.notify_one();
    }
    result
}

fn take_uid(shared: &Shared) -> u32 {
    let mut free = shared
        .free
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    loop {
        if let Some(uid) = free.pop_first() {
            return uid;
        }
        free = shared
            .returned
            .wait(free)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
    }
}

fn serve_session(connection: &OwnedFd, shared: &Arc<Shared>, identity: Identity) -> io::Result<()> {
    let reply = |reply: Reply| wire::send(connection.as_raw_fd(), &reply.encode(), &[]);
    reply(Reply::Hello {
        uid: identity.uid(),
    })?;
    let connection = Arc::new(connection.try_clone()?);
    let mut folder: Option<PathBuf> = None;
    let mut running: Option<Running> = None;
    loop {
        let Ok(Some((message, mut files))) = wire::receive(connection.as_raw_fd()) else {
            break;
        };
        // A job that's finished has nothing left to stop.
        if running.as_ref().is_some_and(|run| run.waiter.is_finished())
            && let Some(run) = running.take()
        {
            let _ = run.waiter.join();
        }
        match Request::decode(&message) {
            Ok(Request::Folder(path)) => match check_folder(shared, &path) {
                Ok(()) if folder.is_none() => folder = Some(path),
                Ok(()) => reply(Reply::Refused("the connection has a folder already".into()))?,
                Err(message) => reply(Reply::Refused(message))?,
            },
            Ok(Request::Run(run)) => {
                if running.is_some() {
                    reply(Reply::Refused("a job is running already".into()))?;
                    continue;
                }
                if files.len() != 3 {
                    reply(Reply::Refused("a job needs three open files".into()))?;
                    continue;
                }
                if let Err(message) = check_run(shared, folder.as_deref(), &run) {
                    reply(Reply::Refused(message))?;
                    continue;
                }
                let error = files.pop().expect("three");
                let output = files.pop().expect("three");
                let input = files.pop().expect("three");
                match start(shared, identity, run, input, output, error) {
                    Ok(child) => {
                        let pid = child.id();
                        let shared = Arc::clone(shared);
                        let connection = Arc::clone(&connection);
                        let folder = folder.clone();
                        let waiter = std::thread::spawn(move || {
                            let status = finish(&shared, identity, child, folder.as_deref());
                            let _ = wire::send(
                                connection.as_raw_fd(),
                                &Reply::Exited { status }.encode(),
                                &[],
                            );
                        });
                        running = Some(Running { pid, waiter });
                    }
                    Err(err) => reply(Reply::Refused(format!("the job couldn't start: {err}")))?,
                }
            }
            Ok(Request::Stop) => {
                if let Some(run) = &running {
                    stop(shared, identity, run.pid);
                }
            }
            Err(err) => reply(Reply::Refused(err.to_string()))?,
        }
    }
    // The server is done with this connection, or gone.
    if let Some(run) = running.take() {
        stop(shared, identity, run.pid);
        let _ = run.waiter.join();
    }
    if let Some(folder) = &folder {
        clean(shared, identity, folder);
    }
    Ok(())
}

/// A connection's folder has to be one of `jobs/`'s own, made by the
/// server.
fn check_folder(shared: &Shared, path: &Path) -> Result<(), String> {
    let refuse = || format!("{} isn't a job's folder", path.display());
    if path.parent() != Some(shared.config.jobs.as_path()) {
        return Err(refuse());
    }
    let found = std::fs::symlink_metadata(path).map_err(|_| refuse())?;
    if !found.is_dir() || found.uid() != shared.server {
        return Err(refuse());
    }
    Ok(())
}

/// Only programs from the system's folders run, and a job writes only in
/// its connection's folder.
fn check_run(shared: &Shared, folder: Option<&Path>, run: &wire::Run) -> Result<(), String> {
    let program = std::fs::canonicalize(&run.program)
        .map_err(|err| format!("couldn't find {}: {err}", run.program.display()))?;
    let allowed = shared
        .config
        .programs
        .iter()
        .any(|allowed| program.starts_with(allowed));
    if !allowed {
        return Err(format!(
            "{} isn't a program the worker service runs",
            run.program.display()
        ));
    }
    for path in &run.write {
        if !folder.is_some_and(|folder| path.starts_with(folder)) {
            return Err(format!(
                "{} isn't in the job's folder, so the job can't write there",
                path.display()
            ));
        }
    }
    Ok(())
}

/// Starts the launcher for `run`, as the connection's user.
fn start(
    shared: &Shared,
    identity: Identity,
    run: wire::Run,
    input: OwnedFd,
    output: OwnedFd,
    error: OwnedFd,
) -> io::Result<Child> {
    let mut command = Command::new(&shared.config.launcher);
    command
        .arg("sandbox")
        .arg("--memory")
        .arg(run.memory.to_string())
        .arg("--cpu")
        .arg(run.cpu.to_string());
    // A job of its own user can't reach the server's files, so Landlock is
    // one more layer where the kernel has it. A job of the worker service's
    // own user needs it, as without the worker service.
    if matches!(identity, Identity::Own(_)) {
        command.arg("--landlock-optional");
    }
    for path in &run.read {
        command.arg("--read").arg(path);
    }
    for path in &run.write {
        command.arg("--write").arg(path);
    }
    if let Some(path) = &run.sockets {
        command.arg("--sockets").arg(path);
    }
    command
        .arg("--")
        .arg(&run.program)
        .args(&run.args)
        .env_clear()
        .envs(run.env)
        .current_dir("/")
        .stdin(Stdio::from(input))
        .stdout(Stdio::from(output))
        .stderr(Stdio::from(error))
        .process_group(0);
    become_job(&mut command, identity);
    command.spawn()
}

/// Waits for the launcher, stops anything it left running, and gives the
/// server what it wrote. Returns its wait status.
fn finish(shared: &Shared, identity: Identity, mut child: Child, folder: Option<&Path>) -> i32 {
    let pid = child.id();
    let status = child
        .wait()
        .map_or(libc::SIGKILL, |status| status.into_raw());
    match identity {
        Identity::Own(_) => {
            let mut settle = vec![OsString::from("--settle")];
            if let Some(folder) = folder {
                settle.push(folder.into());
                settle.push(shared.server.to_string().into());
            }
            helper(shared, identity, &settle);
        }
        // The same user as the server: the files are its own already.
        Identity::Shared(_) => kill_group(pid),
    }
    status
}

/// Stops the running job, and everything it started.
fn stop(shared: &Shared, identity: Identity, pid: u32) {
    match identity {
        Identity::Own(_) => helper(shared, identity, &[OsString::from("--settle")]),
        Identity::Shared(_) => kill_group(pid),
    }
}

/// Empties the connection's folder, as its user, so that nothing a job
/// wrote outlives it.
fn clean(shared: &Shared, identity: Identity, folder: &Path) {
    match identity {
        Identity::Own(_) => helper(
            shared,
            identity,
            &[OsString::from("--clean"), folder.into()],
        ),
        Identity::Shared(_) => empty(folder),
    }
}

fn kill_group(pid: u32) {
    if let Ok(group) = libc::pid_t::try_from(pid) {
        // The group may be gone already, which is fine.
        unsafe { libc::kill(-group, libc::SIGKILL) };
    }
}

/// Helpers stop every process of their user, so two at once could stop
/// each other. They take turns.
static HELPERS: Mutex<()> = Mutex::new(());

/// Runs the launcher in one of its helper modes, as the connection's user.
fn helper(shared: &Shared, identity: Identity, args: &[OsString]) {
    let _turn = HELPERS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let mut command = Command::new(&shared.config.launcher);
    command
        .arg("sandbox")
        .args(args)
        .env_clear()
        .current_dir("/")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());
    become_job(&mut command, identity);
    match command.status() {
        Ok(status) if status.success() => {}
        Ok(status) => eprintln!("workers: the helper {args:?} ended with {status}"),
        Err(err) => eprintln!("workers: the helper {args:?} couldn't start: {err}"),
    }
}

/// Makes `command` run as the job's user, with no groups and no
/// privileges, which it can never get back.
fn become_job(command: &mut Command, identity: Identity) {
    let Identity::Own(uid) = identity else {
        return;
    };
    unsafe {
        command.pre_exec(move || {
            let check = |result: libc::c_int| {
                if result == 0 {
                    Ok(())
                } else {
                    Err(io::Error::last_os_error())
                }
            };
            check(libc::setgroups(0, std::ptr::null()))?;
            check(libc::setresgid(uid, uid, uid))?;
            // Privileges a process was given to keep across programs.
            check(libc::prctl(
                libc::PR_CAP_AMBIENT,
                libc::PR_CAP_AMBIENT_CLEAR_ALL,
                0,
                0,
                0,
            ))?;
            check(libc::setresuid(uid, uid, uid))?;
            // A worker service that isn't root keeps its privileges when it
            // changes user, so they're dropped by hand.
            let mut header = CapHeader {
                version: 0x2008_0522,
                pid: 0,
            };
            let data = [CapData::default(); 2];
            check(libc::syscall(libc::SYS_capset, &raw mut header, data.as_ptr()) as libc::c_int)?;
            check(libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0))
        });
    }
}

#[repr(C)]
struct CapHeader {
    version: u32,
    pid: libc::c_int,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct CapData {
    effective: u32,
    permitted: u32,
    inheritable: u32,
}

/// The launcher's helper modes, which the worker service runs as a job's
/// user. Returns the exit code.
///
/// - `--settle [<folder> <uid>]` stops every other process of this user,
///   then lets `uid` read and write everything in `folder` that this user
///   made.
/// - `--clean <folder>` stops every other process of this user, then
///   removes everything in `folder`, and this user's files in `/tmp`.
pub(crate) fn run_helper(mode: &str, mut args: impl Iterator<Item = OsString>) -> i32 {
    if let Err(err) = stop_my_processes() {
        eprintln!("sandbox: couldn't stop the job's processes: {err}");
        return 1;
    }
    let folder = args.next().map(PathBuf::from);
    let result = match (mode, folder) {
        ("--settle", None) => Ok(()),
        ("--settle", Some(folder)) => {
            let server = args
                .next()
                .and_then(|uid| uid.to_str()?.parse::<u32>().ok());
            match server {
                Some(server) => give(&folder, server),
                None => Err(io::Error::other("--settle needs the server's user ID")),
            }
        }
        ("--clean", Some(folder)) => {
            empty(&folder);
            empty_mine(Path::new("/tmp"));
            Ok(())
        }
        _ => Err(io::Error::other("the helper doesn't know what to do")),
    };
    match result {
        Ok(()) => 0,
        Err(err) => {
            eprintln!("sandbox: {err}");
            1
        }
    }
}

/// Sends SIGKILL to every process of this user but this one, until none
/// is left running.
fn stop_my_processes() -> io::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        // Every process this one may signal: its user's, and no others.
        unsafe { libc::kill(-1, libc::SIGKILL) };
        if !others_running()? {
            return Ok(());
        }
        if Instant::now() > deadline {
            return Err(io::Error::other("they're still running after 5 seconds"));
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Whether a process of this user other than this one is still running,
/// not counting ones that have stopped and wait for their parent.
fn others_running() -> io::Result<bool> {
    let me = std::process::id();
    let uid = unsafe { libc::getuid() };
    for entry in std::fs::read_dir("/proc")? {
        let Ok(entry) = entry else { continue };
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        if pid == me {
            continue;
        }
        let Ok(status) = std::fs::read_to_string(entry.path().join("status")) else {
            continue;
        };
        let field = |name: &str| {
            status
                .lines()
                .find_map(|line| line.strip_prefix(name))
                .map(str::trim)
                .map(str::to_owned)
        };
        let mine = field("Uid:").is_some_and(|uids| {
            uids.split_whitespace()
                .any(|id| id.parse::<u32>().is_ok_and(|id| id == uid))
        });
        let stopped = field("State:").is_some_and(|state| state.starts_with('Z'));
        if mine && !stopped {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Lets `server` read and write everything this user made in `folder`.
fn give(folder: &Path, server: u32) -> io::Result<()> {
    let me = unsafe { libc::getuid() };
    let mut waiting = vec![folder.to_owned()];
    while let Some(dir) = waiting.pop() {
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            let found = std::fs::symlink_metadata(&path)?;
            if found.uid() != me || found.file_type().is_symlink() {
                continue;
            }
            if found.is_dir() {
                std::fs::set_permissions(
                    &path,
                    std::os::unix::fs::PermissionsExt::from_mode(0o700),
                )?;
                acl::grant(&path, server, acl::READ | acl::WRITE | acl::SEARCH)?;
                waiting.push(path);
            } else if found.is_file() {
                acl::grant(&path, server, acl::READ | acl::WRITE)?;
            }
        }
    }
    Ok(())
}

/// Removes everything in `folder`, but not the folder itself.
fn empty(folder: &Path) {
    let Ok(entries) = std::fs::read_dir(folder) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let removed = match entry.file_type() {
            Ok(kind) if kind.is_dir() => std::fs::remove_dir_all(&path),
            _ => std::fs::remove_file(&path),
        };
        if let Err(err) = removed {
            eprintln!("sandbox: couldn't remove {}: {err}", path.display());
        }
    }
}

/// Removes this user's files and folders in `folder`, such as the sockets
/// LibreOffice leaves in `/tmp`.
fn empty_mine(folder: &Path) {
    let me = unsafe { libc::getuid() };
    let Ok(entries) = std::fs::read_dir(folder) else {
        return;
    };
    for entry in entries.flatten() {
        let Ok(found) = entry.path().symlink_metadata() else {
            continue;
        };
        if found.uid() != me {
            continue;
        }
        let _ = if found.is_dir() {
            std::fs::remove_dir_all(entry.path())
        } else {
            std::fs::remove_file(entry.path())
        };
    }
}

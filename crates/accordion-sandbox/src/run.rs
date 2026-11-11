//! The server's side: starts the launcher with a job, feeds it the file, and
//! stops it if it runs too long or writes too much.

use std::ffi::OsString;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;

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

/// Starts workers through a launcher: the server's own program, which
/// answers `sandbox`.
#[derive(Clone, Debug)]
pub struct Sandbox {
    launcher: Arc<Path>,
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
    /// such as a stored file it reads by name.
    pub read: &'a [PathBuf],
    /// Scratch folders the program may write in. It can't run what it
    /// writes.
    pub write: &'a [PathBuf],
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

impl Sandbox {
    /// `launcher` is the program that answers `sandbox`, usually
    /// `std::env::current_exe()`.
    pub fn new(launcher: impl Into<PathBuf>) -> Self {
        Self {
            launcher: Arc::from(launcher.into()),
        }
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
        let limits = job.limits;
        let mut command = Command::new(&*self.launcher);
        command
            .arg("sandbox")
            .arg("--memory")
            .arg(limits.memory.to_string())
            .arg("--cpu")
            .arg(limits.cpu.as_secs().max(1).to_string());
        let system = SYSTEM.iter().map(Path::new);
        for path in system.chain(job.read.iter().map(PathBuf::as_path)) {
            if path.exists() {
                command.arg("--read").arg(path);
            }
        }
        for folder in job.write {
            command.arg("--write").arg(folder);
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
            let read_output = async {
                let output = read_up_to(stdout, limits.output + 1).await;
                // The worker is waiting to write the rest, so it would never
                // finish.
                if cut_off
                    && output
                        .as_ref()
                        .is_ok_and(|output| output.len() as u64 > limits.output)
                {
                    stop(group);
                }
                output
            };
            let (output, message) = tokio::join!(read_output, read_up_to(stderr, MESSAGE_LIMIT));
            let mut output = output.map_err(RunError::Start)?;
            if output.len() as u64 > limits.output {
                if !cut_off {
                    return Err(RunError::TooLarge(limits.output));
                }
                let _ = child.wait().await;
                output.truncate(limits.output as usize);
                return Ok((output, String::new()));
            }
            let status = child.wait().await.map_err(RunError::Start)?;
            let message = String::from_utf8_lossy(&message.unwrap_or_default())
                .trim()
                .to_owned();
            if status.success() {
                return Ok((output, message));
            }
            Err(RunError::Failed { status, message })
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

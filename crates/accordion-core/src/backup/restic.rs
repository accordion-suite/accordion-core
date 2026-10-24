//! Runs restic, the program that makes and checks backups, as a separate
//! process, and turns its exit codes and output into results.

use std::ffi::OsString;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};

use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::Command;

/// How restic exits when the repository doesn't exist.
const NO_REPOSITORY: i32 = 10;
/// How restic exits when another restic process has the repository locked.
const LOCKED: i32 = 11;
/// How restic exits when the password doesn't open the repository.
const WRONG_PASSWORD: i32 = 12;

/// The lines of restic's error output kept for a problem's description.
const PROBLEM_LINES: usize = 3;

/// What restic prints before it tries a failed request to a repository
/// again, followed by the error.
const RETRYING: &str = " returned error, retrying after ";

/// A repository restic opens: where it is and how to reach it.
#[derive(Clone, PartialEq, Eq)]
pub(super) struct Repository {
    /// In restic's form, such as `/media/usb/backups`,
    /// `s3:https://s3.example.com/backups`, or `sftp:me@nas:backups`.
    pub location: String,
    /// A bucket's access key.
    pub keys: Option<Keys>,
    /// For SFTP, the ssh command restic connects with. Its paths are
    /// relative to the data folder, because restic doesn't honor quotes in
    /// it, so a path with a space would break it.
    pub ssh: Option<String>,
}

/// The access key for an S3 bucket.
#[derive(Clone, PartialEq, Eq)]
pub(super) struct Keys {
    pub id: String,
    pub secret: String,
}

impl std::fmt::Debug for Keys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Keys")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

pub(super) struct Restic {
    pub program: PathBuf,
    /// The data folder, which restic runs in unless told otherwise.
    pub home: PathBuf,
    pub key_file: PathBuf,
    pub cache: PathBuf,
    pub temp: PathBuf,
}

#[derive(Debug)]
pub(super) enum Failure {
    /// restic couldn't be started at all.
    Start(io::Error),
    NoRepository,
    Locked,
    WrongKey,
    /// A request to the repository failed, and restic was stopped before it
    /// tried again. Holds the error restic gave.
    Retrying(String),
    /// ssh couldn't connect to an SFTP server.
    Ssh(SshProblem),
    /// Anything else, with restic's last lines of error output.
    Failed {
        code: Option<i32>,
        message: String,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum SshProblem {
    /// The server didn't accept Accordion's key.
    KeyRefused,
    /// The server's host key isn't the one recorded when the target was
    /// added.
    HostKeyChanged,
    /// The server couldn't be reached, with ssh's description.
    Unreachable(String),
    /// Anything else ssh said.
    Other(String),
}

impl Restic {
    /// Runs restic on `repository`, in `dir` if given, and returns what it
    /// printed. restic tries failed requests again for several minutes, which
    /// rides out a network blip during a nightly backup.
    pub async fn run(
        &self,
        repository: &Repository,
        dir: Option<&Path>,
        args: &[OsString],
    ) -> Result<String, Failure> {
        let output = self
            .command(repository, dir, args)
            .output()
            .await
            .map_err(Failure::Start)?;
        if output.status.success() {
            return Ok(String::from_utf8_lossy(&output.stdout).into_owned());
        }
        Err(failure(output.status, &output.stderr))
    }

    /// Like [`Restic::run`], but stops restic the first time a request
    /// fails. For setting up and finding backups while someone waits, where
    /// a mistyped key or bucket should get an answer right away.
    pub async fn run_once(
        &self,
        repository: &Repository,
        dir: Option<&Path>,
        args: &[OsString],
    ) -> Result<String, Failure> {
        let mut child = self
            .command(repository, dir, args)
            .spawn()
            .map_err(Failure::Start)?;
        let mut stdout = child.stdout.take().expect("restic's output is piped");
        let stderr = child.stderr.take().expect("restic's errors are piped");
        let output = async {
            let mut output = Vec::new();
            let _ = stdout.read_to_end(&mut output).await;
            output
        };
        let errors = async {
            let mut errors = String::new();
            let mut retrying = None;
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if retrying.is_none()
                    && let Some((_, rest)) = line.split_once(RETRYING)
                {
                    // After the wait, such as "1.2s: Access Denied."
                    let problem = rest.split_once(": ").map_or(rest, |(_, problem)| problem);
                    retrying = Some(problem.trim().to_owned());
                    let _ = child.start_kill();
                }
                errors.push_str(&line);
                errors.push('\n');
            }
            (errors, retrying)
        };
        let (output, (errors, retrying)) = tokio::join!(output, errors);
        let status = child.wait().await.map_err(Failure::Start)?;
        if let Some(problem) = retrying {
            tracing::debug!(%errors, "stopped restic at its first retry");
            return Err(Failure::Retrying(problem));
        }
        if status.success() {
            return Ok(String::from_utf8_lossy(&output).into_owned());
        }
        Err(failure(status, errors.as_bytes()))
    }

    /// Like [`Restic::run`], but hands each line restic prints to `on_line`
    /// as it arrives, such as progress during a long restore.
    pub async fn run_streaming(
        &self,
        repository: &Repository,
        dir: Option<&Path>,
        args: &[OsString],
        mut on_line: impl FnMut(&str),
    ) -> Result<(), Failure> {
        let mut command = self.command(repository, dir, args);
        // About one progress line a second. Without a terminal, restic
        // otherwise prints dozens.
        command.env("RESTIC_PROGRESS_FPS", "1");
        let mut child = command.spawn().map_err(Failure::Start)?;
        let stdout = child.stdout.take().expect("restic's output is piped");
        let mut stderr = child.stderr.take().expect("restic's errors are piped");
        // Both are read at once, so neither pipe fills up and stalls restic.
        let errors = async {
            let mut errors = Vec::new();
            let _ = stderr.read_to_end(&mut errors).await;
            errors
        };
        let lines = async {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                on_line(&line);
            }
        };
        let (errors, ()) = tokio::join!(errors, lines);
        let status = child.wait().await.map_err(Failure::Start)?;
        if status.success() {
            return Ok(());
        }
        Err(failure(status, &errors))
    }

    fn command(&self, repository: &Repository, dir: Option<&Path>, args: &[OsString]) -> Command {
        let mut command = Command::new(&self.program);
        // Only what restic needs, so nothing from the server's environment,
        // such as other credentials, reaches it.
        command.env_clear();
        if let Some(path) = std::env::var_os("PATH") {
            command.env("PATH", path);
        }
        if let Some(keys) = &repository.keys {
            command
                .env("AWS_ACCESS_KEY_ID", &keys.id)
                .env("AWS_SECRET_ACCESS_KEY", &keys.secret);
        }
        if let Some(ssh) = &repository.ssh {
            command.arg("-o").arg(format!("sftp.command={ssh}"));
        }
        command
            .env("RESTIC_REPOSITORY", &repository.location)
            .env("RESTIC_PASSWORD_FILE", &self.key_file)
            .env("RESTIC_CACHE_DIR", &self.cache)
            // Packs wait here before upload. /tmp is often a small RAM disk.
            .env("TMPDIR", &self.temp)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            // If the server stops partway, restic stops too. Its repository
            // stays usable, and the next run carries on.
            .kill_on_drop(true);
        command.current_dir(dir.unwrap_or(&self.home));
        command
    }
}

/// What a failed run means, from restic's exit code and error output.
fn failure(status: ExitStatus, stderr: &[u8]) -> Failure {
    let stderr = String::from_utf8_lossy(stderr);
    tracing::debug!(code = ?status.code(), %stderr, "restic failed");
    match status.code() {
        Some(NO_REPOSITORY) => Failure::NoRepository,
        Some(LOCKED) => Failure::Locked,
        Some(WRONG_PASSWORD) => Failure::WrongKey,
        code => match ssh_problem(&stderr) {
            Some(problem) => Failure::Ssh(problem),
            None => Failure::Failed {
                code,
                message: last_lines(&stderr),
            },
        },
    }
}

/// What went wrong connecting over SFTP, from the lines restic passes on
/// from ssh, if any.
fn ssh_problem(stderr: &str) -> Option<SshProblem> {
    let lines: Vec<&str> = stderr
        .lines()
        .filter_map(|line| line.strip_prefix("subprocess ssh: "))
        .map(|line| line.trim().trim_start_matches("ssh: "))
        .filter(|line| !line.is_empty() && !line.starts_with("Warning:"))
        .collect();
    let has = |phrase: &str| lines.iter().any(|line| line.contains(phrase));
    if has("Host key verification failed") || has("has changed") {
        return Some(SshProblem::HostKeyChanged);
    }
    if has("Permission denied") {
        return Some(SshProblem::KeyRefused);
    }
    for phrase in [
        "Connection refused",
        "Could not resolve hostname",
        "No route to host",
        "timed out",
        "Network is unreachable",
        "Connection closed",
        "Connection reset",
    ] {
        if let Some(line) = lines.iter().find(|line| line.contains(phrase)) {
            return Some(SshProblem::Unreachable((*line).to_owned()));
        }
    }
    lines
        .last()
        .map(|line| SshProblem::Other((*line).to_owned()))
}

/// The last few lines restic wrote, which say what went wrong.
fn last_lines(text: &str) -> String {
    let lines: Vec<&str> = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect();
    lines[lines.len().saturating_sub(PROBLEM_LINES)..].join(" ")
}

/// The JSON object restic printed with `"message_type"` equal to `kind`,
/// such as the summary at the end of a backup.
pub(super) fn message(stdout: &str, kind: &str) -> Option<serde_json::Value> {
    stdout
        .lines()
        .rev()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .find(|value| value["message_type"] == kind)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_the_last_lines_of_errors() {
        assert_eq!(
            last_lines("one\n\ntwo\nthree\n  four  \n"),
            "two three four"
        );
        assert_eq!(last_lines(""), "");
    }

    #[test]
    fn reads_what_ssh_said() {
        let refused = "subprocess ssh: nas@192.168.1.50: Permission denied (publickey).\n\
                       Fatal: unable to open repository";
        assert_eq!(ssh_problem(refused), Some(SshProblem::KeyRefused));
        let changed = "subprocess ssh: Host key for nas has changed and you have requested \
                       strict checking.\nsubprocess ssh: Host key verification failed.\n";
        assert_eq!(ssh_problem(changed), Some(SshProblem::HostKeyChanged));
        let down = "subprocess ssh: ssh: connect to host nas port 22: Connection refused\n";
        assert_eq!(
            ssh_problem(down),
            Some(SshProblem::Unreachable(
                "connect to host nas port 22: Connection refused".into()
            ))
        );
        assert_eq!(ssh_problem("Fatal: wrong password or no key found\n"), None);
    }

    #[test]
    fn finds_restic_messages() {
        let stdout = "{\"message_type\":\"status\"}\nnot json\n\
                      {\"message_type\":\"summary\",\"snapshot_id\":\"abc\"}\n";
        assert_eq!(message(stdout, "summary").unwrap()["snapshot_id"], "abc");
        assert!(message(stdout, "initialized").is_none());
    }
}

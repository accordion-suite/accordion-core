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

pub(super) struct Restic {
    pub program: PathBuf,
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
    /// Anything else, with restic's last lines of error output.
    Failed {
        code: Option<i32>,
        message: String,
    },
}

impl Restic {
    /// Runs restic on the repository at `repository`, in `dir` if given, and
    /// returns what it printed.
    pub async fn run(
        &self,
        repository: &str,
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

    /// Like [`Restic::run`], but hands each line restic prints to `on_line`
    /// as it arrives, such as progress during a long restore.
    pub async fn run_streaming(
        &self,
        repository: &str,
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

    fn command(&self, repository: &str, dir: Option<&Path>, args: &[OsString]) -> Command {
        let mut command = Command::new(&self.program);
        // Only what restic needs, so nothing from the server's environment,
        // such as other credentials, reaches it.
        command.env_clear();
        if let Some(path) = std::env::var_os("PATH") {
            command.env("PATH", path);
        }
        command
            .env("RESTIC_REPOSITORY", repository)
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
        if let Some(dir) = dir {
            command.current_dir(dir);
        }
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
        code => Failure::Failed {
            code,
            message: last_lines(&stderr),
        },
    }
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
    fn finds_restic_messages() {
        let stdout = "{\"message_type\":\"status\"}\nnot json\n\
                      {\"message_type\":\"summary\",\"snapshot_id\":\"abc\"}\n";
        assert_eq!(message(stdout, "summary").unwrap()["snapshot_id"], "abc");
        assert!(message(stdout, "initialized").is_none());
    }
}

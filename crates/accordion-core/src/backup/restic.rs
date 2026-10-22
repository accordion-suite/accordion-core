//! Runs restic, the program that makes and checks backups, as a separate
//! process, and turns its exit codes and output into results.

use std::ffi::OsString;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Stdio;

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
        let output = command.output().await.map_err(Failure::Start)?;
        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        if output.status.success() {
            return Ok(stdout);
        }
        let stderr = String::from_utf8_lossy(&output.stderr);
        tracing::debug!(code = ?output.status.code(), %stderr, "restic failed");
        Err(match output.status.code() {
            Some(NO_REPOSITORY) => Failure::NoRepository,
            Some(LOCKED) => Failure::Locked,
            Some(WRONG_PASSWORD) => Failure::WrongKey,
            code => Failure::Failed {
                code,
                message: last_lines(&stderr),
            },
        })
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

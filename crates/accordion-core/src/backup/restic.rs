//! Runs restic, the program that makes and checks backups, as a separate
//! process, and turns its exit codes and output into results.

use std::ffi::OsString;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};

use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdout, Command};

/// How restic exits when the repository doesn't exist.
const NO_REPOSITORY: i32 = 10;
/// How restic exits when another restic process has the repository locked.
const LOCKED: i32 = 11;
/// How restic exits when the password doesn't open the repository.
const WRONG_PASSWORD: i32 = 12;

/// The lines of restic's error output kept for a problem's description.
const PROBLEM_LINES: usize = 3;

/// How long to wait for ssh when asking it why a connection failed. Its own
/// connection timeout is 30 seconds.
const SSH_CHECK_TIME: std::time::Duration = std::time::Duration::from_secs(45);

/// What restic prints before it tries a failed request to a repository
/// again, followed by the error.
const RETRYING: &str = " returned error, retrying after ";

/// A repository restic opens: where it is and how to reach it.
#[derive(Clone, PartialEq, Eq)]
pub(super) struct Repository {
    /// In restic's form, such as `/media/usb/backups`,
    /// `s3:https://s3.example.com/backups`, `sftp:me@nas:backups`, or
    /// `rest:https://nas.local:8000/backups`.
    pub location: String,
    /// A bucket's access key.
    pub keys: Option<Keys>,
    /// For SFTP, the ssh command restic connects with. Its paths are
    /// relative to the data folder, because restic doesn't honor quotes in
    /// it, so a path with a space would break it.
    pub ssh: Option<String>,
    /// For a REST server, the username and password from its password
    /// file.
    pub login: Option<Login>,
    /// For a REST server, the certificate Accordion pinned, if the system
    /// doesn't already trust the server's.
    pub cacert: Option<PathBuf>,
}

impl Repository {
    /// A repository restic reaches with nothing but its location, such as a
    /// folder.
    pub fn at(location: String) -> Self {
        Self {
            location,
            keys: None,
            ssh: None,
            login: None,
            cacert: None,
        }
    }
}

/// The username and password for a REST server.
#[derive(Clone, PartialEq, Eq)]
pub(super) struct Login {
    pub username: String,
    pub password: String,
}

impl std::fmt::Debug for Login {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Login")
            .field("username", &self.username)
            .finish_non_exhaustive()
    }
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
    /// Anything else, with restic's last lines of error output to show,
    /// and all of it to look through.
    Failed {
        code: Option<i32>,
        message: String,
        output: String,
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
    /// rides out a network blip during a nightly backup. A REST server's
    /// problem that waiting won't fix, such as a new certificate or a wrong
    /// password, stops it right away.
    pub async fn run(
        &self,
        repository: &Repository,
        dir: Option<&Path>,
        args: &[OsString],
    ) -> Result<String, Failure> {
        let lasting = |problem: &str| {
            repository.login.is_some()
                && super::rest::problem(&Failure::Retrying(problem.to_owned())).is_some()
        };
        self.run_until(repository, dir, args, lasting).await
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
        self.run_until(repository, dir, args, |_| true).await
    }

    /// Runs restic, and stops it when it's about to try a failed request
    /// again for a reason `give_up` accepts.
    async fn run_until(
        &self,
        repository: &Repository,
        dir: Option<&Path>,
        args: &[OsString],
        give_up: impl Fn(&str) -> bool,
    ) -> Result<String, Failure> {
        let mut restic = Running::start(self.command(repository, dir, args))?;
        let (mut stdout, stderr) = restic.pipes();
        // Both are read at once, so neither pipe fills up and stalls restic.
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
                    if give_up(problem) {
                        retrying = Some(problem.trim().to_owned());
                        restic.stop();
                    }
                }
                errors.push_str(&line);
                errors.push('\n');
            }
            (errors, retrying)
        };
        let (output, (errors, retrying)) = tokio::join!(output, errors);
        let status = restic.wait().await?;
        if let Some(problem) = retrying {
            tracing::debug!(%errors, "stopped restic at a retry");
            return Err(Failure::Retrying(problem));
        }
        if status.success() {
            return Ok(String::from_utf8_lossy(&output).into_owned());
        }
        Err(self
            .explain(repository, failure(status, errors.as_bytes()))
            .await)
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
        let mut restic = Running::start(command)?;
        let (stdout, mut stderr) = restic.pipes();
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
        let status = restic.wait().await?;
        if status.success() {
            return Ok(());
        }
        Err(self.explain(repository, failure(status, &errors)).await)
    }

    /// restic passes on what ssh says, but can stop before ssh has said all
    /// of it. When an SFTP connection fails without ssh's reason, or with
    /// only the start of what ssh said, this asks ssh directly, with the same
    /// command.
    async fn explain(&self, repository: &Repository, failure: Failure) -> Failure {
        let cut_off = match &failure {
            Failure::Failed { message, .. } => message.contains("sftp session"),
            Failure::Ssh(SshProblem::Other(_)) => true,
            _ => false,
        };
        let (Some(ssh), true) = (&repository.ssh, cut_off) else {
            return failure;
        };
        // Accordion builds the command, and none of its parts has a space.
        let mut parts = ssh.split_whitespace();
        let Some(program) = parts.next() else {
            return failure;
        };
        let mut command = Command::new(program);
        command.env_clear();
        if let Some(path) = std::env::var_os("PATH") {
            command.env("PATH", path);
        }
        command
            .args(parts)
            .current_dir(&self.home)
            // With no input, the SFTP session ends as soon as it starts.
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let output = match tokio::time::timeout(SSH_CHECK_TIME, command.output()).await {
            Ok(Ok(output)) => output,
            _ => return failure,
        };
        if output.status.success() {
            return failure;
        }
        let stderr = String::from_utf8_lossy(&output.stderr);
        match problem_in(stderr.lines()) {
            Some(problem) => Failure::Ssh(problem),
            None => failure,
        }
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
        // In the environment, not the address, so they stay out of restic's
        // messages and of process lists.
        if let Some(login) = &repository.login
            && !login.username.is_empty()
        {
            command
                .env("RESTIC_REST_USERNAME", &login.username)
                .env("RESTIC_REST_PASSWORD", &login.password);
        }
        if let Some(cacert) = &repository.cacert {
            command.arg("--cacert").arg(cacert);
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
            .stderr(Stdio::piped());
        command.current_dir(dir.unwrap_or(&self.home));
        // In a session of its own, restic has no terminal. With one, restic
        // hands the terminal to ssh for SFTP, in case ssh asks for a
        // password, and doesn't always hand it back. Ctrl+C then stopped
        // reaching Accordion.
        // SAFETY: setsid is safe to call between fork and exec.
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        command
    }
}

/// A restic process. Dropping it before it finishes, such as when the server
/// stops partway through a backup, stops restic. Its repository stays
/// usable, and the next run carries on.
struct Running {
    child: Child,
    finished: bool,
}

impl Running {
    fn start(mut command: Command) -> Result<Self, Failure> {
        let child = command.spawn().map_err(Failure::Start)?;
        Ok(Self {
            child,
            finished: false,
        })
    }

    fn pipes(&mut self) -> (ChildStdout, ChildStderr) {
        let stdout = self.child.stdout.take().expect("restic's output is piped");
        let stderr = self.child.stderr.take().expect("restic's errors are piped");
        (stdout, stderr)
    }

    async fn wait(&mut self) -> Result<ExitStatus, Failure> {
        let status = self.child.wait().await.map_err(Failure::Start)?;
        self.finished = true;
        Ok(status)
    }

    /// Asks restic to stop. Unlike SIGKILL, SIGTERM lets restic remove its
    /// lock from the repository first. A lock left behind blocks weekly
    /// upkeep until restic decides it's stale.
    fn stop(&self) {
        // No ID once the process has been reaped, so the signal can't reach
        // another process that reused the ID.
        let Some(pid) = self
            .child
            .id()
            .and_then(|pid| libc::pid_t::try_from(pid).ok())
        else {
            return;
        };
        // SAFETY: kill only sends a signal, and pid is restic's.
        unsafe {
            libc::kill(pid, libc::SIGTERM);
        }
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        if !self.finished {
            self.stop();
        }
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
                output: stderr.into_owned(),
            },
        },
    }
}

/// What went wrong connecting over SFTP, from the lines restic passes on
/// from ssh, if any.
fn ssh_problem(stderr: &str) -> Option<SshProblem> {
    problem_in(
        stderr
            .lines()
            .filter_map(|line| line.strip_prefix("subprocess ssh: ")),
    )
}

/// What went wrong, from what ssh printed.
fn problem_in<'a>(lines: impl Iterator<Item = &'a str>) -> Option<SshProblem> {
    let lines: Vec<&str> = lines
        .map(|line| line.trim().trim_start_matches("ssh: "))
        .filter(|line| !line.is_empty() && !line.starts_with("Warning:"))
        .collect();
    let has = |phrase: &str| lines.iter().any(|line| line.contains(phrase));
    if has("Host key verification failed")
        || has("has changed")
        // The start of ssh's warning, which arrives even when the rest
        // doesn't.
        || has("REMOTE HOST IDENTIFICATION HAS CHANGED")
    {
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
    let raw: Vec<&str> = text.lines().collect();
    // Some errors end with a Go stack trace: a function's name, then a
    // tab-indented line with its file. That says nothing to an admin.
    let in_trace = |index: usize| {
        let file = |line: &&str| line.starts_with('\t') && line.contains(".go:");
        raw.get(index).is_some_and(file) || raw.get(index + 1).is_some_and(file)
    };
    let lines: Vec<&str> = raw
        .iter()
        .enumerate()
        .filter(|&(index, _)| !in_trace(index))
        .map(|(_, line)| line.trim())
        // Progress, such as "[0:00] 0.00%  0 / 1 files deleted".
        .filter(|line| !line.is_empty() && !line.starts_with('['))
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
        // What restic 0.19.1 printed when a REST server refused a delete.
        let refused = "Remove(<snapshot/97895728d8>) failed: unexpected HTTP response (403): \
                       403 Forbidden\n\
                       unable to remove snapshot/97895728d8 from the repository\n\
                       [0:00] 0.00%  0 / 1 files deleted\n\
                       failed to remove one or more snapshots\n\
                       main.init\n\t/restic/cmd/restic/cmd_forget.go:67\n\
                       runtime.main\n\t/usr/local/go/src/runtime/proc.go:258\n";
        assert_eq!(
            last_lines(refused),
            "Remove(<snapshot/97895728d8>) failed: unexpected HTTP response (403): 403 Forbidden \
             unable to remove snapshot/97895728d8 from the repository \
             failed to remove one or more snapshots"
        );
    }

    #[test]
    fn reads_what_ssh_said() {
        let refused = "subprocess ssh: nas@192.168.1.50: Permission denied (publickey).\n\
                       Fatal: unable to open repository";
        assert_eq!(ssh_problem(refused), Some(SshProblem::KeyRefused));
        let changed = "subprocess ssh: Host key for nas has changed and you have requested \
                       strict checking.\nsubprocess ssh: Host key verification failed.\n";
        assert_eq!(ssh_problem(changed), Some(SshProblem::HostKeyChanged));
        // What restic passed on in CI before it stopped.
        let cut_off = "subprocess ssh: @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n\
                       subprocess ssh: @    WARNING: REMOTE HOST IDENTIFICATION HAS CHANGED!     @\n\
                       subprocess ssh: @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n\
                       subprocess ssh: The fingerprint for the ED25519 key sent by the remote host is\n\
                       subprocess ssh: SHA256:4GTfW+vnArdtqNDhSO+aJjqi3OsZJG0DH8xRjh0JGsg.\n";
        assert_eq!(ssh_problem(cut_off), Some(SshProblem::HostKeyChanged));
        let down = "subprocess ssh: ssh: connect to host nas port 22: Connection refused\n";
        assert_eq!(
            ssh_problem(down),
            Some(SshProblem::Unreachable(
                "connect to host nas port 22: Connection refused".into()
            ))
        );
        assert_eq!(ssh_problem("Fatal: wrong password or no key found\n"), None);
    }

    #[tokio::test]
    async fn asks_ssh_when_restic_drops_its_reason() {
        let dir = tempfile::tempdir().unwrap();
        let ssh = dir.path().join("ssh");
        write_script(
            &ssh,
            "echo 'nas@192.168.1.50: Permission denied (publickey).' >&2\nexit 255\n",
        );
        let restic = Restic {
            program: "restic".into(),
            home: dir.path().to_owned(),
            key_file: "key".into(),
            cache: "cache".into(),
            temp: "temp".into(),
        };
        let repository = Repository {
            ssh: Some(format!("{} -p 22 nas@192.168.1.50 -s sftp", ssh.display())),
            ..Repository::at("sftp:nas@192.168.1.50:backups".into())
        };
        // What restic said on CI, without ssh's lines.
        let dropped = || Failure::Failed {
            code: Some(1),
            message: "unable to start the sftp session, error: error receiving version \
                      packet from server: read |0: file already closed"
                .into(),
            output: String::new(),
        };
        assert!(matches!(
            restic.explain(&repository, dropped()).await,
            Failure::Ssh(SshProblem::KeyRefused)
        ));
        // Only the start of what ssh said, without its reason.
        let cut_off = Failure::Ssh(SshProblem::Other("SHA256:4GTfW+vnArdtqNDhSO+aJjq".into()));
        assert!(matches!(
            restic.explain(&repository, cut_off).await,
            Failure::Ssh(SshProblem::KeyRefused)
        ));
        // Other failures are left as they are.
        let folder = Repository {
            ssh: None,
            ..repository
        };
        assert!(matches!(
            restic.explain(&folder, dropped()).await,
            Failure::Failed { .. }
        ));
    }

    /// Writes a shell script that can run. Writing it here and running it
    /// straight away fails now and then with "Text file busy": a program
    /// another test starts at that moment holds the file open for writing
    /// until it's running. So `cp` writes the script, and has finished
    /// before anything runs it.
    fn write_script(path: &Path, script: &str) {
        use std::os::unix::fs::PermissionsExt;

        let draft = path.with_extension("draft");
        std::fs::write(&draft, format!("#!/bin/sh\n{script}")).unwrap();
        let copied = std::process::Command::new("cp")
            .arg(&draft)
            .arg(path)
            .status()
            .unwrap();
        assert!(copied.success());
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    /// A Restic that runs `script` in place of restic, in `dir`.
    fn fake_restic(dir: &Path, script: &str) -> Restic {
        let program = dir.join("restic");
        write_script(&program, script);
        Restic {
            program,
            home: dir.to_owned(),
            key_file: "key".into(),
            cache: "cache".into(),
            temp: "temp".into(),
        }
    }

    fn folder(dir: &Path) -> Repository {
        Repository::at(dir.join("repo").display().to_string())
    }

    #[tokio::test]
    async fn runs_restic_without_a_terminal() {
        let dir = tempfile::tempdir().unwrap();
        // The sixth field of stat is the session, which is the shell's own ID
        // only in a session of its own.
        let restic = fake_restic(dir.path(), "cut -d' ' -f6 /proc/$$/stat; echo $$\n");
        let output = restic.run(&folder(dir.path()), None, &[]).await.unwrap();
        let ids: Vec<_> = output.lines().collect();
        assert_eq!(ids.len(), 2);
        assert_eq!(ids[0], ids[1]);
    }

    #[tokio::test]
    async fn asks_restic_to_stop_when_a_run_is_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let restic = fake_restic(
            dir.path(),
            "trap 'kill $!; touch stopped; exit 1' TERM\ntouch started\nsleep 30 &\nwait\n",
        );
        let repository = folder(dir.path());
        let run = restic.run(&repository, None, &[]);
        let started = async {
            while !dir.path().join("started").exists() {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        };
        tokio::select! {
            _ = run => panic!("the fake restic finished by itself"),
            () = started => {}
        }
        let stopped = async {
            while !dir.path().join("stopped").exists() {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        };
        tokio::time::timeout(std::time::Duration::from_secs(10), stopped)
            .await
            .expect("restic got SIGTERM");
    }

    #[test]
    fn finds_restic_messages() {
        let stdout = "{\"message_type\":\"status\"}\nnot json\n\
                      {\"message_type\":\"summary\",\"snapshot_id\":\"abc\"}\n";
        assert_eq!(message(stdout, "summary").unwrap()["snapshot_id"], "abc");
        assert!(message(stdout, "initialized").is_none());
    }
}

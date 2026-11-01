//! Runs real programs in the sandbox, and checks that they can do their work
//! and nothing else. The tests use `sh`, `cat`, and `python3`, which every
//! supported system has, and need a kernel with Landlock.

use std::ffi::OsString;
use std::fs::File;
use std::path::Path;
use std::time::{Duration, Instant};

use accordion_sandbox::{Job, Limits, RunError, Sandbox};

const LIMITS: Limits = Limits {
    memory: 1024 * 1024 * 1024,
    cpu: Duration::from_secs(10),
    time: Duration::from_secs(20),
    output: 1024 * 1024,
};

fn sandbox() -> Sandbox {
    Sandbox::new(env!("CARGO_BIN_EXE_accordion-sandbox"))
}

/// Runs `program` with `args`, reading `/dev/null`, unless `limits` says
/// otherwise.
async fn run(program: &str, args: &[&str], limits: Limits) -> Result<String, RunError> {
    let args: Vec<OsString> = args.iter().map(OsString::from).collect();
    let output = sandbox()
        .run(Job {
            program: Path::new(program),
            args: &args,
            env: &[],
            input: File::open("/dev/null").unwrap(),
            read: &[],
            limits,
        })
        .await?;
    Ok(String::from_utf8(output).unwrap())
}

fn failure(result: Result<String, RunError>) -> String {
    match result {
        Err(RunError::Failed { message, .. }) => message,
        other => panic!("expected the worker to fail, not {other:?}"),
    }
}

#[test]
fn landlock_is_here() {
    accordion_sandbox::check().unwrap();
}

#[tokio::test]
async fn reads_its_input_and_writes_its_output() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("photo.jpg");
    std::fs::write(&input, "the photo's bytes").unwrap();
    let output = sandbox()
        .run(Job {
            program: Path::new("/usr/bin/cat"),
            args: &[],
            env: &[],
            input: File::open(&input).unwrap(),
            read: &[],
            limits: LIMITS,
        })
        .await
        .unwrap();
    assert_eq!(output, b"the photo's bytes");
}

#[tokio::test]
async fn cant_read_other_files() {
    let dir = tempfile::tempdir().unwrap();
    let database = dir.path().join("accordion.db");
    std::fs::write(&database, "everyone's accounts").unwrap();
    let message = failure(run("/usr/bin/cat", &[database.to_str().unwrap()], LIMITS).await);
    assert!(message.contains("Permission denied"), "{message}");
}

#[tokio::test]
async fn reads_the_folders_it_is_given() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("data.txt"), "allowed").unwrap();
    let args = [OsString::from(dir.path().join("data.txt"))];
    let output = sandbox()
        .run(Job {
            program: Path::new("/usr/bin/cat"),
            args: &args,
            env: &[],
            input: File::open("/dev/null").unwrap(),
            read: &[dir.path().to_owned()],
            limits: LIMITS,
        })
        .await
        .unwrap();
    assert_eq!(output, b"allowed");
}

#[tokio::test]
async fn writes_nowhere() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("planted.txt");
    let script = format!("echo planted > {}", file.display());
    let message = failure(run("/usr/bin/sh", &["-c", &script], LIMITS).await);
    assert!(message.contains("Permission denied"), "{message}");
    assert!(!file.exists());
}

#[tokio::test]
async fn runs_no_other_program() {
    let output = run(
        "/usr/bin/sh",
        &["-c", "/usr/bin/cat /dev/null 2>&1; echo done"],
        LIMITS,
    )
    .await
    .unwrap();
    assert!(output.contains("Permission denied"), "{output}");
}

#[tokio::test]
async fn makes_no_connections() {
    for family in ["AF_INET", "AF_INET6", "AF_UNIX"] {
        let script = format!("import socket; socket.socket(socket.{family})");
        let message = failure(run("/usr/bin/python3", &["-I", "-c", &script], LIMITS).await);
        assert!(
            message.contains("Operation not permitted"),
            "{family}: {message}"
        );
    }
}

#[tokio::test]
async fn signals_no_other_process() {
    let script = format!("import os; os.kill({}, 0)", std::process::id());
    let message = failure(run("/usr/bin/python3", &["-I", "-c", &script], LIMITS).await);
    assert!(message.contains("Operation not permitted"), "{message}");
}

#[tokio::test]
async fn makes_no_namespaces_but_can_use_threads() {
    let script = "import os; os.unshare(os.CLONE_NEWUSER)";
    let message = failure(run("/usr/bin/python3", &["-I", "-c", script], LIMITS).await);
    assert!(message.contains("Operation not permitted"), "{message}");

    // Image decoders use threads, which the same system calls make.
    let script =
        "import threading; t = threading.Thread(target=print, args=('ok',)); t.start(); t.join()";
    let output = run("/usr/bin/python3", &["-I", "-c", script], LIMITS)
        .await
        .unwrap();
    assert_eq!(output, "ok\n");
}

#[tokio::test]
async fn sees_only_the_environment_it_is_given() {
    let output = sandbox()
        .run(Job {
            program: Path::new("/usr/bin/sh"),
            // The shell's own list, since running env would be another
            // program.
            args: &["-c".into(), "export -p".into()],
            env: &[("VIPS_BLOCK_UNTRUSTED", "1")],
            input: File::open("/dev/null").unwrap(),
            read: &[],
            limits: LIMITS,
        })
        .await
        .unwrap();
    let output = String::from_utf8(output).unwrap();
    let names: Vec<&str> = output
        .lines()
        .filter_map(|line| line.strip_prefix("export ")?.split('=').next())
        // The shell sets its own.
        .filter(|name| !["PWD", "OLDPWD", "SHLVL", "_"].contains(name))
        .collect();
    assert_eq!(names, ["VIPS_BLOCK_UNTRUSTED"]);
}

#[tokio::test]
async fn stops_a_worker_that_takes_too_long() {
    let limits = Limits {
        time: Duration::from_secs(1),
        ..LIMITS
    };
    let started = Instant::now();
    let result = run(
        "/usr/bin/python3",
        &["-I", "-c", "import time; time.sleep(30)"],
        limits,
    )
    .await;
    assert!(matches!(result, Err(RunError::TimedOut(_))), "{result:?}");
    assert!(started.elapsed() < Duration::from_secs(5));
}

#[tokio::test]
async fn stops_a_worker_that_uses_too_much_cpu() {
    let limits = Limits {
        cpu: Duration::from_secs(1),
        ..LIMITS
    };
    let started = Instant::now();
    let result = run("/usr/bin/sh", &["-c", "while :; do :; done"], limits).await;
    assert!(matches!(result, Err(RunError::Failed { .. })), "{result:?}");
    assert!(started.elapsed() < Duration::from_secs(5));
}

#[tokio::test]
async fn stops_a_worker_that_uses_too_much_memory() {
    let limits = Limits {
        memory: 300 * 1024 * 1024,
        ..LIMITS
    };
    let script = "x = bytearray(1024 ** 3)";
    let message = failure(run("/usr/bin/python3", &["-I", "-c", script], limits).await);
    assert!(message.contains("MemoryError"), "{message}");
}

#[tokio::test]
async fn stops_a_worker_that_writes_too_much() {
    let limits = Limits {
        output: 1000,
        ..LIMITS
    };
    let script = "import sys; sys.stdout.write('x' * 100000)";
    let result = run("/usr/bin/python3", &["-I", "-c", script], limits).await;
    assert!(
        matches!(result, Err(RunError::TooLarge(1000))),
        "{result:?}"
    );
}

#[tokio::test]
async fn reads_one_file_it_is_given_and_not_the_rest_of_its_folder() {
    let dir = tempfile::tempdir().unwrap();
    let given = dir.path().join("given.jpg");
    let other = dir.path().join("other.jpg");
    std::fs::write(&given, "allowed").unwrap();
    std::fs::write(&other, "someone else's").unwrap();
    let run_cat = |path: &Path| {
        let args = [OsString::from(path)];
        let read = [given.clone()];
        async move {
            sandbox()
                .run(Job {
                    program: Path::new("/usr/bin/cat"),
                    args: &args,
                    env: &[],
                    input: File::open("/dev/null").unwrap(),
                    read: &read,
                    limits: LIMITS,
                })
                .await
        }
    };
    assert_eq!(run_cat(&given).await.unwrap(), b"allowed");
    let result = run_cat(&other).await;
    assert!(matches!(result, Err(RunError::Failed { .. })), "{result:?}");
}

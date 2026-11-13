//! Runs real programs in the sandbox, and checks that they can do their work
//! and nothing else. The tests use `sh`, `cat`, and `python3`, which every
//! supported system has, and need a kernel with Landlock.

use std::ffi::OsString;
use std::fs::File;
use std::path::Path;
use std::time::{Duration, Instant};

use accordion_sandbox::{Folder, Job, Limits, RunError, Sandbox};

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
            folder: None,
            sockets: None,
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
            folder: None,
            sockets: None,
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
            folder: None,
            sockets: None,
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
            folder: None,
            sockets: None,
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
async fn reads_random_numbers_but_not_other_devices() {
    // Random bytes aren't text, so od shows them as numbers.
    let random = run(
        "/usr/bin/od",
        &["-An", "-N", "16", "-tx1", "/dev/urandom"],
        LIMITS,
    )
    .await
    .unwrap();
    assert_eq!(random.split_whitespace().count(), 16, "{random}");
    let other = run("/usr/bin/od", &["-An", "-N", "16", "/dev/zero"], LIMITS).await;
    assert!(matches!(other, Err(RunError::Failed { .. })), "{other:?}");
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
                    folder: None,
                    sockets: None,
                    limits: LIMITS,
                })
                .await
        }
    };
    assert_eq!(run_cat(&given).await.unwrap(), b"allowed");
    let result = run_cat(&other).await;
    assert!(matches!(result, Err(RunError::Failed { .. })), "{result:?}");
}

/// Runs a Python script that may write in `folder` and make sockets in
/// `sockets`.
async fn run_python(
    script: &str,
    folder: Option<&Folder>,
    sockets: Option<&Path>,
) -> Result<String, RunError> {
    let args = ["-I".into(), "-c".into(), OsString::from(script)];
    let output = sandbox()
        .run(Job {
            program: Path::new("/usr/bin/python3"),
            args: &args,
            env: &[],
            input: File::open("/dev/null").unwrap(),
            read: &[],
            folder,
            sockets,
            limits: LIMITS,
        })
        .await?;
    Ok(String::from_utf8(output).unwrap())
}

#[tokio::test]
async fn writes_in_its_scratch_folder_but_runs_nothing_from_it() {
    let dir = tempfile::tempdir().unwrap();
    let folder = sandbox().folder(dir.path()).await.unwrap();
    let scratch = folder.path().to_owned();
    let script = format!(
        r#"
import os, shutil
os.chdir({scratch:?})
os.mkdir("profile")
with open("profile/settings.xml", "w") as f: f.write("kept")
os.rename("profile/settings.xml", "settings.xml")
print(open("settings.xml").read())
try:
    os.symlink("/etc/passwd", "link")
except PermissionError:
    print("can't link")
shutil.copy("/usr/bin/true", "true")
os.chmod("true", 0o755)
try:
    os.execv("true", ["true"])
except PermissionError:
    print("can't run it")
"#,
        scratch = scratch.to_str().unwrap()
    );
    let output = run_python(&script, Some(&folder), None).await.unwrap();
    assert_eq!(output, "kept\ncan't link\ncan't run it\n");

    let outside = dir.path().join("planted.txt");
    let script = format!("open({:?}, 'w')", outside.to_str().unwrap());
    let message = failure(run_python(&script, Some(&folder), None).await);
    assert!(message.contains("PermissionError"), "{message}");
    assert!(!outside.exists());
}

#[tokio::test]
async fn listens_on_a_local_socket_but_connects_to_nothing() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("secret.txt"), "someone else's").unwrap();
    // Another program's socket, in the same folder.
    let other = std::os::unix::net::UnixListener::bind(dir.path().join("other")).unwrap();
    let script = format!(
        r#"
import os, socket
os.chdir({dir:?})
own = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
own.bind("own")
own.listen()
print("listening")
for attempt in [
    lambda: socket.socket(socket.AF_UNIX).connect("other"),
    lambda: socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM),
    lambda: socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET),
    lambda: socket.socket(socket.AF_INET),
    lambda: open("secret.txt").read(),
]:
    try:
        attempt()
        print("allowed")
    except PermissionError:
        print("refused")
"#,
        dir = dir.path().to_str().unwrap()
    );
    let output = run_python(&script, None, Some(dir.path())).await.unwrap();
    assert_eq!(
        output,
        "listening\nrefused\nrefused\nrefused\nrefused\nrefused\n"
    );
    drop(other);
}

#[tokio::test]
async fn cant_make_memory_both_writable_and_runnable() {
    // As the service can't, through systemd's MemoryDenyWriteExecute.
    let script =
        "import mmap; mmap.mmap(-1, 4096, prot=mmap.PROT_READ | mmap.PROT_WRITE | mmap.PROT_EXEC)";
    let message = failure(run("/usr/bin/python3", &["-I", "-c", script], LIMITS).await);
    assert!(message.contains("PermissionError"), "{message}");
}

//! Runs jobs through the worker service (decision 29). Most tests run it as
//! the test's own user, as in development, so they check the connection,
//! job folders, and stopping jobs, with Landlock as the sandbox.
//!
//! The tests named `..._user_of_its_own...` check the users themselves.
//! Switching users needs root, so they run only when
//! `ACCORDION_TEST_JOB_USERS=1` is set, such as with `sudo` in CI.

use std::ffi::OsString;
use std::fs::File;
use std::ops::Range;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use accordion_sandbox::service::{self, Config};
use accordion_sandbox::{Job, Limits, RunError, Sandbox};

const LIMITS: Limits = Limits {
    memory: 1024 * 1024 * 1024,
    cpu: Duration::from_secs(10),
    time: Duration::from_secs(20),
    output: 1024 * 1024,
};

const LAUNCHER: &str = env!("CARGO_BIN_EXE_accordion-sandbox");

/// A worker service in a thread, with a data folder of its own.
struct Running {
    data: tempfile::TempDir,
    sandbox: Sandbox,
}

impl Running {
    fn start(uids: Option<Range<u32>>) -> Self {
        let data = tempfile::tempdir().unwrap();
        std::fs::set_permissions(data.path(), PermissionsExt::from_mode(0o700)).unwrap();
        let jobs = data.path().join("jobs");
        std::fs::create_dir(&jobs).unwrap();
        std::fs::set_permissions(&jobs, PermissionsExt::from_mode(0o711)).unwrap();
        // The socket isn't in the data folder, as on a server.
        let socket = std::env::temp_dir().join(format!(
            "accordion-workers-{}-{}.sock",
            std::process::id(),
            rand_suffix()
        ));
        let listener = service::listen(&socket).unwrap();
        let config = Config {
            listener,
            jobs: jobs.clone(),
            uids,
            launcher: PathBuf::from(LAUNCHER),
            programs: vec![PathBuf::from("/usr"), PathBuf::from(LAUNCHER)],
        };
        std::thread::spawn(move || service::serve(config));
        let sandbox = Sandbox::new(LAUNCHER).through_service(&socket, &jobs);
        Self { data, sandbox }
    }

    fn jobs(&self) -> PathBuf {
        self.data.path().join("jobs")
    }
}

/// A part of the job users' IDs of its own for each test. Tests run at the
/// same time, and a job's user stops every process of that user when its
/// work is done, so two tests sharing a user would stop each other's jobs.
fn job_uids(test: u32) -> Range<u32> {
    let start = service::JOB_UIDS.start + test * 16;
    start..start + 16
}

fn rand_suffix() -> u64 {
    use std::hash::{BuildHasher, RandomState};
    RandomState::new().hash_one(std::time::SystemTime::now())
}

fn args(args: &[&str]) -> Vec<OsString> {
    args.iter().map(OsString::from).collect()
}

fn job<'a>(program: &'a str, args: &'a [OsString]) -> Job<'a> {
    Job {
        program: Path::new(program),
        args,
        env: &[],
        input: File::open("/dev/null").unwrap(),
        read: &[],
        folder: None,
        sockets: None,
        limits: LIMITS,
    }
}

#[tokio::test]
async fn runs_a_job_and_returns_its_output() {
    let running = Running::start(None);
    running.sandbox.check().unwrap();
    let input = running.data.path().join("photo.jpg");
    std::fs::write(&input, "the photo's bytes").unwrap();
    let no_args = [];
    let output = running
        .sandbox
        .run(Job {
            input: File::open(&input).unwrap(),
            ..job("/usr/bin/cat", &no_args)
        })
        .await
        .unwrap();
    assert_eq!(output, b"the photo's bytes");
}

#[tokio::test]
async fn reads_a_stored_file_by_name_through_its_folder() {
    let running = Running::start(None);
    let stored = running.data.path().join("files");
    std::fs::create_dir(&stored).unwrap();
    let file = stored.join("1f3a");
    std::fs::write(&file, "the video").unwrap();
    let args = [OsString::from(&file)];
    let output = running
        .sandbox
        .run(Job {
            read: std::slice::from_ref(&file),
            ..job("/usr/bin/cat", &args)
        })
        .await
        .unwrap();
    assert_eq!(output, b"the video");
    // Nothing is left behind.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(std::fs::read_dir(running.jobs()).unwrap().count(), 0);
    assert_eq!(std::fs::read_to_string(&file).unwrap(), "the video");
}

#[tokio::test]
async fn writes_in_its_folder_for_the_server_to_read() {
    let running = Running::start(None);
    let folder = running.sandbox.folder(Path::new("/")).await.unwrap();
    assert!(folder.path().starts_with(running.jobs()));
    let made = folder.path().join("out.txt");
    let script = format!("open({:?}, 'w').write('made')", made.to_str().unwrap());
    let args = args(&["-I", "-c", &script]);
    running
        .sandbox
        .run(Job {
            folder: Some(&folder),
            ..job("/usr/bin/python3", &args)
        })
        .await
        .unwrap();
    assert_eq!(std::fs::read_to_string(&made).unwrap(), "made");

    // A second job in the same folder sees the first one's work.
    let read = [OsString::from(&made)];
    let output = running
        .sandbox
        .run(Job {
            folder: Some(&folder),
            ..job("/usr/bin/cat", &read)
        })
        .await
        .unwrap();
    assert_eq!(output, b"made");

    let path = folder.path().to_owned();
    drop(folder);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!path.exists());
}

#[tokio::test]
async fn stops_a_job_that_takes_too_long_and_runs_the_next() {
    let running = Running::start(None);
    let folder = running.sandbox.folder(Path::new("/")).await.unwrap();
    let sleep = args(&["30"]);
    let started = std::time::Instant::now();
    let result = running
        .sandbox
        .run(Job {
            folder: Some(&folder),
            limits: Limits {
                time: Duration::from_millis(500),
                ..LIMITS
            },
            ..job("/usr/bin/sleep", &sleep)
        })
        .await;
    assert!(matches!(result, Err(RunError::TimedOut(_))), "{result:?}");
    assert!(started.elapsed() < Duration::from_secs(10));

    let echo = args(&["next"]);
    let output = running
        .sandbox
        .run(Job {
            folder: Some(&folder),
            ..job("/usr/bin/echo", &echo)
        })
        .await
        .unwrap();
    assert_eq!(output, b"next\n");
}

#[tokio::test]
async fn cuts_off_a_long_output() {
    let running = Running::start(None);
    let script = args(&[
        "-I",
        "-c",
        "import sys\nwhile True: sys.stdout.write('x' * 4096)",
    ]);
    let output = running
        .sandbox
        .run_up_to(Job {
            limits: Limits {
                output: 10_000,
                ..LIMITS
            },
            ..job("/usr/bin/python3", &script)
        })
        .await
        .unwrap();
    assert_eq!(output.len(), 10_000);
}

#[tokio::test]
async fn runs_only_the_systems_programs() {
    let running = Running::start(None);
    let planted = running.data.path().join("planted");
    std::fs::copy("/usr/bin/true", &planted).unwrap();
    let no_args = [];
    let result = running
        .sandbox
        .run(job(planted.to_str().unwrap(), &no_args))
        .await;
    let message = result.unwrap_err().to_string();
    assert!(message.contains("isn't a program"), "{message}");
}

#[tokio::test]
async fn runs_each_connection_as_a_user_of_its_own() {
    if std::env::var_os("ACCORDION_TEST_JOB_USERS").is_none() {
        return;
    }
    let running = Running::start(Some(job_uids(0)));
    // On a server, the worker service sees only jobs/. Here it sees the
    // whole data folder, so jobs need to pass through it, without reading it.
    std::fs::set_permissions(running.data.path(), PermissionsExt::from_mode(0o711)).unwrap();
    running.sandbox.check().unwrap();
    // The server's files, which a job mustn't reach, even through /proc.
    let secret = running.data.path().join("accordion.db");
    std::fs::write(&secret, "everyone's accounts").unwrap();
    std::fs::set_permissions(&secret, PermissionsExt::from_mode(0o600)).unwrap();
    let stored = running.data.path().join("1f3a");
    std::fs::write(&stored, "the video").unwrap();
    std::fs::set_permissions(&stored, PermissionsExt::from_mode(0o600)).unwrap();

    let script = format!(
        r#"
import os
print(os.getuid() >= 2000000000)
print(open({stored:?}).read())
for path in [{secret:?}, {data:?}]:
    try:
        open(path).read() if path.endswith(".db") else os.listdir(path)
        print("allowed")
    except PermissionError:
        print("refused")
"#,
        stored = "STORED",
        secret = secret.to_str().unwrap(),
        data = running.data.path().to_str().unwrap(),
    );
    let folder = running.sandbox.folder(Path::new("/")).await.unwrap();
    let linked = folder.link(&stored, "video").unwrap();
    let script = script.replace("STORED", linked.to_str().unwrap());
    let args = args(&["-I", "-c", &script]);
    let output = running
        .sandbox
        .run(Job {
            folder: Some(&folder),
            ..job("/usr/bin/python3", &args)
        })
        .await
        .unwrap();
    assert_eq!(
        String::from_utf8(output).unwrap(),
        "True\nthe video\nrefused\nrefused\n"
    );
}

/// What LibreOffice does when memory can't be both writable and runnable:
/// it maps the code it makes twice through a file, writable and runnable.
/// A job's user has no home folder, so the file goes in /tmp.
const DOUBLE_MAP: &str = r#"
import mmap, os, tempfile
fd, path = tempfile.mkstemp(dir="/tmp", prefix=".execooo")
os.unlink(path)
os.ftruncate(fd, mmap.PAGESIZE)
mmap.mmap(fd, mmap.PAGESIZE, mmap.MAP_SHARED, mmap.PROT_READ | mmap.PROT_WRITE)
mmap.mmap(fd, mmap.PAGESIZE, mmap.MAP_SHARED, mmap.PROT_READ | mmap.PROT_EXEC)
print("mapped")
"#;

#[tokio::test]
async fn a_user_of_its_own_makes_files_where_it_makes_sockets() {
    if std::env::var_os("ACCORDION_TEST_JOB_USERS").is_none() {
        return;
    }
    let running = Running::start(Some(job_uids(1)));
    std::fs::set_permissions(running.data.path(), PermissionsExt::from_mode(0o711)).unwrap();
    let args = args(&["-I", "-c", DOUBLE_MAP]);
    let output = running
        .sandbox
        .run(Job {
            sockets: Some(Path::new("/tmp")),
            ..job("/usr/bin/python3", &args)
        })
        .await
        .unwrap();
    assert_eq!(String::from_utf8(output).unwrap(), "mapped\n");
    // Without sockets there, nothing may be made there.
    let result = running.sandbox.run(job("/usr/bin/python3", &args)).await;
    let message = result
        .expect_err("a job without sockets made a file in /tmp")
        .to_string();
    assert!(message.contains("PermissionError"), "{message}");
}

/// LibreOffice's first start, as the server makes its settings, crashed as
/// a job's user before it could make files in /tmp: Ubuntu 24.04's
/// LibreOffice 24.2 maps code through a file there as it starts.
#[tokio::test]
async fn libreoffice_starts_as_a_user_of_its_own() {
    const SOFFICE: &str = "/usr/lib/libreoffice/program/soffice.bin";
    if std::env::var_os("ACCORDION_TEST_JOB_USERS").is_none() || !Path::new(SOFFICE).is_file() {
        return;
    }
    let running = Running::start(Some(job_uids(2)));
    std::fs::set_permissions(running.data.path(), PermissionsExt::from_mode(0o711)).unwrap();
    let folder = running.sandbox.folder(Path::new("/")).await.unwrap();
    let home = folder.path().to_str().unwrap().to_owned();
    let profile = folder.path().join("profile");
    let args = args(&[
        &format!("-env:UserInstallation=file://{}", profile.display()),
        "--headless",
        "--norestore",
        "--terminate_after_init",
    ]);
    let env = [("HOME", home.as_str()), ("TMPDIR", home.as_str())];
    let read = [
        "/etc/libreoffice",
        "/etc/fonts",
        "/var/cache/fontconfig",
        "/etc/passwd",
    ]
    .map(PathBuf::from);
    let result = running
        .sandbox
        .run(Job {
            env: &env,
            read: &read,
            folder: Some(&folder),
            sockets: Some(Path::new("/tmp")),
            limits: Limits {
                cpu: Duration::from_secs(60),
                time: Duration::from_secs(90),
                ..LIMITS
            },
            ..job(SOFFICE, &args)
        })
        .await;
    if let Err(err) = result {
        panic!("LibreOffice didn't start: {err}");
    }
    assert!(
        profile.join("user").is_dir(),
        "LibreOffice made no settings"
    );
}

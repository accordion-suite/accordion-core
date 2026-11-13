//! With `ACCORDION_TEST_WORKERS=1`, the thumbnail and search tests send
//! every job through a worker service (decision 29), running as the test's
//! own user, so each kind of worker is tried both ways.

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use accordion_sandbox::Workers;
use accordion_sandbox::service::{self, Config};

const LAUNCHER: &str = env!("CARGO_BIN_EXE_accordion-core-sandbox");

/// A worker service for the data folder `data`, if the tests use one.
pub fn workers(data: &Path) -> Option<Workers> {
    std::env::var_os("ACCORDION_TEST_WORKERS")?;
    let jobs = data.join("jobs");
    std::fs::create_dir_all(&jobs).unwrap();
    std::fs::set_permissions(&jobs, PermissionsExt::from_mode(0o711)).unwrap();
    let socket = data.join("workers.sock");
    let listener = service::listen(&socket).unwrap();
    let config = Config {
        listener,
        jobs: jobs.clone(),
        uids: None,
        launcher: PathBuf::from(LAUNCHER),
        programs: vec![PathBuf::from("/usr"), PathBuf::from(LAUNCHER)],
    };
    std::thread::spawn(move || service::serve(config));
    Some(Workers { socket, jobs })
}

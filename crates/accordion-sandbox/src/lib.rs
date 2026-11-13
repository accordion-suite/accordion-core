//! Runs programs that read untrusted files, such as image decoders, in a
//! sandbox (decision 14 in the accordion repo's `docs/decisions`).
//!
//! The server starts its own program as a launcher, `accordion sandbox`,
//! with the file open on standard input and a pipe on standard output. The
//! launcher shuts itself in, then runs the worker program, which inherits
//! every limit:
//!
//! - Landlock lets it read only the folders it's given, such as `/usr`, run
//!   only its own program, write only in a scratch folder it's given, if
//!   any, and make no TCP connections.
//! - A seccomp filter refuses sockets, tracing, namespaces, signals to other
//!   processes, and io_uring, which seccomp can't see into. A worker that
//!   needs a local socket of its own, as LibreOffice does, can make one and
//!   listen on it, but never connect.
//! - Resource limits cap its writable memory and CPU time, and it runs at
//!   low priority. It can't make memory both writable and runnable, as the
//!   service can't.
//!
//! The server's side, [`Sandbox::run`], gives it an empty environment except
//! for what the job sets, stops it when it runs too long or writes too much,
//! and starts it in its own process group, so stopping it stops everything
//! it started. A worker that outlives the server is stopped by systemd with
//! the rest of the service, or by its CPU limit.
//!
//! Run by the server itself, the worker is the server's own user, so
//! without Landlock there's no sandbox worth having: [`check`] says whether
//! it's there, and the launcher refuses to run without it.
//!
//! On a server, workers go through the worker service instead ([`service`],
//! decision 29). It runs each connection's jobs as a user ID of their own,
//! which owns nothing and can't see the data folder beyond the job's own
//! folder, with no network, so Landlock is one more layer where the kernel
//! has it.

mod acl;
mod launch;
mod run;
pub mod service;
mod wire;

pub use launch::launch;

/// A whole program that's only the launcher, such as the ones tests use:
/// `<program> sandbox <limits> -- <program> [<argument>]...`.
pub fn main() -> ! {
    let mut args = std::env::args_os().skip(1);
    if args.next().is_none_or(|command| command != "sandbox") {
        eprintln!(
            "Usage: {} sandbox --memory <bytes> --cpu <seconds> [--read <path>]... [--write \
             <folder>]... [--sockets <folder>] -- <program> [<argument>]...",
            std::env::args().next().unwrap_or_default()
        );
        std::process::exit(2);
    }
    std::process::exit(launch(args));
}
pub use run::{Folder, Job, Limits, RunError, Sandbox, Workers};

/// Why the sandbox can't run on this system.
#[derive(Debug, thiserror::Error)]
pub enum Unavailable {
    #[error(
        "this system's kernel doesn't have Landlock, which keeps workers away from Accordion's \
         data, so Accordion doesn't run them. Landlock is in Linux 5.13 and later, and some \
         systems turn it off at boot"
    )]
    NoLandlock,
    #[error("the worker service, which runs workers, isn't answering: {0}")]
    NoService(String),
    #[error(
        "the data folder's file system doesn't have POSIX ACLs, which let each worker reach only \
         its own files"
    )]
    NoAcls,
}

/// Whether workers can run in the sandbox here.
pub fn check() -> Result<(), Unavailable> {
    // Asks the kernel for its Landlock version. It fails if the kernel has
    // no Landlock, or has it turned off.
    const LANDLOCK_CREATE_RULESET_VERSION: libc::c_uint = 1;
    let version = unsafe {
        libc::syscall(
            libc::SYS_landlock_create_ruleset,
            std::ptr::null::<libc::c_void>(),
            0usize,
            LANDLOCK_CREATE_RULESET_VERSION,
        )
    };
    if version >= 1 {
        Ok(())
    } else {
        Err(Unavailable::NoLandlock)
    }
}

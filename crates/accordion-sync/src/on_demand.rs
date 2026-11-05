//! Files on demand (decision 23): on Windows and macOS, a file that arrives
//! from the server is a placeholder, with its name, size, and date, and its
//! contents download when something opens it. The system feature for that,
//! Cloud Files on Windows and File Provider on macOS, lives in the desktop
//! app, which hands the engine an [`OnDemand`].
//!
//! Without one, as on Linux, the engine downloads every file.

use std::io;
use std::path::Path;

use crate::client::RemoteItem;

/// What the system knows about a file in the synced folder, without
/// reading it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    /// An ordinary file, not a placeholder. Its contents are here, so the
    /// engine compares them to see whether it changed.
    Ordinary,
    /// A placeholder that matches the server, as last marked.
    InSync,
    /// A placeholder whose contents aren't on this computer, so they can't
    /// have changed here.
    NotHere,
    /// A placeholder that changed since it last matched the server.
    Changed,
}

/// Places files on demand, instead of the engine downloading them.
pub trait OnDemand: Send + Sync {
    /// Puts a placeholder for `item` at `at`, a file or folder, or brings
    /// the placeholder there up to date with it, letting go of old
    /// contents. The folder `at` is in exists. A file kept on this device
    /// starts downloading again.
    fn place(&self, item: &RemoteItem, at: &Path) -> io::Result<()>;

    /// Marks the file or folder at `at` as matching `item` on the server,
    /// such as after uploading it, or when it was already the same. Its
    /// contents stay.
    fn synced(&self, item: &RemoteItem, at: &Path) -> io::Result<()>;

    /// What the system knows about the file at `at`.
    fn status(&self, at: &Path) -> Status;
}

//! What every Accordion module needs to plug into the server: the
//! [`Module`] trait, core's services, sign-in checks, and error responses.
//! Decision 7 in the `accordion` repo's `docs/decisions` explains the
//! design.
//!
//! It's a crate of its own so that `accordion-core` has no web code, and a
//! command-line tool or a sandboxed worker can use core without it.

pub mod auth;
mod error;

pub use auth::{Authenticated, DavUser};
pub use error::ApiError;

use std::error::Error as StdError;
use std::pin::Pin;

use accordion_core::db::Database;
use accordion_core::identity::Identity;
use accordion_core::storage::Storage;
use axum::Router;
use tokio::sync::watch;

pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// Why a module couldn't start, for the server log.
pub type StartError = Box<dyn StdError + Send + Sync>;

/// A module, such as Drive or Notes.
///
/// The server keeps a list of every module and starts only the ones that are
/// on. A module that's off is never started, so it runs nothing and answers
/// no requests.
pub trait Module: Send + Sync + 'static {
    /// Lowercase letters, such as `drive`. The ID names the module's URLs and
    /// its migrations, so it never changes.
    fn id(&self) -> &'static str;

    /// The name the **Modules** page shows, such as "Drive".
    fn name(&self) -> &'static str;

    /// The IDs of modules that must be on for this one to work. Docs and
    /// Notes require Drive, because their files live there.
    fn requires(&self) -> &'static [&'static str] {
        &[]
    }

    /// Starts the module: runs its migrations, then returns its routes and
    /// its background work. The server calls it again each time the module
    /// is turned back on.
    fn start(
        &self,
        services: Services,
        stopping: Stopping,
    ) -> BoxFuture<Result<Started, StartError>>;
}

/// Core's services. Modules use them only through their public APIs.
#[derive(Clone)]
pub struct Services {
    /// For the module's own tables. Migrate them with the module's ID as the
    /// owner, and start each table's name with it, such as `drive_shares`.
    pub db: Database,
    pub identity: Identity,
    pub storage: Storage,
}

/// What a module gives the server when it starts.
#[derive(Default)]
pub struct Started {
    pub routes: Routes,
    /// Work the module does in the background. It should finish soon after
    /// [`Stopping`] fires.
    pub background: Option<BoxFuture<()>>,
}

/// A module's routes, each set with its state already applied. The server
/// mounts each set under a path named after the module's ID, so no module
/// can claim another's paths.
#[derive(Default)]
pub struct Routes {
    /// The JSON API for the web app and the desktop and mobile apps, at
    /// `/api/<id>`.
    pub api: Option<Router>,
    /// WebDAV, CalDAV, or CardDAV for standard clients, at `/dav/<id>`.
    pub dav: Option<Router>,
    /// The module's pages in the web app, at `/<id>`.
    pub pages: Option<Router>,
}

/// Fires when the server stops, or when the admin turns the module off.
#[derive(Clone)]
pub struct Stopping(watch::Receiver<bool>);

impl Stopping {
    /// Fires once `receiver` sees `true`, or its sender is dropped.
    pub fn new(receiver: watch::Receiver<bool>) -> Self {
        Self(receiver)
    }

    /// Waits until it's time to stop.
    pub async fn wait(mut self) {
        let _ = self.0.wait_for(|&stopping| stopping).await;
    }
}

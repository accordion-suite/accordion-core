//! Core services that hold state: identity, search, jobs, storage,
//! thumbnails, backups, and co-editing.
//!
//! Modules use core only through this crate's public API. They never read
//! core's database tables directly, so any module can move into its own
//! process later. See decision 1 in the `accordion` repo's `docs/decisions`.

pub mod backup;
pub mod coedit;
pub mod db;
pub mod events;
pub mod identity;
pub mod live;
mod random;
pub mod storage;
pub mod texts;
pub mod thumbnails;
mod time;

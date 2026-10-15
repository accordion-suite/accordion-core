//! Core services that hold state: identity, search, jobs, and storage.
//!
//! Modules use core only through this crate's public API. They never read
//! core's database tables directly, so any module can move into its own
//! process later. See decision 1 in the `accordion` repo's `docs/decisions`.

pub mod db;

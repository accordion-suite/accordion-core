//! The sync engine (decision 23 in the `accordion` repo's `docs/decisions`).
//!
//! It keeps a folder on a device in sync with one account on one Accordion
//! server, following the server's list of changes. The command-line client,
//! the desktop app, and the phone apps all use it, and each account gets
//! its own engine, so one device can sync several servers at once.

pub mod client;
pub mod engine;
pub mod state;

pub use client::{Client, Error};
pub use engine::{Engine, Report};
pub use state::State;

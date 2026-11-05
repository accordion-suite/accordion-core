//! The sync engine (decision 23 in the `accordion` repo's `docs/decisions`).
//!
//! It keeps a folder on a device in sync with one account on one Accordion
//! server, following the server's list of changes. The command-line client,
//! the desktop app, and the phone apps all use it, and each account gets
//! its own engine, so one device can sync several servers at once.
//!
//! Each account also has its own tunnel ([`tunnel`]), so it reaches its
//! server away from home too, and the client picks the way that answers.

pub mod add;
pub mod client;
pub mod engine;
pub mod oauth;
mod route;
pub mod state;
pub mod tunnel;

pub use client::{Client, Error, Reach};
pub use engine::{Engine, Report};
pub use route::Via;
pub use state::State;

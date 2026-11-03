//! Live updates (decision 19 in the `accordion` repo's `docs/decisions`).
//!
//! Each client keeps a stream open, and core sends it hints about what
//! changed, such as "the list of this folder changed", for the client to
//! load again through the usual addresses. Hints name only IDs, and go only
//! to people who can open what they point to.
//!
//! Core sends hints for its own changes once they commit, and modules send
//! theirs with [`Live::send`]. Hints that arrive close together are combined
//! into one message for each kind, so an upload of 200 files is one reload.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use accordion_types::{ItemId, SessionId, UserId};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{broadcast, mpsc};
use tokio::time::Instant;

use crate::db::Database;
use crate::identity::Identity;
use crate::storage;

/// The lists of these folders changed: items were added, changed, moved,
/// or removed, or a thumbnail is ready.
pub const DRIVE_ITEMS: &str = "drive.items";
/// What's shared with this person changed.
pub const DRIVE_SHARED: &str = "drive.shared";
/// This person's notifications changed: one arrived, or they read or
/// removed some in another tab.
pub const NOTIFICATIONS: &str = "notifications";

/// How long hints are gathered before they're sent.
pub const COMBINE_FOR: Duration = Duration::from_millis(500);
/// How many streams one person can have open. Opening one more closes
/// their oldest, so a broken client can't fill the server's memory.
pub const STREAMS_EACH: usize = 20;
/// How many messages can wait for a stream before it counts as too slow
/// and is closed. The client reconnects and loads everything again.
const WAITING: usize = 64;

/// Something that changed, and who should hear about it.
#[derive(Clone, Debug)]
pub struct Hint {
    pub kind: &'static str,
    /// What changed, such as a folder's ID, if the kind needs one.
    pub id: Option<String>,
    pub audience: Audience,
}

/// Who hears a hint.
#[derive(Clone, Debug)]
pub enum Audience {
    /// Everyone who can open the item when the hint is sent.
    Item(ItemId),
    /// These people.
    People(Vec<UserId>),
}

impl Hint {
    /// A hint about `item`, for everyone who can open it.
    pub fn item(kind: &'static str, item: ItemId) -> Self {
        Self {
            kind,
            id: Some(item.to_string()),
            audience: Audience::Item(item),
        }
    }

    /// A hint for one person, with no ID.
    pub fn person(kind: &'static str, user: UserId) -> Self {
        Self {
            kind,
            id: None,
            audience: Audience::People(vec![user]),
        }
    }
}

/// What a stream receives: a kind, and every ID its hints named, each once.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    pub kind: &'static str,
    pub ids: Vec<String>,
}

/// The open streams, and what sends to them. Cloning it is cheap, and every
/// clone shares the same streams.
#[derive(Clone)]
pub struct Live {
    inner: Arc<Inner>,
}

struct Inner {
    db: Database,
    streams: Mutex<Streams>,
    /// Taken by [`Live::run`]. Subscribed when `Live` is made, so nothing
    /// that commits before it runs is missed.
    receivers: Mutex<Option<Receivers>>,
}

struct Receivers {
    hints: broadcast::Receiver<Hint>,
    ended: broadcast::Receiver<SessionId>,
}

/// Oldest first.
#[derive(Default)]
struct Streams {
    open: Vec<Open>,
    /// Set when the server stops, so no stream opens after that.
    stopped: bool,
}

struct Open {
    user: UserId,
    session: Option<SessionId>,
    sender: mpsc::Sender<Message>,
}

/// One client's stream. It ends when the session ends, when the client
/// falls too far behind, or when the server stops.
pub struct LiveStream {
    receiver: mpsc::Receiver<Message>,
}

impl LiveStream {
    /// The next message, or `None` once the stream has ended.
    pub async fn next(&mut self) -> Option<Message> {
        self.receiver.recv().await
    }
}

impl Live {
    pub fn new(db: Database, identity: &Identity) -> Self {
        let receivers = Receivers {
            hints: db.hints(),
            ended: identity.ended_sessions(),
        };
        Self {
            inner: Arc::new(Inner {
                db,
                streams: Mutex::default(),
                receivers: Mutex::new(Some(receivers)),
            }),
        }
    }

    /// Opens a stream for `user`, signed in with `session`. Ending the
    /// session ends the stream.
    pub fn open(&self, user: UserId, session: Option<SessionId>) -> LiveStream {
        let (sender, receiver) = mpsc::channel(WAITING);
        let mut streams = self.streams();
        if !streams.stopped {
            streams.open.retain(|open| !open.sender.is_closed());
            let theirs = streams.open.iter().filter(|open| open.user == user).count();
            if theirs >= STREAMS_EACH
                && let Some(oldest) = streams.open.iter().position(|open| open.user == user)
            {
                streams.open.remove(oldest);
            }
            streams.open.push(Open {
                user,
                session,
                sender,
            });
        }
        LiveStream { receiver }
    }

    /// Sends a hint about a change that has committed. Send it right after
    /// the change, before answering the request.
    pub fn send(&self, hint: Hint) {
        self.inner.db.send_hint(hint);
    }

    /// How many streams are open, for tests and the health page.
    pub fn open_streams(&self) -> usize {
        let mut streams = self.streams();
        streams.open.retain(|open| !open.sender.is_closed());
        streams.open.len()
    }

    /// Sends hints to streams until `stopped` completes, then ends every
    /// stream so clients reconnect to the next server as soon as it's up.
    ///
    /// # Panics
    ///
    /// If it's run twice.
    pub async fn run(self, stopped: impl Future<Output = ()>) {
        let Receivers {
            mut hints,
            mut ended,
        } = self
            .inner
            .receivers
            .lock()
            .expect("never poisoned")
            .take()
            .expect("live updates run once");
        let mut stopped = std::pin::pin!(stopped);
        let mut pending: Vec<Hint> = Vec::new();
        let mut deadline = Instant::now();
        let mut sessions_open = true;
        loop {
            tokio::select! {
                () = &mut stopped => break,
                hint = hints.recv() => match hint {
                    Ok(hint) => {
                        if pending.is_empty() {
                            deadline = Instant::now() + COMBINE_FOR;
                        }
                        pending.push(hint);
                    }
                    // Hints were missed, so every client reconnects and
                    // loads what it shows again.
                    Err(RecvError::Lagged(missed)) => {
                        tracing::warn!(missed, "live updates fell behind");
                        pending.clear();
                        self.close(|_| true);
                    }
                    Err(RecvError::Closed) => break,
                },
                session = ended.recv(), if sessions_open => match session {
                    Ok(session) => self.close(|open| open.session == Some(session)),
                    // Some ended sessions were missed, so every stream that
                    // belongs to one closes, and its client signs in again
                    // if it has to.
                    Err(RecvError::Lagged(_)) => self.close(|open| open.session.is_some()),
                    Err(RecvError::Closed) => sessions_open = false,
                },
                () = tokio::time::sleep_until(deadline), if !pending.is_empty() => {
                    self.deliver(std::mem::take(&mut pending)).await;
                }
            }
        }
        let mut streams = self.streams();
        streams.stopped = true;
        streams.open.clear();
    }

    /// Works out who hears each hint, and sends each stream one message for
    /// each kind.
    async fn deliver(&self, hints: Vec<Hint>) {
        let items: BTreeSet<ItemId> = hints
            .iter()
            .filter_map(|hint| match hint.audience {
                Audience::Item(item) => Some(item),
                Audience::People(_) => None,
            })
            .collect();
        let readers = if items.is_empty() {
            HashMap::new()
        } else {
            let found = self
                .inner
                .db
                .read(move |conn| {
                    items
                        .into_iter()
                        .map(|item| Ok((item, storage::people_with_access(conn, item)?)))
                        .collect::<Result<HashMap<_, _>, storage::Error>>()
                })
                .await;
            match found {
                Ok(readers) => readers,
                Err(err) => {
                    // Without knowing who may hear them, the hints go
                    // nowhere, and every client loads what it shows again.
                    tracing::warn!(%err, "couldn't work out who hears live updates");
                    self.close(|_| true);
                    return;
                }
            }
        };

        let mut messages: HashMap<UserId, BTreeMap<&'static str, BTreeSet<String>>> =
            HashMap::new();
        for hint in &hints {
            let people = match &hint.audience {
                Audience::Item(item) => readers.get(item).map(Vec::as_slice).unwrap_or_default(),
                Audience::People(people) => people.as_slice(),
            };
            for &user in people {
                let ids = messages
                    .entry(user)
                    .or_default()
                    .entry(hint.kind)
                    .or_default();
                ids.extend(hint.id.clone());
            }
        }

        self.streams().open.retain(|open| {
            if let Some(kinds) = messages.get(&open.user) {
                for (&kind, ids) in kinds {
                    let message = Message {
                        kind,
                        ids: ids.iter().cloned().collect(),
                    };
                    // A full stream is too slow, and a closed one is gone.
                    if open.sender.try_send(message).is_err() {
                        return false;
                    }
                }
            }
            !open.sender.is_closed()
        });
    }

    /// Ends the streams that match `which`.
    fn close(&self, which: impl Fn(&Open) -> bool) {
        self.streams().open.retain(|open| !which(open));
    }

    fn streams(&self) -> std::sync::MutexGuard<'_, Streams> {
        self.inner.streams.lock().expect("never poisoned")
    }
}

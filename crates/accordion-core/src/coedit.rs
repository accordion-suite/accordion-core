//! Co-editing (decision 30 in the `accordion` repo's `docs/decisions`).
//!
//! People editing one file at the same time each hold it as a Yjs document
//! and send each other their edits. The server keeps its own copy of every
//! open document with yrs. It checks each edit, logs it, passes it on, and
//! writes the file itself, so a file only ever holds what the module's
//! format writes.
//!
//! A module gives each file a [`Format`], which turns the document into the
//! file's contents and back: Docs into HTML, Notes into Markdown. Clients
//! speak Yjs's standard sync and awareness messages, which reach the
//! document through a [`Connection`]. `accordion-module` carries them over a
//! WebSocket.
//!
//! - **The first person to open a file loads it,** from the state kept at
//!   its last save and the edits logged since. Everyone after joins the copy
//!   in memory.
//! - **The file is written** 30 seconds after the last edit, at least every
//!   5 minutes while editing goes on, and when the last person leaves. One
//!   session's saves become one version (see [`Storage::write_edit`]).
//! - **A file changed outside the editor,** through WebDAV, sync, or a
//!   restored version, wins: the document changes to match it, as edits
//!   everyone sees.
//! - **Access follows storage.** Viewers see edits but can't make them.
//!   Losing access, or signing out, closes the connection.

mod room;
mod store;
mod text;

pub use text::set_text;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use accordion_types::{ItemId, SessionId, UserId};
use tokio::sync::{mpsc, oneshot};
use yrs::Doc;

use crate::db::{self, Database};
use crate::identity::Identity;
use crate::storage::{self, Access, Storage};

/// The largest message a client may send: one edit, or a cursor. Images go
/// up separately, so edits stay small.
pub const MAX_MESSAGE: usize = 1024 * 1024;
/// The most connections one document can have.
pub const MAX_PEOPLE: usize = 50;
/// The largest file the service opens or writes.
pub const MAX_FILE: u64 = 50 * 1024 * 1024;

/// A file format for co-editing, such as Docs' HTML or Notes' Markdown.
///
/// The documents count text offsets in UTF-8 bytes. [`set_text`] makes a
/// text match new contents with few edits.
pub trait Format: Send + Sync + 'static {
    /// A short name, such as `notes`, kept with each document's state, so
    /// one file is never co-edited in two formats.
    fn name(&self) -> &'static str;

    /// The co-editing state the file carries, if the format keeps one in the
    /// file, as Docs' data block does. It's used when the service has no
    /// state of its own for the file yet.
    fn state_in(&self, contents: &[u8]) -> Option<Vec<u8>> {
        let _ = contents;
        None
    }

    /// Makes `doc` match the file's `contents`, with as few edits as it can.
    /// It's called on an empty document for a file opened the first time,
    /// and on the current one when the file changed outside the editor.
    fn absorb(&self, doc: &Doc, contents: &[u8]) -> Result<(), FormatError>;

    /// The file's contents for the document as it is now.
    fn write(&self, doc: &Doc) -> Result<Vec<u8>, FormatError>;
}

/// Why a format couldn't read or write a file.
#[derive(Clone, Debug, thiserror::Error)]
#[error("{0}")]
pub struct FormatError(pub String);

/// Why someone couldn't open a file for co-editing.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum JoinError {
    /// It doesn't exist, it's a folder, it's in the trash, or they can't
    /// open it. These look the same, so nobody learns what exists.
    #[error("That file doesn't exist. It may have been deleted.")]
    NotFound,

    #[error("{MAX_PEOPLE} people are editing this already. Try again later.")]
    Full,

    #[error("This file is too big to edit here. The limit is 50 MB.")]
    TooBig,

    /// Another module has it open in its own format.
    #[error("This file is open in another app. Try again once it's closed.")]
    OtherFormat,

    #[error("Accordion is restarting. Try again in a minute.")]
    Stopping,

    /// The file, or what's kept for it, couldn't be read. The server log
    /// has the details.
    #[error(
        "Accordion couldn't open this file. Try again, or ask your admin to check the server log."
    )]
    Unreadable,
}

/// Why the server closed a connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Closed {
    /// Their access changed. If they can still open the file, the client
    /// connects again with the new access.
    AccessChanged,
    /// Their session ended.
    SignedOut,
    /// The file was deleted or moved to the trash.
    Gone,
    /// They didn't keep up with the edits. The client connects again and
    /// catches up.
    TooSlow,
    /// They sent a message over [`MAX_MESSAGE`].
    TooBig,
    /// They sent something that isn't a valid message or edit.
    Invalid,
    /// The server's copy of the document failed. Everyone connects again,
    /// and it's loaded fresh from the file and the log.
    Failed,
    /// The server is stopping.
    Stopping,
}

/// When documents are saved. The defaults are decision 30's. Tests shorten
/// them.
#[derive(Clone, Copy, Debug)]
pub struct Timing {
    /// How long after the last edit the file is written.
    pub save_after: Duration,
    /// The longest the file goes unwritten while editing carries on.
    pub save_at_least_every: Duration,
    /// How long changes to sharing are gathered before access is checked
    /// again.
    pub check_access_after: Duration,
}

impl Default for Timing {
    fn default() -> Self {
        Self {
            save_after: Duration::from_secs(30),
            save_at_least_every: Duration::from_secs(5 * 60),
            check_access_after: Duration::from_millis(500),
        }
    }
}

/// The co-editing service. Cloning it is cheap, and every clone shares the
/// same open documents.
#[derive(Clone)]
pub struct CoEditing {
    inner: Arc<Inner>,
}

struct Inner {
    db: Database,
    storage: Storage,
    identity: Identity,
    timing: Timing,
    rooms: Mutex<Rooms>,
}

#[derive(Default)]
struct Rooms {
    open: HashMap<ItemId, RoomHandle>,
    next_id: u64,
    stopping: bool,
}

#[derive(Clone)]
struct RoomHandle {
    id: u64,
    inbound: mpsc::Sender<room::Inbound>,
}

impl CoEditing {
    /// Starts the service, creating its tables, and forgets documents whose
    /// files were deleted for good.
    pub async fn open(
        db: Database,
        storage: Storage,
        identity: Identity,
        timing: Timing,
    ) -> Result<Self, db::Error> {
        store::migrate(&db).await?;
        store::prune(&db).await?;
        Ok(Self {
            inner: Arc::new(Inner {
                db,
                storage,
                identity,
                timing,
                rooms: Mutex::default(),
            }),
        })
    }

    /// Opens `item` for `user`, signed in with `session`, in `format`.
    /// Ending the session closes the connection.
    pub async fn join(
        &self,
        item: ItemId,
        user: UserId,
        session: Option<SessionId>,
        format: Arc<dyn Format>,
    ) -> Result<Connection, JoinError> {
        let access = match self.inner.storage.access(user, item).await {
            Ok(Some(access)) => access,
            Ok(None) | Err(storage::Error::NotFound) => return Err(JoinError::NotFound),
            Err(err) => {
                tracing::error!(%err, %item, "couldn't check access for co-editing");
                return Err(JoinError::Unreadable);
            }
        };
        // A room that's closing turns joins away, and the next try starts a
        // new one.
        for _ in 0..5 {
            let room = self.room(item, &format)?;
            let (reply, joined) = oneshot::channel();
            let join = room::Inbound::Join {
                user,
                session,
                access,
                format: format.name(),
                reply,
            };
            if room.inbound.send(join).await.is_err() {
                self.forget(item, room.id);
                continue;
            }
            match joined.await {
                Ok(result) => return result,
                Err(_) => self.forget(item, room.id),
            }
        }
        Err(JoinError::Unreadable)
    }

    /// How many documents are open, for tests and the health page.
    pub fn open_documents(&self) -> usize {
        let mut rooms = self.rooms();
        rooms.open.retain(|_, room| !room.inbound.is_closed());
        rooms.open.len()
    }

    /// Saves every open document and closes every connection. New joins are
    /// turned away from then on.
    pub async fn stop(&self) {
        let rooms: Vec<RoomHandle> = {
            let mut rooms = self.rooms();
            rooms.stopping = true;
            rooms.open.values().cloned().collect()
        };
        let mut waiting = Vec::new();
        for room in rooms {
            let (done, stopped) = oneshot::channel();
            if room
                .inbound
                .send(room::Inbound::Stop { done })
                .await
                .is_ok()
            {
                waiting.push(stopped);
            }
        }
        for stopped in waiting {
            let _ = stopped.await;
        }
    }

    /// The room for `item`, started if there isn't one.
    fn room(&self, item: ItemId, format: &Arc<dyn Format>) -> Result<RoomHandle, JoinError> {
        let mut rooms = self.rooms();
        if rooms.stopping {
            return Err(JoinError::Stopping);
        }
        if let Some(room) = rooms.open.get(&item)
            && !room.inbound.is_closed()
        {
            return Ok(room.clone());
        }
        rooms.next_id += 1;
        let id = rooms.next_id;
        let (inbound, receiver) = mpsc::channel(room::INBOUND);
        let handle = RoomHandle { id, inbound };
        rooms.open.insert(item, handle.clone());
        let room = room::Room::new(
            self.clone(),
            id,
            item,
            Arc::clone(format),
            receiver,
            handle.inbound.clone(),
        );
        tokio::spawn(room.run());
        Ok(handle)
    }

    /// Lets go of room `id`, if it's still the room for `item`.
    fn forget(&self, item: ItemId, id: u64) {
        let mut rooms = self.rooms();
        if rooms.open.get(&item).is_some_and(|room| room.id == id) {
            rooms.open.remove(&item);
        }
    }

    fn rooms(&self) -> std::sync::MutexGuard<'_, Rooms> {
        self.inner.rooms.lock().expect("never poisoned")
    }
}

/// One person's connection to an open document. Pass each message from the
/// client to [`Connection::send`], and each from [`Connection::recv`] to the
/// client. Dropping it leaves the document.
pub struct Connection {
    id: u64,
    access: Access,
    room: mpsc::Sender<room::Inbound>,
    left: mpsc::UnboundedSender<u64>,
    outbound: mpsc::Receiver<Arc<[u8]>>,
    closed: Arc<OnceLock<Closed>>,
}

impl Connection {
    /// What they may do. Viewers' edits are ignored.
    pub fn access(&self) -> Access {
        self.access
    }

    /// Passes a message from the client to the document. Returns `false`
    /// once the connection is closed.
    pub async fn send(&self, message: Vec<u8>) -> bool {
        self.room
            .send(room::Inbound::Message {
                from: self.id,
                bytes: message,
            })
            .await
            .is_ok()
    }

    /// The next message for the client, or `None` once the connection is
    /// closed. [`Connection::closed_because`] then says why.
    pub async fn recv(&mut self) -> Option<Arc<[u8]>> {
        self.outbound.recv().await
    }

    /// Why the server closed the connection, once it has.
    pub fn closed_because(&self) -> Option<Closed> {
        self.closed.get().copied()
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        let _ = self.left.send(self.id);
    }
}

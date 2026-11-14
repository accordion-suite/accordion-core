//! One open document: the server's copy, the people connected to it, and
//! when it's saved. Each room is a task of its own, so documents never wait
//! on each other, and everything a room does happens in order.

use std::collections::{HashMap, HashSet};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, OnceLock};

use accordion_types::{ItemId, SessionId, UserId};
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use uuid::Uuid;
use yrs::encoding::read::Cursor;
use yrs::sync::awareness::AwarenessUpdateEntry;
use yrs::sync::{AwarenessUpdate, Message, MessageReader, SyncMessage};
use yrs::updates::decoder::{Decode, DecoderV1};
use yrs::updates::encoder::Encode;
use yrs::{ClientID, Doc, OffsetKind, Options, ReadTxn, StateVector, Transact, Update};

use super::store;
use super::{
    Closed, CoEditing, Connection, Format, FormatError, JoinError, MAX_FILE, MAX_MESSAGE,
    MAX_PEOPLE,
};
use crate::events::{Actor, Via};
use crate::storage::{self, Access, ContentHash, ItemKind};

/// How many messages can wait for a room.
pub(super) const INBOUND: usize = 256;
/// How many messages can wait for a client before it counts as too slow.
const OUTBOUND: usize = 512;
/// How many Yjs clients one connection may speak for in awareness. A
/// browser tab is one, and a few more allow for an editor's subdocuments.
const CLIENTS_EACH: usize = 8;
/// The largest awareness state: a name, a color, and a cursor.
const MAX_AWARENESS: usize = 16 * 1024;

pub(super) enum Inbound {
    Join {
        user: UserId,
        session: Option<SessionId>,
        access: Access,
        format: &'static str,
        reply: oneshot::Sender<Result<Connection, JoinError>>,
    },
    Message {
        from: u64,
        bytes: Vec<u8>,
    },
    Stop {
        done: oneshot::Sender<()>,
    },
}

pub(super) struct Room {
    service: CoEditing,
    id: u64,
    item: ItemId,
    format: Arc<dyn Format>,
    inbound: mpsc::Receiver<Inbound>,
    /// Given to each connection, to send its messages.
    sender: mpsc::Sender<Inbound>,
    left_sender: mpsc::UnboundedSender<u64>,
    left: mpsc::UnboundedReceiver<u64>,

    doc: Doc,
    /// The contents of the file the document was last read from or written
    /// to, which the next save must find still there.
    file_hash: ContentHash,
    /// A state whose contents are the file's, from the document's own
    /// history, when there is one. A change from outside is the difference
    /// between it and the new file, so edits made since are kept.
    base: Option<Vec<u8>>,
    next_seq: i64,
    /// This session's ID, so its saves become one version.
    edit: Uuid,
    people: HashMap<u64, Person>,
    next_person: u64,
    /// Each Yjs client's cursor and name, and whose connection it is.
    awareness: HashMap<ClientID, Peer>,
    unsaved: Option<Unsaved>,
    /// The copy in memory can't be trusted, since reading an edit panicked.
    failed: bool,
    /// The file was deleted or moved to the trash.
    gone: bool,
}

struct Person {
    user: UserId,
    session: Option<SessionId>,
    access: Access,
    outbound: mpsc::Sender<Arc<[u8]>>,
    closed: Arc<OnceLock<Closed>>,
    clients: HashSet<ClientID>,
}

struct Peer {
    person: u64,
    clock: u32,
    json: Arc<str>,
}

#[derive(Clone, Copy)]
struct Unsaved {
    since: Instant,
    last_edit: Instant,
    /// Who made the last edit, for the version and the event log. `None`
    /// when the change came from the file itself.
    editor: Option<UserId>,
    /// When a save that failed is tried again.
    retry_at: Option<Instant>,
}

impl Room {
    pub(super) fn new(
        service: CoEditing,
        id: u64,
        item: ItemId,
        format: Arc<dyn Format>,
        inbound: mpsc::Receiver<Inbound>,
        sender: mpsc::Sender<Inbound>,
    ) -> Self {
        let (left_sender, left) = mpsc::unbounded_channel();
        Self {
            service,
            id,
            item,
            format,
            inbound,
            sender,
            left_sender,
            left,
            doc: new_doc(),
            file_hash: ContentHash([0; 32]),
            base: None,
            next_seq: 1,
            edit: random_uuid(),
            people: HashMap::new(),
            next_person: 0,
            awareness: HashMap::new(),
            unsaved: None,
            failed: false,
            gone: false,
        }
    }

    pub(super) async fn run(mut self) {
        let inner = Arc::clone(&self.service.inner);
        let mut hints = inner.db.hints();
        let mut ended = inner.identity.ended_sessions();
        let mut saved = inner.storage.saved();
        saved.mark_unchanged();
        let (mut hints_open, mut ended_open, mut saved_open) = (true, true, true);

        if let Err(err) = self.load().await {
            self.turn_away(err).await;
            return;
        }

        let mut check_access_at: Option<Instant> = None;
        loop {
            let save_at = self.save_at();
            tokio::select! {
                message = self.inbound.recv() => match message {
                    Some(message) => {
                        if let Some(done) = self.handle(message).await {
                            self.stop(done).await;
                            return;
                        }
                    }
                    // The room holds a sender itself, so this doesn't happen.
                    None => return,
                },
                Some(person) = self.left.recv() => {
                    // Everything a connection sent is queued before it
                    // leaves, so it's handled first, and a last edit sent
                    // just before closing isn't lost.
                    while let Ok(message) = self.inbound.try_recv() {
                        if let Some(done) = self.handle(message).await {
                            self.stop(done).await;
                            return;
                        }
                    }
                    self.leave(person);
                }
                hint = hints.recv(), if hints_open => match hint {
                    Ok(_) | Err(RecvError::Lagged(_)) => {
                        check_access_at.get_or_insert(Instant::now() + inner.timing.check_access_after);
                    }
                    Err(RecvError::Closed) => hints_open = false,
                },
                session = ended.recv(), if ended_open => match session {
                    Ok(session) => self.close_where(Closed::SignedOut, |person| person.session == Some(session)),
                    // Some ended sessions were missed, so every connection
                    // that belongs to one closes, and signs in again if it
                    // can.
                    Err(RecvError::Lagged(_)) => self.close_where(Closed::SignedOut, |person| person.session.is_some()),
                    Err(RecvError::Closed) => ended_open = false,
                },
                changed = saved.changed(), if saved_open => match changed {
                    Ok(()) => self.check_file().await,
                    Err(_) => saved_open = false,
                },
                () = sleep_until(save_at), if save_at.is_some() => self.save().await,
                () = sleep_until(check_access_at), if check_access_at.is_some() => {
                    check_access_at = None;
                    self.check_access().await;
                }
            }

            if self.failed {
                self.close_all(Closed::Failed);
                self.end().await;
                return;
            }
            if self.gone {
                self.close_all(Closed::Gone);
                self.end().await;
                return;
            }
            if self.people.is_empty() {
                // A save that fails leaves the edits in the log, for the
                // next person who opens the file.
                self.save().await;
                if self.people.is_empty() && !self.failed && self.try_end() {
                    return;
                }
            }
        }
    }

    /// Handles a message for the room. Returns where to answer once the
    /// room has stopped, if it's asked to stop.
    async fn handle(&mut self, message: Inbound) -> Option<oneshot::Sender<()>> {
        match message {
            Inbound::Join {
                user,
                session,
                access,
                format,
                reply,
            } => {
                let joined = self.join(user, session, access, format);
                let _ = reply.send(joined);
            }
            Inbound::Message { from, bytes } => self.receive(from, bytes).await,
            Inbound::Stop { done } => return Some(done),
        }
        None
    }

    async fn stop(&mut self, done: oneshot::Sender<()>) {
        self.save().await;
        self.close_all(Closed::Stopping);
        self.end().await;
        let _ = done.send(());
    }

    /// Reads the file, the state kept at its last save, and the edits
    /// logged since.
    async fn load(&mut self) -> Result<(), JoinError> {
        let storage = &self.service.inner.storage;
        let blob = match storage.item(self.item).await {
            Ok(Some(item)) => match item.kind {
                ItemKind::File { blob, .. } => blob,
                ItemKind::Folder => return Err(JoinError::NotFound),
            },
            Ok(None) => return Err(JoinError::NotFound),
            Err(err) => return Err(self.unreadable("couldn't find the file", &err)),
        };
        match storage.outside_trash(self.item).await {
            Ok(true) => {}
            Ok(false) => return Err(JoinError::NotFound),
            Err(err) => return Err(self.unreadable("couldn't find the file", &err)),
        }
        if blob.size > MAX_FILE {
            return Err(JoinError::TooBig);
        }
        let contents = self.read(blob.hash).await?;
        let kept = store::load(&self.service.inner.db, self.item)
            .await
            .map_err(|err| self.unreadable("couldn't load the kept state", &err))?;

        let mut state_matches = false;
        if let Some(state) = &kept.state {
            if state.format != self.format.name() {
                return Err(JoinError::OtherFormat);
            }
            self.load_update(&state.state)?;
            state_matches = state.file_hash == blob.hash;
            if !state.unsaved {
                self.base = Some(state.state.clone());
            }
        } else if let Some(state) = guarded(|| self.format.state_in(&contents)).flatten() {
            self.load_update(&state)?;
            self.base = Some(state);
        }
        for update in &kept.updates {
            self.load_update(update)?;
        }
        self.next_seq = kept.next_seq;
        self.file_hash = blob.hash;
        if !kept.updates.is_empty() || kept.state.as_ref().is_some_and(|state| state.unsaved) {
            self.edited(None);
        }
        if !state_matches {
            // The file changed since its last save here, or this is its
            // first time.
            self.absorb(&contents).await;
            if self.failed {
                return Err(JoinError::Unreadable);
            }
        }
        Ok(())
    }

    fn load_update(&self, update: &[u8]) -> Result<(), JoinError> {
        match guarded(|| apply(&self.doc, update)) {
            Some(Ok(())) => Ok(()),
            _ => {
                tracing::error!(item = %self.item, "a kept co-editing state or edit didn't load");
                Err(JoinError::Unreadable)
            }
        }
    }

    /// Reads the file's contents with the hash `hash`.
    async fn read(&self, hash: ContentHash) -> Result<Vec<u8>, JoinError> {
        let file = self
            .service
            .inner
            .storage
            .open_contents(hash)
            .await
            .map_err(|err| self.unreadable("couldn't open the file", &err))?;
        let mut contents = Vec::new();
        file.take(MAX_FILE + 1)
            .read_to_end(&mut contents)
            .await
            .map_err(|err| self.unreadable("couldn't read the file", &err))?;
        if contents.len() as u64 > MAX_FILE {
            return Err(JoinError::TooBig);
        }
        Ok(contents)
    }

    fn unreadable(&self, what: &str, err: &dyn std::fmt::Display) -> JoinError {
        tracing::error!(item = %self.item, %err, "co-editing {what}");
        JoinError::Unreadable
    }

    /// Answers every join with `err`, since the document didn't load.
    async fn turn_away(mut self, err: JoinError) {
        self.service.forget(self.item, self.id);
        self.inbound.close();
        while let Some(message) = self.inbound.recv().await {
            match message {
                Inbound::Join { reply, .. } => {
                    let _ = reply.send(Err(err.clone()));
                }
                Inbound::Stop { done } => {
                    let _ = done.send(());
                }
                Inbound::Message { .. } => {}
            }
        }
    }

    fn join(
        &mut self,
        user: UserId,
        session: Option<SessionId>,
        access: Access,
        format: &'static str,
    ) -> Result<Connection, JoinError> {
        if format != self.format.name() {
            return Err(JoinError::OtherFormat);
        }
        if self.people.len() >= MAX_PEOPLE {
            return Err(JoinError::Full);
        }
        self.next_person += 1;
        let id = self.next_person;
        let (outbound, receiver) = mpsc::channel(OUTBOUND);
        let closed = Arc::new(OnceLock::new());
        self.people.insert(
            id,
            Person {
                user,
                session,
                access,
                outbound,
                closed: Arc::clone(&closed),
                clients: HashSet::new(),
            },
        );
        // The server asks for what the client has that it doesn't, and
        // shows who else is here.
        let step1 = Message::Sync(SyncMessage::SyncStep1(self.doc.transact().state_vector()));
        self.send(id, step1.encode_v1().into());
        if let Some(everyone) = self.everyones_awareness() {
            self.send(id, everyone);
        }
        Ok(Connection {
            id,
            access,
            room: self.sender.clone(),
            left: self.left_sender.clone(),
            outbound: receiver,
            closed,
        })
    }

    async fn receive(&mut self, from: u64, bytes: Vec<u8>) {
        let Some(person) = self.people.get(&from) else {
            return;
        };
        let (user, can_edit) = (person.user, person.access >= Access::Editor);
        if bytes.len() > MAX_MESSAGE {
            self.close(from, Closed::TooBig);
            return;
        }
        let Some(Ok(messages)) = guarded(|| read_messages(&bytes)) else {
            self.close(from, Closed::Invalid);
            return;
        };
        for message in messages {
            match message {
                Message::Sync(SyncMessage::SyncStep1(theirs)) => {
                    let missing = self.doc.transact().encode_diff_v1(&theirs);
                    let step2 = Message::Sync(SyncMessage::SyncStep2(missing));
                    self.send(from, step2.encode_v1().into());
                }
                Message::Sync(SyncMessage::SyncStep2(update) | SyncMessage::Update(update)) => {
                    // Viewers' editors send their edits anyway. Ignoring
                    // them keeps the viewer's copy apart, and nobody else
                    // ever sees it.
                    if !can_edit {
                        continue;
                    }
                    if let Err(closed) = self.accept(from, user, update).await {
                        self.close(from, closed);
                        return;
                    }
                }
                Message::Awareness(update) => self.awareness_from(from, update),
                Message::AwarenessQuery => {
                    if let Some(everyone) = self.everyones_awareness() {
                        self.send(from, everyone);
                    }
                }
                Message::Auth(_) | Message::Custom(..) => {}
            }
            if self.failed || !self.people.contains_key(&from) {
                return;
            }
        }
    }

    /// Applies an edit, logs it, and passes it on.
    async fn accept(&mut self, from: u64, user: UserId, update: Vec<u8>) -> Result<(), Closed> {
        // Decoding changes nothing, so an edit that can't be decoded, even
        // one that makes yrs panic, only closes its sender.
        let Some(Ok(decoded)) = guarded(|| Update::decode_v1(&update)) else {
            return Err(Closed::Invalid);
        };
        match guarded(|| self.doc.transact_mut().apply_update(decoded)) {
            Some(Ok(())) => {}
            Some(Err(_)) => return Err(Closed::Invalid),
            None => {
                // The copy in memory may be half changed. Every edit anyone
                // saw is kept, so it's rebuilt from them, and only the
                // sender is closed.
                tracing::warn!(item = %self.item, "a co-editing edit panicked, so the document is reloaded");
                self.reload().await;
                return Err(Closed::Invalid);
            }
        }
        // Logged before it's passed on, so a crash loses nothing anyone saw.
        if let Err(err) = self.log(update.clone()).await {
            tracing::error!(item = %self.item, %err, "couldn't log a co-editing edit");
            self.failed = true;
            return Err(Closed::Failed);
        }
        let message: Arc<[u8]> = Message::Sync(SyncMessage::Update(update))
            .encode_v1()
            .into();
        self.send_all(Some(from), &message);
        self.edited(Some(user));
        Ok(())
    }

    /// Rebuilds the document from its kept state and the edits logged
    /// since, which hold everything anyone was sent.
    async fn reload(&mut self) {
        let kept = match store::load(&self.service.inner.db, self.item).await {
            Ok(kept) => kept,
            Err(err) => {
                tracing::error!(item = %self.item, %err, "couldn't reload a co-edited document");
                self.failed = true;
                return;
            }
        };
        let doc = new_doc();
        let loaded = kept
            .state
            .iter()
            .map(|state| &state.state)
            .chain(&kept.updates)
            .all(|update| matches!(guarded(|| apply(&doc, update)), Some(Ok(()))));
        if loaded {
            self.doc = doc;
        } else {
            tracing::error!(item = %self.item, "a co-edited document didn't reload");
            self.failed = true;
        }
    }

    async fn log(&mut self, update: Vec<u8>) -> Result<(), crate::db::Error> {
        store::append(&self.service.inner.db, self.item, self.next_seq, update).await?;
        self.next_seq += 1;
        Ok(())
    }

    fn edited(&mut self, editor: Option<UserId>) {
        let now = Instant::now();
        let unsaved = self.unsaved.get_or_insert(Unsaved {
            since: now,
            last_edit: now,
            editor,
            retry_at: None,
        });
        unsaved.last_edit = now;
        if editor.is_some() {
            unsaved.editor = editor;
        }
    }

    fn save_at(&self) -> Option<Instant> {
        let unsaved = self.unsaved?;
        let timing = self.service.inner.timing;
        Some(unsaved.retry_at.unwrap_or_else(|| {
            (unsaved.last_edit + timing.save_after).min(unsaved.since + timing.save_at_least_every)
        }))
    }

    fn retry_later(&mut self) {
        let at = Instant::now() + self.service.inner.timing.save_after;
        if let Some(unsaved) = &mut self.unsaved {
            unsaved.retry_at = Some(at);
        }
    }

    /// Writes the file, if anything changed, and keeps the document's state.
    async fn save(&mut self) {
        let Some(unsaved) = self.unsaved else {
            return;
        };
        let contents = match guarded(|| self.format.write(&self.doc)) {
            Some(Ok(contents)) => contents,
            Some(Err(err)) => {
                tracing::error!(item = %self.item, %err, "couldn't write a co-edited file");
                self.retry_later();
                return;
            }
            None => {
                tracing::error!(item = %self.item, "writing a co-edited file panicked");
                self.failed = true;
                return;
            }
        };
        if contents.len() as u64 > MAX_FILE {
            tracing::warn!(item = %self.item, "a co-edited file is over 50 MB, so it isn't saved");
            self.retry_later();
            return;
        }
        let hash = ContentHash(Sha256::digest(&contents).into());
        let mut file_hash = self.file_hash;
        if hash != file_hash {
            let actor = unsaved
                .editor
                .map_or(Actor::SERVER, |user| Actor::person(user, Via::Web));
            let written = self
                .service
                .inner
                .storage
                .acting_as(actor)
                .write_edit(self.item, file_hash, self.edit, &contents[..])
                .await;
            match written {
                Ok(item) => match item.kind {
                    ItemKind::File { blob, .. } => file_hash = blob.hash,
                    ItemKind::Folder => {
                        self.gone = true;
                        return;
                    }
                },
                // Changed outside the editor since it was read. The file
                // wins, and the merged document is saved next.
                Err(storage::Error::Changed) => {
                    self.check_file().await;
                    if let Some(unsaved) = &mut self.unsaved {
                        unsaved.retry_at = Some(Instant::now());
                    }
                    return;
                }
                Err(
                    storage::Error::NotFound | storage::Error::InTrash | storage::Error::NotAFile,
                ) => {
                    self.gone = true;
                    return;
                }
                Err(err) => {
                    tracing::warn!(item = %self.item, %err, "couldn't save a co-edited file");
                    self.retry_later();
                    return;
                }
            }
        }
        self.file_hash = file_hash;
        match self.keep(false).await {
            Ok(state) => {
                self.base = Some(state);
                self.unsaved = None;
            }
            Err(err) => {
                tracing::warn!(item = %self.item, %err, "couldn't keep a co-edited file's state");
                self.retry_later();
            }
        }
    }

    /// Takes in the file if it changed outside the editor.
    async fn check_file(&mut self) {
        let blob = match self.service.inner.storage.item(self.item).await {
            Ok(Some(item)) => match item.kind {
                ItemKind::File { blob, .. } => blob,
                ItemKind::Folder => return,
            },
            // Losing the file is noticed when access is checked, or at the
            // next save.
            _ => return,
        };
        if blob.hash == self.file_hash || blob.size > MAX_FILE {
            return;
        }
        let Ok(contents) = self.read(blob.hash).await else {
            return;
        };
        self.file_hash = blob.hash;
        self.absorb(&contents).await;
    }

    /// Keeps the document's state, with the file it has taken in, and lets
    /// go of the logged edits it holds. Returns the state.
    async fn keep(&self, unsaved: bool) -> Result<Vec<u8>, crate::db::Error> {
        let state = self
            .doc
            .transact()
            .encode_state_as_update_v1(&StateVector::default());
        store::keep(
            &self.service.inner.db,
            self.item,
            self.format.name(),
            self.file_hash,
            state.clone(),
            unsaved,
            self.next_seq,
        )
        .await?;
        Ok(state)
    }

    /// Takes in `contents`, the file with [`Room::file_hash`], changed
    /// outside the editor, as edits passed on like anyone's.
    ///
    /// With a base, the change is the difference between the base and the
    /// file, merged with edits made since, as a three-way merge would.
    /// Without one, the document is made to match the file. Either way the
    /// state is kept at once, with the file's hash, so after a restart the
    /// file counts as taken in.
    async fn absorb(&mut self, contents: &[u8]) {
        let matched = match guarded(|| self.format.write(&self.doc)) {
            Some(Ok(current)) => current == contents,
            Some(Err(_)) => false,
            None => {
                self.failed = true;
                return;
            }
        };
        if matched {
            // Nothing to take in, and nothing the file doesn't have.
            self.unsaved = None;
            match self.keep(false).await {
                Ok(state) => self.base = Some(state),
                Err(err) => {
                    tracing::warn!(item = %self.item, %err, "couldn't keep a co-edited file's state");
                }
            }
            return;
        }
        let before = self.doc.transact().state_vector();
        let taken = guarded(|| match &self.base {
            Some(base) => merge_in(&*self.format, base, &self.doc, contents).map(Some),
            None => self.format.absorb(&self.doc, contents).map(|()| None),
        });
        let new_base = match taken {
            Some(Ok(new_base)) => new_base,
            Some(Err(err)) => {
                // The document stays as it was, and the next save writes
                // it over the file, keeping the file as an older version.
                tracing::warn!(item = %self.item, %err, "couldn't read a co-edited file changed outside the editor");
                self.edited(None);
                return;
            }
            None => {
                self.failed = true;
                return;
            }
        };
        // Deletions don't move the state vector, but a diff carries every
        // deletion, so this holds all of the change.
        let update = self.doc.transact().encode_diff_v1(&before);
        // Edits the file didn't have survive the merge, and still need
        // saving.
        let unsaved = !matches!(
            guarded(|| self.format.write(&self.doc)),
            Some(Ok(current)) if current == contents
        );
        match self.keep(unsaved).await {
            Ok(state) => self.base = if unsaved { new_base } else { Some(state) },
            Err(err) => {
                tracing::error!(item = %self.item, %err, "couldn't keep a co-edited file's outside change");
                self.failed = true;
                return;
            }
        }
        let message: Arc<[u8]> = Message::Sync(SyncMessage::Update(update))
            .encode_v1()
            .into();
        self.send_all(None, &message);
        if unsaved {
            self.edited(None);
        } else {
            self.unsaved = None;
        }
    }

    /// Closes the connections of people whose access changed, and every
    /// connection if the file is gone or in the trash.
    async fn check_access(&mut self) {
        let storage = &self.service.inner.storage;
        match storage.outside_trash(self.item).await {
            Ok(true) => {}
            Ok(false) => {
                self.gone = true;
                return;
            }
            Err(err) => {
                tracing::warn!(item = %self.item, %err, "couldn't check a co-edited file");
                return;
            }
        }
        let users: HashSet<UserId> = self.people.values().map(|person| person.user).collect();
        let mut access = HashMap::new();
        for user in users {
            match storage.access(user, self.item).await {
                Ok(level) => {
                    access.insert(user, level);
                }
                Err(err) => {
                    tracing::warn!(item = %self.item, %err, "couldn't check access to a co-edited file");
                    return;
                }
            }
        }
        self.close_where(Closed::AccessChanged, |person| {
            access.get(&person.user).copied().flatten() != Some(person.access)
        });
    }

    fn awareness_from(&mut self, from: u64, update: AwarenessUpdate) {
        let mut passed_on = HashMap::new();
        for (client, entry) in update.clients {
            if entry.json.len() > MAX_AWARENESS {
                continue;
            }
            match self.awareness.get(&client) {
                // Nobody speaks for someone else's client.
                Some(peer) if peer.person != from => continue,
                Some(peer) if entry.clock < peer.clock => continue,
                Some(_) => {}
                None => {
                    let theirs = self
                        .people
                        .get(&from)
                        .map_or(0, |person| person.clients.len());
                    if theirs >= CLIENTS_EACH {
                        continue;
                    }
                }
            }
            if &*entry.json == "null" {
                self.awareness.remove(&client);
                if let Some(person) = self.people.get_mut(&from) {
                    person.clients.remove(&client);
                }
            } else {
                self.awareness.insert(
                    client,
                    Peer {
                        person: from,
                        clock: entry.clock,
                        json: Arc::clone(&entry.json),
                    },
                );
                if let Some(person) = self.people.get_mut(&from) {
                    person.clients.insert(client);
                }
            }
            passed_on.insert(client, entry);
        }
        if !passed_on.is_empty() {
            let message = Message::Awareness(AwarenessUpdate { clients: passed_on });
            self.send_all(Some(from), &message.encode_v1().into());
        }
    }

    fn everyones_awareness(&self) -> Option<Arc<[u8]>> {
        if self.awareness.is_empty() {
            return None;
        }
        let clients = self
            .awareness
            .iter()
            .map(|(&client, peer)| {
                let entry = AwarenessUpdateEntry {
                    clock: peer.clock,
                    json: Arc::clone(&peer.json),
                };
                (client, entry)
            })
            .collect();
        Some(
            Message::Awareness(AwarenessUpdate { clients })
                .encode_v1()
                .into(),
        )
    }

    /// Tells everyone that `clients` left.
    fn forget_clients(&mut self, clients: HashSet<ClientID>) {
        let mut gone = HashMap::new();
        for client in clients {
            if let Some(peer) = self.awareness.remove(&client) {
                let entry = AwarenessUpdateEntry {
                    clock: peer.clock + 1,
                    json: "null".into(),
                };
                gone.insert(client, entry);
            }
        }
        if !gone.is_empty() {
            let message = Message::Awareness(AwarenessUpdate { clients: gone });
            self.send_all(None, &message.encode_v1().into());
        }
    }

    fn send(&mut self, to: u64, message: Arc<[u8]>) {
        let Some(person) = self.people.get(&to) else {
            return;
        };
        match person.outbound.try_send(message) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => self.close(to, Closed::TooSlow),
            Err(mpsc::error::TrySendError::Closed(_)) => self.leave(to),
        }
    }

    fn send_all(&mut self, except: Option<u64>, message: &Arc<[u8]>) {
        let everyone: Vec<u64> = self
            .people
            .keys()
            .copied()
            .filter(|&id| Some(id) != except)
            .collect();
        for id in everyone {
            self.send(id, Arc::clone(message));
        }
    }

    fn leave(&mut self, id: u64) {
        if let Some(person) = self.people.remove(&id) {
            self.forget_clients(person.clients);
        }
    }

    fn close(&mut self, id: u64, why: Closed) {
        if let Some(person) = self.people.remove(&id) {
            let _ = person.closed.set(why);
            self.forget_clients(person.clients);
        }
    }

    fn close_where(&mut self, why: Closed, which: impl Fn(&Person) -> bool) {
        let closing: Vec<u64> = self
            .people
            .iter()
            .filter(|(_, person)| which(person))
            .map(|(&id, _)| id)
            .collect();
        for id in closing {
            self.close(id, why);
        }
    }

    fn close_all(&mut self, why: Closed) {
        self.close_where(why, |_| true);
    }

    /// Ends the room if nobody is waiting to join it.
    fn try_end(&mut self) -> bool {
        let mut rooms = self.service.rooms();
        if !self.inbound.is_empty() {
            return false;
        }
        if rooms
            .open
            .get(&self.item)
            .is_some_and(|room| room.id == self.id)
        {
            rooms.open.remove(&self.item);
        }
        drop(rooms);
        self.inbound.close();
        true
    }

    /// Ends the room, turning away anyone still waiting to join, who then
    /// start a new one.
    async fn end(&mut self) {
        self.service.forget(self.item, self.id);
        self.inbound.close();
        while let Some(message) = self.inbound.recv().await {
            if let Inbound::Stop { done } = message {
                let _ = done.send(());
            }
        }
    }
}

fn new_doc() -> Doc {
    Doc::with_options(Options {
        offset_kind: OffsetKind::Bytes,
        ..Options::default()
    })
}

fn random_uuid() -> Uuid {
    let mut bytes = [0; 16];
    getrandom::fill(&mut bytes).expect("the OS random number generator failed");
    Uuid::from_bytes(bytes)
}

/// Takes `contents` into `doc` as the change from `base`, a state from
/// `doc`'s history whose contents were the file's before it changed. The
/// change is worked out on a copy of `base`, so it lands where the file
/// changed, amid any edits `doc` has had since. Returns the copy's state,
/// whose contents are now the file's.
fn merge_in(
    format: &dyn Format,
    base: &[u8],
    doc: &Doc,
    contents: &[u8],
) -> Result<Vec<u8>, FormatError> {
    let file = new_doc();
    apply(&file, base).map_err(|()| FormatError("the kept state didn't load".into()))?;
    let at_base = file.transact().state_vector();
    format.absorb(&file, contents)?;
    let change = file.transact().encode_diff_v1(&at_base);
    apply(doc, &change).map_err(|()| FormatError("the change didn't apply".into()))?;
    Ok(file
        .transact()
        .encode_state_as_update_v1(&StateVector::default()))
}

/// Applies an encoded edit. `Err` if it isn't one.
fn apply(doc: &Doc, update: &[u8]) -> Result<(), ()> {
    let update = Update::decode_v1(update).map_err(|_| ())?;
    doc.transact_mut().apply_update(update).map_err(|_| ())
}

/// Every message in a frame. Clients send one, but the protocol allows
/// several.
fn read_messages(bytes: &[u8]) -> Result<Vec<Message>, ()> {
    let mut decoder = DecoderV1::new(Cursor::new(bytes));
    MessageReader::new(&mut decoder)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| ())
}

/// Runs `f`, catching a panic, since yrs reads edits from people and its
/// code isn't all safe Rust. `None` if it panicked. This needs panics to
/// unwind, as they do by default, so the server must never be built with
/// `panic = "abort"`.
fn guarded<T>(f: impl FnOnce() -> T) -> Option<T> {
    catch_unwind(AssertUnwindSafe(f)).ok()
}

async fn sleep_until(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

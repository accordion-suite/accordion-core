//! Co-editing (decision 30): simulated Yjs editors typing into one file at
//! once, saving, outside changes, access, and hostile messages.

use std::sync::Arc;
use std::time::Duration;

use accordion_core::coedit::{
    self, Closed, CoEditing, Connection, Format, FormatError, JoinError, Timing,
};
use accordion_core::db::Database;
use accordion_core::events::{Actor, Via};
use accordion_core::identity::{AuthMethod, Client, Identity, NewUser};
use accordion_core::storage::{Access, ItemKind, ShareLevel, Storage};
use accordion_types::{ItemId, Role, SessionId, UserId};
use tokio::io::AsyncReadExt;
use yrs::encoding::read::Cursor;
use yrs::sync::awareness::AwarenessUpdateEntry;
use yrs::sync::{AwarenessUpdate, Message, MessageReader, SyncMessage};
use yrs::updates::decoder::{Decode, DecoderV1};
use yrs::updates::encoder::Encode;
use yrs::{ClientID, Doc, GetString, OffsetKind, Options, ReadTxn, Text, Transact, Update};

/// A file that's all text, for the tests.
struct Plain;

impl Format for Plain {
    fn name(&self) -> &'static str {
        "plain"
    }

    fn absorb(&self, doc: &Doc, contents: &[u8]) -> Result<(), FormatError> {
        let new = std::str::from_utf8(contents).map_err(|err| FormatError(err.to_string()))?;
        let body = doc.get_or_insert_text("body");
        coedit::set_text(&mut doc.transact_mut(), &body, new);
        Ok(())
    }

    fn write(&self, doc: &Doc) -> Result<Vec<u8>, FormatError> {
        let body = doc.get_or_insert_text("body");
        Ok(body.get_string(&doc.transact()).into_bytes())
    }
}

/// Another format, to open the same file in.
struct Other;

impl Format for Other {
    fn name(&self) -> &'static str {
        "other"
    }

    fn absorb(&self, doc: &Doc, contents: &[u8]) -> Result<(), FormatError> {
        Plain.absorb(doc, contents)
    }

    fn write(&self, doc: &Doc) -> Result<Vec<u8>, FormatError> {
        Plain.write(doc)
    }
}

struct Server {
    dir: tempfile::TempDir,
    db: Database,
    storage: Storage,
    identity: Identity,
    coediting: CoEditing,
}

const QUICK: Timing = Timing {
    save_after: Duration::from_millis(200),
    save_at_least_every: Duration::from_secs(2),
    check_access_after: Duration::from_millis(50),
};

/// Long enough that nothing saves during a test unless everyone leaves.
const SLOW: Timing = Timing {
    save_after: Duration::from_secs(3600),
    save_at_least_every: Duration::from_secs(3600),
    check_access_after: Duration::from_millis(50),
};

async fn start(timing: Timing) -> Server {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(&dir.path().join("accordion.db")).unwrap();
    let identity = Identity::new(db.clone()).await.unwrap();
    let storage = Storage::open(db.clone(), identity.clone(), &dir.path().join("storage"))
        .await
        .unwrap();
    let coediting = CoEditing::open(db.clone(), storage.clone(), identity.clone(), timing)
        .await
        .unwrap();
    Server {
        dir,
        db,
        storage,
        identity,
        coediting,
    }
}

impl Server {
    async fn user(&self, username: &str) -> UserId {
        self.identity
            .create_user(NewUser {
                username: username.into(),
                display_name: username.into(),
                role: Role::User,
                password: "correct horse".into(),
            })
            .await
            .unwrap()
            .id
    }

    async fn file(&self, owner: UserId, name: &str, contents: &str) -> ItemId {
        let top = self.storage.top_folder(owner).await.unwrap();
        self.storage
            .write_file(top, name, contents.as_bytes())
            .await
            .unwrap()
            .id
    }

    async fn contents(&self, item: ItemId) -> String {
        let mut file = self.storage.open_file(item).await.unwrap();
        let mut contents = String::new();
        file.read_to_string(&mut contents).await.unwrap();
        contents
    }

    async fn versions(&self, item: ItemId) -> usize {
        self.storage.versions(item).await.unwrap().len()
    }

    /// Saves new contents as if from File Explorer, outside the editor.
    async fn change_outside(&self, item: ItemId, contents: &str) {
        let owner = self.storage.item(item).await.unwrap().unwrap().owner;
        let top = self.storage.top_folder(owner).await.unwrap();
        let name = self.storage.item(item).await.unwrap().unwrap().name;
        self.storage
            .acting_as(Actor::person(owner, Via::WebDav))
            .write_file(top, &name, contents.as_bytes())
            .await
            .unwrap();
    }

    async fn join(&self, item: ItemId, user: UserId) -> Editor {
        self.join_as(item, user, None, Doc::with_options(options()))
            .await
            .unwrap()
    }

    async fn join_as(
        &self,
        item: ItemId,
        user: UserId,
        session: Option<SessionId>,
        doc: Doc,
    ) -> Result<Editor, JoinError> {
        let connection = self
            .coediting
            .join(item, user, session, Arc::new(Plain))
            .await?;
        let mut editor = Editor {
            doc,
            connection,
            awareness: AwarenessUpdate {
                clients: Default::default(),
            },
        };
        // As Yjs's WebSocket client does when it connects.
        let step1 = Message::Sync(SyncMessage::SyncStep1(editor.doc.transact().state_vector()));
        editor.send(step1.encode_v1()).await;
        editor.settle().await;
        Ok(editor)
    }

    /// Waits until every document is closed, saved, and let go.
    async fn all_closed(&self) {
        for _ in 0..100 {
            if self.coediting.open_documents() == 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("a document stayed open");
    }
}

fn options() -> Options {
    Options {
        offset_kind: OffsetKind::Bytes,
        ..Options::default()
    }
}

/// A browser with the document open, speaking Yjs's sync protocol.
struct Editor {
    doc: Doc,
    connection: Connection,
    /// The cursors it has heard about.
    awareness: AwarenessUpdate,
}

impl Editor {
    fn text(&self) -> String {
        let body = self.doc.get_or_insert_text("body");
        body.get_string(&self.doc.transact())
    }

    async fn send(&self, message: Vec<u8>) -> bool {
        self.connection.send(message).await
    }

    /// Edits the local copy and sends the edit, as y-websocket does.
    async fn edit(&self, change: impl FnOnce(&yrs::TextRef, &mut yrs::TransactionMut)) {
        let body = self.doc.get_or_insert_text("body");
        let before = self.doc.transact().state_vector();
        change(&body, &mut self.doc.transact_mut());
        let update = self.doc.transact().encode_diff_v1(&before);
        self.send(Message::Sync(SyncMessage::Update(update)).encode_v1())
            .await;
    }

    async fn type_at(&self, at: u32, text: &str) {
        self.edit(|body, txn| body.insert(txn, at, text)).await;
    }

    async fn type_at_end(&self, text: &str) {
        let at = self.text().len() as u32;
        self.type_at(at, text).await;
    }

    async fn say(&self, client: u64, clock: u32, json: &str) -> bool {
        let mut clients = std::collections::HashMap::new();
        clients.insert(
            ClientID::new(client),
            AwarenessUpdateEntry {
                clock,
                json: json.into(),
            },
        );
        self.send(Message::Awareness(AwarenessUpdate { clients }).encode_v1())
            .await
    }

    /// Handles what the server sends until it's quiet for a moment. `false`
    /// if the connection closed.
    async fn settle(&mut self) -> bool {
        loop {
            let next =
                tokio::time::timeout(Duration::from_millis(150), self.connection.recv()).await;
            match next {
                Err(_) => return true,
                Ok(None) => return false,
                Ok(Some(bytes)) => self.handle(&bytes).await,
            }
        }
    }

    async fn handle(&mut self, bytes: &[u8]) {
        let mut decoder = DecoderV1::new(Cursor::new(bytes));
        let messages: Vec<Message> = MessageReader::new(&mut decoder)
            .collect::<Result<_, _>>()
            .unwrap();
        for message in messages {
            match message {
                Message::Sync(SyncMessage::SyncStep1(theirs)) => {
                    let missing = self.doc.transact().encode_diff_v1(&theirs);
                    self.send(Message::Sync(SyncMessage::SyncStep2(missing)).encode_v1())
                        .await;
                }
                Message::Sync(SyncMessage::SyncStep2(update) | SyncMessage::Update(update)) => {
                    self.doc
                        .transact_mut()
                        .apply_update(Update::decode_v1(&update).unwrap())
                        .unwrap();
                }
                Message::Awareness(update) => {
                    for (client, entry) in update.clients {
                        self.awareness.clients.insert(client, entry);
                    }
                }
                _ => {}
            }
        }
    }

    /// Waits for the server to close the connection, and says why.
    async fn closed(&mut self) -> Closed {
        let wait = async {
            while let Some(bytes) = self.connection.recv().await {
                self.handle(&bytes).await;
            }
        };
        tokio::time::timeout(Duration::from_secs(5), wait)
            .await
            .expect("the connection stayed open");
        self.connection
            .closed_because()
            .expect("closed with a reason")
    }

    fn heard(&self, client: u64) -> Option<&str> {
        self.awareness
            .clients
            .get(&ClientID::new(client))
            .map(|entry| &*entry.json)
    }
}

async fn settle_all(editors: &mut [&mut Editor]) {
    for _ in 0..2 {
        for editor in editors.iter_mut() {
            editor.settle().await;
        }
    }
}

#[tokio::test]
async fn two_editors_end_with_the_same_file() {
    let s = start(SLOW).await;
    let alex = s.user("alex").await;
    let sam = s.user("sam").await;
    let item = s.file(alex, "list.txt", "Milk\n").await;
    s.storage
        .share(item, sam, ShareLevel::Editor, alex)
        .await
        .unwrap();

    let mut a = s.join(item, alex).await;
    let mut b = s.join(item, sam).await;
    assert_eq!(a.text(), "Milk\n");
    assert_eq!(b.text(), "Milk\n");
    assert_eq!(a.connection.access(), Access::Owner);
    assert_eq!(b.connection.access(), Access::Editor);

    // Both type before seeing the other's edit.
    a.type_at(0, "Shopping\n").await;
    b.type_at_end("Eggs\n").await;
    settle_all(&mut [&mut a, &mut b]).await;
    assert_eq!(a.text(), "Shopping\nMilk\nEggs\n");
    assert_eq!(b.text(), a.text());

    drop((a, b));
    s.all_closed().await;
    assert_eq!(s.contents(item).await, "Shopping\nMilk\nEggs\n");
}

#[tokio::test]
async fn three_editors_typing_at_once_end_with_the_same_file() {
    let s = start(SLOW).await;
    let alex = s.user("alex").await;
    let item = s.file(alex, "story.txt", "").await;
    let mut editors = Vec::new();
    for _ in 0..3 {
        editors.push(s.join(item, alex).await);
    }
    for round in 0..20 {
        for (i, editor) in editors.iter().enumerate() {
            let at = (round * 7 + i * 3) as u32 % (editor.text().len() as u32 + 1);
            let at = editor.text().floor_char_boundary(at as usize) as u32;
            editor.type_at(at, &format!("{i}é")).await;
        }
        // Every few rounds, everyone catches up, so edits land amid others'.
        if round % 5 == 4 {
            for editor in &mut editors {
                editor.settle().await;
            }
        }
    }
    for _ in 0..2 {
        for editor in &mut editors {
            editor.settle().await;
        }
    }
    let text = editors[0].text();
    assert_eq!(text.chars().count(), 3 * 20 * 2);
    for editor in &editors {
        assert_eq!(editor.text(), text);
    }

    s.coediting.stop().await;
    assert_eq!(s.contents(item).await, text);
    for editor in &mut editors {
        assert_eq!(editor.closed().await, Closed::Stopping);
    }
}

#[tokio::test]
async fn saves_after_a_pause_as_one_version() {
    let s = start(QUICK).await;
    let alex = s.user("alex").await;
    let item = s.file(alex, "note.txt", "Hi").await;
    let mut a = s.join(item, alex).await;

    a.type_at_end(" there").await;
    a.settle().await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(s.contents(item).await, "Hi there");
    assert_eq!(s.versions(item).await, 2);

    // More typing in the same session replaces that version.
    a.type_at_end(", Sam").await;
    a.settle().await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(s.contents(item).await, "Hi there, Sam");
    assert_eq!(s.versions(item).await, 2);
    drop(a);
    s.all_closed().await;

    // A new session starts a new version.
    let a = s.join(item, alex).await;
    a.type_at_end(".").await;
    drop(a);
    s.all_closed().await;
    assert_eq!(s.contents(item).await, "Hi there, Sam.");
    assert_eq!(s.versions(item).await, 3);
}

#[tokio::test]
async fn edits_survive_a_restart_before_they_were_saved() {
    let s = start(SLOW).await;
    let alex = s.user("alex").await;
    let item = s.file(alex, "note.txt", "Draft").await;
    let a = s.join(item, alex).await;
    a.type_at_end(" one").await;
    a.type_at_end(" two").await;
    // Wait for the room to log both edits.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // A second server on the same database, as after a crash: the first
    // one never saved.
    let after = CoEditing::open(s.db.clone(), s.storage.clone(), s.identity.clone(), SLOW)
        .await
        .unwrap();
    let connection = after.join(item, alex, None, Arc::new(Plain)).await.unwrap();
    let mut b = Editor {
        doc: Doc::with_options(options()),
        connection,
        awareness: AwarenessUpdate {
            clients: Default::default(),
        },
    };
    let step1 = Message::Sync(SyncMessage::SyncStep1(b.doc.transact().state_vector()));
    b.send(step1.encode_v1()).await;
    b.settle().await;
    assert_eq!(b.text(), "Draft one two");
    assert_eq!(s.contents(item).await, "Draft");
    drop(a);
    drop(b);
    after.stop().await;
    s.coediting.stop().await;
    let _ = &s.dir;
}

#[tokio::test]
async fn a_change_from_elsewhere_merges_with_unsaved_edits() {
    let s = start(SLOW).await;
    let alex = s.user("alex").await;
    let sam = s.user("sam").await;
    let item = s.file(alex, "list.txt", "Milk\nEggs\n").await;
    s.storage
        .share(item, sam, ShareLevel::Editor, alex)
        .await
        .unwrap();
    let mut a = s.join(item, alex).await;
    let mut b = s.join(item, sam).await;
    a.type_at_end("Bread\n").await;
    settle_all(&mut [&mut a, &mut b]).await;

    // Saved from File Explorer before the editor saved: the file's change
    // lands where it was made, and the unsaved line stays.
    s.change_outside(item, "Oat milk\nEggs\n").await;
    settle_all(&mut [&mut a, &mut b]).await;
    assert_eq!(a.text(), "Oat milk\nEggs\nBread\n");
    assert_eq!(b.text(), a.text());

    // A second change from outside merges the same way. Lines added at the
    // same place from both sides can land in either order.
    s.change_outside(item, "Oat milk\nEggs\nButter\n").await;
    settle_all(&mut [&mut a, &mut b]).await;
    let merged = a.text();
    assert!(
        merged == "Oat milk\nEggs\nButter\nBread\n" || merged == "Oat milk\nEggs\nBread\nButter\n",
        "{merged:?}"
    );
    assert_eq!(b.text(), merged);

    b.type_at_end("Jam\n").await;
    drop((a, b));
    s.all_closed().await;
    assert_eq!(s.contents(item).await, format!("{merged}Jam\n"));
}

#[tokio::test]
async fn a_change_made_while_the_server_was_off_merges_with_logged_edits() {
    let s = start(SLOW).await;
    let alex = s.user("alex").await;
    let item = s.file(alex, "list.txt", "Milk\n").await;
    // Saved once, so a state is kept.
    let a = s.join(item, alex).await;
    a.type_at_end("Eggs\n").await;
    drop(a);
    s.all_closed().await;

    // Edits logged but never saved, as before a crash.
    let a = s.join(item, alex).await;
    a.type_at_end("Bread\n").await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let after = CoEditing::open(s.db.clone(), s.storage.clone(), s.identity.clone(), SLOW)
        .await
        .unwrap();
    // Then the file changes before the server is back.
    s.change_outside(item, "Oat milk\nEggs\n").await;
    let connection = after.join(item, alex, None, Arc::new(Plain)).await.unwrap();
    let mut b = Editor {
        doc: Doc::with_options(options()),
        connection,
        awareness: AwarenessUpdate {
            clients: Default::default(),
        },
    };
    let step1 = Message::Sync(SyncMessage::SyncStep1(b.doc.transact().state_vector()));
    b.send(step1.encode_v1()).await;
    b.settle().await;
    assert_eq!(b.text(), "Oat milk\nEggs\nBread\n");
    drop((a, b));
    after.stop().await;
    s.coediting.stop().await;
}

#[tokio::test]
async fn an_editor_with_an_old_copy_gets_the_new_file_without_repeats() {
    let s = start(SLOW).await;
    let alex = s.user("alex").await;
    let item = s.file(alex, "list.txt", "Milk\n").await;
    let mut a = s.join(item, alex).await;
    a.type_at_end("Eggs\n").await;
    a.settle().await;
    let Editor {
        doc, connection, ..
    } = a;
    drop(connection);
    s.all_closed().await;

    // Changed while nobody had it open. The browser kept its copy, and
    // connects again with it.
    s.change_outside(item, "Milk\nEggs\nBread\n").await;
    let a = s.join_as(item, alex, None, doc).await.unwrap();
    assert_eq!(a.text(), "Milk\nEggs\nBread\n");
    drop(a);
    s.all_closed().await;
    assert_eq!(s.contents(item).await, "Milk\nEggs\nBread\n");
}

#[tokio::test]
async fn viewers_see_edits_but_cant_make_them() {
    let s = start(SLOW).await;
    let alex = s.user("alex").await;
    let sam = s.user("sam").await;
    let item = s.file(alex, "plan.txt", "Plan").await;
    s.storage
        .share(item, sam, ShareLevel::Viewer, alex)
        .await
        .unwrap();
    let mut a = s.join(item, alex).await;
    let mut b = s.join(item, sam).await;
    assert_eq!(b.connection.access(), Access::Viewer);

    b.type_at_end(" (Sam was here)").await;
    a.type_at(0, "The ").await;
    settle_all(&mut [&mut a, &mut b]).await;
    assert_eq!(a.text(), "The Plan");
    assert!(b.text().contains("The Plan"));

    drop((a, b));
    s.all_closed().await;
    assert_eq!(s.contents(item).await, "The Plan");
}

#[tokio::test]
async fn losing_access_closes_the_connection() {
    let s = start(SLOW).await;
    let alex = s.user("alex").await;
    let sam = s.user("sam").await;
    let item = s.file(alex, "plan.txt", "Plan").await;
    s.storage
        .share(item, sam, ShareLevel::Editor, alex)
        .await
        .unwrap();
    let mut a = s.join(item, alex).await;
    let mut b = s.join(item, sam).await;

    // Editor to viewer: they connect again, read-only.
    s.storage
        .share(item, sam, ShareLevel::Viewer, alex)
        .await
        .unwrap();
    assert_eq!(b.closed().await, Closed::AccessChanged);
    let mut b = s.join(item, sam).await;
    assert_eq!(b.connection.access(), Access::Viewer);

    s.storage.unshare(item, sam, alex).await.unwrap();
    assert_eq!(b.closed().await, Closed::AccessChanged);
    let again = s
        .join_as(item, sam, None, Doc::with_options(options()))
        .await;
    assert!(matches!(again, Err(JoinError::NotFound)));
    assert!(a.settle().await, "the owner stays connected");
}

#[tokio::test]
async fn signing_out_closes_the_connection() {
    let s = start(SLOW).await;
    let alex = s.user("alex").await;
    let item = s.file(alex, "plan.txt", "Plan").await;
    let (_, phone) = s
        .identity
        .create_session(alex, Client::Browser, "Phone", AuthMethod::Password)
        .await
        .unwrap();
    let (_, laptop) = s
        .identity
        .create_session(alex, Client::Browser, "Laptop", AuthMethod::Password)
        .await
        .unwrap();
    let doc = || Doc::with_options(options());
    let mut on_phone = s.join_as(item, alex, Some(phone.id), doc()).await.unwrap();
    let mut on_laptop = s.join_as(item, alex, Some(laptop.id), doc()).await.unwrap();

    s.identity.sign_out(alex, phone.id).await.unwrap();
    assert_eq!(on_phone.closed().await, Closed::SignedOut);
    assert!(on_laptop.settle().await, "the laptop stays connected");
}

#[tokio::test]
async fn moving_the_file_to_the_trash_closes_everyone() {
    let s = start(SLOW).await;
    let alex = s.user("alex").await;
    let item = s.file(alex, "plan.txt", "Plan").await;
    let mut a = s.join(item, alex).await;
    s.storage
        .acting_as(Actor::person(alex, Via::Web))
        .delete(item)
        .await
        .unwrap();
    assert_eq!(a.closed().await, Closed::Gone);
    let again = s
        .join_as(item, alex, None, Doc::with_options(options()))
        .await;
    assert!(matches!(again, Err(JoinError::NotFound)));
}

#[tokio::test]
async fn a_bad_message_closes_only_its_sender() {
    let s = start(SLOW).await;
    let alex = s.user("alex").await;
    let item = s.file(alex, "plan.txt", "Plan").await;
    let mut a = s.join(item, alex).await;
    let mut garbled = s.join(item, alex).await;
    let mut crafted = s.join(item, alex).await;
    let mut huge = s.join(item, alex).await;

    garbled.send(vec![0, 7, 255, 255, 255, 255, 255]).await;
    assert_eq!(garbled.closed().await, Closed::Invalid);

    // A sync message whose edit isn't one.
    let bad = Message::Sync(SyncMessage::Update(vec![200, 1, 2, 3, 4, 5, 6, 7, 8, 9]));
    crafted.send(bad.encode_v1()).await;
    assert_eq!(crafted.closed().await, Closed::Invalid);

    huge.send(vec![0; coedit::MAX_MESSAGE + 1]).await;
    assert_eq!(huge.closed().await, Closed::TooBig);

    a.type_at_end(", still fine").await;
    assert!(a.settle().await);
    drop(a);
    s.all_closed().await;
    assert_eq!(s.contents(item).await, "Plan, still fine");
}

#[tokio::test]
async fn cursors_reach_everyone_else_and_go_when_their_owner_leaves() {
    let s = start(SLOW).await;
    let alex = s.user("alex").await;
    let item = s.file(alex, "plan.txt", "Plan").await;
    let mut a = s.join(item, alex).await;
    let mut b = s.join(item, alex).await;
    let mut c = s.join(item, alex).await;

    a.say(11, 1, r#"{"user":{"name":"Alex"}}"#).await;
    settle_all(&mut [&mut a, &mut b, &mut c]).await;
    assert_eq!(b.heard(11), Some(r#"{"user":{"name":"Alex"}}"#));

    // Someone else can't speak for Alex's client.
    c.say(11, 5, r#"{"user":{"name":"Not Alex"}}"#).await;
    settle_all(&mut [&mut a, &mut b, &mut c]).await;
    assert_eq!(b.heard(11), Some(r#"{"user":{"name":"Alex"}}"#));

    // Someone who joins later hears about Alex.
    let d = s.join(item, alex).await;
    assert_eq!(d.heard(11), Some(r#"{"user":{"name":"Alex"}}"#));

    drop(a);
    settle_all(&mut [&mut b, &mut c]).await;
    assert_eq!(b.heard(11), Some("null"));
}

#[tokio::test]
async fn joins_are_turned_away_when_they_cant_be_served() {
    let s = start(SLOW).await;
    let alex = s.user("alex").await;
    let sam = s.user("sam").await;
    let item = s.file(alex, "plan.txt", "Plan").await;
    let doc = || Doc::with_options(options());

    // Someone with no access, and a folder.
    let no_access = s.join_as(item, sam, None, doc()).await;
    assert!(matches!(no_access, Err(JoinError::NotFound)));
    let top = s.storage.top_folder(alex).await.unwrap();
    let folder = s.storage.create_folder(top, "Folder").await.unwrap();
    let a_folder = s.join_as(folder.id, alex, None, doc()).await;
    assert!(matches!(a_folder, Err(JoinError::NotFound)));

    // Open in another format.
    let mut editors = vec![s.join(item, alex).await];
    let other = s.coediting.join(item, alex, None, Arc::new(Other)).await;
    assert!(matches!(other, Err(JoinError::OtherFormat)));

    // Full.
    for _ in 1..coedit::MAX_PEOPLE {
        let connection = s
            .coediting
            .join(item, alex, None, Arc::new(Plain))
            .await
            .unwrap();
        editors.push(Editor {
            doc: doc(),
            connection,
            awareness: AwarenessUpdate {
                clients: Default::default(),
            },
        });
    }
    let full = s.coediting.join(item, alex, None, Arc::new(Plain)).await;
    assert!(matches!(full, Err(JoinError::Full)));
    drop(editors);
    s.all_closed().await;

    // Once it's saved, the file keeps its format: another one can't take
    // over its state.
    let edited = s.join(item, alex).await;
    edited.type_at_end("!").await;
    drop(edited);
    s.all_closed().await;
    let other = s.coediting.join(item, alex, None, Arc::new(Other)).await;
    assert!(matches!(other, Err(JoinError::OtherFormat)));

    // And once the server stops, nobody joins.
    s.coediting.stop().await;
    let stopped = s.join_as(item, alex, None, doc()).await;
    assert!(matches!(stopped, Err(JoinError::Stopping)));
}

#[tokio::test]
async fn the_saved_version_is_by_the_last_editor() {
    let s = start(SLOW).await;
    let alex = s.user("alex").await;
    let sam = s.user("sam").await;
    let item = s.file(alex, "plan.txt", "Plan").await;
    s.storage
        .share(item, sam, ShareLevel::Editor, alex)
        .await
        .unwrap();
    let b = s.join(item, sam).await;
    b.type_at_end(" B").await;
    drop(b);
    s.all_closed().await;
    let ItemKind::File { .. } = s.storage.item(item).await.unwrap().unwrap().kind else {
        panic!("still a file");
    };
    let events =
        s.db.read(move |conn| {
            conn.query_row(
                "SELECT count(*) FROM events_log WHERE item_id = ?1 AND actor_id = ?2",
                (item.as_uuid(), sam.as_uuid()),
                |row| row.get::<_, i64>(0),
            )
        })
        .await;
    assert_eq!(events.unwrap(), 1, "the edit is Sam's");
}

/// A small random number generator, so the test needs no crate and runs the
/// same each time.
struct XorShift(u64);

impl XorShift {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn bytes(&mut self, len: usize) -> Vec<u8> {
        (0..len).map(|_| self.next() as u8).collect()
    }
}

#[tokio::test]
async fn random_and_mangled_messages_never_break_the_document() {
    let s = start(SLOW).await;
    let alex = s.user("alex").await;
    let item = s.file(alex, "plan.txt", "Plan").await;
    let mut a = s.join(item, alex).await;

    // Real edits to mangle.
    let sample = Doc::with_options(options());
    let body = sample.get_or_insert_text("body");
    body.insert(
        &mut sample.transact_mut(),
        0,
        "Some text to mangle, café 🍮",
    );
    let real = sample.transact().encode_diff_v1(&Default::default());

    let mut random = XorShift(0x9e37_79b9_7f4a_7c15);
    let mut fuzzer = s.join(item, alex).await;
    for round in 0..3000 {
        let message = match round % 3 {
            // Random bytes.
            0 => {
                let len = (random.next() % 64) as usize;
                random.bytes(len)
            }
            // A sync message around random bytes.
            1 => {
                let len = (random.next() % 128) as usize;
                Message::Sync(SyncMessage::Update(random.bytes(len))).encode_v1()
            }
            // A real edit with a few bytes changed.
            _ => {
                let mut update = real.clone();
                for _ in 0..1 + random.next() % 4 {
                    let at = (random.next() as usize) % update.len();
                    update[at] = random.next() as u8;
                }
                Message::Sync(SyncMessage::Update(update)).encode_v1()
            }
        };
        if !fuzzer.send(message).await || fuzzer.connection.closed_because().is_some() {
            // Closed for a bad message: connect again and carry on.
            fuzzer = match s
                .join_as(item, alex, None, Doc::with_options(options()))
                .await
            {
                Ok(editor) => editor,
                Err(err) => panic!("couldn't join again after round {round}: {err}"),
            };
        }
        if round % 500 == 0 {
            fuzzer.settle().await;
        }
    }
    drop(fuzzer);

    // The document still works, for someone who was there all along and for
    // someone new.
    a.type_at_end(", still here").await;
    a.settle().await;
    let b = s.join(item, alex).await;
    assert_eq!(b.text(), a.text());
    assert!(a.text().ends_with(", still here"), "{:?}", a.text());
    drop((a, b));
    s.all_closed().await;
    let saved = s.contents(item).await;
    assert!(saved.ends_with(", still here"), "{saved:?}");
}

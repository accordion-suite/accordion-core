//! The event log: what each change records, who sees it, and how long it's
//! kept (decision 18).

use std::net::{IpAddr, Ipv4Addr};

use accordion_core::db::Database;
use accordion_core::events::{Action, Actor, Event, Events, NewEvent, Via};
use accordion_core::identity::{AppPasswordUse, AuthMethod, Client, Identity, NewUser, SignInFrom};
use accordion_core::storage::{LeaveFiles, ShareLevel, Storage};
use accordion_types::{ItemId, Role, UserId};
use serde_json::json;

const HOME: IpAddr = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 20));

struct Server {
    _dir: tempfile::TempDir,
    db: Database,
    storage: Storage,
    identity: Identity,
    events: Events,
}

async fn start() -> Server {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(&dir.path().join("accordion.db")).unwrap();
    let identity = Identity::new(db.clone()).await.unwrap();
    let storage = Storage::open(db.clone(), identity.clone(), &dir.path().join("storage"))
        .await
        .unwrap();
    let events = Events::open(db.clone()).await.unwrap();
    Server {
        _dir: dir,
        db,
        storage,
        identity,
        events,
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

    /// Storage acting as `user` in the web app.
    fn by(&self, user: UserId) -> Storage {
        self.storage.acting_as(Actor::person(user, Via::Web))
    }

    async fn activity(&self, reader: UserId, id: ItemId) -> Vec<Event> {
        self.events.activity(reader, id, None, 100).await.unwrap()
    }

    /// Each notification's action, and whether it's been seen and read.
    async fn notified(&self, user: UserId) -> Vec<(Action, bool, bool)> {
        self.events
            .notifications(user, 50)
            .await
            .unwrap()
            .into_iter()
            .map(|notice| (notice.event.action, notice.seen, notice.read))
            .collect()
    }

    async fn count(&self) -> u32 {
        self.db
            .read(|conn| conn.query_row("SELECT count(*) FROM events_log", [], |row| row.get(0)))
            .await
            .unwrap()
    }
}

fn actions(events: &[Event]) -> Vec<Action> {
    events.iter().map(|event| event.action).collect()
}

#[tokio::test]
async fn each_change_records_one_event_as_whoever_made_it() {
    let s = start().await;
    let alex = s.user("alex").await;
    let storage = s.by(alex);
    let top = storage.top_folder(alex).await.unwrap();
    let start = s.count().await;

    let folder = storage.create_folder(top, "Taxes").await.unwrap();
    let file = storage
        .write_file(folder.id, "2025.pdf", &b"one"[..])
        .await
        .unwrap();
    let other = storage.create_folder(top, "Old").await.unwrap();
    storage
        .move_item(file.id, other.id, "2025 final.pdf")
        .await
        .unwrap();
    storage.delete(file.id).await.unwrap();
    storage.restore(file.id).await.unwrap();
    let copy = storage
        .copy_file(file.id, folder.id, "copy.pdf")
        .await
        .unwrap();

    let events = s.activity(alex, top).await;
    // Putting it back right away undid moving it to the trash.
    assert_eq!(s.count().await - start, 6);
    assert_eq!(
        actions(&events),
        [
            Action::Added,
            Action::Renamed,
            Action::Moved,
            Action::Added,
            Action::Added,
            Action::Added,
        ]
    );
    for event in &events {
        assert_eq!(event.actor, Some(alex));
        assert_eq!(event.via, Via::Web);
        assert_eq!(event.owner, Some(alex));
    }
    assert_eq!(events[0].item, Some(copy.id));
    assert_eq!(events[0].details, json!({ "how": "copied" }));
    assert_eq!(
        events[1].details,
        json!({ "from": "2025.pdf", "to": "2025 final.pdf" })
    );
    assert_eq!(
        events[2].details,
        json!({ "from": folder.id, "to": other.id })
    );
    assert_eq!(events[3].details, json!({ "how": "created" }));
    assert_eq!(events[4].details, json!({ "how": "uploaded" }));

    // Someone else restoring it is a change of its own.
    storage.delete(file.id).await.unwrap();
    s.storage.restore(file.id).await.unwrap();
    assert_eq!(
        actions(&s.activity(alex, file.id).await)[..2],
        [Action::Restored, Action::Trashed]
    );
    let start = s.count().await;

    // A change that fails records nothing.
    assert!(storage.create_folder(top, "Taxes").await.is_err());
    assert!(storage.delete(top).await.is_err());
    assert_eq!(s.count().await, start);

    // A handle with nobody acting is the server.
    s.storage
        .create_folder(top, "From the server")
        .await
        .unwrap();
    let latest = &s.activity(alex, top).await[0];
    assert_eq!((latest.actor, latest.via), (None, Via::Server));
}

#[tokio::test]
async fn repeated_edits_become_one_event() {
    let s = start().await;
    let alex = s.user("alex").await;
    let sam = s.user("sam").await;
    let top = s.storage.top_folder(alex).await.unwrap();
    let storage = s.by(alex);

    // File Explorer saves a new file empty, then again with its contents.
    let file = storage
        .write_file(top, "notes.txt", tokio::io::empty())
        .await
        .unwrap();
    storage
        .write_file(top, "notes.txt", &b"hello"[..])
        .await
        .unwrap();
    storage
        .write_file(top, "notes.txt", &b"hello again"[..])
        .await
        .unwrap();
    assert_eq!(actions(&s.activity(alex, file.id).await), [Action::Added]);

    // Someone else's edit is its own event, and so is the owner's next one.
    let folder = storage.create_folder(top, "Shared").await.unwrap();
    let file = storage
        .write_file(folder.id, "plan.txt", &b"a"[..])
        .await
        .unwrap();
    storage
        .share(folder.id, sam, ShareLevel::Editor, alex)
        .await
        .unwrap();
    s.by(sam)
        .write_file(folder.id, "plan.txt", &b"b"[..])
        .await
        .unwrap();
    storage
        .write_file(folder.id, "plan.txt", &b"c"[..])
        .await
        .unwrap();
    assert_eq!(
        actions(&s.activity(alex, file.id).await),
        [Action::Edited, Action::Edited, Action::Added]
    );
}

#[tokio::test]
async fn sharing_is_recorded_and_notifies_the_person() {
    let s = start().await;
    let alex = s.user("alex").await;
    let sam = s.user("sam").await;
    let top = s.storage.top_folder(alex).await.unwrap();
    let storage = s.by(alex);
    let folder = storage.create_folder(top, "Holiday").await.unwrap();

    storage
        .share(folder.id, sam, ShareLevel::Viewer, alex)
        .await
        .unwrap();
    // The same level again changes nothing.
    storage
        .share(folder.id, sam, ShareLevel::Viewer, alex)
        .await
        .unwrap();
    storage
        .share(folder.id, sam, ShareLevel::Editor, alex)
        .await
        .unwrap();
    assert_eq!(
        s.notified(sam).await,
        [
            (Action::ShareChanged, false, false),
            (Action::Shared, false, false)
        ]
    );

    // An editor's changes reach the owner, and the owner's own don't.
    s.by(sam)
        .write_file(folder.id, "beach.jpg", &b"sand"[..])
        .await
        .unwrap();
    storage
        .write_file(folder.id, "mine.jpg", &b"own"[..])
        .await
        .unwrap();
    assert_eq!(s.notified(alex).await, [(Action::Added, false, false)]);

    // Sam sees sharing with Sam, but not who else has it.
    let kim = s.user("kim").await;
    storage
        .share(folder.id, kim, ShareLevel::Viewer, alex)
        .await
        .unwrap();
    let sams = actions(&s.activity(sam, folder.id).await);
    assert!(sams.contains(&Action::Shared) && sams.contains(&Action::ShareChanged));
    assert_eq!(
        s.activity(sam, folder.id)
            .await
            .iter()
            .filter(|event| event.other == Some(kim))
            .count(),
        0
    );
    assert_eq!(
        actions(&s.activity(alex, folder.id).await)[..4],
        [
            Action::Shared,
            Action::Added,
            Action::Added,
            Action::ShareChanged
        ]
    );

    // Opening the list sees everything, and selecting one reads it.
    let notices = s.events.notifications(sam, 50).await.unwrap();
    let (latest, oldest) = (notices[0].event.id, notices[1].event.id);
    s.events.mark_seen(sam, latest).await.unwrap();
    s.events.mark_read(sam, vec![oldest]).await.unwrap();
    assert_eq!(
        s.notified(sam).await,
        [
            (Action::ShareChanged, true, false),
            (Action::Shared, true, true)
        ]
    );

    // Losing access: removing it doesn't notify, and what's left about the
    // folder disappears.
    storage.unshare(folder.id, sam, alex).await.unwrap();
    assert_eq!(s.notified(sam).await, []);
    assert!(s.events.activity(sam, folder.id, None, 10).await.is_err());

    // The sharing history moved into the log.
    let history = s.storage.share_history(folder.id).await.unwrap();
    let levels: Vec<_> = history.iter().map(|event| event.level).collect();
    assert_eq!(
        levels,
        [
            Some(ShareLevel::Viewer),
            Some(ShareLevel::Editor),
            Some(ShareLevel::Viewer),
            None
        ]
    );
}

#[tokio::test]
async fn only_the_owner_sees_the_trash_and_what_was_deleted_for_good() {
    let s = start().await;
    let alex = s.user("alex").await;
    let sam = s.user("sam").await;
    let top = s.storage.top_folder(alex).await.unwrap();
    let storage = s.by(alex);
    let folder = storage.create_folder(top, "Holiday").await.unwrap();
    let kept = storage
        .write_file(folder.id, "kept.jpg", &b"1"[..])
        .await
        .unwrap();
    let binned = storage
        .write_file(folder.id, "binned.jpg", &b"2"[..])
        .await
        .unwrap();
    storage
        .share(folder.id, sam, ShareLevel::Editor, alex)
        .await
        .unwrap();
    s.by(sam).delete(binned.id).await.unwrap();

    let sams = s.activity(sam, folder.id).await;
    assert!(sams.iter().all(|event| event.item != Some(binned.id)));
    assert!(sams.iter().any(|event| event.item == Some(kept.id)));
    let alexs = s.activity(alex, folder.id).await;
    assert_eq!(alexs[0].action, Action::Trashed);
    assert_eq!(alexs[0].actor, Some(sam));

    // Deleting for good takes the item's activity with it, and leaves one
    // event on the folder it was in, which only the owner sees.
    storage.delete_forever(binned.id).await.unwrap();
    let alexs = s.activity(alex, folder.id).await;
    assert!(alexs.iter().all(|event| event.item != Some(binned.id)));
    assert_eq!(alexs[0].action, Action::Deleted);
    assert_eq!(alexs[0].item, Some(folder.id));
    assert_eq!(alexs[0].details, json!({ "name": "binned.jpg" }));
    assert!(
        s.activity(sam, folder.id)
            .await
            .iter()
            .all(|event| event.action != Action::Deleted)
    );

    // Nobody without access sees anything.
    let kim = s.user("kim").await;
    assert!(s.events.activity(kim, folder.id, None, 10).await.is_err());
}

#[tokio::test]
async fn sign_ins_and_account_changes_make_up_the_activity_log() {
    let s = start().await;
    let alex = s.user("alex").await;
    let sam = s.user("sam").await;
    let from = SignInFrom {
        address: HOME,
        home_network: true,
    };
    let identity = s.identity.acting_as(Actor::visitor(Via::Web, HOME));

    identity
        .check_password("sam", "wrong", from)
        .await
        .unwrap_err();
    // An unknown username has no account to record it for.
    identity
        .check_password("nobody", "wrong", from)
        .await
        .unwrap_err();
    identity
        .check_password("sam", "correct horse", from)
        .await
        .unwrap();
    let (_, session) = identity
        .create_session(
            sam,
            Client::Browser,
            "Firefox on Windows",
            AuthMethod::Password,
        )
        .await
        .unwrap();
    let sams = s.identity.acting_as(Actor::person(sam, Via::Web));
    sams.create_app_password(sam, AppPasswordUse::Dav, "Laptop")
        .await
        .unwrap();
    sams.sign_out(sam, session.id).await.unwrap();

    let log = s.events.account_log(sam, None, 50).await.unwrap();
    assert_eq!(
        actions(&log),
        [
            Action::SignedOut,
            Action::AppPasswordCreated,
            Action::SignedIn,
            Action::SignInFailed,
        ]
    );
    assert_eq!(
        log[2].details,
        json!({ "device": "Firefox on Windows", "address": "192.168.1.20" })
    );
    assert_eq!(log[3].actor, None);
    assert_eq!(log[3].other, Some(sam));

    // The admin log has the failed sign-in and admin actions, and nobody's
    // files.
    let admin = s.identity.acting_as(Actor::person(alex, Via::Web));
    admin.set_role(sam, Role::Admin).await.unwrap();
    s.by(alex)
        .create_folder(s.storage.top_folder(alex).await.unwrap(), "Private")
        .await
        .unwrap();
    let admin_log = s.events.admin_log(None, 50).await.unwrap();
    assert_eq!(
        actions(&admin_log),
        [
            Action::AccountChanged,
            Action::SignInFailed,
            Action::AccountCreated,
            Action::AccountCreated,
        ]
    );
    assert_eq!(admin_log[0].actor, Some(alex));
    assert_eq!(
        admin_log[0].details,
        json!({ "change": "role", "role": "admin" })
    );

    // Deleting the account deletes its own log, and the admin log keeps
    // the deletion with the name.
    admin.set_role(sam, Role::User).await.unwrap();
    admin.turn_off_member(sam).await.unwrap();
    s.storage
        .remove_owner(sam, LeaveFiles::Delete)
        .await
        .unwrap();
    admin.delete_member(sam).await.unwrap();
    assert_eq!(s.events.account_log(sam, None, 50).await.unwrap(), []);
    let latest = &s.events.admin_log(None, 1).await.unwrap()[0];
    assert_eq!(latest.action, Action::AccountDeleted);
    assert_eq!(latest.details, json!({ "name": "sam" }));
}

#[tokio::test]
async fn modules_record_their_own_events() {
    let s = start().await;
    let alex = s.user("alex").await;
    let top = s.storage.top_folder(alex).await.unwrap();
    let file = s
        .by(alex)
        .write_file(top, "a.txt", &b"a"[..])
        .await
        .unwrap();
    s.events
        .record(
            Actor::person(alex, Via::Web),
            NewEvent::about(Action::DescriptionChanged, file.id, alex),
        )
        .await
        .unwrap();
    assert_eq!(
        actions(&s.activity(alex, file.id).await),
        [Action::DescriptionChanged, Action::Added]
    );
}

#[tokio::test]
async fn expired_events_and_old_read_notifications_are_pruned() {
    let s = start().await;
    let alex = s.user("alex").await;
    let sam = s.user("sam").await;
    let top = s.storage.top_folder(alex).await.unwrap();
    let folder = s.by(alex).create_folder(top, "Holiday").await.unwrap();
    s.by(alex)
        .share(folder.id, sam, ShareLevel::Viewer, alex)
        .await
        .unwrap();
    let notice = s.events.notifications(sam, 1).await.unwrap()[0].event.id;
    s.events.mark_read(sam, vec![notice]).await.unwrap();

    assert_eq!(s.events.keep_days().await.unwrap(), Some(365));
    assert!(
        s.events
            .set_keep_days(Actor::person(alex, Via::Web), Some(30))
            .await
            .is_err()
    );
    s.events
        .set_keep_days(Actor::person(alex, Via::Web), Some(90))
        .await
        .unwrap();

    // Age everything but the setting change past 90 days.
    s.db.write(|tx| {
        tx.execute(
            "UPDATE events_log SET at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-100 days')
             WHERE action != 'retention_changed'",
            [],
        )?;
        tx.execute(
            "UPDATE events_notices SET read_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-100 days')",
            [],
        )
    })
    .await
    .unwrap();
    let total = s.count().await;
    let pruned = s.events.prune().await.unwrap();
    assert_eq!(pruned.events as u32, total - 1);
    assert_eq!(s.count().await, 1);
    assert_eq!(s.notified(sam).await, []);

    // Forever keeps everything.
    s.events
        .set_keep_days(Actor::person(alex, Via::Web), None)
        .await
        .unwrap();
    s.db.write(|tx| {
        tx.execute(
            "UPDATE events_log SET at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-10 years')",
            [],
        )
    })
    .await
    .unwrap();
    assert_eq!(s.events.prune().await.unwrap().events, 0);
    assert_eq!(s.count().await, 2);
}

#[tokio::test]
async fn notifications_can_be_removed_one_at_a_time_or_all_at_once() {
    let s = start().await;
    let alex = s.user("alex").await;
    let sam = s.user("sam").await;
    let top = s.storage.top_folder(alex).await.unwrap();
    let mut shared = Vec::new();
    for name in ["One", "Two", "Three"] {
        let folder = s.by(alex).create_folder(top, name).await.unwrap();
        s.by(alex)
            .share(folder.id, sam, ShareLevel::Viewer, alex)
            .await
            .unwrap();
        shared.push(folder.id);
    }
    let notices = s.events.notifications(sam, 50).await.unwrap();
    assert_eq!(notices.len(), 3);

    s.events
        .remove_notifications(sam, vec![notices[1].event.id])
        .await
        .unwrap();
    let left: Vec<_> = s
        .events
        .notifications(sam, 50)
        .await
        .unwrap()
        .into_iter()
        .map(|notice| notice.event.item)
        .collect();
    assert_eq!(left, [Some(shared[2]), Some(shared[0])]);
    // Only Sam's notifications go. The events stay in the log.
    assert_eq!(s.activity(alex, shared[1]).await.len(), 2);

    // Clearing stops at what was shown, so a newer one stays.
    let shown = notices[0].event.id;
    let four = s.by(alex).create_folder(top, "Four").await.unwrap();
    s.by(alex)
        .share(four.id, sam, ShareLevel::Viewer, alex)
        .await
        .unwrap();
    s.events.clear_notifications(sam, shown).await.unwrap();
    let left = s.events.notifications(sam, 50).await.unwrap();
    assert_eq!(left.len(), 1);
    assert_eq!(left[0].event.item, Some(four.id));
}

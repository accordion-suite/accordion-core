//! Live updates: which hints each change sends, who hears them, and when
//! streams end (decision 19).

use std::time::Duration;

use accordion_core::db::Database;
use accordion_core::events::{Actor, Events, Via};
use accordion_core::identity::{AuthMethod, Client, Identity, NewUser};
use accordion_core::live::{self, Live, LiveStream, Message, STREAMS_EACH};
use accordion_core::storage::{ShareLevel, Storage};
use accordion_types::{ItemId, Role, UserId};
use tokio::sync::watch;

struct Server {
    _dir: tempfile::TempDir,
    storage: Storage,
    identity: Identity,
    events: Events,
    live: Live,
    stop: watch::Sender<bool>,
}

async fn start() -> Server {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(&dir.path().join("accordion.db")).unwrap();
    let identity = Identity::new(db.clone()).await.unwrap();
    let storage = Storage::open(db.clone(), identity.clone(), &dir.path().join("storage"))
        .await
        .unwrap();
    let events = Events::open(db.clone()).await.unwrap();
    let live = Live::new(db, &identity);
    let (stop, mut stopped) = watch::channel(false);
    tokio::spawn(live.clone().run(async move {
        let _ = stopped.wait_for(|&stopped| stopped).await;
    }));
    Server {
        _dir: dir,
        storage,
        identity,
        events,
        live,
        stop,
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

    fn by(&self, user: UserId) -> Storage {
        self.storage.acting_as(Actor::person(user, Via::Web))
    }
}

/// The messages that arrive within a second and a half, by which time
/// combined hints have been sent.
async fn received(stream: &mut LiveStream) -> Vec<Message> {
    let mut messages = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_millis(1500);
    while let Ok(Some(message)) = tokio::time::timeout_at(deadline, stream.next()).await {
        messages.push(message);
    }
    messages
}

fn items(folders: &[ItemId]) -> Message {
    let mut ids: Vec<String> = folders.iter().map(ToString::to_string).collect();
    ids.sort();
    Message {
        kind: live::DRIVE_ITEMS,
        ids,
    }
}

fn person(kind: &'static str) -> Message {
    Message {
        kind,
        ids: Vec::new(),
    }
}

#[tokio::test]
async fn changes_reach_everyone_who_can_open_the_folder_and_nobody_else() {
    let s = start().await;
    let alex = s.user("alex").await;
    let sam = s.user("sam").await;
    let kim = s.user("kim").await;
    let storage = s.by(alex);
    let top = storage.top_folder(alex).await.unwrap();
    let holiday = storage.create_folder(top, "Holiday").await.unwrap();
    let inside = storage.create_folder(holiday.id, "Beach").await.unwrap();
    storage
        .share(holiday.id, sam, ShareLevel::Editor, alex)
        .await
        .unwrap();
    // Let the hints from setting up go by.
    tokio::time::sleep(Duration::from_secs(1)).await;

    let mut alex_stream = s.live.open(alex, None);
    let mut sam_stream = s.live.open(sam, None);
    let mut kim_stream = s.live.open(kim, None);
    // Hints close together become one message, naming the folder once.
    for name in ["one.jpg", "two.jpg", "three.jpg"] {
        s.by(sam)
            .write_file(inside.id, name, &b"sand"[..])
            .await
            .unwrap();
    }
    assert_eq!(
        received(&mut alex_stream).await,
        [items(&[inside.id]), person(live::NOTIFICATIONS)]
    );
    assert_eq!(received(&mut sam_stream).await, [items(&[inside.id])]);
    assert_eq!(received(&mut kim_stream).await, []);
}

#[tokio::test]
async fn moves_name_both_folders_each_to_who_can_open_it() {
    let s = start().await;
    let alex = s.user("alex").await;
    let sam = s.user("sam").await;
    let storage = s.by(alex);
    let top = storage.top_folder(alex).await.unwrap();
    let shared = storage.create_folder(top, "Shared").await.unwrap();
    let private = storage.create_folder(top, "Private").await.unwrap();
    let file = storage
        .write_file(shared.id, "plan.txt", &b"a"[..])
        .await
        .unwrap();
    storage
        .share(shared.id, sam, ShareLevel::Viewer, alex)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;

    let mut alex_stream = s.live.open(alex, None);
    let mut sam_stream = s.live.open(sam, None);
    storage
        .move_item(file.id, private.id, "plan.txt")
        .await
        .unwrap();
    assert_eq!(
        received(&mut alex_stream).await,
        [items(&[shared.id, private.id])]
    );
    assert_eq!(received(&mut sam_stream).await, [items(&[shared.id])]);
}

#[tokio::test]
async fn sharing_tells_the_other_person_even_when_it_stops() {
    let s = start().await;
    let alex = s.user("alex").await;
    let sam = s.user("sam").await;
    let storage = s.by(alex);
    let top = storage.top_folder(alex).await.unwrap();
    let folder = storage.create_folder(top, "Recipes").await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Sam hears about the folder itself, but not Alex's top folder, which
    // Sam can't open.
    let mut sam_stream = s.live.open(sam, None);
    storage
        .share(folder.id, sam, ShareLevel::Viewer, alex)
        .await
        .unwrap();
    assert_eq!(
        received(&mut sam_stream).await,
        [
            items(&[folder.id]),
            person(live::DRIVE_SHARED),
            person(live::NOTIFICATIONS)
        ]
    );
    storage.unshare(folder.id, sam, alex).await.unwrap();
    assert_eq!(
        received(&mut sam_stream).await,
        [person(live::DRIVE_SHARED)]
    );
}

#[tokio::test]
async fn reading_notifications_tells_the_other_tabs() {
    let s = start().await;
    let alex = s.user("alex").await;
    let sam = s.user("sam").await;
    let storage = s.by(alex);
    let top = storage.top_folder(alex).await.unwrap();
    let folder = storage.create_folder(top, "Recipes").await.unwrap();
    storage
        .share(folder.id, sam, ShareLevel::Viewer, alex)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;

    let mut sam_stream = s.live.open(sam, None);
    let notices = s.events.notifications(sam, 10).await.unwrap();
    let newest = notices[0].event.id;
    s.events.mark_seen(sam, newest).await.unwrap();
    assert_eq!(
        received(&mut sam_stream).await,
        [person(live::NOTIFICATIONS)]
    );
    // Nothing changed this time, so nothing is sent.
    s.events.mark_seen(sam, newest).await.unwrap();
    assert_eq!(received(&mut sam_stream).await, []);
    s.events.clear_notifications(sam, newest).await.unwrap();
    assert_eq!(
        received(&mut sam_stream).await,
        [person(live::NOTIFICATIONS)]
    );
}

#[tokio::test]
async fn a_failed_change_sends_nothing() {
    let s = start().await;
    let alex = s.user("alex").await;
    let storage = s.by(alex);
    let top = storage.top_folder(alex).await.unwrap();
    storage.create_folder(top, "Taxes").await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;

    let mut stream = s.live.open(alex, None);
    storage.create_folder(top, "Taxes").await.unwrap_err();
    assert_eq!(received(&mut stream).await, []);
}

#[tokio::test]
async fn ending_a_session_ends_its_streams() {
    let s = start().await;
    let alex = s.user("alex").await;
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
    let mut phone_stream = s.live.open(alex, Some(phone.id));
    let mut laptop_stream = s.live.open(alex, Some(laptop.id));

    s.identity.sign_out(alex, phone.id).await.unwrap();
    let ended = tokio::time::timeout(Duration::from_secs(5), phone_stream.next()).await;
    assert_eq!(ended.unwrap(), None);
    assert!(
        tokio::time::timeout(Duration::from_millis(500), laptop_stream.next())
            .await
            .is_err(),
        "the laptop stays connected"
    );
    assert_eq!(s.live.open_streams(), 1);
}

#[tokio::test]
async fn opening_too_many_streams_closes_the_oldest() {
    let s = start().await;
    let alex = s.user("alex").await;
    let sam = s.user("sam").await;
    let _sam_stream = s.live.open(sam, None);
    let mut first = s.live.open(alex, None);
    let _rest: Vec<LiveStream> = (0..STREAMS_EACH).map(|_| s.live.open(alex, None)).collect();
    assert_eq!(first.next().await, None);
    assert_eq!(s.live.open_streams(), STREAMS_EACH + 1);
}

#[tokio::test]
async fn stopping_ends_every_stream() {
    let s = start().await;
    let alex = s.user("alex").await;
    let mut stream = s.live.open(alex, None);
    s.stop.send(true).unwrap();
    let ended = tokio::time::timeout(Duration::from_secs(5), stream.next()).await;
    assert_eq!(ended.unwrap(), None);
    // Nothing opens once the server is stopping.
    let mut late = s.live.open(alex, None);
    assert_eq!(late.next().await, None);
}

//! The list of changes that sync follows (decision 23): what each person's
//! devices hear, and when they have to start over.

use accordion_core::db::Database;
use accordion_core::identity::{Identity, NewUser};
use accordion_core::storage::{
    Access, Error, ItemKind, LeaveFiles, SYNC_PAGE, ShareLevel, Storage, SyncChange, SyncItem,
};
use accordion_types::{ItemId, Role, UserId};

struct Server {
    _dir: tempfile::TempDir,
    db: Database,
    storage: Storage,
    identity: Identity,
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

    /// Where `user`'s device starts.
    async fn cursor(&self, user: UserId) -> String {
        self.storage.sync_start(user).await.unwrap().cursor
    }

    /// Everything that changed for `user` after `cursor`, following every
    /// page, and the cursor to use next.
    async fn changes(&self, user: UserId, cursor: &str) -> (Vec<SyncChange>, String) {
        let mut cursor = cursor.to_owned();
        let mut all = Vec::new();
        loop {
            let page = self.storage.changes(user, &cursor).await.unwrap();
            all.extend(page.changes);
            cursor = page.cursor;
            if !page.more {
                return (all, cursor);
            }
        }
    }

    /// Runs SQL on the database directly, to stand in for time passing or
    /// a restored backup.
    async fn sql(&self, sql: &'static str) {
        self.db
            .write(move |tx| tx.execute_batch(sql))
            .await
            .unwrap();
    }
}

async fn start() -> Server {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(&dir.path().join("accordion.db")).unwrap();
    let identity = Identity::new(db.clone()).await.unwrap();
    let storage = Storage::open(db.clone(), identity.clone(), &dir.path().join("storage"))
        .await
        .unwrap();
    Server {
        _dir: dir,
        db,
        storage,
        identity,
    }
}

/// Alex owns a "Holiday" folder with a photo in an "Album" folder inside
/// it. Sam is family, with nothing shared yet.
struct Family {
    server: Server,
    alex: UserId,
    sam: UserId,
    top: ItemId,
    holiday: ItemId,
    album: ItemId,
    photo: ItemId,
}

async fn family() -> Family {
    let server = start().await;
    let alex = server.user("alex").await;
    let sam = server.user("sam").await;
    let top = server.storage.top_folder(alex).await.unwrap();
    let holiday = server
        .storage
        .create_folder(top, "Holiday")
        .await
        .unwrap()
        .id;
    let album = server
        .storage
        .create_folder(holiday, "Album")
        .await
        .unwrap()
        .id;
    let photo = server
        .storage
        .write_file(album, "beach.jpg", &b"sand"[..])
        .await
        .unwrap()
        .id;
    Family {
        server,
        alex,
        sam,
        top,
        holiday,
        album,
        photo,
    }
}

fn changed(changes: &[SyncChange]) -> Vec<&SyncItem> {
    changes
        .iter()
        .filter_map(|change| match change {
            SyncChange::Changed(item) => Some(item.as_ref()),
            SyncChange::Gone(_) => None,
        })
        .collect()
}

fn ids(changes: &[SyncChange]) -> Vec<ItemId> {
    changes
        .iter()
        .map(|change| match change {
            SyncChange::Changed(item) => item.item.id,
            SyncChange::Gone(id) => *id,
        })
        .collect()
}

fn gone(changes: &[SyncChange], id: ItemId) -> bool {
    changes.contains(&SyncChange::Gone(id))
}

#[tokio::test]
async fn a_new_device_lists_everything_then_follows_changes() {
    let f = family().await;
    let start = f.server.storage.sync_start(f.alex).await.unwrap();
    assert_eq!(start.roots.len(), 1);
    assert_eq!(start.roots[0].item.id, f.top);
    assert_eq!(start.roots[0].parent, None);
    assert_eq!(start.roots[0].access, Access::Owner);

    let page = f
        .server
        .storage
        .sync_list(f.alex, f.top, None)
        .await
        .unwrap();
    let mut listed: Vec<ItemId> = page.items.iter().map(|entry| entry.item.id).collect();
    listed.sort_unstable();
    let mut expected = vec![f.holiday, f.album, f.photo];
    expected.sort_unstable();
    assert_eq!(listed, expected);
    assert_eq!(page.next, None);
    let photo = page
        .items
        .iter()
        .find(|entry| entry.item.id == f.photo)
        .unwrap();
    assert_eq!(photo.parent, Some(f.album));

    // Nothing changed since the cursor was taken.
    let (changes, cursor) = f.server.changes(f.alex, &start.cursor).await;
    assert!(changes.is_empty());

    let notes = f
        .server
        .storage
        .write_file(f.top, "notes.txt", &b"one"[..])
        .await
        .unwrap();
    let (changes, cursor) = f.server.changes(f.alex, &cursor).await;
    assert_eq!(ids(&changes), [notes.id]);
    let (changes, _) = f.server.changes(f.alex, &cursor).await;
    assert!(changes.is_empty(), "the cursor moved past what was seen");
}

#[tokio::test]
async fn an_item_that_changed_many_times_comes_once_as_it_is_now() {
    let f = family().await;
    let cursor = f.server.cursor(f.alex).await;
    for contents in ["sea", "sun", "shells"] {
        f.server
            .storage
            .write_file(f.album, "beach.jpg", contents.as_bytes())
            .await
            .unwrap();
    }
    f.server
        .storage
        .move_item(f.photo, f.holiday, "best.jpg")
        .await
        .unwrap();
    let (changes, _) = f.server.changes(f.alex, &cursor).await;
    assert_eq!(ids(&changes), [f.photo]);
    let photo = changed(&changes)[0];
    assert_eq!(photo.item.name, "best.jpg");
    assert_eq!(photo.parent, Some(f.holiday));
    let ItemKind::File { blob, .. } = &photo.item.kind else {
        panic!("a file");
    };
    assert_eq!(blob.size, "shells".len() as u64);
}

#[tokio::test]
async fn sharing_brings_the_whole_folder_and_unsharing_takes_it_away() {
    let f = family().await;
    let cursor = f.server.cursor(f.sam).await;
    f.server
        .storage
        .share(f.holiday, f.sam, ShareLevel::Viewer, f.alex)
        .await
        .unwrap();
    let (changes, cursor) = f.server.changes(f.sam, &cursor).await;
    let holiday = changed(&changes)[0];
    assert_eq!(holiday.item.id, f.holiday);
    assert!(holiday.whole, "Sam's device lists what's inside");
    assert_eq!(holiday.parent, None, "Alex's top folder stays hidden");
    assert_eq!(holiday.access, Access::Viewer);

    let start = f.server.storage.sync_start(f.sam).await.unwrap();
    let roots: Vec<ItemId> = start.roots.iter().map(|root| root.item.id).collect();
    assert!(roots.contains(&f.holiday));
    let page = f
        .server
        .storage
        .sync_list(f.sam, f.holiday, None)
        .await
        .unwrap();
    assert_eq!(page.items.len(), 2);
    assert!(
        page.items
            .iter()
            .all(|entry| entry.access == Access::Viewer)
    );

    // Changes inside reach Sam.
    f.server
        .storage
        .write_file(f.album, "beach.jpg", &b"new"[..])
        .await
        .unwrap();
    let (changes, cursor) = f.server.changes(f.sam, &cursor).await;
    assert_eq!(ids(&changes), [f.photo]);

    // A higher level is a change to everything inside.
    f.server
        .storage
        .share(f.holiday, f.sam, ShareLevel::Editor, f.alex)
        .await
        .unwrap();
    let (changes, cursor) = f.server.changes(f.sam, &cursor).await;
    let holiday = changed(&changes)[0];
    assert!(holiday.whole);
    assert_eq!(holiday.access, Access::Editor);

    f.server
        .storage
        .unshare(f.holiday, f.sam, f.alex)
        .await
        .unwrap();
    let (changes, _) = f.server.changes(f.sam, &cursor).await;
    assert_eq!(changes, [SyncChange::Gone(f.holiday)]);
}

#[tokio::test]
async fn nobody_hears_about_items_they_cant_open() {
    let f = family().await;
    let sams = f.server.cursor(f.sam).await;
    f.server
        .storage
        .write_file(f.top, "private.txt", &b"mine"[..])
        .await
        .unwrap();
    f.server
        .storage
        .write_file(f.album, "beach.jpg", &b"again"[..])
        .await
        .unwrap();
    let (changes, _) = f.server.changes(f.sam, &sams).await;
    assert!(changes.is_empty());
    assert!(matches!(
        f.server.storage.sync_list(f.sam, f.holiday, None).await,
        Err(Error::NotFound)
    ));
}

#[tokio::test]
async fn moving_in_or_out_of_a_shared_folder_changes_who_has_it() {
    let f = family().await;
    f.server
        .storage
        .share(f.holiday, f.sam, ShareLevel::Viewer, f.alex)
        .await
        .unwrap();
    let alexs = f.server.cursor(f.alex).await;
    let sams = f.server.cursor(f.sam).await;

    // Out: Sam hears it's gone, and Alex that it moved.
    f.server
        .storage
        .move_item(f.album, f.top, "Album")
        .await
        .unwrap();
    let (changes, sams) = f.server.changes(f.sam, &sams).await;
    assert_eq!(changes, [SyncChange::Gone(f.album)]);
    let (changes, alexs) = f.server.changes(f.alex, &alexs).await;
    let album = changed(&changes)[0];
    assert_eq!(album.parent, Some(f.top));
    assert!(!album.whole, "Alex had everything inside already");

    // Back in: Sam lists everything inside it.
    f.server
        .storage
        .move_item(f.album, f.holiday, "Album")
        .await
        .unwrap();
    let (changes, _) = f.server.changes(f.sam, &sams).await;
    let album = changed(&changes)[0];
    assert_eq!(album.item.id, f.album);
    assert!(album.whole);
    assert_eq!(album.parent, Some(f.holiday));
    let (changes, _) = f.server.changes(f.alex, &alexs).await;
    assert!(!changed(&changes)[0].whole);
}

#[tokio::test]
async fn the_trash_is_gone_until_its_restored() {
    let f = family().await;
    f.server
        .storage
        .share(f.holiday, f.sam, ShareLevel::Editor, f.alex)
        .await
        .unwrap();
    let alexs = f.server.cursor(f.alex).await;
    let sams = f.server.cursor(f.sam).await;
    f.server.storage.delete(f.album).await.unwrap();
    let (changes, alexs) = f.server.changes(f.alex, &alexs).await;
    assert_eq!(changes, [SyncChange::Gone(f.album)]);
    let (changes, sams) = f.server.changes(f.sam, &sams).await;
    assert_eq!(changes, [SyncChange::Gone(f.album)]);
    let page = f
        .server
        .storage
        .sync_list(f.alex, f.top, None)
        .await
        .unwrap();
    assert!(page.items.iter().all(|entry| entry.item.id != f.photo));

    f.server.storage.restore(f.album).await.unwrap();
    for (user, cursor) in [(f.alex, &alexs), (f.sam, &sams)] {
        let (changes, _) = f.server.changes(user, cursor).await;
        let album = changed(&changes)[0];
        assert_eq!(album.item.id, f.album);
        assert!(album.whole, "what's inside was out of sight");
    }

    f.server.storage.delete(f.album).await.unwrap();
    let (_, alexs) = f.server.changes(f.alex, &alexs).await;
    f.server.storage.delete_forever(f.album).await.unwrap();
    let (changes, _) = f.server.changes(f.alex, &alexs).await;
    assert!(gone(&changes, f.album));
}

#[tokio::test]
async fn a_deleted_account_takes_its_shares_with_it() {
    let f = family().await;
    f.server
        .storage
        .share(f.holiday, f.sam, ShareLevel::Viewer, f.alex)
        .await
        .unwrap();
    let sams = f.server.cursor(f.sam).await;
    f.server
        .storage
        .remove_owner(f.alex, LeaveFiles::Delete)
        .await
        .unwrap();
    let (changes, _) = f.server.changes(f.sam, &sams).await;
    assert_eq!(changes, [SyncChange::Gone(f.holiday)]);
}

#[tokio::test]
async fn files_given_to_someone_arrive_whole() {
    let f = family().await;
    let sams = f.server.cursor(f.sam).await;
    let folder = f
        .server
        .storage
        .remove_owner(
            f.alex,
            LeaveFiles::GiveTo {
                owner: f.sam,
                folder_name: "Alex's files".into(),
            },
        )
        .await
        .unwrap()
        .unwrap();
    let (changes, _) = f.server.changes(f.sam, &sams).await;
    let given = changed(&changes);
    assert_eq!(given.len(), 1);
    assert_eq!(given[0].item.id, folder.id);
    assert!(given[0].whole);
    let page = f
        .server
        .storage
        .sync_list(f.sam, folder.id, None)
        .await
        .unwrap();
    assert_eq!(page.items.len(), 3);
}

#[tokio::test]
async fn a_long_list_comes_in_pages() {
    let f = family().await;
    let cursor = f.server.cursor(f.alex).await;
    let count = SYNC_PAGE + 5;
    for n in 0..count {
        f.server
            .storage
            .create_folder(f.top, &format!("Folder {n}"))
            .await
            .unwrap();
    }
    let first = f.server.storage.changes(f.alex, &cursor).await.unwrap();
    assert_eq!(first.changes.len(), SYNC_PAGE);
    assert!(first.more);
    let (rest, _) = f.server.changes(f.alex, &first.cursor).await;
    assert_eq!(rest.len(), 5);

    let mut listed = 0;
    let mut after = None;
    loop {
        let page = f
            .server
            .storage
            .sync_list(f.alex, f.top, after)
            .await
            .unwrap();
        listed += page.items.len();
        match page.next {
            Some(next) => after = Some(next),
            None => break,
        }
    }
    assert_eq!(listed, count + 3);
}

#[tokio::test]
async fn a_cursor_the_server_cant_follow_means_starting_over() {
    let f = family().await;
    let cursor = f.server.cursor(f.alex).await;
    for broken in ["", "nonsense", "00.1", "zz.5"] {
        assert!(matches!(
            f.server.storage.changes(f.alex, broken).await,
            Err(Error::StartOver)
        ));
    }
    // A cursor from the future, as after a restored backup.
    let (generation, seq) = cursor.split_once('.').unwrap();
    let ahead = format!("{generation}.{}", seq.parse::<i64>().unwrap() + 10);
    assert!(matches!(
        f.server.storage.changes(f.alex, &ahead).await,
        Err(Error::StartOver)
    ));

    // A restored backup gets a new generation.
    f.server
        .sql("UPDATE storage_sync SET generation = randomblob(16)")
        .await;
    assert!(matches!(
        f.server.storage.changes(f.alex, &cursor).await,
        Err(Error::StartOver)
    ));
}

#[tokio::test]
async fn changes_older_than_90_days_are_pruned() {
    let f = family().await;
    let old = f.server.cursor(f.alex).await;
    f.server
        .storage
        .write_file(f.top, "a.txt", &b"a"[..])
        .await
        .unwrap();
    let (_, recent) = f.server.changes(f.alex, &old).await;
    f.server
        .sql("UPDATE storage_changes SET at = '2000-01-01T00:00:00.000Z'")
        .await;
    // The family's folders and photo, and a.txt.
    assert_eq!(f.server.storage.prune_changes().await.unwrap(), 4);
    assert!(matches!(
        f.server.storage.changes(f.alex, &old).await,
        Err(Error::StartOver)
    ));
    // A device that kept up is still fine.
    let (changes, _) = f.server.changes(f.alex, &recent).await;
    assert!(changes.is_empty());
}

#[tokio::test]
async fn uploads_can_refuse_a_taken_name_or_a_changed_file() {
    let f = family().await;
    let upload = |contents: &'static [u8]| {
        let storage = f.server.storage.clone();
        let alex = f.alex;
        async move {
            let upload = storage
                .start_upload(alex, contents.len() as u64)
                .await
                .unwrap();
            storage.append(upload.id, 0, contents).await.unwrap();
            upload.id
        }
    };
    let first = upload(b"one").await;
    assert!(matches!(
        f.server
            .storage
            .finish_new_upload(first, f.album, "beach.jpg")
            .await,
        Err(Error::NameTaken { .. })
    ));
    let new = f
        .server
        .storage
        .finish_new_upload(first, f.album, "waves.jpg")
        .await
        .unwrap();

    let ItemKind::File { blob, .. } = new.kind else {
        panic!("a file");
    };
    f.server
        .storage
        .write_file(f.album, "waves.jpg", &b"two"[..])
        .await
        .unwrap();
    let stale = upload(b"three").await;
    assert!(matches!(
        f.server
            .storage
            .finish_upload_over(stale, new.id, blob.hash)
            .await,
        Err(Error::Changed)
    ));
    let now = f.server.storage.item(new.id).await.unwrap().unwrap();
    let ItemKind::File { blob, .. } = now.kind else {
        panic!("a file");
    };
    let saved = f
        .server
        .storage
        .finish_upload_over(stale, new.id, blob.hash)
        .await
        .unwrap();
    assert_eq!(saved.id, new.id);
}

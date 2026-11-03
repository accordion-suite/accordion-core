//! Sharing items with other people, and what each person may then do
//! (decision 16).

use accordion_core::db::Database;
use accordion_core::identity::{Identity, NewUser};
use accordion_core::storage::{Access, Error, Found, Hit, LeaveFiles, Search, ShareLevel, Storage};
use accordion_core::texts::Words;
use accordion_types::{ItemId, Role, UserId};

struct Server {
    _dir: tempfile::TempDir,
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

    async fn access(&self, user: UserId, id: ItemId) -> Option<Access> {
        self.storage.access(user, id).await.unwrap()
    }
}

async fn start() -> Server {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(&dir.path().join("accordion.db")).unwrap();
    let identity = Identity::new(db.clone()).await.unwrap();
    let storage = Storage::open(db, identity.clone(), &dir.path().join("storage"))
        .await
        .unwrap();
    Server {
        _dir: dir,
        storage,
        identity,
    }
}

/// A family: Alex owns a "Holiday" folder with a photo in an "Album"
/// folder inside it.
struct Family {
    server: Server,
    alex: UserId,
    sam: UserId,
    kim: UserId,
    top: ItemId,
    holiday: ItemId,
    album: ItemId,
    photo: ItemId,
}

async fn family() -> Family {
    let server = start().await;
    let alex = server.user("alex").await;
    let sam = server.user("sam").await;
    let kim = server.user("kim").await;
    let top = server.storage.top_folder(alex).await.unwrap();
    let holiday = server.storage.create_folder(top, "Holiday").await.unwrap();
    let album = server
        .storage
        .create_folder(holiday.id, "Album")
        .await
        .unwrap();
    let photo = server
        .storage
        .write_file(album.id, "beach.jpg", &b"sand"[..])
        .await
        .unwrap();
    Family {
        server,
        alex,
        sam,
        kim,
        top,
        holiday: holiday.id,
        album: album.id,
        photo: photo.id,
    }
}

fn founds(hits: Vec<Hit>) -> Vec<Found> {
    hits.into_iter().map(|hit| hit.found).collect()
}

fn paths(found: &[Found]) -> Vec<String> {
    found
        .iter()
        .map(|found| {
            [&found.folders[..], std::slice::from_ref(&found.item.name)]
                .concat()
                .join("/")
        })
        .collect()
}

#[tokio::test]
async fn a_folder_share_covers_everything_inside_it() {
    let f = family().await;
    let s = &f.server;
    assert_eq!(s.access(f.alex, f.photo).await, Some(Access::Owner));
    assert_eq!(s.access(f.sam, f.photo).await, None);

    s.storage
        .share(f.holiday, f.sam, ShareLevel::Viewer, f.alex)
        .await
        .unwrap();
    for id in [f.holiday, f.album, f.photo] {
        assert_eq!(s.access(f.sam, id).await, Some(Access::Viewer));
    }
    // Not the folder above it, and not anyone else.
    assert_eq!(s.access(f.sam, f.top).await, None);
    assert_eq!(s.access(f.kim, f.photo).await, None);

    // What's added later is covered too.
    let added = s
        .storage
        .write_file(f.album, "later.jpg", &b"new"[..])
        .await
        .unwrap();
    assert_eq!(s.access(f.sam, added.id).await, Some(Access::Viewer));

    // The highest level wins, wherever it's given.
    s.storage
        .share(f.album, f.sam, ShareLevel::Editor, f.alex)
        .await
        .unwrap();
    assert_eq!(s.access(f.sam, f.holiday).await, Some(Access::Viewer));
    assert_eq!(s.access(f.sam, f.photo).await, Some(Access::Editor));
}

#[tokio::test]
async fn removing_someone_works_at_once() {
    let f = family().await;
    let s = &f.server;
    s.storage
        .share(f.holiday, f.sam, ShareLevel::Editor, f.alex)
        .await
        .unwrap();
    s.storage
        .share(f.holiday, f.kim, ShareLevel::Viewer, f.alex)
        .await
        .unwrap();
    s.storage.unshare(f.holiday, f.sam, f.alex).await.unwrap();
    assert_eq!(s.access(f.sam, f.photo).await, None);
    assert_eq!(s.access(f.kim, f.photo).await, Some(Access::Viewer));
    assert!(s.storage.shared_with(f.sam).await.unwrap().is_empty());
    // Removing someone who wasn't there changes nothing.
    s.storage.unshare(f.holiday, f.sam, f.alex).await.unwrap();

    let history: Vec<_> = s
        .storage
        .share_history(f.holiday)
        .await
        .unwrap()
        .into_iter()
        .map(|event| (event.user, event.level, event.by))
        .collect();
    assert_eq!(
        history,
        [
            (f.sam, Some(ShareLevel::Editor), f.alex),
            (f.kim, Some(ShareLevel::Viewer), f.alex),
            (f.sam, None, f.alex),
        ]
    );
}

#[tokio::test]
async fn lists_shares_and_whats_shared() {
    let f = family().await;
    let s = &f.server;
    s.storage
        .share(f.photo, f.sam, ShareLevel::Viewer, f.alex)
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let share = s
        .storage
        .share(f.holiday, f.sam, ShareLevel::Editor, f.alex)
        .await
        .unwrap();
    assert_eq!(share.level, ShareLevel::Editor);
    assert_eq!(share.shared_by, f.alex);
    // Sharing again at the same level changes nothing.
    let again = s
        .storage
        .share(f.holiday, f.sam, ShareLevel::Editor, f.alex)
        .await
        .unwrap();
    assert_eq!(again, share);
    assert_eq!(s.storage.share_history(f.holiday).await.unwrap().len(), 1);

    let shared: Vec<_> = s
        .storage
        .shared_with(f.sam)
        .await
        .unwrap()
        .into_iter()
        .map(|shared| (shared.item.name, shared.share.level))
        .collect();
    assert_eq!(
        shared,
        [
            ("Holiday".to_owned(), ShareLevel::Editor),
            ("beach.jpg".to_owned(), ShareLevel::Viewer),
        ]
    );
    let shares = s.storage.shares(f.holiday).await.unwrap();
    assert_eq!(shares.len(), 1);
    assert_eq!(shares[0].user, f.sam);
    let mut by_alex = s.storage.shared_by_owner(f.alex).await.unwrap();
    by_alex.sort();
    let mut expected = vec![f.holiday, f.photo];
    expected.sort();
    assert_eq!(by_alex, expected);
}

#[tokio::test]
async fn refuses_shares_that_make_no_sense() {
    let f = family().await;
    let s = &f.server;
    let err = s
        .storage
        .share(f.holiday, f.alex, ShareLevel::Viewer, f.alex)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::ShareWithOwner));
    let err = s
        .storage
        .share(f.top, f.sam, ShareLevel::Viewer, f.alex)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::ShareTopFolder));
    let err = s
        .storage
        .share(f.holiday, UserId::generate(), ShareLevel::Viewer, f.alex)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::UserNotFound));
    s.storage.delete(f.album).await.unwrap();
    let err = s
        .storage
        .share(f.photo, f.sam, ShareLevel::Viewer, f.alex)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InTrash));
}

#[tokio::test]
async fn the_trash_is_the_owners_alone() {
    let f = family().await;
    let s = &f.server;
    s.storage
        .share(f.holiday, f.sam, ShareLevel::Editor, f.alex)
        .await
        .unwrap();
    // An editor's delete goes to the owner's trash.
    s.storage.delete(f.album).await.unwrap();
    let trash = s.storage.trash(f.alex).await.unwrap();
    assert_eq!(trash.len(), 1);
    assert_eq!(trash[0].id, f.album);
    assert!(s.storage.trash(f.sam).await.unwrap().is_empty());
    // Only the owner can still see it.
    assert_eq!(s.access(f.sam, f.album).await, None);
    assert_eq!(s.access(f.sam, f.photo).await, None);
    assert_eq!(s.access(f.alex, f.album).await, Some(Access::Owner));
    // The shared folder itself, in the trash, drops out of Shared with me.
    s.storage.delete(f.holiday).await.unwrap();
    assert!(s.storage.shared_with(f.sam).await.unwrap().is_empty());
    s.storage.restore(f.holiday).await.unwrap();
    assert_eq!(s.storage.shared_with(f.sam).await.unwrap().len(), 1);
}

#[tokio::test]
async fn moving_out_of_a_shared_folder_ends_access() {
    let f = family().await;
    let s = &f.server;
    s.storage
        .share(f.holiday, f.sam, ShareLevel::Viewer, f.alex)
        .await
        .unwrap();
    s.storage
        .move_item(f.photo, f.top, "beach.jpg")
        .await
        .unwrap();
    assert_eq!(s.access(f.sam, f.photo).await, None);
    // Items move only within the owner's storage, so an editor can't take
    // something into their own.
    let sams = s.storage.top_folder(f.sam).await.unwrap();
    let err = s
        .storage
        .move_item(f.album, sams, "Album")
        .await
        .unwrap_err();
    assert!(matches!(err, Error::DifferentOwner));
}

#[tokio::test]
async fn what_editors_add_counts_toward_the_owner() {
    let f = family().await;
    let s = &f.server;
    s.storage
        .share(f.holiday, f.sam, ShareLevel::Editor, f.alex)
        .await
        .unwrap();
    let added = s
        .storage
        .write_file(f.holiday, "from sam.txt", &b"hello"[..])
        .await
        .unwrap();
    assert_eq!(added.owner, f.alex);
    assert_eq!(s.storage.usage(f.alex).await.unwrap().used, 9);
    assert_eq!(s.storage.usage(f.sam).await.unwrap().used, 0);

    // A resumable upload counts toward the owner's quota as it arrives.
    s.storage.set_quota(f.alex, Some(15)).await.unwrap();
    let err = s
        .storage
        .start_upload_by(f.alex, f.sam, 7)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::QuotaFull));
    let upload = s.storage.start_upload_by(f.alex, f.sam, 6).await.unwrap();
    assert_eq!(upload.owner, f.alex);
    assert_eq!(upload.started_by, f.sam);
    assert_eq!(s.storage.usage(f.alex).await.unwrap().uploading, 6);
    s.storage
        .append(upload.id, 0, &b"upload"[..])
        .await
        .unwrap();
    // It can only finish in the owner's storage.
    let sams = s.storage.top_folder(f.sam).await.unwrap();
    let err = s
        .storage
        .finish_upload(upload.id, sams, "up.txt")
        .await
        .unwrap_err();
    assert!(matches!(err, Error::DifferentOwner));
    let file = s
        .storage
        .finish_upload(upload.id, f.holiday, "up.txt")
        .await
        .unwrap();
    assert_eq!(file.owner, f.alex);
    assert_eq!(s.storage.usage(f.alex).await.unwrap().used, 15);
}

#[tokio::test]
async fn recent_and_search_include_shared_items() {
    let f = family().await;
    let s = &f.server;
    let sams = s.storage.top_folder(f.sam).await.unwrap();
    s.storage
        .write_file(sams, "mine.jpg", &b"x"[..])
        .await
        .unwrap();
    assert_eq!(
        paths(&s.storage.recent(f.sam, 10).await.unwrap()),
        ["mine.jpg"]
    );

    s.storage
        .share(f.holiday, f.sam, ShareLevel::Viewer, f.alex)
        .await
        .unwrap();
    // Shared on its own too, so it's reachable twice. It appears once,
    // under the outer share.
    s.storage
        .share(f.album, f.sam, ShareLevel::Viewer, f.alex)
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    s.storage
        .write_file(f.album, "beach.jpg", &b"newer"[..])
        .await
        .unwrap();
    assert_eq!(
        paths(&s.storage.recent(f.sam, 10).await.unwrap()),
        ["Holiday/Album/beach.jpg", "mine.jpg"]
    );
    assert_eq!(
        paths(&s.storage.recent(f.sam, 1).await.unwrap()),
        ["Holiday/Album/beach.jpg"]
    );
    let search = |text: &str| Search {
        name: Words::parse(text),
        limit: 10,
        ..Search::default()
    };
    let mut found = paths(&founds(s.storage.search(f.sam, search("")).await.unwrap()));
    found.sort();
    assert_eq!(
        found,
        [
            "Holiday",
            "Holiday/Album",
            "Holiday/Album/beach.jpg",
            "mine.jpg"
        ]
    );
    // Alex's own results don't change.
    let mut found = paths(&founds(s.storage.search(f.alex, search("")).await.unwrap()));
    found.sort();
    assert_eq!(
        found,
        ["Holiday", "Holiday/Album", "Holiday/Album/beach.jpg"]
    );

    s.storage.unshare(f.holiday, f.sam, f.alex).await.unwrap();
    s.storage.unshare(f.album, f.sam, f.alex).await.unwrap();
    assert_eq!(
        paths(&founds(
            s.storage.search(f.sam, search("beach")).await.unwrap()
        )),
        Vec::<String>::new()
    );
}

#[tokio::test]
async fn removing_an_account_removes_its_shares() {
    let f = family().await;
    let s = &f.server;
    s.storage
        .share(f.holiday, f.sam, ShareLevel::Editor, f.alex)
        .await
        .unwrap();
    s.storage
        .share(f.holiday, f.kim, ShareLevel::Viewer, f.alex)
        .await
        .unwrap();
    let upload = s.storage.start_upload_by(f.alex, f.sam, 3).await.unwrap();

    // Sam leaves: their access and their uploads go.
    s.identity.turn_off_member(f.sam).await.unwrap();
    s.storage
        .remove_owner(f.sam, LeaveFiles::Delete)
        .await
        .unwrap();
    assert_eq!(s.storage.shares(f.holiday).await.unwrap().len(), 1);
    let err = s.storage.upload(upload.id).await.unwrap_err();
    assert!(matches!(err, Error::UploadNotFound));
    s.identity.delete_member(f.sam).await.unwrap();

    // Alex leaves and gives their files to Kim. Kim owns the folder now,
    // and Sam's share is gone with Sam.
    s.identity.turn_off_member(f.alex).await.unwrap();
    s.storage
        .remove_owner(
            f.alex,
            LeaveFiles::GiveTo {
                owner: f.kim,
                folder_name: "Alex's files".into(),
            },
        )
        .await
        .unwrap();
    assert!(s.storage.shares(f.holiday).await.unwrap().is_empty());
    assert_eq!(s.access(f.kim, f.photo).await, Some(Access::Owner));
    s.identity.delete_member(f.alex).await.unwrap();
}

#[tokio::test]
async fn shared_by_lists_what_the_owner_shared_and_unshare_all_ends_it() {
    let f = family().await;
    let s = &f.server;
    assert!(s.storage.shared_by(f.alex).await.unwrap().is_empty());
    s.storage
        .share(f.holiday, f.sam, ShareLevel::Viewer, f.alex)
        .await
        .unwrap();
    s.storage
        .share(f.holiday, f.kim, ShareLevel::Editor, f.alex)
        .await
        .unwrap();
    s.storage
        .share(f.photo, f.sam, ShareLevel::Editor, f.alex)
        .await
        .unwrap();

    let out = s.storage.shared_by(f.alex).await.unwrap();
    let names: Vec<&str> = out.iter().map(|out| out.found.item.name.as_str()).collect();
    assert_eq!(out.len(), 2, "{names:?}");
    let holiday = out
        .iter()
        .find(|out| out.found.item.id == f.holiday)
        .unwrap();
    assert!(holiday.found.folders.is_empty());
    let with: Vec<UserId> = holiday.shares.iter().map(|share| share.user).collect();
    assert_eq!(with, [f.sam, f.kim]);
    let photo = out.iter().find(|out| out.found.item.id == f.photo).unwrap();
    assert_eq!(photo.found.folders, ["Holiday", "Album"]);
    assert_eq!(photo.shares.len(), 1);
    // Nobody shared anything of Sam's.
    assert!(s.storage.shared_by(f.sam).await.unwrap().is_empty());

    // The trash leaves it out, as it does everywhere else.
    s.storage.delete(f.album).await.unwrap();
    assert_eq!(s.storage.shared_by(f.alex).await.unwrap().len(), 1);
    s.storage.restore(f.album).await.unwrap();

    let removed = s.storage.unshare_all(f.holiday, f.alex).await.unwrap();
    assert_eq!(removed.len(), 2);
    assert_eq!(s.access(f.kim, f.holiday).await, None);
    // Sam still has the photo through its own share.
    assert_eq!(s.access(f.sam, f.photo).await, Some(Access::Editor));
    let out = s.storage.shared_by(f.alex).await.unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].found.item.id, f.photo);
    let history = s.storage.share_history(f.holiday).await.unwrap();
    assert_eq!(
        history.iter().filter(|event| event.level.is_none()).count(),
        2
    );
    assert!(
        s.storage
            .unshare_all(f.holiday, f.alex)
            .await
            .unwrap()
            .is_empty()
    );
}

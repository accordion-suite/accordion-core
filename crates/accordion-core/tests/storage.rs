//! Stores files and folders, and checks the plain view on disk.

use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use accordion_core::db::Database;
use accordion_core::identity::{Identity, NewUser};
use accordion_core::storage::{Error, Found, ItemKind, LeaveFiles, Only, Search, Storage};
use accordion_types::{Role, UserId};
use tokio::io::AsyncReadExt;

struct Server {
    dir: tempfile::TempDir,
    storage: Storage,
    identity: Identity,
}

impl Server {
    fn view(&self) -> PathBuf {
        self.dir.path().join("storage").join("files")
    }

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
}

async fn start() -> Server {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(&dir.path().join("accordion.db")).unwrap();
    let identity = Identity::new(db.clone()).await.unwrap();
    let storage = Storage::open(db, identity.clone(), &dir.path().join("storage"))
        .await
        .unwrap();
    Server {
        dir,
        storage,
        identity,
    }
}

async fn read(storage: &Storage, id: accordion_types::ItemId) -> String {
    let mut text = String::new();
    storage
        .open_file(id)
        .await
        .unwrap()
        .read_to_string(&mut text)
        .await
        .unwrap();
    text
}

fn blob_count(dir: &Path) -> usize {
    walk(&dir.join("storage").join("blobs"))
}

fn walk(dir: &Path) -> usize {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                walk(&entry.path())
            } else {
                1
            }
        })
        .sum()
}

#[tokio::test]
async fn stores_files_and_shows_them_by_name() {
    let server = start().await;
    let alex = server.user("alex").await;
    let top = server.storage.top_folder(alex).await.unwrap();
    assert_eq!(server.storage.top_folder(alex).await.unwrap(), top);

    let documents = server
        .storage
        .create_folder(top, "Documents")
        .await
        .unwrap();
    let report = server
        .storage
        .write_file(documents.id, "Report: 2026 ✓.txt", &b"hello"[..])
        .await
        .unwrap();
    assert!(matches!(report.kind, ItemKind::File { blob, .. } if blob.size == 5));
    assert_eq!(read(&server.storage, report.id).await, "hello");

    let children = server.storage.children(documents.id).await.unwrap();
    assert_eq!(children, std::slice::from_ref(&report));

    let shown = server.view().join("alex/Documents/Report: 2026 ✓.txt");
    assert_eq!(std::fs::read_to_string(&shown).unwrap(), "hello");
}

#[tokio::test]
async fn stores_identical_contents_once() {
    let server = start().await;
    let alex = server.user("alex").await;
    let top = server.storage.top_folder(alex).await.unwrap();
    for name in ["a.txt", "b.txt"] {
        server
            .storage
            .write_file(top, name, &b"same"[..])
            .await
            .unwrap();
    }
    assert_eq!(blob_count(server.dir.path()), 1);
    // The stored contents and both names in the view are one file on disk.
    let metadata = std::fs::metadata(server.view().join("alex/a.txt")).unwrap();
    assert_eq!(metadata.nlink(), 3);
}

#[tokio::test]
async fn replaces_an_empty_version_instead_of_keeping_it() {
    let server = start().await;
    let alex = server.user("alex").await;
    let top = server.storage.top_folder(alex).await.unwrap();
    // As File Explorer saves a new file: empty first, then the contents.
    let file = server
        .storage
        .write_file(top, "photo.jpg", &b""[..])
        .await
        .unwrap();
    server
        .storage
        .write_file(top, "photo.jpg", &b"pixels"[..])
        .await
        .unwrap();
    let versions = server.storage.versions(file.id).await.unwrap();
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].number, 1);
    assert_eq!(read(&server.storage, file.id).await, "pixels");

    // Emptying a file keeps what it held before.
    server
        .storage
        .write_file(top, "photo.jpg", &b""[..])
        .await
        .unwrap();
    server
        .storage
        .write_file(top, "photo.jpg", &b"new pixels"[..])
        .await
        .unwrap();
    let versions = server.storage.versions(file.id).await.unwrap();
    assert_eq!(
        versions.iter().map(|v| v.number).collect::<Vec<_>>(),
        [2, 1]
    );
    assert_eq!(read(&server.storage, file.id).await, "new pixels");
}

#[tokio::test]
async fn keeps_every_version() {
    let server = start().await;
    let alex = server.user("alex").await;
    let top = server.storage.top_folder(alex).await.unwrap();
    let first = server
        .storage
        .write_file(top, "notes.md", &b"first"[..])
        .await
        .unwrap();
    let second = server
        .storage
        .write_file(top, "notes.md", &b"second"[..])
        .await
        .unwrap();
    assert_eq!(first.id, second.id);
    assert_eq!(read(&server.storage, first.id).await, "second");

    let versions = server.storage.versions(first.id).await.unwrap();
    assert_eq!(
        versions.iter().map(|v| v.number).collect::<Vec<_>>(),
        [2, 1]
    );
    let mut old = String::new();
    server
        .storage
        .open_version(first.id, versions[1].id)
        .await
        .unwrap()
        .read_to_string(&mut old)
        .await
        .unwrap();
    assert_eq!(old, "first");

    server
        .storage
        .restore_version(first.id, versions[1].id)
        .await
        .unwrap();
    assert_eq!(read(&server.storage, first.id).await, "first");
    assert_eq!(server.storage.versions(first.id).await.unwrap().len(), 3);
    let shown = server.view().join("alex/notes.md");
    assert_eq!(std::fs::read_to_string(shown).unwrap(), "first");
}

#[tokio::test]
async fn renames_and_moves() {
    let server = start().await;
    let alex = server.user("alex").await;
    let top = server.storage.top_folder(alex).await.unwrap();
    let photos = server.storage.create_folder(top, "Photos").await.unwrap();
    let trip = server
        .storage
        .create_folder(photos.id, "Trip")
        .await
        .unwrap();
    server
        .storage
        .write_file(trip.id, "beach.jpg", &b"jpeg"[..])
        .await
        .unwrap();
    let other = server.storage.create_folder(top, "Archive").await.unwrap();

    server
        .storage
        .move_item(trip.id, other.id, "Summer trip")
        .await
        .unwrap();
    let view = server.view();
    assert!(!view.join("alex/Photos/Trip").exists());
    assert_eq!(
        std::fs::read(view.join("alex/Archive/Summer trip/beach.jpg")).unwrap(),
        b"jpeg"
    );

    let err = server
        .storage
        .move_item(other.id, other.id, "Archive")
        .await
        .unwrap_err();
    assert!(matches!(err, Error::MoveIntoItself));
    let summer = server.storage.children(other.id).await.unwrap()[0].id;
    let err = server
        .storage
        .move_item(other.id, summer, "Archive")
        .await
        .unwrap_err();
    assert!(matches!(err, Error::MoveIntoItself));
    let err = server
        .storage
        .move_item(photos.id, top, "Archive")
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NameTaken { folder: true }));
    assert_eq!(
        err.to_string(),
        "This folder already has a folder with that name. Choose a different name."
    );
    let err = server
        .storage
        .create_folder(summer, "beach.jpg")
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NameTaken { folder: false }));
    assert_eq!(
        err.to_string(),
        "This folder already has a file with that name. Choose a different name."
    );
    let err = server
        .storage
        .move_item(top, top, "Mine")
        .await
        .unwrap_err();
    assert!(matches!(err, Error::TopFolder));

    let sam = server.user("sam").await;
    let sams_top = server.storage.top_folder(sam).await.unwrap();
    let err = server
        .storage
        .move_item(photos.id, sams_top, "Photos")
        .await
        .unwrap_err();
    assert!(matches!(err, Error::DifferentOwner));
}

#[tokio::test]
async fn deletes_to_the_trash_and_restores() {
    let server = start().await;
    let alex = server.user("alex").await;
    let top = server.storage.top_folder(alex).await.unwrap();
    let folder = server.storage.create_folder(top, "Taxes").await.unwrap();
    server
        .storage
        .write_file(folder.id, "2025.pdf", &b"pdf"[..])
        .await
        .unwrap();

    server.storage.delete(folder.id).await.unwrap();
    assert!(!server.view().join("alex/Taxes").exists());
    assert!(server.storage.children(top).await.unwrap().is_empty());
    let trash = server.storage.trash(alex).await.unwrap();
    assert_eq!(trash.len(), 1);
    assert_eq!(trash[0].name, "Taxes");
    let err = server
        .storage
        .write_file(folder.id, "2026.pdf", &b"pdf"[..])
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InTrash));

    // A new folder can take the deleted one's name, which then blocks the
    // restore until one of them is renamed.
    let replacement = server.storage.create_folder(top, "Taxes").await.unwrap();
    let err = server.storage.restore(folder.id).await.unwrap_err();
    assert!(matches!(err, Error::NameTaken { folder: true }));
    server.storage.delete(replacement.id).await.unwrap();

    server.storage.restore(folder.id).await.unwrap();
    assert_eq!(
        std::fs::read(server.view().join("alex/Taxes/2025.pdf")).unwrap(),
        b"pdf"
    );
}

#[tokio::test]
async fn rebuilds_the_view_from_the_database() {
    let server = start().await;
    let alex = server.user("alex").await;
    let top = server.storage.top_folder(alex).await.unwrap();
    server
        .storage
        .write_file(top, "keep.txt", &b"keep"[..])
        .await
        .unwrap();
    let view = server.view();
    std::fs::remove_file(view.join("alex/keep.txt")).unwrap();
    std::fs::write(view.join("alex/stray.txt"), b"stray").unwrap();

    server.storage.rebuild_view().await.unwrap();
    assert_eq!(std::fs::read(view.join("alex/keep.txt")).unwrap(), b"keep");
    assert!(!view.join("alex/stray.txt").exists());
}

#[tokio::test]
async fn refuses_names_the_view_cant_show() {
    let server = start().await;
    let alex = server.user("alex").await;
    let top = server.storage.top_folder(alex).await.unwrap();
    for name in ["", ".", "..", "a/b", "nul\0", &"x".repeat(256)] {
        let err = server.storage.create_folder(top, name).await.unwrap_err();
        assert!(matches!(err, Error::InvalidName), "{name:?}");
    }
    // 255 bytes is fine, even when some characters take several bytes.
    let long = format!("{}é", "x".repeat(253));
    assert_eq!(long.len(), 255);
    assert!(server.storage.create_folder(top, &long).await.is_ok());
}

#[tokio::test]
async fn finds_items_by_path() {
    let server = start().await;
    let alex = server.user("alex").await;
    let top = server.storage.top_folder(alex).await.unwrap();
    let docs = server
        .storage
        .create_folder(top, "Documents")
        .await
        .unwrap();
    let report = server
        .storage
        .write_file(docs.id, "report.pdf", &b"pdf"[..])
        .await
        .unwrap();
    let find = |names: &[&str]| {
        server
            .storage
            .find(top, names.iter().map(|&name| name.to_owned()).collect())
    };

    assert_eq!(find(&[]).await.unwrap().unwrap().id, top);
    assert_eq!(find(&["Documents"]).await.unwrap().unwrap().id, docs.id);
    let found = find(&["Documents", "report.pdf"]).await.unwrap().unwrap();
    assert_eq!(found.id, report.id);
    for missing in [
        &["Photos"][..],
        &["Documents", "report.pdf", "page 1"],
        &["documents"],
    ] {
        assert_eq!(find(missing).await.unwrap(), None, "{missing:?}");
    }

    server.storage.delete(report.id).await.unwrap();
    assert_eq!(find(&["Documents", "report.pdf"]).await.unwrap(), None);
}

#[tokio::test]
async fn gives_a_removed_users_files_to_someone_else() {
    let server = start().await;
    let alex = server.user("alex").await;
    let sam = server.user("sam").await;
    let alex_top = server.storage.top_folder(alex).await.unwrap();
    server
        .storage
        .create_folder(alex_top, "Sam's files")
        .await
        .unwrap();
    let sam_top = server.storage.top_folder(sam).await.unwrap();
    let photos = server
        .storage
        .create_folder(sam_top, "Photos")
        .await
        .unwrap();
    let beach = server
        .storage
        .write_file(photos.id, "beach.jpg", &b"first"[..])
        .await
        .unwrap();
    server
        .storage
        .write_file(photos.id, "beach.jpg", &b"second"[..])
        .await
        .unwrap();
    let old = server
        .storage
        .write_file(sam_top, "old.txt", &b"old"[..])
        .await
        .unwrap();
    server.storage.delete(old.id).await.unwrap();
    server.storage.set_quota(sam, Some(1000)).await.unwrap();
    let upload = server.storage.start_upload(sam, 6).await.unwrap();
    let alex_before = server.storage.usage(alex).await.unwrap().used;

    // The database won't delete an account that still has files.
    assert!(server.identity.delete_member(sam).await.is_err());

    let folder = server
        .storage
        .remove_owner(
            sam,
            LeaveFiles::GiveTo {
                owner: alex,
                folder_name: "Sam's files".into(),
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(folder.name, "Sam's files (2)");
    assert_eq!(folder.owner, alex);
    assert_eq!(folder.parent, Some(alex_top));
    assert_eq!(server.storage.top_folder(alex).await.unwrap(), alex_top);

    // Everything arrives as it was: folders, versions, and the trash.
    let names: Vec<_> = server
        .storage
        .children(folder.id)
        .await
        .unwrap()
        .into_iter()
        .map(|item| (item.name, item.owner))
        .collect();
    assert_eq!(names, [("Photos".to_owned(), alex)]);
    assert_eq!(server.storage.versions(beach.id).await.unwrap().len(), 2);
    assert_eq!(read(&server.storage, beach.id).await, "second");
    let trash = server.storage.trash(alex).await.unwrap();
    assert_eq!(trash.len(), 1);
    assert_eq!(trash[0].name, "old.txt");
    server.storage.restore(old.id).await.unwrap();
    assert_eq!(
        server.storage.usage(alex).await.unwrap().used,
        alex_before + 14
    );
    assert_eq!(
        std::fs::read(server.view().join("alex/Sam's files (2)/Photos/beach.jpg")).unwrap(),
        b"second"
    );
    assert!(!server.view().join("sam").exists());
    let err = server.storage.upload(upload.id).await.unwrap_err();
    assert!(matches!(err, Error::UploadNotFound));

    // Nothing refers to the account now, so it can go.
    server.identity.delete_member(sam).await.unwrap();
}

#[tokio::test]
async fn deletes_a_removed_users_files() {
    let server = start().await;
    let sam = server.user("sam").await;
    let top = server.storage.top_folder(sam).await.unwrap();
    let folder = server.storage.create_folder(top, "Taxes").await.unwrap();
    let file = server
        .storage
        .write_file(folder.id, "2025.pdf", &b"pdf"[..])
        .await
        .unwrap();

    let given = server
        .storage
        .remove_owner(sam, LeaveFiles::Delete)
        .await
        .unwrap();
    assert_eq!(given, None);
    assert_eq!(server.storage.item(file.id).await.unwrap(), None);
    assert!(!server.view().join("sam").exists());
    server.identity.delete_member(sam).await.unwrap();

    // Someone who never stored anything has nothing to move.
    let kim = server.user("kim").await;
    let given = server
        .storage
        .remove_owner(kim, LeaveFiles::Delete)
        .await
        .unwrap();
    assert_eq!(given, None);
    server.identity.delete_member(kim).await.unwrap();
}

#[tokio::test]
async fn checks_who_gets_a_removed_users_files() {
    let server = start().await;
    let alex = server.user("alex").await;
    let sam = server.user("sam").await;
    let top = server.storage.top_folder(sam).await.unwrap();
    server
        .storage
        .write_file(top, "notes.txt", &b"notes"[..])
        .await
        .unwrap();
    let give = |owner, folder_name: &str| LeaveFiles::GiveTo {
        owner,
        folder_name: folder_name.into(),
    };

    let err = server
        .storage
        .remove_owner(sam, give(sam, "Sam's files"))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::GiveToSelf));
    let err = server
        .storage
        .remove_owner(sam, give(UserId::generate(), "Sam's files"))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::UserNotFound));
    let err = server
        .storage
        .remove_owner(sam, give(alex, "Sam/Alex"))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InvalidName));
    assert_eq!(server.storage.children(top).await.unwrap().len(), 1);

    // A name too long for the plain view is shortened, never cut partway
    // through a character.
    let long = "é".repeat(200);
    let folder = server
        .storage
        .remove_owner(sam, give(alex, &long))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(folder.name, "é".repeat(127));
}

#[tokio::test]
async fn follows_a_new_username_after_a_rebuild() {
    let server = start().await;
    let alex = server.user("alex").await;
    let top = server.storage.top_folder(alex).await.unwrap();
    server
        .storage
        .write_file(top, "keep.txt", &b"keep"[..])
        .await
        .unwrap();

    server
        .identity
        .rename_member(alex, "alexander", "Alexander")
        .await
        .unwrap();
    server.storage.rebuild_view().await.unwrap();
    assert_eq!(
        std::fs::read(server.view().join("alexander/keep.txt")).unwrap(),
        b"keep"
    );
    assert!(!server.view().join("alex").exists());
}

/// Timestamps count milliseconds, so this keeps two changes in order.
async fn tick() {
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
}

fn names(found: &[Found]) -> Vec<String> {
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
async fn lists_recent_files_outside_the_trash() {
    let server = start().await;
    let alex = server.user("alex").await;
    let sam = server.user("sam").await;
    let top = server.storage.top_folder(alex).await.unwrap();
    let trip = server.storage.create_folder(top, "Trip").await.unwrap();
    let old = server.storage.create_folder(top, "Old").await.unwrap();
    for (folder, name) in [(top, "a.txt"), (trip.id, "b.jpg"), (old.id, "c.txt")] {
        server
            .storage
            .write_file(folder, name, &b"x"[..])
            .await
            .unwrap();
        tick().await;
    }
    server
        .storage
        .write_file(top, "a.txt", &b"newer"[..])
        .await
        .unwrap();
    server.storage.delete(old.id).await.unwrap();
    let sams = server.storage.top_folder(sam).await.unwrap();
    server
        .storage
        .write_file(sams, "sam.txt", &b"x"[..])
        .await
        .unwrap();

    let recent = server.storage.recent(alex, 10).await.unwrap();
    assert_eq!(names(&recent), ["a.txt", "Trip/b.jpg"]);
    assert_eq!(
        names(&server.storage.recent(alex, 1).await.unwrap()),
        ["a.txt"]
    );
}

#[tokio::test]
async fn searches_names_kinds_and_dates() {
    let server = start().await;
    let alex = server.user("alex").await;
    let top = server.storage.top_folder(alex).await.unwrap();
    let taxes = server.storage.create_folder(top, "Taxes").await.unwrap();
    let trashed = server
        .storage
        .create_folder(top, "Old taxes")
        .await
        .unwrap();
    tick().await;
    for (folder, name) in [
        (taxes.id, "Return 2025.PDF"),
        (taxes.id, "Receipt.jpg"),
        (top, "Ölgemälde.png"),
        (top, ".tax"),
        (trashed.id, "Return 2019.pdf"),
    ] {
        server
            .storage
            .write_file(folder, name, &b"x"[..])
            .await
            .unwrap();
        tick().await;
    }
    server.storage.delete(trashed.id).await.unwrap();
    let search = |search: Search| {
        server.storage.search(
            alex,
            Search {
                limit: 10,
                ..search
            },
        )
    };

    let found = search(Search {
        text: "TAX".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    assert_eq!(names(&found), [".tax", "Taxes"]);
    // Case is ignored beyond ASCII.
    let found = search(Search {
        text: "ölge".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    assert_eq!(names(&found), ["Ölgemälde.png"]);
    let found = search(Search {
        only: Some(Only::Folders),
        ..Default::default()
    })
    .await
    .unwrap();
    assert_eq!(names(&found), ["Taxes"]);
    // A name that starts with its only dot has no extension.
    let only = Only::Extensions(vec!["pdf".into(), "tax".into()]);
    let found = search(Search {
        only: Some(only),
        ..Default::default()
    })
    .await
    .unwrap();
    assert_eq!(names(&found), ["Taxes/Return 2025.PDF"]);

    let all = search(Search::default()).await.unwrap();
    assert_eq!(all.len(), 5);
    let since = all[1].item.modified_at.clone();
    let found = search(Search {
        modified_since: Some(since.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    assert_eq!(names(&found), [".tax", "Ölgemälde.png"]);
    let found = search(Search {
        modified_before: Some(since),
        ..Default::default()
    })
    .await
    .unwrap();
    assert_eq!(
        names(&found),
        ["Taxes/Receipt.jpg", "Taxes/Return 2025.PDF", "Taxes"]
    );
    let found = server
        .storage
        .search(
            alex,
            Search {
                limit: 2,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(found.len(), 2);
}

#[tokio::test]
async fn copies_a_file() {
    let server = start().await;
    let alex = server.user("alex").await;
    let sam = server.user("sam").await;
    let top = server.storage.top_folder(alex).await.unwrap();
    let photos = server.storage.create_folder(top, "Photos").await.unwrap();
    let beach = server
        .storage
        .write_file(top, "Beach.jpg", &b"sand"[..])
        .await
        .unwrap();

    let copy = server
        .storage
        .copy_file(beach.id, photos.id, "Copy of Beach.jpg")
        .await
        .unwrap();
    assert_ne!(copy.id, beach.id);
    assert_eq!(read(&server.storage, copy.id).await, "sand");
    assert_eq!(server.storage.versions(copy.id).await.unwrap().len(), 1);
    assert_eq!(blob_count(server.dir.path()), 1);
    assert_eq!(server.storage.usage(alex).await.unwrap().used, 8);
    assert_eq!(
        std::fs::read(server.view().join("alex/Photos/Copy of Beach.jpg")).unwrap(),
        b"sand"
    );

    let taken = server
        .storage
        .copy_file(beach.id, top, "Beach.jpg")
        .await
        .unwrap_err();
    assert!(matches!(taken, Error::NameTaken { folder: false }));
    let folder = server
        .storage
        .copy_file(photos.id, top, "Photos 2")
        .await
        .unwrap_err();
    assert!(matches!(folder, Error::NotAFile));
    let sams = server.storage.top_folder(sam).await.unwrap();
    let theirs = server
        .storage
        .copy_file(beach.id, sams, "Beach.jpg")
        .await
        .unwrap_err();
    assert!(matches!(theirs, Error::DifferentOwner));
    server.storage.set_quota(alex, Some(10)).await.unwrap();
    let full = server
        .storage
        .copy_file(beach.id, top, "Again.jpg")
        .await
        .unwrap_err();
    assert!(matches!(full, Error::QuotaFull));
}

#[tokio::test]
async fn finds_live_items_by_id() {
    let server = start().await;
    let alex = server.user("alex").await;
    let top = server.storage.top_folder(alex).await.unwrap();
    let trip = server.storage.create_folder(top, "Trip").await.unwrap();
    let day = server
        .storage
        .create_folder(trip.id, "Day 1")
        .await
        .unwrap();
    let photo = server
        .storage
        .write_file(day.id, "a.jpg", &b"x"[..])
        .await
        .unwrap();
    let old = server.storage.create_folder(top, "Old").await.unwrap();
    let inside = server
        .storage
        .write_file(old.id, "b.jpg", &b"x"[..])
        .await
        .unwrap();
    let gone = server
        .storage
        .write_file(top, "c.jpg", &b"x"[..])
        .await
        .unwrap();
    server.storage.delete(old.id).await.unwrap();
    server.storage.delete(gone.id).await.unwrap();
    server.storage.delete_forever(gone.id).await.unwrap();

    let found = server
        .storage
        .live_items(vec![photo.id, inside.id, gone.id, trip.id])
        .await
        .unwrap();
    assert_eq!(names(&found), ["Trip/Day 1/a.jpg", "Trip"]);
}

//! Quotas, resumable uploads, and upkeep: purging, cleanup, and the
//! integrity check.

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use accordion_core::db::Database;
use accordion_core::identity::{Identity, NewUser};
use accordion_core::storage::{
    Cleaned, Error, Problem, Purged, Retention, Storage, StorageStatus, Usage,
};
use accordion_types::{ItemId, Role, UserId};
use tokio::io::AsyncReadExt;

/// A time long enough ago for anything to have expired.
const LONG_AGO: &str = "'2000-01-01T00:00:00.000Z'";

/// The SHA-256 hash of "hello".
const HELLO: &str = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";

struct Server {
    dir: tempfile::TempDir,
    db: Database,
    identity: Identity,
    storage: Storage,
}

impl Server {
    fn dir(&self) -> PathBuf {
        self.dir.path().join("storage")
    }

    async fn user(&self, username: &str) -> (UserId, ItemId) {
        let id = self
            .identity
            .create_user(NewUser {
                username: username.into(),
                display_name: username.into(),
                role: Role::User,
                password: "correct horse".into(),
            })
            .await
            .unwrap()
            .id;
        (id, self.storage.top_folder(id).await.unwrap())
    }

    /// Changes the database directly, to make things look older than they
    /// are.
    async fn sql(&self, sql: String) {
        self.db
            .write(move |tx| tx.execute_batch(&sql))
            .await
            .unwrap();
    }

    /// Opens storage again on the same data, as after a restart.
    async fn restart(&self) -> Storage {
        Storage::open(self.db.clone(), self.identity.clone(), &self.dir())
            .await
            .unwrap()
    }

    fn blob_files(&self) -> usize {
        count_files(&self.dir().join("blobs"))
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
        dir,
        db,
        identity,
        storage,
    }
}

fn count_files(dir: &Path) -> usize {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                count_files(&entry.path())
            } else {
                1
            }
        })
        .sum()
}

async fn read(storage: &Storage, id: ItemId) -> String {
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

#[tokio::test]
async fn quotas_count_versions_the_trash_and_uploads() {
    let server = start().await;
    let storage = &server.storage;
    let (alex, top) = server.user("alex").await;
    let usage = |used, uploading, quota| Usage {
        used,
        uploading,
        quota,
    };
    assert_eq!(storage.usage(alex).await.unwrap(), usage(0, 0, None));

    storage.set_quota(alex, Some(10)).await.unwrap();
    let a = storage
        .write_file(top, "a.txt", &b"12345"[..])
        .await
        .unwrap();
    storage.write_file(top, "a.txt", &b"678"[..]).await.unwrap();
    assert_eq!(storage.usage(alex).await.unwrap(), usage(8, 0, Some(10)));
    let err = storage
        .write_file(top, "b.txt", &b"abc"[..])
        .await
        .unwrap_err();
    assert!(matches!(err, Error::QuotaFull));
    // Refused contents never reach the disk.
    assert_eq!(server.blob_files(), 2);

    // The trash still counts, until it's emptied.
    storage.delete(a.id).await.unwrap();
    assert_eq!(storage.usage(alex).await.unwrap().used, 8);
    storage.empty_trash(alex).await.unwrap();
    assert!(storage.trash(alex).await.unwrap().is_empty());
    assert_eq!(storage.usage(alex).await.unwrap().used, 0);

    // Identical contents count in full for each file that has them.
    storage
        .write_file(top, "c.txt", &b"same"[..])
        .await
        .unwrap();
    storage
        .write_file(top, "d.txt", &b"same"[..])
        .await
        .unwrap();
    assert_eq!(storage.usage(alex).await.unwrap().used, 8);

    // Uploads in progress hold their space.
    storage.set_quota(alex, Some(20)).await.unwrap();
    storage.start_upload(alex, 6).await.unwrap();
    assert_eq!(storage.usage(alex).await.unwrap(), usage(8, 6, Some(20)));
    let err = storage
        .write_file(top, "e.txt", &b"1234567"[..])
        .await
        .unwrap_err();
    assert!(matches!(err, Error::QuotaFull));
    let err = storage.start_upload(alex, 7).await.unwrap_err();
    assert!(matches!(err, Error::QuotaFull));

    storage.set_quota(alex, None).await.unwrap();
    storage
        .write_file(top, "e.txt", &b"1234567"[..])
        .await
        .unwrap();
    let err = storage
        .set_quota(UserId::generate(), Some(1))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::UserNotFound));
}

#[tokio::test]
async fn resumes_uploads_after_a_dropped_connection_or_a_restart() {
    let server = start().await;
    let (alex, top) = server.user("alex").await;
    let upload = server.storage.start_upload(alex, 11).await.unwrap();
    assert_eq!(upload.received, 0);

    let progress = server
        .storage
        .append(upload.id, 0, &b"hello "[..])
        .await
        .unwrap();
    assert_eq!(progress.received, 6);
    let err = server
        .storage
        .append(upload.id, 3, &b"x"[..])
        .await
        .unwrap_err();
    assert!(matches!(err, Error::UploadOffset { expected: 6 }));
    let err = server
        .storage
        .finish_upload(upload.id, top, "greeting.txt")
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        Error::UploadIncomplete {
            received: 6,
            size: 11
        }
    ));

    // A restarted server rehashes what arrived, and carries on.
    let storage = server.restart().await;
    assert_eq!(storage.upload(upload.id).await.unwrap().received, 6);
    let err = storage
        .append(upload.id, 6, &b"world!"[..])
        .await
        .unwrap_err();
    assert!(matches!(err, Error::UploadTooLong { size: 11 }));
    assert_eq!(storage.upload(upload.id).await.unwrap().received, 6);
    storage.append(upload.id, 6, &b"world"[..]).await.unwrap();

    // A refused finish keeps the upload, so it can try again.
    storage.create_folder(top, "taken").await.unwrap();
    let err = storage
        .finish_upload(upload.id, top, "taken")
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NameTaken { folder: true }));
    let item = storage
        .finish_upload(upload.id, top, "greeting.txt")
        .await
        .unwrap();
    assert_eq!(read(&storage, item.id).await, "hello world");
    let shown = server.dir().join("files/alex/greeting.txt");
    assert_eq!(std::fs::read_to_string(shown).unwrap(), "hello world");

    let err = storage.upload(upload.id).await.unwrap_err();
    assert!(matches!(err, Error::UploadNotFound));
    let usage = storage.usage(alex).await.unwrap();
    assert_eq!((usage.used, usage.uploading), (11, 0));
}

#[tokio::test]
async fn cancels_uploads_and_removes_stalled_ones() {
    let server = start().await;
    let storage = &server.storage;
    let (alex, _) = server.user("alex").await;
    let uploads = server.dir().join("uploads");

    let canceled = storage.start_upload(alex, 4).await.unwrap();
    storage.append(canceled.id, 0, &b"ab"[..]).await.unwrap();
    storage.cancel_upload(canceled.id).await.unwrap();
    assert!(matches!(
        storage.upload(canceled.id).await.unwrap_err(),
        Error::UploadNotFound
    ));
    assert!(matches!(
        storage.cancel_upload(canceled.id).await.unwrap_err(),
        Error::UploadNotFound
    ));
    assert_eq!(count_files(&uploads), 0);

    let stalled = storage.start_upload(alex, 4).await.unwrap();
    let active = storage.start_upload(alex, 4).await.unwrap();
    assert_eq!(storage.clean_up().await.unwrap().uploads, 0);
    server
        .sql(format!(
            "UPDATE storage_uploads SET updated_at = {LONG_AGO} WHERE id = x'{}'",
            stalled.id.as_uuid().simple()
        ))
        .await;
    assert_eq!(storage.clean_up().await.unwrap().uploads, 1);
    assert!(storage.upload(stalled.id).await.is_err());
    assert!(storage.upload(active.id).await.is_ok());
    assert_eq!(count_files(&uploads), 1);
}

#[tokio::test]
async fn purges_old_trash_and_replaced_versions() {
    let server = start().await;
    let storage = &server.storage;
    let (alex, top) = server.user("alex").await;
    let old = storage.create_folder(top, "Old").await.unwrap();
    let inside = storage
        .write_file(old.id, "inside.txt", &b"x"[..])
        .await
        .unwrap();
    storage.delete(old.id).await.unwrap();
    let notes = storage
        .write_file(top, "notes.md", &b"one"[..])
        .await
        .unwrap();
    storage
        .write_file(top, "notes.md", &b"two"[..])
        .await
        .unwrap();
    storage
        .write_file(top, "notes.md", &b"three"[..])
        .await
        .unwrap();
    assert_eq!(
        storage.purge(Retention::default()).await.unwrap(),
        Purged::default()
    );

    server
        .sql(format!(
            "UPDATE storage_items SET trashed_at = {LONG_AGO} WHERE trashed_at IS NOT NULL;
             UPDATE storage_versions SET created_at = {LONG_AGO} WHERE number < 3;"
        ))
        .await;
    let purged = storage.purge(Retention::default()).await.unwrap();
    // Version 1 was replaced long ago. Version 2 was replaced just now, so
    // it stays for another 30 days.
    assert_eq!(
        purged,
        Purged {
            items: 1,
            versions: 1
        }
    );
    let numbers: Vec<u32> = storage
        .versions(notes.id)
        .await
        .unwrap()
        .iter()
        .map(|version| version.number)
        .collect();
    assert_eq!(numbers, [3, 2]);
    assert!(storage.trash(alex).await.unwrap().is_empty());
    assert_eq!(storage.item(old.id).await.unwrap(), None);
    assert_eq!(storage.item(inside.id).await.unwrap(), None);
    assert_eq!(read(storage, notes.id).await, "three");
}

#[tokio::test]
async fn removes_contents_a_day_after_nothing_uses_them() {
    let server = start().await;
    let storage = &server.storage;
    let (_, top) = server.user("alex").await;
    storage
        .write_file(top, "a.txt", &b"same"[..])
        .await
        .unwrap();
    storage
        .write_file(top, "b.txt", &b"same"[..])
        .await
        .unwrap();
    let gone = storage
        .write_file(top, "gone.txt", &b"gone"[..])
        .await
        .unwrap();
    assert_eq!(server.blob_files(), 2);
    let make_old = || {
        server.sql(format!(
            "UPDATE storage_blobs SET unused_since = {LONG_AGO} WHERE unused_since IS NOT NULL"
        ))
    };

    let err = storage.delete_forever(gone.id).await.unwrap_err();
    assert!(matches!(err, Error::NotInTrash));
    storage.delete(gone.id).await.unwrap();
    storage.delete_forever(gone.id).await.unwrap();
    assert_eq!(storage.clean_up().await.unwrap(), Cleaned::default());
    assert_eq!(server.blob_files(), 2);

    // Within the day, the same contents can come back without a new copy,
    // and they're in use again.
    let again = storage
        .write_file(top, "again.txt", &b"gone"[..])
        .await
        .unwrap();
    make_old().await;
    assert_eq!(storage.clean_up().await.unwrap().contents, 0);

    storage.delete(again.id).await.unwrap();
    storage.delete_forever(again.id).await.unwrap();
    storage.clean_up().await.unwrap();
    make_old().await;
    let cleaned = storage.clean_up().await.unwrap();
    assert_eq!((cleaned.contents, cleaned.bytes), (1, 4));
    assert_eq!(server.blob_files(), 1);
}

#[tokio::test]
async fn keeps_contents_while_a_backup_holds_them() {
    let server = start().await;
    let storage = &server.storage;
    let (_, top) = server.user("alex").await;
    let gone = storage
        .write_file(top, "gone.txt", &b"gone"[..])
        .await
        .unwrap();
    storage.delete(gone.id).await.unwrap();
    storage.delete_forever(gone.id).await.unwrap();
    storage.clean_up().await.unwrap();
    server
        .sql(format!(
            "UPDATE storage_blobs SET unused_since = {LONG_AGO} WHERE unused_since IS NOT NULL"
        ))
        .await;

    let hold = storage.hold_contents().await;
    assert_eq!(storage.clean_up().await.unwrap().contents, 0);
    assert_eq!(server.blob_files(), 1);
    drop(hold);
    assert_eq!(storage.clean_up().await.unwrap().contents, 1);
    assert_eq!(server.blob_files(), 0);
}

#[tokio::test]
async fn keeps_what_modules_store_until_they_release_it() {
    let server = start().await;
    let storage = &server.storage;
    let blob = storage
        .store("photos", "thumbnail/1", &b"tiny"[..])
        .await
        .unwrap();
    let mut text = String::new();
    storage
        .open_contents(blob.hash)
        .await
        .unwrap()
        .read_to_string(&mut text)
        .await
        .unwrap();
    assert_eq!(text, "tiny");

    storage.clean_up().await.unwrap();
    server
        .sql(format!(
            "UPDATE storage_blobs SET unused_since = {LONG_AGO}"
        ))
        .await;
    assert_eq!(storage.clean_up().await.unwrap().contents, 0);

    storage.release("photos", "thumbnail/1").await.unwrap();
    storage.clean_up().await.unwrap();
    server
        .sql(format!(
            "UPDATE storage_blobs SET unused_since = {LONG_AGO} WHERE unused_since IS NOT NULL"
        ))
        .await;
    assert_eq!(storage.clean_up().await.unwrap().contents, 1);
    assert!(matches!(
        storage.open_contents(blob.hash).await.unwrap_err(),
        Error::NotFound
    ));
}

#[tokio::test]
async fn removes_files_a_crash_left_behind() {
    let server = start().await;
    let storage = &server.storage;
    let (_, top) = server.user("alex").await;
    let kept = storage
        .write_file(top, "keep.txt", &b"keep"[..])
        .await
        .unwrap();

    let stray = server
        .dir()
        .join("blobs/sha256/ab/cd")
        .join(format!("abcd{}", "0".repeat(60)));
    std::fs::create_dir_all(stray.parent().unwrap()).unwrap();
    std::fs::write(&stray, b"stray").unwrap();
    assert_eq!(storage.remove_strays().await.unwrap(), 1);
    assert!(!stray.exists());
    assert_eq!(read(storage, kept.id).await, "keep");

    // Pieces of an upload in progress stay. Old ones are from a crash.
    let uploads = server.dir().join("uploads");
    let fresh = uploads.join("fresh.part");
    let abandoned = uploads.join("abandoned.part");
    std::fs::write(&fresh, b"").unwrap();
    std::fs::write(&abandoned, b"").unwrap();
    std::fs::File::options()
        .write(true)
        .open(&abandoned)
        .unwrap()
        .set_modified(SystemTime::now() - Duration::from_secs(2 * 24 * 60 * 60))
        .unwrap();
    storage.clean_up().await.unwrap();
    assert!(fresh.exists());
    assert!(!abandoned.exists());
}

#[tokio::test]
async fn finds_damage_and_repairs_it_from_a_fresh_copy() {
    let server = start().await;
    let storage = &server.storage;
    let (_, top) = server.user("alex").await;
    let file = storage
        .write_file(top, "a.txt", &b"hello"[..])
        .await
        .unwrap();
    let checked = storage.check_stored(u64::MAX).await.unwrap();
    assert_eq!((checked.contents, checked.damaged), (1, 0));

    // The disk changes the stored file.
    let stored = server.dir().join("blobs/sha256/2c/f2").join(HELLO);
    std::fs::set_permissions(&stored, std::fs::Permissions::from_mode(0o600)).unwrap();
    std::fs::write(&stored, b"jello").unwrap();
    let checked = storage.check_stored(u64::MAX).await.unwrap();
    assert_eq!(checked.damaged, 1);
    assert_eq!(storage.status().borrow().damaged, 1);
    let damage = storage.damage().await.unwrap();
    assert_eq!(damage.len(), 1);
    assert_eq!(damage[0].problem, Problem::Changed);
    assert_eq!(damage[0].blob.to_hex(), HELLO);

    // Uploading the same contents again repairs every file that has them.
    storage
        .write_file(top, "copy.txt", &b"hello"[..])
        .await
        .unwrap();
    assert_eq!(read(storage, file.id).await, "hello");
    let shown = server.dir().join("files/alex/a.txt");
    assert_eq!(std::fs::read_to_string(shown).unwrap(), "hello");
    assert_eq!(storage.check_stored(u64::MAX).await.unwrap().damaged, 0);
    assert_eq!(storage.status().borrow().damaged, 0);
    assert!(storage.damage().await.unwrap().is_empty());
}

#[tokio::test]
async fn upkeep_rebuilds_the_view_when_it_starts() {
    let server = start().await;
    let (_, top) = server.user("alex").await;
    server
        .storage
        .write_file(top, "keep.txt", &b"keep"[..])
        .await
        .unwrap();
    let shown = server.dir().join("files/alex/keep.txt");
    std::fs::remove_file(&shown).unwrap();

    let status = server.storage.status();
    // Already stopped, so it runs one round and returns.
    server.storage.clone().maintain(async {}).await;
    assert_eq!(std::fs::read_to_string(&shown).unwrap(), "keep");
    assert_eq!(*status.borrow(), StorageStatus::default());
}

//! Storage: files and folders, their versions, and the trash.
//!
//! Each file's contents are stored once, named by their SHA-256 hash (see
//! [`blobs`]). Names, folders, versions, and the trash live only in the
//! database, so a rename, a move, or a restore is one transaction. A
//! read-only plain view shows each user's current files under their real
//! names, as hard links to the stored contents. It's rebuilt from the
//! database whenever it might be out of date. See decision 6 in the
//! `accordion` repo's `docs/decisions`.
//!
//! Each user can have a quota, which counts every version and the trash.
//! Large files arrive through resumable uploads ([`Storage::start_upload`]).
//! Upkeep ([`Storage::maintain`]) empties old trash and versions, removes
//! contents nothing uses, and rehashes stored files to find damage.
//!
//! Drive, Docs, Notes, Photos, and Takeout import all store files here,
//! through this API. Who may open what is up to them.

pub mod blobs;
mod upkeep;
mod uploads;
mod view;

pub use blobs::{Blob, ContentHash, Problem};
pub use upkeep::{Checked, Cleaned, Damage, Purged, Retention, StorageStatus};
pub use uploads::Upload;

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use accordion_types::{ItemId, UploadId, UserId, VersionId};
use rusqlite::{Connection, OptionalExtension, Row};
use tokio::io::AsyncRead;
use tokio::sync::watch;

use crate::db::{self, Database, Migration};
use crate::identity::{self, Identity};
use crate::sql_now;
use blobs::{Blobs, Received};
use view::{Entry, View};

const OWNER: &str = "storage";

static MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "Create items, versions, and blobs",
        sql: "
        -- Contents stored under blobs/, by hash.
        CREATE TABLE storage_blobs (
            hash BLOB PRIMARY KEY CHECK (length(hash) = 32),
            size INTEGER NOT NULL CHECK (size >= 0),
            created_at TEXT NOT NULL
        ) STRICT, WITHOUT ROWID;

        -- Files and folders. Each user has one root folder, with no parent
        -- and an empty name.
        CREATE TABLE storage_items (
            id BLOB PRIMARY KEY CHECK (length(id) = 16),
            owner_id BLOB NOT NULL REFERENCES identity_users (id),
            parent_id BLOB REFERENCES storage_items (id),
            name TEXT NOT NULL,
            kind TEXT NOT NULL CHECK (kind IN ('folder', 'file')),
            created_at TEXT NOT NULL,
            modified_at TEXT NOT NULL,
            -- Set on the item that was deleted, not on what's inside it.
            trashed_at TEXT
        ) STRICT, WITHOUT ROWID;

        CREATE UNIQUE INDEX storage_items_one_root ON storage_items (owner_id)
            WHERE parent_id IS NULL;
        -- Names that differ only in case are allowed. Sync decides how to
        -- handle them on Windows and macOS.
        CREATE UNIQUE INDEX storage_items_names ON storage_items (parent_id, name)
            WHERE trashed_at IS NULL;
        CREATE INDEX storage_items_trashed ON storage_items (owner_id, trashed_at)
            WHERE trashed_at IS NOT NULL;

        -- Every saved version of a file. The highest number is the current one.
        CREATE TABLE storage_versions (
            id BLOB PRIMARY KEY CHECK (length(id) = 16),
            item_id BLOB NOT NULL REFERENCES storage_items (id) ON DELETE CASCADE,
            number INTEGER NOT NULL CHECK (number >= 1),
            blob BLOB NOT NULL REFERENCES storage_blobs (hash),
            created_at TEXT NOT NULL,
            UNIQUE (item_id, number)
        ) STRICT, WITHOUT ROWID;

        CREATE INDEX storage_versions_blob ON storage_versions (blob);
    ",
    },
    Migration {
        version: 2,
        name: "Add quotas, resumable uploads, module contents, and integrity checks",
        sql: "
        -- When nothing used the contents any more, so cleanup can wait a
        -- day before removing them. NULL while something uses them.
        ALTER TABLE storage_blobs ADD COLUMN unused_since TEXT;
        -- When the integrity check last rehashed the contents, and what was
        -- wrong if they failed.
        ALTER TABLE storage_blobs ADD COLUMN checked_at TEXT;
        ALTER TABLE storage_blobs ADD COLUMN damage TEXT
            CHECK (damage IN ('missing', 'changed'));
        UPDATE storage_blobs SET checked_at = created_at;
        CREATE INDEX storage_blobs_checked ON storage_blobs (checked_at);
        CREATE INDEX storage_blobs_damaged ON storage_blobs (damage)
            WHERE damage IS NOT NULL;

        CREATE INDEX storage_items_owner ON storage_items (owner_id);

        -- Each user's limit in bytes. Users without a row have no limit.
        CREATE TABLE storage_quotas (
            owner_id BLOB PRIMARY KEY REFERENCES identity_users (id),
            bytes INTEGER NOT NULL CHECK (bytes >= 0)
        ) STRICT, WITHOUT ROWID;

        -- Resumable uploads in progress. Their contents collect in uploads/.
        CREATE TABLE storage_uploads (
            id BLOB PRIMARY KEY CHECK (length(id) = 16),
            owner_id BLOB NOT NULL REFERENCES identity_users (id),
            size INTEGER NOT NULL CHECK (size >= 0),
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        ) STRICT, WITHOUT ROWID;
        CREATE INDEX storage_uploads_owner ON storage_uploads (owner_id);

        -- Contents a module keeps for its own use, such as thumbnails.
        CREATE TABLE storage_blob_refs (
            module TEXT NOT NULL,
            key TEXT NOT NULL,
            blob BLOB NOT NULL REFERENCES storage_blobs (hash),
            created_at TEXT NOT NULL,
            PRIMARY KEY (module, key)
        ) STRICT, WITHOUT ROWID;
        CREATE INDEX storage_blob_refs_blob ON storage_blob_refs (blob);
    ",
    },
];

/// The longest name, in bytes of UTF-8. Most file systems allow 255, so the
/// plain view can show every name exactly.
pub const MAX_NAME_LEN: usize = 255;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(
        "Names can be up to {MAX_NAME_LEN} bytes long, can't contain a slash, and can't be \
         \".\" or \"..\"."
    )]
    InvalidName,

    #[error("Something with that name is already in this folder. Choose a different name.")]
    NameTaken,

    #[error("That file or folder doesn't exist. It may have been deleted.")]
    NotFound,

    #[error("That's a file, not a folder.")]
    NotAFolder,

    #[error("That's a folder, not a file.")]
    NotAFile,

    #[error("Your top folder can't be renamed, moved, or deleted.")]
    TopFolder,

    #[error("A folder can't be moved into itself, or into a folder inside it.")]
    MoveIntoItself,

    #[error("Files and folders can only move within one person's storage.")]
    DifferentOwner,

    #[error("That's in the trash. Restore it first, then try again.")]
    InTrash,

    #[error("That isn't in the trash.")]
    NotInTrash,

    #[error(
        "There isn't enough space left in your storage. Delete some files and empty the \
         trash, or ask your admin for more space."
    )]
    QuotaFull,

    #[error("That user doesn't exist. They may have been removed.")]
    UserNotFound,

    #[error(
        "That upload doesn't exist. It may have finished, been canceled, or been removed after \
         a day without progress. Start it again."
    )]
    UploadNotFound,

    #[error(
        "The upload has {expected} bytes so far, so it can only continue from byte {expected}."
    )]
    UploadOffset { expected: u64 },

    #[error("That's more than the {size} bytes the upload said it would send.")]
    UploadTooLong { size: u64 },

    #[error(
        "The upload isn't finished. {received} of {size} bytes have arrived. Send the rest, \
         then try again."
    )]
    UploadIncomplete { received: u64, size: u64 },

    #[error(
        "Accordion can't create hard links in its storage folder, {0}. Keep Accordion's data \
         on one file system that supports them, such as ext4, XFS, Btrfs, or ZFS."
    )]
    NoHardLinks(String),

    #[error(transparent)]
    Io(#[from] io::Error),

    #[error(transparent)]
    Database(#[from] db::Error),

    #[error(transparent)]
    Identity(#[from] identity::Error),
}

impl From<rusqlite::Error> for Error {
    fn from(err: rusqlite::Error) -> Self {
        Self::Database(err.into())
    }
}

/// A file or folder.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Item {
    pub id: ItemId,
    pub owner: UserId,
    /// `None` for a user's top folder.
    pub parent: Option<ItemId>,
    pub name: String,
    pub kind: ItemKind,
    pub created_at: String,
    pub modified_at: String,
    /// When it was deleted, if it's in the trash itself. Items inside a
    /// deleted folder don't have this set.
    pub trashed_at: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ItemKind {
    Folder,
    /// A file, with its current version.
    File {
        version: VersionId,
        blob: Blob,
    },
}

/// One saved version of a file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Version {
    pub id: VersionId,
    /// 1 for the first version, counting up.
    pub number: u32,
    pub blob: Blob,
    pub created_at: String,
}

/// How much space a user's files take, and their limit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Usage {
    /// Every version of every file they own, including the trash, each at
    /// its full size even when its contents are shared with another file.
    pub used: u64,
    /// Space set aside for uploads in progress.
    pub uploading: u64,
    /// `None` when there's no limit.
    pub quota: Option<u64>,
}

/// The storage service. Cloning it is cheap.
#[derive(Clone)]
pub struct Storage {
    inner: Arc<Inner>,
}

struct Inner {
    db: Database,
    identity: Identity,
    blobs: Blobs,
    view: View,
    /// Held across each change and its update to the plain view, so the view
    /// sees changes in the order the database does. Storing contents under
    /// `blobs/` and committing what uses them also happen under it, so
    /// cleanup never removes contents that a commit is about to use.
    changes: tokio::sync::Mutex<()>,
    uploads: uploads::InProgress,
    status: watch::Sender<StorageStatus>,
    /// How many [`ContentsHold`]s there are.
    holds: AtomicUsize,
}

/// While one exists, upkeep doesn't remove stored contents, so a copy of the
/// database taken after it was made only refers to contents that are still
/// on disk. Backups hold one while they run. Dropping it lets upkeep carry
/// on.
#[must_use = "contents are only held until this is dropped"]
pub struct ContentsHold {
    inner: Arc<Inner>,
}

impl Drop for ContentsHold {
    fn drop(&mut self) {
        self.inner.holds.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Storage {
    /// Starts the storage service in `dir`, creating its tables and folders
    /// first, and checks that the plain view can use hard links.
    pub async fn open(db: Database, identity: Identity, dir: &Path) -> Result<Self, Error> {
        db.migrate(OWNER, MIGRATIONS).await?;
        let blobs = Blobs::open(dir)?;
        let view = View::open(&dir.join("files"))?;
        check_hard_links(blobs.uploads(), view.root())?;
        Ok(Self {
            inner: Arc::new(Inner {
                db,
                identity,
                blobs,
                view,
                changes: tokio::sync::Mutex::new(()),
                uploads: uploads::InProgress::default(),
                status: watch::Sender::new(StorageStatus::default()),
                holds: AtomicUsize::new(0),
            }),
        })
    }

    /// Stops upkeep from removing stored contents until the hold is dropped.
    pub async fn hold_contents(&self) -> ContentsHold {
        // Taken under the lock, so a round of cleanup that's already removing
        // contents finishes its batch first.
        let _changes = self.inner.changes.lock().await;
        self.inner.holds.fetch_add(1, Ordering::SeqCst);
        ContentsHold {
            inner: Arc::clone(&self.inner),
        }
    }

    /// Where stored contents are kept, relative to the storage folder, such
    /// as `blobs/sha256/3f/9a/3f9a…`. Backups use it to find contents in a
    /// restored copy.
    pub fn contents_path(hash: ContentHash) -> PathBuf {
        Path::new("blobs").join("sha256").join(blobs::shard(hash))
    }

    /// `owner`'s top folder, created the first time it's needed.
    pub async fn top_folder(&self, owner: UserId) -> Result<ItemId, Error> {
        if let Some(id) = self.find_top_folder(owner).await? {
            return Ok(id);
        }
        let _changes = self.inner.changes.lock().await;
        let id = ItemId::generate();
        self.inner
            .db
            .write(move |tx| {
                tx.execute(
                    concat!(
                        "INSERT OR IGNORE INTO storage_items
                         (id, owner_id, parent_id, name, kind, created_at, modified_at)
                         VALUES (?1, ?2, NULL, '', 'folder', ",
                        sql_now!(),
                        ", ",
                        sql_now!(),
                        ")"
                    ),
                    (id.as_uuid(), owner.as_uuid()),
                )
                .map_err(Error::from)
            })
            .await?;
        let id = self.find_top_folder(owner).await?.ok_or(Error::NotFound)?;
        self.update_view(Change::Show(id)).await;
        Ok(id)
    }

    async fn find_top_folder(&self, owner: UserId) -> Result<Option<ItemId>, Error> {
        self.inner
            .db
            .read(move |conn| {
                conn.query_row(
                    "SELECT id FROM storage_items WHERE owner_id = ?1 AND parent_id IS NULL",
                    [owner.as_uuid()],
                    |row| Ok(ItemId::from_uuid(row.get(0)?)),
                )
                .optional()
                .map_err(Error::from)
            })
            .await
    }

    pub async fn item(&self, id: ItemId) -> Result<Option<Item>, Error> {
        self.inner.db.read(move |conn| item(conn, id)).await
    }

    /// What's in a folder, apart from the trash, sorted by name.
    pub async fn children(&self, folder: ItemId) -> Result<Vec<Item>, Error> {
        self.inner
            .db
            .read(move |conn| {
                let parent = item(conn, folder)?.ok_or(Error::NotFound)?;
                if parent.kind != ItemKind::Folder {
                    return Err(Error::NotAFolder);
                }
                let mut statement = conn.prepare(&format!(
                    "{ITEM_SELECT} WHERE i.parent_id = ?1 AND i.trashed_at IS NULL ORDER BY i.name"
                ))?;
                let items = statement
                    .query_map([folder.as_uuid()], item_from_row)?
                    .collect::<Result<_, _>>()?;
                Ok(items)
            })
            .await
    }

    /// Follows `names` down from `folder`, such as `["Documents",
    /// "report.pdf"]`, skipping anything in the trash. Returns `None` if any
    /// name along the way is missing, or names a file before the last one.
    /// An empty list finds `folder` itself.
    pub async fn find(&self, folder: ItemId, names: Vec<String>) -> Result<Option<Item>, Error> {
        self.inner
            .db
            .read(move |conn| {
                let mut found = item(conn, folder)?;
                for name in &names {
                    match found {
                        Some(parent) if parent.kind == ItemKind::Folder => {
                            found = child_named(conn, parent.id, name)?;
                        }
                        _ => return Ok(None),
                    }
                }
                Ok(found)
            })
            .await
    }

    /// What `owner` has deleted, newest first.
    pub async fn trash(&self, owner: UserId) -> Result<Vec<Item>, Error> {
        self.inner
            .db
            .read(move |conn| {
                let mut statement = conn.prepare(&format!(
                    "{ITEM_SELECT} WHERE i.owner_id = ?1 AND i.trashed_at IS NOT NULL
                     ORDER BY i.trashed_at DESC"
                ))?;
                let items = statement
                    .query_map([owner.as_uuid()], item_from_row)?
                    .collect::<Result<_, _>>()?;
                Ok(items)
            })
            .await
    }

    pub async fn create_folder(&self, parent: ItemId, name: &str) -> Result<Item, Error> {
        let name = check_name(name)?;
        let _changes = self.inner.changes.lock().await;
        let id = ItemId::generate();
        let item = self
            .inner
            .db
            .write(move |tx| {
                let parent = writable_folder(tx, parent)?;
                if child_named(tx, parent.id, &name)?.is_some() {
                    return Err(Error::NameTaken);
                }
                insert_item(tx, id, parent.owner, parent.id, &name, "folder")?;
                item(tx, id)?.ok_or(Error::NotFound)
            })
            .await?;
        self.update_view(Change::Show(item.id)).await;
        Ok(item)
    }

    /// Saves `contents` as the file `name` in `parent`. If a file with that
    /// name is already there, this becomes its new version.
    pub async fn write_file(
        &self,
        parent: ItemId,
        name: &str,
        contents: impl AsyncRead + Unpin,
    ) -> Result<Item, Error> {
        let name = check_name(name)?;
        // Receiving the contents is the slow part, so it happens before
        // taking the lock.
        let received = self.inner.blobs.receive(contents).await?;
        let result = self.save(parent, name, &received, None).await;
        // Gone already if it was stored. Otherwise nothing uses it.
        let _ = tokio::fs::remove_file(&received.path).await;
        result
    }

    /// Stores received contents as the file `name` in `parent`, as a new
    /// version if a file with that name is already there. For a resumable
    /// upload, this also ends the upload.
    async fn save(
        &self,
        parent: ItemId,
        name: String,
        received: &Received,
        upload: Option<UploadId>,
    ) -> Result<Item, Error> {
        let blob = received.blob;
        let _changes = self.inner.changes.lock().await;
        // Check before storing the contents, so refused contents never reach
        // blobs/. The lock keeps the answer true until the commit.
        let checked = name.clone();
        self.inner
            .db
            .read(move |conn| check_save(conn, parent, &checked, blob.size, upload))
            .await?;
        let kept = self.keep(received).await?;
        let stored = kept.stored;
        let new_id = ItemId::generate();
        let item = self
            .inner
            .db
            .write(move |tx| {
                let (parent, existing) = check_save(tx, parent, &name, blob.size, upload)?;
                insert_blob(tx, blob, stored)?;
                let id = match existing {
                    Some(id) => id,
                    None => {
                        insert_item(tx, new_id, parent.owner, parent.id, &name, "file")?;
                        new_id
                    }
                };
                add_version(tx, id, blob)?;
                if let Some(upload) = upload {
                    tx.execute(
                        "DELETE FROM storage_uploads WHERE id = ?1",
                        [upload.as_uuid()],
                    )?;
                }
                item_or_missing(tx, id)
            })
            .await?;
        if kept.repaired {
            self.show_repair().await;
        } else {
            self.update_view(Change::Show(item.id)).await;
        }
        Ok(item)
    }

    /// Stores received contents under their hash. Call it holding the
    /// `changes` lock, and commit what uses the contents before letting go.
    async fn keep(&self, received: &Received) -> Result<Kept, Error> {
        let hash = received.blob.hash;
        // Contents the integrity check found damaged are replaced by the
        // fresh copy, which repairs every file that uses them.
        let damaged = self
            .inner
            .db
            .read(move |conn| {
                Ok::<_, Error>(conn.query_row(
                    "SELECT EXISTS (SELECT 1 FROM storage_blobs
                                    WHERE hash = ?1 AND damage IS NOT NULL)",
                    [&hash.0[..]],
                    |row| row.get::<_, bool>(0),
                )?)
            })
            .await?;
        let path = received.path.clone();
        let stored = self
            .on_disk(move |blobs| {
                if damaged {
                    blobs.remove(hash)?;
                }
                blobs.keep(&path, hash)
            })
            .await?;
        Ok(Kept {
            stored,
            repaired: damaged,
        })
    }

    /// The plain view still links to the damaged copy of repaired contents,
    /// wherever they appear, so it's rebuilt. Repairs are rare.
    async fn show_repair(&self) {
        if let Err(err) = self.rebuild_view_locked().await {
            tracing::error!(%err, "couldn't rebuild the plain view after a repair");
        }
    }

    /// Runs blocking work on `blobs/` and `uploads/` off the async threads.
    async fn on_disk<T: Send + 'static>(
        &self,
        f: impl FnOnce(&Blobs) -> io::Result<T> + Send + 'static,
    ) -> Result<T, Error> {
        let inner = Arc::clone(&self.inner);
        Ok(tokio::task::spawn_blocking(move || f(&inner.blobs))
            .await
            .expect("a storage task on the disk panicked")?)
    }

    /// Opens the current version of a file.
    pub async fn open_file(&self, id: ItemId) -> Result<tokio::fs::File, Error> {
        let item = self.item(id).await?.ok_or(Error::NotFound)?;
        match item.kind {
            ItemKind::File { blob, .. } => self.open_blob(blob).await,
            ItemKind::Folder => Err(Error::NotAFile),
        }
    }

    /// Opens an older version of a file.
    pub async fn open_version(
        &self,
        id: ItemId,
        version: VersionId,
    ) -> Result<tokio::fs::File, Error> {
        let versions = self.versions(id).await?;
        let version = versions
            .iter()
            .find(|v| v.id == version)
            .ok_or(Error::NotFound)?;
        self.open_blob(version.blob).await
    }

    async fn open_blob(&self, blob: Blob) -> Result<tokio::fs::File, Error> {
        self.open_contents(blob.hash).await
    }

    /// Every saved version of a file, newest first.
    pub async fn versions(&self, id: ItemId) -> Result<Vec<Version>, Error> {
        self.inner
            .db
            .read(move |conn| {
                let item = item(conn, id)?.ok_or(Error::NotFound)?;
                if item.kind == ItemKind::Folder {
                    return Err(Error::NotAFile);
                }
                let mut statement = conn.prepare(
                    "SELECT v.id, v.number, v.blob, b.size, v.created_at
                     FROM storage_versions v JOIN storage_blobs b ON b.hash = v.blob
                     WHERE v.item_id = ?1 ORDER BY v.number DESC",
                )?;
                let versions = statement
                    .query_map([id.as_uuid()], |row| {
                        Ok(Version {
                            id: VersionId::from_uuid(row.get(0)?),
                            number: row.get(1)?,
                            blob: Blob {
                                hash: hash_from(row, 2)?,
                                size: size_from(row, 3)?,
                            },
                            created_at: row.get(4)?,
                        })
                    })?
                    .collect::<Result<_, _>>()?;
                Ok(versions)
            })
            .await
    }

    /// Makes an older version current again, by saving it as a new version.
    /// Every version stays in the history, apart from an empty current one.
    pub async fn restore_version(&self, id: ItemId, version: VersionId) -> Result<Item, Error> {
        let _changes = self.inner.changes.lock().await;
        let item = self
            .inner
            .db
            .write(move |tx| {
                let item = item(tx, id)?.ok_or(Error::NotFound)?;
                if !location(tx, id)?.visible {
                    return Err(Error::InTrash);
                }
                let blob = tx
                    .query_row(
                        "SELECT v.blob, b.size FROM storage_versions v
                         JOIN storage_blobs b ON b.hash = v.blob
                         WHERE v.item_id = ?1 AND v.id = ?2",
                        (item.id.as_uuid(), version.as_uuid()),
                        |row| {
                            Ok(Blob {
                                hash: hash_from(row, 0)?,
                                size: size_from(row, 1)?,
                            })
                        },
                    )
                    .optional()?
                    .ok_or(Error::NotFound)?;
                check_space(tx, item.owner, blob.size, None)?;
                add_version(tx, id, blob)?;
                item_or_missing(tx, id)
            })
            .await?;
        self.update_view(Change::Show(item.id)).await;
        Ok(item)
    }

    /// Renames an item, moves it to another folder, or both.
    pub async fn move_item(
        &self,
        id: ItemId,
        new_parent: ItemId,
        new_name: &str,
    ) -> Result<Item, Error> {
        let new_name = check_name(new_name)?;
        let _changes = self.inner.changes.lock().await;
        let before = self.view_path(id).await?;
        let item = self
            .inner
            .db
            .write(move |tx| {
                let item = item(tx, id)?.ok_or(Error::NotFound)?;
                if item.parent.is_none() {
                    return Err(Error::TopFolder);
                }
                if item.trashed_at.is_some() {
                    return Err(Error::InTrash);
                }
                let parent = writable_folder(tx, new_parent)?;
                if parent.owner != item.owner {
                    return Err(Error::DifferentOwner);
                }
                if is_inside(tx, parent.id, id)? {
                    return Err(Error::MoveIntoItself);
                }
                if child_named(tx, parent.id, &new_name)?.is_some_and(|other| other.id != id) {
                    return Err(Error::NameTaken);
                }
                tx.execute(
                    concat!(
                        "UPDATE storage_items SET parent_id = ?2, name = ?3, modified_at = ",
                        sql_now!(),
                        " WHERE id = ?1"
                    ),
                    (id.as_uuid(), parent.id.as_uuid(), &new_name),
                )?;
                item_or_missing(tx, id)
            })
            .await?;
        self.update_view(Change::Move { id, before }).await;
        Ok(item)
    }

    /// Moves an item to the trash, with everything inside it.
    pub async fn delete(&self, id: ItemId) -> Result<Item, Error> {
        let _changes = self.inner.changes.lock().await;
        let before = self.view_path(id).await?;
        let item = self
            .inner
            .db
            .write(move |tx| {
                let item = item(tx, id)?.ok_or(Error::NotFound)?;
                if item.parent.is_none() {
                    return Err(Error::TopFolder);
                }
                if item.trashed_at.is_some() {
                    return Err(Error::InTrash);
                }
                tx.execute(
                    concat!(
                        "UPDATE storage_items SET trashed_at = ",
                        sql_now!(),
                        " WHERE id = ?1"
                    ),
                    [id.as_uuid()],
                )?;
                item_or_missing(tx, id)
            })
            .await?;
        self.update_view(Change::Hide { before }).await;
        Ok(item)
    }

    /// Takes an item out of the trash, back to the folder it was in.
    pub async fn restore(&self, id: ItemId) -> Result<Item, Error> {
        let _changes = self.inner.changes.lock().await;
        let item = self
            .inner
            .db
            .write(move |tx| {
                let item = item(tx, id)?.ok_or(Error::NotFound)?;
                if item.trashed_at.is_none() {
                    return Err(Error::NotInTrash);
                }
                let parent = item.parent.ok_or(Error::NotInTrash)?;
                if child_named(tx, parent, &item.name)?.is_some() {
                    return Err(Error::NameTaken);
                }
                tx.execute(
                    "UPDATE storage_items SET trashed_at = NULL WHERE id = ?1",
                    [id.as_uuid()],
                )?;
                item_or_missing(tx, id)
            })
            .await?;
        self.update_view(Change::Show(item.id)).await;
        Ok(item)
    }

    /// Deletes an item in the trash for good, with everything inside it.
    /// Its contents are removed a day later, unless something else uses
    /// them.
    pub async fn delete_forever(&self, id: ItemId) -> Result<(), Error> {
        let _changes = self.inner.changes.lock().await;
        self.inner
            .db
            .write(move |tx| {
                let item = item(tx, id)?.ok_or(Error::NotFound)?;
                if item.trashed_at.is_none() {
                    return Err(Error::NotInTrash);
                }
                delete_subtree(tx, id)
            })
            .await
    }

    /// Deletes everything in `owner`'s trash for good.
    pub async fn empty_trash(&self, owner: UserId) -> Result<(), Error> {
        let _changes = self.inner.changes.lock().await;
        self.inner
            .db
            .write(move |tx| {
                let trashed: Vec<ItemId> = tx
                    .prepare(
                        "SELECT id FROM storage_items
                         WHERE owner_id = ?1 AND trashed_at IS NOT NULL",
                    )?
                    .query_map([owner.as_uuid()], |row| Ok(ItemId::from_uuid(row.get(0)?)))?
                    .collect::<Result<_, _>>()?;
                for id in trashed {
                    delete_subtree(tx, id)?;
                }
                Ok(())
            })
            .await
    }

    /// How much space `owner`'s files take, and their limit.
    pub async fn usage(&self, owner: UserId) -> Result<Usage, Error> {
        self.inner
            .db
            .read(move |conn| {
                Ok(Usage {
                    used: space_used(conn, owner)?,
                    uploading: space_reserved(conn, owner, None)?,
                    quota: quota(conn, owner)?,
                })
            })
            .await
    }

    /// Limits how much space `owner`'s files can take, or lifts the limit
    /// with `None`. A limit below what they already use stops new files and
    /// versions, but keeps everything they have.
    pub async fn set_quota(&self, owner: UserId, bytes: Option<u64>) -> Result<(), Error> {
        self.inner
            .identity
            .user(owner)
            .await?
            .ok_or(Error::UserNotFound)?;
        self.inner
            .db
            .write(move |tx| {
                match bytes {
                    Some(bytes) => tx.execute(
                        "INSERT INTO storage_quotas (owner_id, bytes) VALUES (?1, ?2)
                         ON CONFLICT (owner_id) DO UPDATE SET bytes = excluded.bytes",
                        (owner.as_uuid(), to_sql_size(bytes)),
                    )?,
                    None => tx.execute(
                        "DELETE FROM storage_quotas WHERE owner_id = ?1",
                        [owner.as_uuid()],
                    )?,
                };
                Ok(())
            })
            .await
    }

    /// Stores contents for a module's own use under `key`, such as a
    /// thumbnail, replacing what was there. They stay until the module
    /// releases `key`, and don't count toward anyone's quota.
    pub async fn store(
        &self,
        module: &'static str,
        key: &str,
        contents: impl AsyncRead + Unpin,
    ) -> Result<Blob, Error> {
        let received = self.inner.blobs.receive(contents).await?;
        let blob = received.blob;
        let key = key.to_owned();
        let result = async {
            let _changes = self.inner.changes.lock().await;
            let kept = self.keep(&received).await?;
            self.inner
                .db
                .write(move |tx| {
                    insert_blob(tx, blob, kept.stored)?;
                    tx.execute(
                        concat!(
                            "INSERT INTO storage_blob_refs (module, key, blob, created_at)
                             VALUES (?1, ?2, ?3, ",
                            sql_now!(),
                            ") ON CONFLICT (module, key) DO UPDATE
                             SET blob = excluded.blob, created_at = excluded.created_at"
                        ),
                        (module, &key, &blob.hash.0[..]),
                    )?;
                    Ok::<_, Error>(())
                })
                .await?;
            if kept.repaired {
                self.show_repair().await;
            }
            Ok(())
        }
        .await;
        let _ = tokio::fs::remove_file(&received.path).await;
        result.map(|()| blob)
    }

    /// Lets go of what a module stored under `key`.
    pub async fn release(&self, module: &'static str, key: &str) -> Result<(), Error> {
        let key = key.to_owned();
        self.inner
            .db
            .write(move |tx| {
                tx.execute(
                    "DELETE FROM storage_blob_refs WHERE module = ?1 AND key = ?2",
                    (module, &key),
                )?;
                Ok(())
            })
            .await
    }

    /// Opens stored contents by their hash, such as what [`Storage::store`]
    /// returned.
    pub async fn open_contents(&self, hash: ContentHash) -> Result<tokio::fs::File, Error> {
        match tokio::fs::File::open(self.inner.blobs.path(hash)).await {
            Err(err) if err.kind() == io::ErrorKind::NotFound => Err(Error::NotFound),
            other => Ok(other?),
        }
    }

    /// Recreates the plain view from the database. [`Storage::maintain`]
    /// does this when it starts, in case the server stopped partway through
    /// a change.
    pub async fn rebuild_view(&self) -> Result<(), Error> {
        let _changes = self.inner.changes.lock().await;
        self.rebuild_view_locked().await
    }

    async fn rebuild_view_locked(&self) -> Result<(), Error> {
        let tops: Vec<(UserId, ItemId)> = self
            .inner
            .db
            .read(|conn| {
                let mut statement =
                    conn.prepare("SELECT owner_id, id FROM storage_items WHERE parent_id IS NULL")?;
                let tops = statement
                    .query_map([], |row| {
                        Ok((
                            UserId::from_uuid(row.get(0)?),
                            ItemId::from_uuid(row.get(1)?),
                        ))
                    })?
                    .collect::<Result<_, _>>()?;
                Ok::<_, Error>(tops)
            })
            .await?;
        let mut users = Vec::new();
        for (owner, top) in tops {
            let username = self.username(owner).await?;
            users.push((username, self.entries(top).await?));
        }
        let view = self.inner.view.clone();
        self.inner
            .status
            .send_modify(|status| status.rebuilding_view = true);
        let result = tokio::task::spawn_blocking(move || view.rebuild(users))
            .await
            .expect("rebuilding the view panicked");
        self.inner
            .status
            .send_modify(|status| status.rebuilding_view = false);
        Ok(result?)
    }

    async fn username(&self, owner: UserId) -> Result<String, Error> {
        Ok(self
            .inner
            .identity
            .user(owner)
            .await?
            .ok_or(Error::NotFound)?
            .username)
    }

    /// Where an item appears in the plain view, or `None` while it's in the
    /// trash.
    async fn view_path(&self, id: ItemId) -> Result<Option<PathBuf>, Error> {
        let location = self.inner.db.read(move |conn| location(conn, id)).await?;
        if !location.visible {
            return Ok(None);
        }
        let username = self.username(location.owner).await?;
        Ok(Some(self.inner.view.path(&username, &location.names)))
    }

    /// Everything in a folder that isn't in the trash, for the plain view.
    async fn entries(&self, top: ItemId) -> Result<Vec<Entry>, Error> {
        let rows = self.inner.db.read(move |conn| subtree(conn, top)).await?;
        Ok(rows
            .into_iter()
            .map(|(names, hash)| Entry {
                names,
                contents: hash.map(|hash| self.inner.blobs.path(hash)),
            })
            .collect())
    }

    /// Brings the plain view in line with a change that just committed. The
    /// database is already right, so a failure here only costs a rebuild.
    async fn update_view(&self, change: Change) {
        if let Err(err) = self.try_update_view(change).await {
            tracing::warn!(%err, "rebuilding the plain view after it fell out of step");
            if let Err(err) = self.rebuild_view_locked().await {
                tracing::error!(%err, "couldn't rebuild the plain view");
            }
        }
    }

    async fn try_update_view(&self, change: Change) -> Result<(), Error> {
        let view = self.inner.view.clone();
        let (before, after) = match change {
            Change::Show(id) => (None, self.shown(id).await?),
            Change::Hide { before } => (before, None),
            Change::Move { id, before } => (before, self.shown(id).await?),
        };
        tokio::task::spawn_blocking(move || view.apply(before, after))
            .await
            .expect("updating the view panicked")?;
        Ok(())
    }

    /// An item's place in the plain view and what's inside it, or `None`
    /// while it's in the trash.
    async fn shown(&self, id: ItemId) -> Result<Option<(PathBuf, Vec<Entry>)>, Error> {
        match self.view_path(id).await? {
            Some(path) => Ok(Some((path, self.entries(id).await?))),
            None => Ok(None),
        }
    }
}

/// What [`Storage::keep`] did.
struct Kept {
    /// The contents weren't on disk, so they were written just now.
    stored: bool,
    /// They replaced a copy the integrity check found damaged.
    repaired: bool,
}

enum Change {
    /// The item appeared or changed.
    Show(ItemId),
    /// The item went to the trash from `before`.
    Hide { before: Option<PathBuf> },
    /// The item moved from `before`.
    Move { id: ItemId, before: Option<PathBuf> },
}

/// Checks that hard links work between `from` and `to`, which the plain view
/// depends on.
fn check_hard_links(from: &Path, to: &Path) -> Result<(), Error> {
    let source = from.join(format!("link-check-{}", blobs::random_name()));
    let link = to.join(format!(".link-check-{}", blobs::random_name()));
    std::fs::write(&source, b"")?;
    let linked = std::fs::hard_link(&source, &link);
    let _ = std::fs::remove_file(&link);
    std::fs::remove_file(&source)?;
    linked.map_err(|err| {
        Error::NoHardLinks(format!("{} ({err})", to.parent().unwrap_or(to).display()))
    })
}

fn check_name(name: &str) -> Result<String, Error> {
    let valid = !name.is_empty()
        && name.len() <= MAX_NAME_LEN
        && name != "."
        && name != ".."
        && !name.contains(['/', '\0']);
    if valid {
        Ok(name.to_owned())
    } else {
        Err(Error::InvalidName)
    }
}

/// Selects an item's columns and its current version, for [`item_from_row`].
const ITEM_SELECT: &str = "
    SELECT i.id, i.owner_id, i.parent_id, i.name, i.kind, i.created_at, i.modified_at,
           i.trashed_at, v.id, v.blob, b.size
    FROM storage_items i
    LEFT JOIN storage_versions v ON v.item_id = i.id
        AND v.number = (SELECT max(number) FROM storage_versions WHERE item_id = i.id)
    LEFT JOIN storage_blobs b ON b.hash = v.blob";

fn item_from_row(row: &Row<'_>) -> rusqlite::Result<Item> {
    let kind: String = row.get(4)?;
    let kind = if kind == "folder" {
        ItemKind::Folder
    } else {
        ItemKind::File {
            version: VersionId::from_uuid(row.get(8)?),
            blob: Blob {
                hash: hash_from(row, 9)?,
                size: size_from(row, 10)?,
            },
        }
    };
    Ok(Item {
        id: ItemId::from_uuid(row.get(0)?),
        owner: UserId::from_uuid(row.get(1)?),
        parent: row.get::<_, Option<uuid::Uuid>>(2)?.map(ItemId::from_uuid),
        name: row.get(3)?,
        kind,
        created_at: row.get(5)?,
        modified_at: row.get(6)?,
        trashed_at: row.get(7)?,
    })
}

fn hash_from(row: &Row<'_>, index: usize) -> rusqlite::Result<ContentHash> {
    let bytes: Vec<u8> = row.get(index)?;
    bytes.try_into().map(ContentHash).map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            rusqlite::types::Type::Blob,
            "a content hash isn't 32 bytes".into(),
        )
    })
}

/// SQLite stores integers as `i64`. Sizes are never negative, which the
/// table checks.
fn size_from(row: &Row<'_>, index: usize) -> rusqlite::Result<u64> {
    Ok(row.get::<_, i64>(index)?.unsigned_abs())
}

fn item(conn: &Connection, id: ItemId) -> Result<Option<Item>, Error> {
    Ok(conn
        .query_row(
            &format!("{ITEM_SELECT} WHERE i.id = ?1"),
            [id.as_uuid()],
            item_from_row,
        )
        .optional()?)
}

fn item_or_missing(conn: &Connection, id: ItemId) -> Result<Item, Error> {
    item(conn, id)?.ok_or(Error::NotFound)
}

fn child_named(conn: &Connection, parent: ItemId, name: &str) -> Result<Option<Item>, Error> {
    Ok(conn
        .query_row(
            &format!(
                "{ITEM_SELECT} WHERE i.parent_id = ?1 AND i.name = ?2 AND i.trashed_at IS NULL"
            ),
            (parent.as_uuid(), name),
            item_from_row,
        )
        .optional()?)
}

/// A folder that new items can go into: it exists, and it isn't in the
/// trash.
fn writable_folder(conn: &Connection, id: ItemId) -> Result<Item, Error> {
    let folder = item(conn, id)?.ok_or(Error::NotFound)?;
    if folder.kind != ItemKind::Folder {
        return Err(Error::NotAFolder);
    }
    if !location(conn, id)?.visible {
        return Err(Error::InTrash);
    }
    Ok(folder)
}

fn insert_item(
    conn: &Connection,
    id: ItemId,
    owner: UserId,
    parent: ItemId,
    name: &str,
    kind: &str,
) -> Result<(), Error> {
    conn.execute(
        concat!(
            "INSERT INTO storage_items
             (id, owner_id, parent_id, name, kind, created_at, modified_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ",
            sql_now!(),
            ", ",
            sql_now!(),
            ")"
        ),
        (id.as_uuid(), owner.as_uuid(), parent.as_uuid(), name, kind),
    )?;
    Ok(())
}

/// Records stored contents. `stored` says they were written just now, which
/// also repairs contents the integrity check found missing or changed.
fn insert_blob(conn: &Connection, blob: Blob, stored: bool) -> Result<(), Error> {
    conn.execute(
        concat!(
            "INSERT INTO storage_blobs (hash, size, created_at, checked_at)
             VALUES (?1, ?2, ",
            sql_now!(),
            ", ",
            sql_now!(),
            ") ON CONFLICT (hash) DO UPDATE SET
                 unused_since = NULL,
                 damage = CASE WHEN ?3 THEN NULL ELSE damage END,
                 checked_at = CASE WHEN ?3 THEN excluded.checked_at ELSE checked_at END"
        ),
        (&blob.hash.0[..], to_sql_size(blob.size), stored),
    )?;
    Ok(())
}

/// SQLite stores integers as `i64`. No file comes near the difference.
fn to_sql_size(size: u64) -> i64 {
    i64::try_from(size).unwrap_or(i64::MAX)
}

/// Checks that `name` in `parent` can take a new file or version of
/// `size` bytes. Returns the folder, and the file already there if any.
fn check_save(
    conn: &Connection,
    parent: ItemId,
    name: &str,
    size: u64,
    upload: Option<UploadId>,
) -> Result<(Item, Option<ItemId>), Error> {
    let parent = writable_folder(conn, parent)?;
    let existing = match child_named(conn, parent.id, name)? {
        Some(existing) if existing.kind == ItemKind::Folder => return Err(Error::NameTaken),
        existing => existing.map(|existing| existing.id),
    };
    if let Some(upload) = upload {
        let owner = uploads::owner(conn, upload)?;
        if owner != parent.owner {
            return Err(Error::DifferentOwner);
        }
    }
    check_space(conn, parent.owner, size, upload)?;
    Ok((parent, existing))
}

/// Refuses `adding` more bytes for `owner` if it would go over their quota.
/// Space set aside for `upload` is already part of `adding`.
fn check_space(
    conn: &Connection,
    owner: UserId,
    adding: u64,
    upload: Option<UploadId>,
) -> Result<(), Error> {
    let Some(quota) = quota(conn, owner)? else {
        return Ok(());
    };
    let used = space_used(conn, owner)? + space_reserved(conn, owner, upload)?;
    if used.saturating_add(adding) > quota {
        return Err(Error::QuotaFull);
    }
    Ok(())
}

fn quota(conn: &Connection, owner: UserId) -> Result<Option<u64>, Error> {
    Ok(conn
        .query_row(
            "SELECT bytes FROM storage_quotas WHERE owner_id = ?1",
            [owner.as_uuid()],
            |row| size_from(row, 0),
        )
        .optional()?)
}

fn space_used(conn: &Connection, owner: UserId) -> Result<u64, Error> {
    Ok(conn.query_row(
        "SELECT coalesce(sum(b.size), 0) FROM storage_items i
         JOIN storage_versions v ON v.item_id = i.id
         JOIN storage_blobs b ON b.hash = v.blob
         WHERE i.owner_id = ?1",
        [owner.as_uuid()],
        |row| size_from(row, 0),
    )?)
}

/// Space set aside for `owner`'s uploads in progress, apart from `except`.
fn space_reserved(
    conn: &Connection,
    owner: UserId,
    except: Option<UploadId>,
) -> Result<u64, Error> {
    Ok(conn.query_row(
        "SELECT coalesce(sum(size), 0) FROM storage_uploads
         WHERE owner_id = ?1 AND id IS NOT ?2",
        (owner.as_uuid(), except.map(|id| id.as_uuid())),
        |row| size_from(row, 0),
    )?)
}

/// Up to `count` stored contents chosen at random from the database `conn`,
/// leaving out any the integrity check already found damaged. Restore tests
/// use it on a database restored from a backup.
pub(crate) fn sample_contents(conn: &Connection, count: usize) -> rusqlite::Result<Vec<Blob>> {
    conn.prepare(
        "SELECT hash, size FROM storage_blobs WHERE damage IS NULL ORDER BY random() LIMIT ?1",
    )?
    .query_map([i64::try_from(count).unwrap_or(i64::MAX)], |row| {
        Ok(Blob {
            hash: hash_from(row, 0)?,
            size: size_from(row, 1)?,
        })
    })?
    .collect()
}

/// Deletes an item, everything inside it, and all their versions.
fn delete_subtree(conn: &Connection, id: ItemId) -> Result<(), Error> {
    conn.execute(
        "WITH RECURSIVE down (id) AS (
             SELECT ?1
             UNION ALL
             SELECT i.id FROM storage_items i JOIN down ON i.parent_id = down.id
         )
         DELETE FROM storage_items WHERE id IN (SELECT id FROM down)",
        [id.as_uuid()],
    )?;
    Ok(())
}

/// Adds `blob` as the newest version of `item`. An empty current version
/// is replaced instead of kept, because there's nothing in it to go back to.
/// File Explorer on Windows, for example, saves every new file empty first,
/// then saves its contents over it.
fn add_version(conn: &Connection, item: ItemId, blob: Blob) -> Result<(), Error> {
    let replaced = conn.execute(
        concat!(
            "UPDATE storage_versions SET blob = ?2, created_at = ",
            sql_now!(),
            " WHERE id = (SELECT v.id FROM storage_versions v
                          JOIN storage_blobs b ON b.hash = v.blob
                          WHERE v.item_id = ?1 AND b.size = 0
                            AND v.number = (SELECT max(number) FROM storage_versions
                                            WHERE item_id = ?1))"
        ),
        (item.as_uuid(), &blob.hash.0[..]),
    )?;
    if replaced == 0 {
        insert_version(conn, item, blob)?;
    }
    conn.execute(
        concat!(
            "UPDATE storage_items SET modified_at = ",
            sql_now!(),
            " WHERE id = ?1"
        ),
        [item.as_uuid()],
    )?;
    Ok(())
}

fn insert_version(conn: &Connection, item: ItemId, blob: Blob) -> Result<(), Error> {
    conn.execute(
        concat!(
            "INSERT INTO storage_versions (id, item_id, number, blob, created_at)
             VALUES (?1, ?2,
                     (SELECT coalesce(max(number), 0) + 1 FROM storage_versions WHERE item_id = ?2),
                     ?3, ",
            sql_now!(),
            ")"
        ),
        (
            VersionId::generate().as_uuid(),
            item.as_uuid(),
            &blob.hash.0[..],
        ),
    )?;
    Ok(())
}

/// Where an item sits: its owner, the names of the folders above it (not
/// counting the top folder) and its own, and whether it's out of the trash.
struct Location {
    owner: UserId,
    names: Vec<String>,
    visible: bool,
}

fn location(conn: &Connection, id: ItemId) -> Result<Location, Error> {
    let mut statement = conn.prepare(
        "WITH RECURSIVE up (id, parent_id, owner_id, name, trashed_at, depth) AS (
             SELECT id, parent_id, owner_id, name, trashed_at, 0
             FROM storage_items WHERE id = ?1
             UNION ALL
             SELECT i.id, i.parent_id, i.owner_id, i.name, i.trashed_at, up.depth + 1
             FROM storage_items i JOIN up ON i.id = up.parent_id
         )
         SELECT owner_id, name, parent_id IS NULL, trashed_at IS NOT NULL
         FROM up ORDER BY depth DESC",
    )?;
    let rows = statement
        .query_map([id.as_uuid()], |row| {
            Ok((
                UserId::from_uuid(row.get(0)?),
                row.get::<_, String>(1)?,
                row.get::<_, bool>(2)?,
                row.get::<_, bool>(3)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let (owner, ..) = *rows.first().ok_or(Error::NotFound)?;
    Ok(Location {
        owner,
        names: rows
            .iter()
            .filter(|(_, _, top, _)| !top)
            .map(|(_, name, ..)| name.clone())
            .collect(),
        visible: !rows.iter().any(|(.., trashed)| *trashed),
    })
}

/// Whether `id` is `ancestor` or inside it.
fn is_inside(conn: &Connection, id: ItemId, ancestor: ItemId) -> Result<bool, Error> {
    Ok(conn.query_row(
        "WITH RECURSIVE up (id, parent_id) AS (
             SELECT id, parent_id FROM storage_items WHERE id = ?1
             UNION ALL
             SELECT i.id, i.parent_id FROM storage_items i JOIN up ON i.id = up.parent_id
         )
         SELECT EXISTS (SELECT 1 FROM up WHERE id = ?2)",
        (id.as_uuid(), ancestor.as_uuid()),
        |row| row.get(0),
    )?)
}

/// A path below some folder, with the current contents if it's a file.
type TreeEntry = (Vec<String>, Option<ContentHash>);

/// `top` and everything inside it that isn't in the trash, as paths below
/// `top` with each file's current contents. Folders come before what's in
/// them.
fn subtree(conn: &Connection, top: ItemId) -> Result<Vec<TreeEntry>, Error> {
    let mut statement = conn.prepare(
        "WITH RECURSIVE down (id, depth) AS (
             SELECT ?1, 0
             UNION ALL
             SELECT i.id, down.depth + 1 FROM storage_items i JOIN down ON i.parent_id = down.id
             WHERE i.trashed_at IS NULL
         )
         SELECT i.id, i.parent_id, i.name, i.kind,
                (SELECT blob FROM storage_versions v WHERE v.item_id = i.id
                 ORDER BY v.number DESC LIMIT 1)
         FROM down JOIN storage_items i ON i.id = down.id
         ORDER BY down.depth",
    )?;
    let rows = statement
        .query_map([top.as_uuid()], |row| {
            let kind: String = row.get(3)?;
            Ok((
                ItemId::from_uuid(row.get(0)?),
                row.get::<_, Option<uuid::Uuid>>(1)?.map(ItemId::from_uuid),
                row.get::<_, String>(2)?,
                if kind == "file" {
                    Some(hash_from(row, 4)?)
                } else {
                    None
                },
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;

    let mut paths: HashMap<ItemId, Vec<String>> = HashMap::new();
    let mut entries = Vec::with_capacity(rows.len());
    for (id, parent, name, hash) in rows {
        let names = if id == top {
            Vec::new()
        } else {
            let Some(parent_path) = parent.and_then(|parent| paths.get(&parent)) else {
                continue;
            };
            let mut names = parent_path.clone();
            names.push(name);
            names
        };
        paths.insert(id, names.clone());
        entries.push((names, hash));
    }
    Ok(entries)
}

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
//! through this API. Each item belongs to the owner of its folder tree, and
//! owners can share items with other people ([`Storage::share`]). Every
//! module asks [`Storage::access`] before it shows or changes an item, so
//! sharing works the same way everywhere. See decision 16.

pub mod blobs;
mod changes;
mod shares;
mod upkeep;
mod uploads;
mod view;

pub use blobs::{Blob, ContentHash, Problem};
pub(crate) use changes::forget_cursors;
pub use changes::{Changes, PAGE as SYNC_PAGE, SyncChange, SyncItem, SyncPage, SyncStart};
pub use shares::{Access, Share, ShareEvent, ShareLevel, Shared, SharedOut};
pub(crate) use shares::{access, people_with_access};
pub use upkeep::{Checked, Cleaned, Damage, Purged, RETENTION_CHOICES, Retention, StorageStatus};
pub use uploads::Upload;

use std::collections::{HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use accordion_types::{ItemId, UploadId, UserId, VersionId};
use rusqlite::{Connection, OptionalExtension, Row};
use tokio::io::AsyncRead;
use tokio::sync::watch;
use uuid::Uuid;

use crate::db::{self, Database, Migration};
use crate::events::{self, Action, Actor, NewEvent};
use crate::identity::{self, Identity};
use crate::sql_now;
use crate::texts::{Matched, Words};
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
    Migration {
        version: 3,
        name: "Add sharing",
        sql: "
        -- Who else may open an item, and everything inside it.
        CREATE TABLE storage_shares (
            item_id BLOB NOT NULL REFERENCES storage_items (id) ON DELETE CASCADE,
            user_id BLOB NOT NULL REFERENCES identity_users (id) ON DELETE CASCADE,
            level TEXT NOT NULL CHECK (level IN ('viewer', 'editor')),
            -- Not a reference, so the record outlives the account.
            shared_by BLOB NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (item_id, user_id)
        ) STRICT, WITHOUT ROWID;
        CREATE INDEX storage_shares_user ON storage_shares (user_id);

        -- Every share, change, and removal, for the audit log. Rows stay
        -- after the item and the people are gone.
        CREATE TABLE storage_share_events (
            id INTEGER PRIMARY KEY,
            at TEXT NOT NULL,
            item_id BLOB NOT NULL,
            user_id BLOB NOT NULL,
            -- NULL when the share was removed.
            level TEXT CHECK (level IN ('viewer', 'editor')),
            by_id BLOB NOT NULL
        ) STRICT;
        CREATE INDEX storage_share_events_item ON storage_share_events (item_id);

        -- Who's sending an upload into someone else's storage, such as an
        -- editor of a shared folder. NULL when it's the owner.
        ALTER TABLE storage_uploads ADD COLUMN started_by BLOB;
    ",
    },
    Migration {
        version: 4,
        name: "Move sharing history into the event log",
        sql: "
        -- Earlier versions didn't record how a share was made or whether it
        -- changed someone's access, so each becomes a share from the web app.
        INSERT INTO events_log (at, actor_id, via, action, item_id, owner_id, other_id, details)
        SELECT e.at, e.by_id, 'web',
               CASE WHEN e.level IS NULL THEN 'unshared' ELSE 'shared' END,
               e.item_id, i.owner_id, e.user_id,
               CASE WHEN e.level IS NULL THEN NULL
                    ELSE json_object('level', e.level) END
        FROM storage_share_events e LEFT JOIN storage_items i ON i.id = e.item_id
        ORDER BY e.id;
        DROP TABLE storage_share_events;
    ",
    },
    Migration {
        version: 5,
        name: "Add the list of changes for sync",
        sql: "
        -- What changed for each person, for sync (decision 23). An entry
        -- names the item, not what happened to it: devices load it as it is
        -- now. Each change has one seq, shared by everyone it affects.
        CREATE TABLE storage_changes (
            user_id BLOB NOT NULL REFERENCES identity_users (id) ON DELETE CASCADE,
            seq INTEGER NOT NULL,
            -- Not a reference, so the entry outlives an item deleted for good.
            item_id BLOB NOT NULL,
            -- 1 when everything inside changed for them too, such as when it
            -- was shared with them.
            whole INTEGER NOT NULL CHECK (whole IN (0, 1)),
            at TEXT NOT NULL,
            PRIMARY KEY (user_id, seq)
        ) STRICT, WITHOUT ROWID;
        CREATE INDEX storage_changes_at ON storage_changes (at);

        -- Where the list is up to. A restored backup gets a new generation,
        -- so devices with a cursor from the old one start over.
        CREATE TABLE storage_sync (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            generation BLOB NOT NULL,
            last_seq INTEGER NOT NULL,
            -- Changes up to here were pruned.
            pruned_through INTEGER NOT NULL
        ) STRICT;
        INSERT INTO storage_sync VALUES (1, randomblob(16), 0, 0);
    ",
    },
    Migration {
        version: 6,
        name: "Add the admin's choice of how long trash and versions are kept",
        sql: "
        CREATE TABLE storage_settings (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            trash_days INTEGER NOT NULL CHECK (trash_days > 0),
            version_days INTEGER NOT NULL CHECK (version_days > 0)
        ) STRICT;
        INSERT INTO storage_settings VALUES (1, 30, 30);
    ",
    },
    Migration {
        version: 7,
        name: "Group an editing session's saves into one version",
        sql: "
        -- The co-editing session that saved the version, and when its first
        -- save was. Later saves from the same session replace the version
        -- for 30 minutes, so typing doesn't make a version every 30 seconds.
        ALTER TABLE storage_versions ADD COLUMN edit_id BLOB CHECK (length(edit_id) = 16);
        ALTER TABLE storage_versions ADD COLUMN edit_started_at TEXT;
    ",
    },
    Migration {
        version: 8,
        name: "Give each app a place of its own beside My Drive",
        sql: "
        -- Which place a top folder is: 'drive' for My Drive, or an app's
        -- name, such as 'notes'. Only top folders have one. Each person has
        -- at most one top folder for each place (decision 32).
        ALTER TABLE storage_items ADD COLUMN place TEXT
            CHECK (place IS NULL OR (parent_id IS NULL AND length(place) BETWEEN 1 AND 32));
        UPDATE storage_items SET place = 'drive' WHERE parent_id IS NULL;
        DROP INDEX storage_items_one_root;
        CREATE UNIQUE INDEX storage_items_one_root ON storage_items (owner_id, place)
            WHERE parent_id IS NULL;
    ",
    },
];

/// My Drive's place. Its top folder has an empty name.
pub const MY_DRIVE: &str = "drive";

/// A place an app keeps its files in, with a top folder of its own for each
/// person, beside My Drive (decision 32). Drive shows each one as a tab.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AppPlace {
    /// Lowercase letters only, such as "notes". Usually the app's module
    /// name.
    pub id: &'static str,
    /// What people see, such as "Notes". It names the top folder, and its
    /// folder on computers and in the plain view.
    pub name: &'static str,
}

/// Who a move would let open an item, and who couldn't open it anymore.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Reach {
    /// People who can't open the item now, but could after the move.
    pub gains: Vec<UserId>,
    /// People who can open it now, but couldn't after the move.
    pub loses: Vec<UserId>,
}

/// How long saves from one editing session replace the version the session
/// started, before the next save starts a new one.
pub const EDIT_VERSION_MINUTES: u32 = 30;

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

    #[error(
        "This folder already has a {} with that name. Choose a different name.",
        if *.folder { "folder" } else { "file" }
    )]
    NameTaken {
        /// Whether a folder has the name, rather than a file.
        folder: bool,
    },

    #[error("That file or folder doesn't exist. It may have been deleted.")]
    NotFound,

    #[error("That's a file, not a folder.")]
    NotAFolder,

    #[error("That's a folder, not a file.")]
    NotAFile,

    #[error("Your top folder can't be renamed, moved, or deleted.")]
    TopFolder,

    #[error("Accordion doesn't have an app called {0} that keeps files.")]
    UnknownPlace(String),

    #[error("A folder can't be moved into itself, or into a folder inside it.")]
    MoveIntoItself,

    #[error("Files and folders can only move within one person's storage.")]
    DifferentOwner,

    #[error("That's in the trash. Restore it first, then try again.")]
    InTrash,

    #[error("That isn't in the trash.")]
    NotInTrash,

    #[error("Someone changed this file since you opened it. Open it again to see their changes.")]
    Changed,

    #[error(
        "Accordion can't tell what changed since this device last synced, so it needs to \
         compare everything again."
    )]
    StartOver,

    #[error(
        "There isn't enough space left in your storage. Delete some files and empty the \
         trash, or ask your admin for more space."
    )]
    QuotaFull,

    #[error("That user doesn't exist. They may have been removed.")]
    UserNotFound,

    #[error("Someone's files can't be given to themselves. Choose someone else.")]
    GiveToSelf,

    #[error("They own it already, so it can't be shared with them. Choose someone else.")]
    ShareWithOwner,

    #[error("A whole Drive can't be shared. Share a folder inside it instead.")]
    ShareTopFolder,

    #[error("Trash and versions can be kept for 7, 30, 90, or 365 days. Choose one of those.")]
    InvalidRetention,

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
    /// Which place a top folder is, such as [`MY_DRIVE`]. `None` for
    /// everything else.
    pub place: Option<String>,
}

impl Item {
    /// The error for another item wanting this one's name.
    fn name_taken(&self) -> Error {
        Error::NameTaken {
            folder: self.kind == ItemKind::Folder,
        }
    }
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

/// What [`Storage::search`] looks for. Every part that's set has to match.
#[derive(Clone, Debug, Default)]
pub struct Search {
    /// Words that all have to be in the name, ignoring case and accents.
    /// Empty matches every name.
    pub name: Words,
    /// The contents whose text matched words that have to be in the text.
    /// `None` doesn't look at the text.
    pub text: Option<Matched>,
    /// Words that have to be in the name, unless the item's contents are
    /// among those whose text matched them.
    pub name_or_text: Option<(Words, Matched)>,
    pub only: Option<Only>,
    pub owner: Option<Owner>,
    pub place: Option<Place>,
    /// Looks in the user's own trash, rather than outside it.
    pub trash: bool,
    /// Only items the user shared with this person themselves.
    pub shared_with: Option<UserId>,
    /// Only these items, such as the ones the user starred.
    pub among: Option<HashSet<ItemId>>,
    /// Changed at or after this time, in the database's timestamp format
    /// (see [`sql_now`]).
    pub modified_since: Option<String>,
    /// Changed before this time.
    pub modified_before: Option<String>,
    /// The most results to return.
    pub limit: usize,
}

/// Whose items a [`Search`] finds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Owner {
    Me,
    Others,
    /// Items this person owns.
    Person(UserId),
    /// The user's own items that they shared with someone.
    SharedByMe,
    /// Items this person owns, and the user's own items that they shared
    /// with them, as Drive's People filter means by a person.
    Involving(UserId),
}

/// Where a [`Search`] looks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Place {
    /// The user's My Drive. Apps' places aren't part of it.
    MyDrive,
    /// What others shared with the user.
    Shared,
    /// This folder and the folders inside it.
    Folder(ItemId),
}

/// An item a search found, and how it matched.
#[derive(Clone, Debug, PartialEq)]
pub struct Hit {
    pub found: Found,
    /// Whether its name had the words of [`Search::name_or_text`].
    pub by_name: bool,
    /// How well its text matched, lower being better, if it did.
    pub text_rank: Option<f64>,
}

/// Which kinds of item a [`Search`] finds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Only {
    Folders,
    Files,
    /// Files whose names end in one of these extensions, lowercase and
    /// without the dot, such as "jpg".
    Extensions(Vec<String>),
}

/// An item that a search or list found, with where it is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Found {
    pub item: Item,
    /// The names of the folders it's in, outermost first, not counting the
    /// top folder. Empty for an item in the top folder.
    pub folders: Vec<String>,
    /// The place its top folder is, such as [`MY_DRIVE`], when the path in
    /// `folders` starts there. `None` when it starts at something shared
    /// with the person.
    pub place: Option<String>,
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

/// How much space one file takes, for showing what kinds of file use a
/// user's space.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileSpace {
    pub name: String,
    /// Every version, counted as [`Usage::used`] counts them.
    pub bytes: u64,
}

/// What happens to a user's files when [`Storage::remove_owner`] takes them
/// out of storage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LeaveFiles {
    /// Move them, with their versions and trash, into a new folder in each
    /// of another user's places: what was in My Drive into their My Drive,
    /// what was in Notes into their Notes, and so on. If `folder_name` is taken, the folder gets
    /// a number after it, such as "Sam's files (2)". The other user's quota
    /// doesn't stop this, like a quota set below what someone already uses.
    GiveTo { owner: UserId, folder_name: String },
    /// Delete them for good. Their contents are removed a day later, unless
    /// something else uses them.
    Delete,
}

/// The storage service. Cloning it is cheap.
///
/// Each change is recorded in the event log as the handle's actor, which is
/// the server unless the handle came from [`Storage::acting_as`].
#[derive(Clone)]
pub struct Storage {
    inner: Arc<Inner>,
    actor: Actor,
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
    /// Counts files saved, so services such as thumbnails know to look.
    saved: watch::Sender<u64>,
    /// The apps that keep files in places of their own.
    places: std::sync::Mutex<Vec<AppPlace>>,
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
        // The event log comes first, since migration 4 moves sharing
        // history into it.
        events::migrate(&db).await?;
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
                saved: watch::Sender::new(0),
                places: std::sync::Mutex::new(Vec::new()),
                holds: AtomicUsize::new(0),
            }),
            actor: Actor::SERVER,
        })
    }

    /// A handle that records its changes in the event log as `actor`, such
    /// as the person making a request.
    pub fn acting_as(&self, actor: Actor) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            actor,
        }
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

    /// `owner`'s top folder in My Drive, created the first time it's needed.
    pub async fn top_folder(&self, owner: UserId) -> Result<ItemId, Error> {
        self.top_folder_in(owner, MY_DRIVE, "").await
    }

    /// Adds an app's place, so [`Storage::place_folder`] can make its top
    /// folders and Drive shows it. Apps add theirs when the server starts.
    pub fn add_place(&self, place: AppPlace) {
        assert!(
            !place.id.is_empty()
                && place.id.len() <= 32
                && place.id.bytes().all(|b| b.is_ascii_lowercase())
                && place.id != MY_DRIVE,
            "a place's id is up to 32 lowercase letters, and isn't My Drive's"
        );
        let mut places = self.inner.places.lock().expect("places lock");
        if !places.iter().any(|known| known.id == place.id) {
            places.push(place);
        }
    }

    /// The apps' places, in the order they were added.
    pub fn places(&self) -> Vec<AppPlace> {
        self.inner.places.lock().expect("places lock").clone()
    }

    /// `owner`'s top folder in an app's place, such as Notes', created the
    /// first time it's needed.
    pub async fn place_folder(&self, owner: UserId, place: &str) -> Result<ItemId, Error> {
        let known = self
            .places()
            .into_iter()
            .find(|known| known.id == place)
            .ok_or_else(|| Error::UnknownPlace(place.to_owned()))?;
        self.top_folder_in(owner, known.id, known.name).await
    }

    /// `owner`'s top folders that exist, My Drive first, then the others in
    /// the order they were made. An app that's gone keeps its place here,
    /// so its files stay reachable.
    pub async fn own_places(&self, owner: UserId) -> Result<Vec<Item>, Error> {
        self.inner
            .db
            .read(move |conn| {
                let mut statement = conn.prepare(&format!(
                    "{ITEM_SELECT} WHERE i.owner_id = ?1 AND i.parent_id IS NULL
                     ORDER BY i.place <> 'drive', i.created_at, i.place"
                ))?;
                let items = statement
                    .query_map([owner.as_uuid()], item_from_row)?
                    .collect::<Result<_, _>>()?;
                Ok(items)
            })
            .await
    }

    async fn top_folder_in(
        &self,
        owner: UserId,
        place: &'static str,
        name: &'static str,
    ) -> Result<ItemId, Error> {
        if let Some(id) = self.find_top_folder(owner, place).await? {
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
                         (id, owner_id, parent_id, name, kind, created_at, modified_at, place)
                         VALUES (?1, ?2, NULL, ?3, 'folder', ",
                        sql_now!(),
                        ", ",
                        sql_now!(),
                        ", ?4)"
                    ),
                    (id.as_uuid(), owner.as_uuid(), name, place),
                )
                .map_err(Error::from)
            })
            .await?;
        let id = self
            .find_top_folder(owner, place)
            .await?
            .ok_or(Error::NotFound)?;
        self.update_view(Change::Show(id)).await;
        Ok(id)
    }

    async fn find_top_folder(&self, owner: UserId, place: &str) -> Result<Option<ItemId>, Error> {
        let place = place.to_owned();
        self.inner
            .db
            .read(move |conn| top_in(conn, owner, &place))
            .await
    }

    pub async fn item(&self, id: ItemId) -> Result<Option<Item>, Error> {
        self.inner.db.read(move |conn| item(conn, id)).await
    }

    /// The items from `root` down to `id`, if `id` is `root` or inside it,
    /// and neither it nor any folder above it is in the trash. This is how
    /// a link reaches an item: what's in a linked folder is reachable, and
    /// nothing else is (decision 22).
    pub async fn path_within(&self, root: ItemId, id: ItemId) -> Result<Option<Vec<Item>>, Error> {
        self.inner
            .db
            .read(move |conn| {
                let mut path = Vec::new();
                let mut next = Some(id);
                let mut reached = false;
                while let Some(current) = next {
                    let Some(found) = item(conn, current)? else {
                        return Ok(None);
                    };
                    if found.trashed_at.is_some() {
                        return Ok(None);
                    }
                    next = found.parent;
                    if !reached {
                        path.push(found);
                        reached = current == root;
                    }
                }
                if !reached {
                    return Ok(None);
                }
                path.reverse();
                Ok(Some(path))
            })
            .await
    }

    /// Which of `ids` are `owner`'s items outside the trash, with where
    /// each is, such as for **Shared by me** to list what a module shared
    /// in its own way. Other people's items, and ids that don't exist, are
    /// left out.
    pub async fn found_items(&self, owner: UserId, ids: Vec<ItemId>) -> Result<Vec<Found>, Error> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        self.inner
            .db
            .read(move |conn| {
                let placeholders = vec!["?"; ids.len()].join(", ");
                let mut statement =
                    conn.prepare(&format!("{FOUND_SELECT} WHERE i.id IN ({placeholders})"))?;
                let mut found = Vec::new();
                for top in tops(conn, owner)? {
                    let rows = statement.query_map(
                        rusqlite::params_from_iter(
                            std::iter::once(top.as_uuid()).chain(ids.iter().map(|id| id.as_uuid())),
                        ),
                        found_from_row,
                    )?;
                    for row in rows {
                        found.push(row?);
                    }
                }
                Ok(found)
            })
            .await
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

    /// `user`'s files outside the trash, and files in what others shared
    /// with them, most recently changed first.
    pub async fn recent(&self, user: UserId, limit: usize) -> Result<Vec<Found>, Error> {
        self.inner
            .db
            .read(move |conn| {
                let mut statement = conn.prepare(&format!(
                    "{FOUND_SELECT} WHERE i.kind = 'file' ORDER BY i.modified_at DESC, i.name LIMIT ?2"
                ))?;
                let mut found = Vec::new();
                for root in shares::roots(conn, user)? {
                    let rows = statement.query_map((root.as_uuid(), limit as i64), found_from_row)?;
                    for row in rows {
                        found.push(row?);
                    }
                }
                Ok(newest_first(found, limit))
            })
            .await
    }

    /// `owner`'s largest files outside the trash, largest first, for
    /// finding what takes their space. Each counts its current version.
    pub async fn largest(&self, owner: UserId, limit: usize) -> Result<Vec<Found>, Error> {
        self.inner
            .db
            .read(move |conn| {
                let mut statement = conn.prepare(&format!(
                    "{FOUND_SELECT} WHERE i.kind = 'file' ORDER BY b.size DESC, i.name LIMIT ?2"
                ))?;
                let mut found = Vec::new();
                for top in tops(conn, owner)? {
                    let rows =
                        statement.query_map((top.as_uuid(), limit as i64), found_from_row)?;
                    for row in rows {
                        found.push(row?);
                    }
                }
                found.sort_by(|a, b| {
                    size_of(&b.item)
                        .cmp(&size_of(&a.item))
                        .then_with(|| a.item.name.cmp(&b.item.name))
                });
                found.truncate(limit);
                Ok(found)
            })
            .await
    }

    /// How much each folder in `folders` holds: the current version of
    /// every file inside it, however deep, leaving out the trash.
    pub async fn folder_sizes(&self, folders: Vec<ItemId>) -> Result<HashMap<ItemId, u64>, Error> {
        self.inner
            .db
            .read(move |conn| {
                let mut statement = conn.prepare(
                    "WITH RECURSIVE inside (id) AS (
                         SELECT ?1
                         UNION ALL
                         SELECT i.id FROM storage_items i JOIN inside ON i.parent_id = inside.id
                         WHERE i.trashed_at IS NULL
                     )
                     SELECT coalesce(sum(b.size), 0) FROM inside
                     JOIN storage_versions v ON v.item_id = inside.id
                         AND v.number = (SELECT max(number) FROM storage_versions
                                         WHERE item_id = inside.id)
                     JOIN storage_blobs b ON b.hash = v.blob",
                )?;
                let mut sizes = HashMap::new();
                for folder in folders {
                    let size = statement.query_row([folder.as_uuid()], |row| size_from(row, 0))?;
                    sizes.insert(folder, size);
                }
                Ok(sizes)
            })
            .await
    }

    /// `user`'s items outside the trash, and items in what others shared
    /// with them, that match `search`. Items whose names matched come
    /// first, most recently changed first, then items whose text matched,
    /// best first. Names are matched in Rust, because SQLite only ignores
    /// case for ASCII, and names are in every language.
    pub async fn search(&self, user: UserId, search: Search) -> Result<Vec<Hit>, Error> {
        self.inner
            .db
            .read(move |conn| {
                let by_rank = search.text.is_some() || search.name_or_text.is_some();
                let mut among = search.among.clone();
                let mut narrow = |ids: HashSet<ItemId>| {
                    among = Some(match among.take() {
                        Some(among) => among.intersection(&ids).copied().collect(),
                        None => ids,
                    });
                };
                if let Some(person) = search.shared_with {
                    narrow(shares::shared_by_with(conn, user, person)?);
                }
                if let Some(Place::Folder(folder)) = search.place {
                    if shares::access(conn, user, folder)?.is_none() {
                        return Ok(Vec::new());
                    }
                    narrow(inside(conn, folder)?);
                }
                let mut involving = None;
                match search.owner {
                    Some(Owner::SharedByMe) => narrow(shares::shared_by_anyone(conn, user)?),
                    Some(Owner::Involving(person)) => {
                        involving = Some(shares::shared_by_with(conn, user, person)?);
                    }
                    _ => {}
                }
                let matches =
                    |found: &Found| hit(&search, user, among.as_ref(), involving.as_ref(), found);

                let mut hits = Vec::new();
                if search.trash {
                    let mut statement = conn.prepare(&format!(
                        "{ITEM_SELECT} WHERE i.owner_id = ?1 AND i.trashed_at IS NOT NULL"
                    ))?;
                    for item in statement.query_map([user.as_uuid()], item_from_row)? {
                        let item = item?;
                        let place = location(conn, item.id)?.place;
                        let found = Found {
                            item,
                            folders: Vec::new(),
                            place,
                        };
                        if let Some(hit) = matches(&found) {
                            hits.push(hit);
                        }
                    }
                    return Ok(best_first(hits, search.limit, by_rank));
                }

                let kind = match search.only {
                    Some(Only::Folders) => "AND i.kind = 'folder'",
                    Some(_) => "AND i.kind = 'file'",
                    None => "",
                };
                let mut statement = conn.prepare(&format!(
                    "{FOUND_SELECT}
                     WHERE i.parent_id IS NOT NULL {kind}
                       AND (?2 IS NULL OR i.modified_at >= ?2)
                       AND (?3 IS NULL OR i.modified_at < ?3)
                     ORDER BY i.modified_at DESC, i.name"
                ))?;
                let own = tops(conn, user)?;
                let my_drive = top_in(conn, user, MY_DRIVE)?;
                for root in shares::roots(conn, user)? {
                    let mine = own.contains(&root);
                    match search.place {
                        Some(Place::MyDrive) if my_drive != Some(root) => continue,
                        Some(Place::Shared) if mine => continue,
                        _ => {}
                    }
                    let rows = statement.query_map(
                        (
                            root.as_uuid(),
                            &search.modified_since,
                            &search.modified_before,
                        ),
                        found_from_row,
                    )?;
                    // Each root's matches come newest first, so when results
                    // are newest first, the newest overall are among each
                    // root's first `limit`.
                    let mut matched = 0;
                    for row in rows {
                        let Some(hit) = matches(&row?) else {
                            continue;
                        };
                        hits.push(hit);
                        matched += 1;
                        if !by_rank && matched == search.limit {
                            break;
                        }
                    }
                }
                Ok(best_first(hits, search.limit, by_rank))
            })
            .await
    }

    /// The items among `ids` that exist and are outside the trash, in the
    /// same order, with where each one is. An item inside a deleted folder
    /// is in the trash too.
    pub async fn live_items(&self, ids: Vec<ItemId>) -> Result<Vec<Found>, Error> {
        self.inner
            .db
            .read(move |conn| {
                let mut found = Vec::new();
                for id in ids {
                    let Some(item) = item(conn, id)? else {
                        continue;
                    };
                    let mut location = location(conn, id)?;
                    if location.visible {
                        location.names.pop();
                        found.push(Found {
                            item,
                            folders: location.names,
                            place: location.place,
                        });
                    }
                }
                Ok(found)
            })
            .await
    }

    pub async fn create_folder(&self, parent: ItemId, name: &str) -> Result<Item, Error> {
        let name = check_name(name)?;
        let _changes = self.inner.changes.lock().await;
        let id = ItemId::generate();
        let actor = self.actor;
        let item = self
            .inner
            .db
            .write(move |tx| {
                let parent = writable_folder(tx, parent)?;
                if let Some(other) = child_named(tx, parent.id, &name)? {
                    return Err(other.name_taken());
                }
                insert_item(tx, id, parent.owner, parent.id, &name, "folder")?;
                record(tx, actor, added(id, parent.owner, "created"))?;
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
        self.receive_and_save(parent, name, contents, Over::Any)
            .await
    }

    /// Saves `contents` as a new file `name` in `parent`, refusing the name
    /// if a file already has it, rather than adding a version to that file.
    pub async fn create_file(
        &self,
        parent: ItemId,
        name: &str,
        contents: impl AsyncRead + Unpin,
    ) -> Result<Item, Error> {
        let name = check_name(name)?;
        self.receive_and_save(parent, name, contents, Over::Nothing)
            .await
    }

    /// Saves `contents` as the file's new version, but only if its contents
    /// are still `base`, so an editor doesn't save over changes it never saw.
    pub async fn write_version(
        &self,
        id: ItemId,
        base: ContentHash,
        contents: impl AsyncRead + Unpin,
    ) -> Result<Item, Error> {
        let file = self.item(id).await?.ok_or(Error::NotFound)?;
        if file.kind == ItemKind::Folder {
            return Err(Error::NotAFile);
        }
        if file.trashed_at.is_some() {
            return Err(Error::InTrash);
        }
        let parent = file.parent.ok_or(Error::NotAFile)?;
        self.receive_and_save(parent, file.name, contents, Over::File(id, base, None))
            .await
    }

    /// Saves `contents` as the file's new version, as
    /// [`Storage::write_version`] does, for a co-editing session (decision
    /// 30). When the file's current version came from the same `edit` less
    /// than [`EDIT_VERSION_MINUTES`] after that session's first save to it,
    /// this replaces that version, so a session's saves become one version.
    pub async fn write_edit(
        &self,
        id: ItemId,
        base: ContentHash,
        edit: Uuid,
        contents: impl AsyncRead + Unpin,
    ) -> Result<Item, Error> {
        let file = self.item(id).await?.ok_or(Error::NotFound)?;
        if file.kind == ItemKind::Folder {
            return Err(Error::NotAFile);
        }
        if file.trashed_at.is_some() {
            return Err(Error::InTrash);
        }
        let parent = file.parent.ok_or(Error::NotAFile)?;
        self.receive_and_save(
            parent,
            file.name,
            contents,
            Over::File(id, base, Some(edit)),
        )
        .await
    }

    async fn receive_and_save(
        &self,
        parent: ItemId,
        name: String,
        contents: impl AsyncRead + Unpin,
        over: Over,
    ) -> Result<Item, Error> {
        // Receiving the contents is the slow part, so it happens before
        // taking the lock.
        let received = self.inner.blobs.receive(contents).await?;
        let result = self.save(parent, name, &received, None, over).await;
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
        over: Over,
    ) -> Result<Item, Error> {
        let blob = received.blob;
        let _changes = self.inner.changes.lock().await;
        // Check before storing the contents, so refused contents never reach
        // blobs/. The lock keeps the answer true until the commit.
        let checked = name.clone();
        self.inner
            .db
            .read(move |conn| check_save(conn, parent, &checked, blob.size, upload, over))
            .await?;
        let kept = self.keep(received).await?;
        let stored = kept.stored;
        let new_id = ItemId::generate();
        let actor = self.actor;
        let item = self
            .inner
            .db
            .write(move |tx| {
                let (parent, existing) = check_save(tx, parent, &name, blob.size, upload, over)?;
                insert_blob(tx, blob, stored)?;
                let id = match existing {
                    Some(id) => {
                        record(tx, actor, NewEvent::about(Action::Edited, id, parent.owner))?;
                        id
                    }
                    None => {
                        insert_item(tx, new_id, parent.owner, parent.id, &name, "file")?;
                        record(tx, actor, added(new_id, parent.owner, "uploaded"))?;
                        new_id
                    }
                };
                let edit = match over {
                    Over::File(_, _, edit) => edit,
                    Over::Any | Over::Nothing => None,
                };
                add_version(tx, id, blob, edit)?;
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
        self.inner.saved.send_modify(|count| *count += 1);
        Ok(item)
    }

    /// Changes each time a file is saved, including new versions.
    pub fn saved(&self) -> watch::Receiver<u64> {
        self.inner.saved.subscribe()
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

    /// Whether the item exists and is outside the trash, counting a folder
    /// above it that's in the trash.
    pub async fn outside_trash(&self, id: ItemId) -> Result<bool, Error> {
        self.inner
            .db
            .read(move |conn| {
                if item(conn, id)?.is_none() {
                    return Ok(false);
                }
                Ok(location(conn, id)?.visible)
            })
            .await
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
        let actor = self.actor;
        let item = self
            .inner
            .db
            .write(move |tx| {
                let item = item(tx, id)?.ok_or(Error::NotFound)?;
                if !location(tx, id)?.visible {
                    return Err(Error::InTrash);
                }
                let (blob, number) = tx
                    .query_row(
                        "SELECT v.blob, b.size, v.number FROM storage_versions v
                         JOIN storage_blobs b ON b.hash = v.blob
                         WHERE v.item_id = ?1 AND v.id = ?2",
                        (item.id.as_uuid(), version.as_uuid()),
                        |row| {
                            Ok((
                                Blob {
                                    hash: hash_from(row, 0)?,
                                    size: size_from(row, 1)?,
                                },
                                row.get::<_, u32>(2)?,
                            ))
                        },
                    )
                    .optional()?
                    .ok_or(Error::NotFound)?;
                check_space(tx, item.owner, blob.size, None)?;
                add_version(tx, id, blob, None)?;
                record(
                    tx,
                    actor,
                    NewEvent::about(Action::VersionRestored, id, item.owner)
                        .details(serde_json::json!({ "number": number })),
                )?;
                item_or_missing(tx, id)
            })
            .await?;
        self.update_view(Change::Show(item.id)).await;
        Ok(item)
    }

    /// Copies a file's current version into `parent` as a new file named
    /// `name`. The copy shares the stored contents, but counts toward the
    /// quota of `parent`'s owner like any other file, which is how someone
    /// keeps their own copy of a file shared with them. Folders can't be
    /// copied, as in Google Drive.
    pub async fn copy_file(&self, id: ItemId, parent: ItemId, name: &str) -> Result<Item, Error> {
        let name = check_name(name)?;
        let _changes = self.inner.changes.lock().await;
        let new_id = ItemId::generate();
        let actor = self.actor;
        let item = self
            .inner
            .db
            .write(move |tx| {
                let original = item(tx, id)?.ok_or(Error::NotFound)?;
                let ItemKind::File { blob, .. } = original.kind else {
                    return Err(Error::NotAFile);
                };
                if !location(tx, id)?.visible {
                    return Err(Error::InTrash);
                }
                let parent = writable_folder(tx, parent)?;
                if let Some(other) = child_named(tx, parent.id, &name)? {
                    return Err(other.name_taken());
                }
                check_space(tx, parent.owner, blob.size, None)?;
                insert_item(tx, new_id, parent.owner, parent.id, &name, "file")?;
                add_version(tx, new_id, blob, None)?;
                record(tx, actor, added(new_id, parent.owner, "copied"))?;
                item_or_missing(tx, new_id)
            })
            .await?;
        self.update_view(Change::Show(item.id)).await;
        Ok(item)
    }

    /// Who moving `id` into `folder` would let open it, and who couldn't
    /// open it anymore, so an app can ask before a move shares something
    /// (decision 32). The owner is never in either list. Shares on the item
    /// itself go with it, so only the folders above it count.
    pub async fn move_reach(&self, id: ItemId, folder: ItemId) -> Result<Reach, Error> {
        self.inner
            .db
            .read(move |conn| {
                let item = item(conn, id)?.ok_or(Error::NotFound)?;
                let now: HashSet<UserId> = people_with_access(conn, id)?.into_iter().collect();
                let own: HashSet<UserId> = conn
                    .prepare("SELECT user_id FROM storage_shares WHERE item_id = ?1")?
                    .query_map([id.as_uuid()], |row| Ok(UserId::from_uuid(row.get(0)?)))?
                    .collect::<Result<_, _>>()?;
                let mut after: HashSet<UserId> =
                    people_with_access(conn, folder)?.into_iter().collect();
                after.extend(own);
                let mut gains: Vec<UserId> = after
                    .difference(&now)
                    .filter(|user| **user != item.owner)
                    .copied()
                    .collect();
                let mut loses: Vec<UserId> = now
                    .difference(&after)
                    .filter(|user| **user != item.owner)
                    .copied()
                    .collect();
                gains.sort();
                loses.sort();
                Ok(Reach { gains, loses })
            })
            .await
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
        let actor = self.actor;
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
                if let Some(other) = child_named(tx, parent.id, &new_name)?
                    && other.id != id
                {
                    return Err(other.name_taken());
                }
                tx.execute(
                    concat!(
                        "UPDATE storage_items SET parent_id = ?2, name = ?3, modified_at = ",
                        sql_now!(),
                        " WHERE id = ?1"
                    ),
                    (id.as_uuid(), parent.id.as_uuid(), &new_name),
                )?;
                if item.parent != Some(parent.id) {
                    record(
                        tx,
                        actor,
                        NewEvent::about(Action::Moved, id, item.owner).details(serde_json::json!({
                            "from": item.parent,
                            "to": parent.id,
                        })),
                    )?;
                }
                if item.name != new_name {
                    record(
                        tx,
                        actor,
                        NewEvent::about(Action::Renamed, id, item.owner).details(
                            serde_json::json!({
                                "from": item.name,
                                "to": new_name,
                            }),
                        ),
                    )?;
                }
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
        let actor = self.actor;
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
                record(tx, actor, NewEvent::about(Action::Trashed, id, item.owner))?;
                item_or_missing(tx, id)
            })
            .await?;
        self.update_view(Change::Hide { before }).await;
        Ok(item)
    }

    /// Takes an item out of the trash, back to the folder it was in.
    pub async fn restore(&self, id: ItemId) -> Result<Item, Error> {
        let _changes = self.inner.changes.lock().await;
        let actor = self.actor;
        let item = self
            .inner
            .db
            .write(move |tx| {
                let item = item(tx, id)?.ok_or(Error::NotFound)?;
                if item.trashed_at.is_none() {
                    return Err(Error::NotInTrash);
                }
                let parent = item.parent.ok_or(Error::NotInTrash)?;
                if let Some(other) = child_named(tx, parent, &item.name)? {
                    return Err(other.name_taken());
                }
                tx.execute(
                    "UPDATE storage_items SET trashed_at = NULL WHERE id = ?1",
                    [id.as_uuid()],
                )?;
                record(tx, actor, NewEvent::about(Action::Restored, id, item.owner))?;
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
        let actor = self.actor;
        self.inner
            .db
            .write(move |tx| {
                let item = item(tx, id)?.ok_or(Error::NotFound)?;
                if item.trashed_at.is_none() {
                    return Err(Error::NotInTrash);
                }
                delete_for_good(tx, &item, actor)
            })
            .await
    }

    /// Deletes everything in `owner`'s trash for good.
    pub async fn empty_trash(&self, owner: UserId) -> Result<(), Error> {
        let _changes = self.inner.changes.lock().await;
        let actor = self.actor;
        self.inner
            .db
            .write(move |tx| {
                let trashed: Vec<Item> = tx
                    .prepare(&format!(
                        "{ITEM_SELECT} WHERE i.owner_id = ?1 AND i.trashed_at IS NOT NULL"
                    ))?
                    .query_map([owner.as_uuid()], item_from_row)?
                    .collect::<Result<_, _>>()?;
                for item in trashed {
                    delete_for_good(tx, &item, actor)?;
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

    /// How much space each of `owner`'s files takes, by name, including the
    /// trash, so the sizes add up to [`Usage::used`]. Modules group them by
    /// kind, such as photos, which storage doesn't know.
    pub async fn space_by_file(&self, owner: UserId) -> Result<Vec<FileSpace>, Error> {
        self.inner
            .db
            .read(move |conn| {
                let mut statement = conn.prepare(
                    "SELECT i.name, coalesce(sum(b.size), 0) FROM storage_items i
                     JOIN storage_versions v ON v.item_id = i.id
                     JOIN storage_blobs b ON b.hash = v.blob
                     WHERE i.owner_id = ?1
                     GROUP BY i.id",
                )?;
                let files = statement
                    .query_map([owner.as_uuid()], |row| {
                        Ok(FileSpace {
                            name: row.get(0)?,
                            bytes: size_from(row, 1)?,
                        })
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(files)
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
        let actor = self.actor;
        self.inner
            .db
            .write(move |tx| {
                record(
                    tx,
                    actor,
                    NewEvent::new(Action::AccountChanged)
                        .other(owner)
                        .details(serde_json::json!({ "change": "quota", "bytes": bytes })),
                )?;
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

    /// Takes everything `owner` has out of storage, so their account can be
    /// deleted: their files and trash, their quota, their uploads in
    /// progress, and what others shared with them. What they shared stays
    /// shared, now from whoever gets their files. Returns the folder their
    /// My Drive went into, if their files were given to someone and they
    /// had a My Drive.
    ///
    /// Turn the account off first, so nothing new arrives while this runs.
    pub async fn remove_owner(
        &self,
        owner: UserId,
        files: LeaveFiles,
    ) -> Result<Option<Item>, Error> {
        let to = match files {
            LeaveFiles::GiveTo {
                owner: to,
                folder_name,
            } => {
                if to == owner {
                    return Err(Error::GiveToSelf);
                }
                self.inner
                    .identity
                    .user(to)
                    .await?
                    .ok_or(Error::UserNotFound)?;
                // So their My Drive exists, and the folder there comes back.
                self.top_folder(to).await?;
                Some((to, folder_name))
            }
            LeaveFiles::Delete => None,
        };
        let uploads: Vec<UploadId> = self
            .inner
            .db
            .read(move |conn| {
                conn.prepare(
                    "SELECT id FROM storage_uploads WHERE owner_id = ?1 OR started_by = ?1",
                )?
                .query_map([owner.as_uuid()], |row| {
                    Ok(UploadId::from_uuid(row.get(0)?))
                })?
                .collect::<Result<_, _>>()
                .map_err(Error::from)
            })
            .await?;
        for id in uploads {
            match self.cancel_upload(id).await {
                Ok(()) | Err(Error::UploadNotFound) => {}
                Err(err) => return Err(err),
            }
        }

        let _changes = self.inner.changes.lock().await;
        let folder = self
            .inner
            .db
            .write(move |tx| {
                let tops: Vec<(ItemId, String, String)> = tx
                    .prepare(
                        "SELECT id, place, name FROM storage_items
                         WHERE owner_id = ?1 AND parent_id IS NULL",
                    )?
                    .query_map([owner.as_uuid()], |row| {
                        Ok((ItemId::from_uuid(row.get(0)?), row.get(1)?, row.get(2)?))
                    })?
                    .collect::<Result<_, _>>()?;
                let mut folder = None;
                match to {
                    Some(_) if tops.is_empty() => {}
                    Some((to, wanted)) => {
                        for (top, place, top_name) in &tops {
                            let their_top = match top_in(tx, to, place)? {
                                Some(top) => top,
                                None => {
                                    let id = ItemId::generate();
                                    tx.execute(
                                        concat!(
                                            "INSERT INTO storage_items (id, owner_id, parent_id,
                                                 name, kind, created_at, modified_at, place)
                                             VALUES (?1, ?2, NULL, ?3, 'folder', ",
                                            sql_now!(),
                                            ", ",
                                            sql_now!(),
                                            ", ?4)"
                                        ),
                                        (id.as_uuid(), to.as_uuid(), top_name, place),
                                    )?;
                                    changes::note(tx, id, [(to, true)])?;
                                    id
                                }
                            };
                            let id = ItemId::generate();
                            let name = free_name(tx, their_top, &wanted)?;
                            insert_item(tx, id, to, their_top, &name, "folder")?;
                            tx.execute(
                                "UPDATE storage_items SET parent_id = ?2 WHERE parent_id = ?1",
                                (top.as_uuid(), id.as_uuid()),
                            )?;
                            tx.execute("DELETE FROM storage_items WHERE id = ?1", [top.as_uuid()])?;
                            changes::note(tx, id, [(to, true)])?;
                            if place == MY_DRIVE {
                                folder = Some(id);
                            }
                        }
                        tx.execute(
                            "UPDATE storage_items SET owner_id = ?2 WHERE owner_id = ?1",
                            (owner.as_uuid(), to.as_uuid()),
                        )?;
                        events::change_owner(tx, owner, to)?;
                        // What they shared with the new owner is the new
                        // owner's own now.
                        tx.execute(
                            "DELETE FROM storage_shares WHERE user_id = ?1 AND item_id IN
                                 (SELECT id FROM storage_items WHERE owner_id = ?1)",
                            [to.as_uuid()],
                        )?;
                    }
                    None if tops.is_empty() => {}
                    None => {
                        changes::record_owner_deleting(tx, owner)?;
                        for (top, ..) in &tops {
                            delete_subtree(tx, *top)?;
                        }
                    }
                }
                let folder = folder.map(|id| item_or_missing(tx, id)).transpose()?;
                tx.execute(
                    "DELETE FROM storage_quotas WHERE owner_id = ?1",
                    [owner.as_uuid()],
                )?;
                tx.execute(
                    "DELETE FROM storage_shares WHERE user_id = ?1",
                    [owner.as_uuid()],
                )?;
                Ok::<_, Error>(folder)
            })
            .await?;
        // A whole top folder moved or went, so rebuild rather than patch.
        // The database is already right, so a failure only leaves the view
        // behind until the next rebuild.
        if let Err(err) = self.rebuild_view_locked().await {
            tracing::error!(%err, "couldn't rebuild the plain view");
        }
        Ok(folder)
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

    /// The full path of stored contents, for a worker that has to be given
    /// a path rather than an open file. Nothing may write to it.
    pub fn contents_file(&self, hash: ContentHash) -> PathBuf {
        self.inner.blobs.path(hash)
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
        let tops: Vec<(UserId, ItemId, String)> = self
            .inner
            .db
            .read(|conn| {
                let mut statement = conn.prepare(
                    "SELECT owner_id, id, place, name FROM storage_items WHERE parent_id IS NULL",
                )?;
                let tops = statement
                    .query_map([], |row| {
                        let place: Option<String> = row.get(2)?;
                        let name: String = row.get(3)?;
                        Ok((
                            UserId::from_uuid(row.get(0)?),
                            ItemId::from_uuid(row.get(1)?),
                            place_folder_name(place.as_deref(), &name),
                        ))
                    })?
                    .collect::<Result<_, _>>()?;
                Ok::<_, Error>(tops)
            })
            .await?;
        let mut users = Vec::new();
        for (owner, top, folder) in tops {
            let username = self.username(owner).await?;
            users.push((
                PathBuf::from(username).join(folder),
                self.entries(top).await?,
            ));
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
        let folder = place_folder_name(location.place.as_deref(), &location.top_name);
        Ok(Some(self.inner.view.path(
            &username,
            &folder,
            &location.names,
        )))
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

/// `wanted`, or `wanted (2)`, `wanted (3)`, and so on, whichever is free in
/// `parent`, shortened if it's too long to fit [`MAX_NAME_LEN`].
fn free_name(conn: &Connection, parent: ItemId, wanted: &str) -> Result<String, Error> {
    for number in 1u32.. {
        let suffix = match number {
            1 => String::new(),
            _ => format!(" ({number})"),
        };
        let mut end = wanted.len().min(MAX_NAME_LEN - suffix.len());
        while !wanted.is_char_boundary(end) {
            end -= 1;
        }
        let name = check_name(&format!("{}{suffix}", &wanted[..end]))?;
        if child_named(conn, parent, &name)?.is_none() {
            return Ok(name);
        }
    }
    unreachable!("some number is free")
}

/// Selects items as `i`, from `$from`, with their current versions, for
/// [`item_from_row`]. `$extra` adds columns after the item's.
macro_rules! item_select {
    ($with:literal, $extra:literal, $from:literal) => {
        concat!(
            $with,
            "
    SELECT i.id, i.owner_id, i.parent_id, i.name, i.kind, i.created_at, i.modified_at,
           i.trashed_at, v.id, v.blob, b.size, i.place",
            $extra,
            "
    FROM ",
            $from,
            "
    LEFT JOIN storage_versions v ON v.item_id = i.id
        AND v.number = (SELECT max(number) FROM storage_versions WHERE item_id = i.id)
    LEFT JOIN storage_blobs b ON b.hash = v.blob"
        )
    };
}

const ITEM_SELECT: &str = item_select!("", "", "storage_items i");

/// Selects the items outside the trash under `?1`, for [`found_from_row`]:
/// someone's top folder, or a folder or file shared with them. `live` walks
/// down from it, skipping deleted items, so it leaves out what's inside a
/// deleted folder too. It carries each item's path, its folder names joined
/// by "/", which names can't contain, and the path of the folder it's in:
/// null for a top folder, and empty for what's in it. A shared item is the
/// first folder in its own path, so what's inside it says where it is from
/// there. Each also carries the place of the top folder it was reached
/// from, or null when that's a shared item.
const FOUND_SELECT: &str = item_select!(
    "
    WITH RECURSIVE live (id, path, inside, top) AS (
        SELECT id, CASE WHEN parent_id IS NULL THEN '' ELSE name END,
               CASE WHEN parent_id IS NULL THEN NULL ELSE '' END, place
        FROM storage_items WHERE id = ?1
        UNION ALL
        SELECT i.id, CASE live.path WHEN '' THEN i.name ELSE live.path || '/' || i.name END,
               live.path, live.top
        FROM storage_items i JOIN live ON i.parent_id = live.id
        WHERE i.trashed_at IS NULL
    )",
    ", l.inside, l.top",
    "live l JOIN storage_items i ON i.id = l.id"
);

/// Shares joined to their items, for [`item_from_row`], with the share's
/// columns from index 12 (see `shares::share_from_row`).
const SHARED_SELECT: &str = item_select!(
    "",
    ", s.item_id, s.user_id, s.level, s.shared_by, s.created_at",
    "storage_shares s JOIN storage_items i ON i.id = s.item_id"
);

/// The first `limit` of `found`, newest first. An item reached from more
/// than one root, such as a folder shared on its own and inside a shared
/// folder, appears once, with the longest path.
fn newest_first(mut found: Vec<Found>, limit: usize) -> Vec<Found> {
    let mut longest: HashMap<ItemId, usize> = HashMap::new();
    for found in &found {
        let len = longest.entry(found.item.id).or_default();
        *len = (*len).max(found.folders.len() + 1);
    }
    found.retain(|found| {
        let len = found.folders.len() + 1;
        match longest.get_mut(&found.item.id) {
            Some(longest) if *longest == len => {
                // Taken, so a second copy at the same length is dropped.
                *longest = 0;
                true
            }
            _ => false,
        }
    });
    found.sort_by(|a, b| {
        b.item
            .modified_at
            .cmp(&a.item.modified_at)
            .then_with(|| a.item.name.cmp(&b.item.name))
    });
    found.truncate(limit);
    found
}

/// How `found` matches `search`, if it does, apart from what SQL checked:
/// its time and kind.
fn hit(
    search: &Search,
    user: UserId,
    among: Option<&HashSet<ItemId>>,
    involving: Option<&HashSet<ItemId>>,
    found: &Found,
) -> Option<Hit> {
    let item = &found.item;
    if among.is_some_and(|among| !among.contains(&item.id)) {
        return None;
    }
    match &search.owner {
        Some(Owner::Me) if item.owner != user => return None,
        Some(Owner::Others) if item.owner == user => return None,
        Some(Owner::Person(person)) if item.owner != *person => return None,
        Some(Owner::SharedByMe) if item.owner != user => return None,
        Some(Owner::Involving(person)) => {
            let shared = item.owner == user && involving.is_some_and(|ids| ids.contains(&item.id));
            if item.owner != *person && !shared {
                return None;
            }
        }
        _ => {}
    }
    // The trash isn't narrowed by SQL.
    if search.trash {
        let kind_matches = match search.only {
            Some(Only::Folders) => item.kind == ItemKind::Folder,
            Some(_) => item.kind != ItemKind::Folder,
            None => true,
        };
        let since = search
            .modified_since
            .as_ref()
            .is_none_or(|since| item.modified_at >= *since);
        let before = search
            .modified_before
            .as_ref()
            .is_none_or(|before| item.modified_at < *before);
        if !(kind_matches && since && before) {
            return None;
        }
    }
    if let Some(Only::Extensions(extensions)) = &search.only {
        let name = item.name.to_lowercase();
        let has = name.rsplit_once('.').is_some_and(|(stem, extension)| {
            !stem.is_empty() && extensions.iter().any(|e| e == extension)
        });
        if !has {
            return None;
        }
    }
    if !search.name.in_name(&item.name) {
        return None;
    }
    let hash = match &item.kind {
        ItemKind::File { blob, .. } => Some(blob.hash),
        ItemKind::Folder => None,
    };
    let rank = |matched: &Matched| hash.and_then(|hash| matched.rank(hash));
    let text_rank = search.text.as_ref().map(rank);
    if text_rank == Some(None) {
        return None;
    }
    let mut by_name = false;
    let mut either_rank = None;
    if let Some((words, matched)) = &search.name_or_text {
        by_name = words.in_name(&item.name);
        either_rank = rank(matched);
        if !by_name && either_rank.is_none() {
            return None;
        }
    }
    Some(Hit {
        found: found.clone(),
        by_name,
        text_rank: either_rank.or(text_rank.flatten()),
    })
}

/// The first `limit` of `hits`: names that matched first, newest first,
/// then text that matched, best first, or all newest first unless
/// `by_rank`. An item reached from more than one root appears once, with
/// the longest path, as in [`newest_first`].
fn best_first(mut hits: Vec<Hit>, limit: usize, by_rank: bool) -> Vec<Hit> {
    let mut longest: HashMap<ItemId, usize> = HashMap::new();
    for hit in &hits {
        let len = longest.entry(hit.found.item.id).or_default();
        *len = (*len).max(hit.found.folders.len() + 1);
    }
    hits.retain(|hit| {
        let len = hit.found.folders.len() + 1;
        match longest.get_mut(&hit.found.item.id) {
            Some(longest) if *longest == len => {
                *longest = 0;
                true
            }
            _ => false,
        }
    });
    let group = |hit: &Hit| u8::from(by_rank && !hit.by_name);
    hits.sort_by(|a, b| {
        let ranked = by_rank && !a.by_name && !b.by_name;
        group(a)
            .cmp(&group(b))
            .then_with(|| {
                if ranked {
                    let rank = |hit: &Hit| hit.text_rank.unwrap_or(f64::MAX);
                    rank(a).total_cmp(&rank(b))
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .then_with(|| b.found.item.modified_at.cmp(&a.found.item.modified_at))
            .then_with(|| a.found.item.name.cmp(&b.found.item.name))
    });
    hits.truncate(limit);
    hits
}

/// The items inside `folder`, however deep, outside the trash.
fn inside(conn: &Connection, folder: ItemId) -> Result<HashSet<ItemId>, Error> {
    let ids = conn
        .prepare(
            "WITH RECURSIVE inside (id) AS (
                 SELECT id FROM storage_items WHERE parent_id = ?1 AND trashed_at IS NULL
                 UNION ALL
                 SELECT i.id FROM storage_items i JOIN inside ON i.parent_id = inside.id
                 WHERE i.trashed_at IS NULL
             )
             SELECT id FROM inside",
        )?
        .query_map([folder.as_uuid()], |row| Ok(ItemId::from_uuid(row.get(0)?)))?
        .collect::<Result<_, _>>()?;
    Ok(ids)
}

fn found_from_row(row: &Row<'_>) -> rusqlite::Result<Found> {
    let inside: Option<String> = row.get(12)?;
    Ok(Found {
        item: item_from_row(row)?,
        folders: match inside.as_deref() {
            None | Some("") => Vec::new(),
            Some(path) => path.split('/').map(str::to_owned).collect(),
        },
        place: row.get(13)?,
    })
}

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
        place: row.get(11)?,
    })
}

/// A file's size, or 0 for a folder.
fn size_of(item: &Item) -> u64 {
    match &item.kind {
        ItemKind::File { blob, .. } => blob.size,
        ItemKind::Folder => 0,
    }
}

/// `owner`'s top folders: My Drive and each app's place they have.
fn tops(conn: &Connection, owner: UserId) -> Result<Vec<ItemId>, Error> {
    Ok(conn
        .prepare(
            "SELECT id FROM storage_items WHERE owner_id = ?1 AND parent_id IS NULL
             ORDER BY place <> 'drive', created_at, place",
        )?
        .query_map([owner.as_uuid()], |row| Ok(ItemId::from_uuid(row.get(0)?)))?
        .collect::<Result<_, _>>()?)
}

/// `owner`'s top folder in `place`, if they have one.
fn top_in(conn: &Connection, owner: UserId, place: &str) -> Result<Option<ItemId>, Error> {
    Ok(conn
        .query_row(
            "SELECT id FROM storage_items
             WHERE owner_id = ?1 AND parent_id IS NULL AND place = ?2",
            (owner.as_uuid(), place),
            |row| Ok(ItemId::from_uuid(row.get(0)?)),
        )
        .optional()?)
}

/// The folder a top folder appears as in the plain view, and on computers.
fn place_folder_name(place: Option<&str>, name: &str) -> String {
    match place {
        Some(MY_DRIVE) | None => "My Drive".to_owned(),
        Some(_) => name.to_owned(),
    }
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

/// An item's folder, or `Some(None)` for a top folder, or `None` if the item
/// doesn't exist.
pub(crate) fn item_parent(
    conn: &Connection,
    id: ItemId,
) -> rusqlite::Result<Option<Option<ItemId>>> {
    conn.query_row(
        "SELECT parent_id FROM storage_items WHERE id = ?1",
        [id.as_uuid()],
        |row| Ok(row.get::<_, Option<uuid::Uuid>>(0)?.map(ItemId::from_uuid)),
    )
    .optional()
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

/// What a save may write over.
#[derive(Clone, Copy)]
enum Over {
    /// The file with the same name, if there is one, as its new version.
    Any,
    /// Nothing: the name must be free.
    Nothing,
    /// This file, and only while its contents are still these. Contents
    /// rather than a version ID, because a save over an empty version
    /// keeps its ID. With an editing session's ID, a recent version from
    /// the same session is replaced.
    File(ItemId, ContentHash, Option<Uuid>),
}

/// Checks that `name` in `parent` can take a new file or version of
/// `size` bytes. Returns the folder, and the file already there if any.
fn check_save(
    conn: &Connection,
    parent: ItemId,
    name: &str,
    size: u64,
    upload: Option<UploadId>,
    over: Over,
) -> Result<(Item, Option<ItemId>), Error> {
    let parent = writable_folder(conn, parent)?;
    let existing = match (child_named(conn, parent.id, name)?, over) {
        (Some(existing), Over::Nothing) => return Err(existing.name_taken()),
        (Some(existing), _) if existing.kind == ItemKind::Folder => {
            return Err(existing.name_taken());
        }
        // Renamed, moved, or deleted since, or holding other contents.
        (
            Some(Item {
                id,
                kind: ItemKind::File { blob, .. },
                ..
            }),
            Over::File(file, base, _),
        ) if id == file && blob.hash == base => Some(id),
        (_, Over::File(..)) => return Err(Error::Changed),
        (existing, _) => existing.map(|existing| existing.id),
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

/// Deletes an item, everything inside it, all their versions, and their
/// activity.
fn delete_subtree(conn: &Connection, id: ItemId) -> Result<(), Error> {
    events::forget_items(conn, id)?;
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

/// Deletes an item in the trash for good, and records it on the folder it
/// was in, since its own activity goes with it.
fn delete_for_good(conn: &Connection, item: &Item, actor: Actor) -> Result<(), Error> {
    changes::record_deleting(conn, item.id)?;
    delete_subtree(conn, item.id)?;
    if let Some(parent) = item.parent {
        let mut details = serde_json::json!({ "name": item.name });
        if item.kind == ItemKind::Folder {
            details["folder"] = true.into();
        }
        record(
            conn,
            actor,
            NewEvent::about(Action::Deleted, parent, item.owner).details(details),
        )?;
    }
    Ok(())
}

fn record(conn: &Connection, actor: Actor, event: NewEvent) -> Result<(), Error> {
    changes::record_event(conn, &event)?;
    Ok(events::record(conn, actor, &event)?)
}

/// An event for an item that was just added, `how` being "created",
/// "uploaded", or "copied".
fn added(id: ItemId, owner: UserId, how: &str) -> NewEvent {
    NewEvent::about(Action::Added, id, owner).details(serde_json::json!({ "how": how }))
}

/// Adds `blob` as the newest version of `item`. An empty current version
/// is replaced instead of kept, because there's nothing in it to go back to.
/// File Explorer on Windows, for example, saves every new file empty first,
/// then saves its contents over it.
fn add_version(
    conn: &Connection,
    item: ItemId,
    blob: Blob,
    edit: Option<Uuid>,
) -> Result<(), Error> {
    let mut replaced = 0;
    if let Some(edit) = edit {
        replaced = conn.execute(
            concat!(
                "UPDATE storage_versions SET blob = ?2, created_at = ",
                sql_now!(),
                " WHERE id = (SELECT id FROM storage_versions WHERE item_id = ?1
                              ORDER BY number DESC LIMIT 1)
                    AND edit_id = ?3
                    AND edit_started_at > strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?4)"
            ),
            (
                item.as_uuid(),
                &blob.hash.0[..],
                edit,
                format!("-{EDIT_VERSION_MINUTES} minutes"),
            ),
        )?;
    }
    if replaced == 0 {
        replaced = conn.execute(
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
    }
    if replaced == 0 {
        insert_version(conn, item, blob)?;
    }
    if let Some(edit) = edit {
        // A version this session started, or one it took over from an empty
        // file, starts the 30 minutes now. A replaced one keeps its start.
        conn.execute(
            concat!(
                "UPDATE storage_versions SET edit_started_at = ",
                sql_now!(),
                ", edit_id = ?2
                 WHERE id = (SELECT id FROM storage_versions WHERE item_id = ?1
                             ORDER BY number DESC LIMIT 1)
                   AND (edit_id IS NOT ?2 OR edit_started_at IS NULL)"
            ),
            (item.as_uuid(), edit),
        )?;
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
/// counting the top folder) and its own, whether it's out of the trash, and
/// its top folder's place and name.
struct Location {
    owner: UserId,
    names: Vec<String>,
    visible: bool,
    place: Option<String>,
    top_name: String,
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
         SELECT up.owner_id, up.name, up.parent_id IS NULL, up.trashed_at IS NOT NULL, i.place
         FROM up JOIN storage_items i ON i.id = up.id ORDER BY depth DESC",
    )?;
    let rows = statement
        .query_map([id.as_uuid()], |row| {
            Ok((
                UserId::from_uuid(row.get(0)?),
                row.get::<_, String>(1)?,
                row.get::<_, bool>(2)?,
                row.get::<_, bool>(3)?,
                row.get::<_, Option<String>>(4)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let (owner, ref top_name, _, _, ref place) = *rows.first().ok_or(Error::NotFound)?;
    Ok(Location {
        owner,
        names: rows
            .iter()
            .filter(|(_, _, top, ..)| !top)
            .map(|(_, name, ..)| name.clone())
            .collect(),
        visible: !rows.iter().any(|(_, _, _, trashed, _)| *trashed),
        place: place.clone(),
        top_name: top_name.clone(),
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

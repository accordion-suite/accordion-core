//! The event log: what changed, who changed it, and when, for the
//! **Activity** tab, notifications, each person's activity log, and the
//! admin log (decision 18 in the `accordion` repo's `docs/decisions`).
//!
//! Core writes its own events in the same transaction as the change they
//! record, so a saved change always has its event. Storage records changes
//! to files and sharing, and identity records sign-ins and account changes,
//! each as the [`Actor`] its handle was given with `acting_as`. Modules and
//! the server record their own events with [`Events::record`], right after
//! the change.
//!
//! The log records changes, not views: nothing here says who opened,
//! downloaded, or searched for what. Folders are stored by ID and named
//! when an event is read, so a folder the reader can't open is never named.

use std::net::IpAddr;
use std::time::Duration;

use accordion_types::{ItemId, UserId};
use rusqlite::{Connection, OptionalExtension, Row};
use serde_json::Value;

use crate::db::{self, Database, Migration};
use crate::sql_now;
use crate::storage::{self, Access};

const OWNER: &str = "events";

static MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "Create the event log",
        sql: "
    -- One row for each change. Nothing here references other tables, so
    -- events outlive the items and people they're about.
    CREATE TABLE events_log (
        id INTEGER PRIMARY KEY,
        at TEXT NOT NULL,
        -- Who did it. NULL for the server itself, such as emptying old
        -- trash, or for someone who isn't signed in.
        actor_id BLOB,
        via TEXT NOT NULL CHECK (via IN ('web', 'webdav', 'app', 'server')),
        action TEXT NOT NULL,
        item_id BLOB,
        -- The item's owner when it happened.
        owner_id BLOB,
        -- Someone else it's about, such as who an item was shared with, or
        -- the account an admin changed.
        other_id BLOB,
        -- A little JSON, such as a rename's old and new names.
        details TEXT
    ) STRICT;
    CREATE INDEX events_log_item ON events_log (item_id) WHERE item_id IS NOT NULL;
    CREATE INDEX events_log_actor ON events_log (actor_id) WHERE actor_id IS NOT NULL;
    CREATE INDEX events_log_owner ON events_log (owner_id) WHERE owner_id IS NOT NULL;
    CREATE INDEX events_log_other ON events_log (other_id) WHERE other_id IS NOT NULL;
    -- The admin log reads only these, so it doesn't scan everything else.
    CREATE INDEX events_log_admin ON events_log (id) WHERE action IN (
        'account_created', 'account_changed', 'account_deleted', 'module_changed',
        'backup_changed', 'backup_restored', 'update_installed', 'retention_changed',
        'sign_in_failed');

    -- Notifications: events someone should hear about, and whether they've
    -- seen them.
    CREATE TABLE events_notices (
        user_id BLOB NOT NULL,
        event_id INTEGER NOT NULL REFERENCES events_log (id) ON DELETE CASCADE,
        read_at TEXT,
        PRIMARY KEY (user_id, event_id)
    ) STRICT, WITHOUT ROWID;
    CREATE INDEX events_notices_event ON events_notices (event_id);

    CREATE TABLE events_settings (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        -- NULL keeps events forever.
        keep_days INTEGER CHECK (keep_days > 0)
    ) STRICT;
    INSERT INTO events_settings (id, keep_days) VALUES (1, 365);
",
    },
    Migration {
        version: 2,
        name: "Separate seeing notifications from reading them",
        sql: "
        -- Set when the person opens the list that shows the notification,
        -- which clears the count. read_at waits until they select it.
        ALTER TABLE events_notices ADD COLUMN seen_at TEXT;
        UPDATE events_notices SET seen_at = read_at;
    ",
    },
];

/// The actions the admin log shows. It matches the `events_log_admin`
/// index, so the admin log's query can use it.
const ADMIN_ACTIONS: &str = "'account_created', 'account_changed', 'account_deleted', \
    'module_changed', 'backup_changed', 'backup_restored', 'update_installed', \
    'retention_changed', 'sign_in_failed'";

/// Sign-ins and account changes, which make up someone's own activity log.
const ACCOUNT_ACTIONS: &str = "'signed_in', 'sign_in_failed', 'signed_out', 'session_ended', \
    'password_changed', 'app_password_created', 'app_password_removed'";

const SHARE_ACTIONS: &str = "'shared', 'share_changed', 'unshared'";

/// Edits to one file by one person this close together become one event,
/// and a restore this soon after the same person trashed it undoes it.
const MERGE_WITHIN: &str = "-10 minutes";

/// How long a notification stays after it's read.
const KEEP_READ_NOTICES: &str = "-90 days";

/// Rows each write deletes, so pruning never holds up other writes for long.
const BATCH: i64 = 1_000;

/// How often [`Events::maintain`] deletes expired events.
const PRUNE_EVERY: Duration = Duration::from_secs(24 * 60 * 60);

/// The choices for how long events are kept, in days. `None` keeps them
/// forever.
pub const KEEP_CHOICES: [Option<u32>; 4] = [Some(90), Some(365), Some(730), None];

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Choose 90 days, one year, two years, or forever.")]
    InvalidRetention,

    #[error("That file or folder doesn't exist. It may have been deleted.")]
    NotFound,

    #[error(transparent)]
    Database(#[from] db::Error),

    #[error(transparent)]
    Storage(#[from] Box<storage::Error>),
}

impl From<storage::Error> for Error {
    fn from(err: storage::Error) -> Self {
        Self::Storage(Box::new(err))
    }
}

impl From<rusqlite::Error> for Error {
    fn from(err: rusqlite::Error) -> Self {
        Self::Database(err.into())
    }
}

/// How a change was made.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Via {
    /// The web app.
    Web,
    /// A WebDAV client, such as File Explorer.
    WebDav,
    /// The desktop or mobile app.
    App,
    /// The server itself, such as upkeep.
    Server,
}

impl Via {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Web => "web",
            Self::WebDav => "webdav",
            Self::App => "app",
            Self::Server => "server",
        }
    }

    fn from_column(row: &Row<'_>, index: usize) -> rusqlite::Result<Self> {
        match row.get_ref(index)?.as_str()? {
            "web" => Ok(Self::Web),
            "webdav" => Ok(Self::WebDav),
            "app" => Ok(Self::App),
            "server" => Ok(Self::Server),
            other => Err(unknown(index, "way of making a change", other)),
        }
    }
}

/// Who's making a change, and how. Storage and identity record their events
/// as the actor their handle was given with `acting_as`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Actor {
    /// `None` for the server itself, or someone who isn't signed in yet.
    pub user: Option<UserId>,
    pub via: Via,
    /// Where the request came from. Only sign-in events keep it.
    pub address: Option<IpAddr>,
}

impl Actor {
    /// The server, such as upkeep emptying old trash.
    pub const SERVER: Self = Self {
        user: None,
        via: Via::Server,
        address: None,
    };

    /// Someone signed in.
    pub fn person(user: UserId, via: Via) -> Self {
        Self {
            user: Some(user),
            via,
            address: None,
        }
    }

    /// Someone who isn't signed in yet, such as on the sign-in page.
    pub fn visitor(via: Via, address: IpAddr) -> Self {
        Self {
            user: None,
            via,
            address: Some(address),
        }
    }

    /// The same actor, with the address the request came from.
    pub fn from(self, address: IpAddr) -> Self {
        Self {
            address: Some(address),
            ..self
        }
    }
}

/// What happened.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Action {
    /// A file uploaded, a folder created, or a file copied. The details say
    /// which, as `how`.
    Added,
    /// A new version of a file.
    Edited,
    /// Details: `from` and `to`, the old and new names.
    Renamed,
    /// Details: `from` and `to`, the old and new folders' IDs.
    Moved,
    Trashed,
    Restored,
    /// Deleted for good. The event is on the folder it was in, since the
    /// item is gone. Details: its `name`, and `folder` if it was one.
    Deleted,
    /// Details: `number`, the version that became current again.
    VersionRestored,
    DescriptionChanged,
    /// `other` is who it was shared with. Details: `level`.
    Shared,
    /// Details: `level`, the new one.
    ShareChanged,
    Unshared,
    /// Details: `device`, and `address` when it's known.
    SignedIn,
    /// `other` is the account. Details: `address`.
    SignInFailed,
    SignedOut,
    /// A signed-in device was signed out from elsewhere. Details: `device`.
    SessionEnded,
    /// `other` is whose password, when an admin set it.
    PasswordChanged,
    /// Details: `device`.
    AppPasswordCreated,
    /// Details: `device`.
    AppPasswordRemoved,
    /// `other` is the new account. Details: `role`.
    AccountCreated,
    /// `other` is the account. Details: `change`, one of `role`, `name`,
    /// `turned_off`, `turned_on`, `password_link`, or `quota`, and the new
    /// value where there is one.
    AccountChanged,
    /// Details: the account's `name`, which is otherwise gone.
    AccountDeleted,
    /// Details: `module` and `on`.
    ModuleChanged,
    BackupChanged,
    /// Details: `snapshot`.
    BackupRestored,
    /// Details: `version`.
    UpdateInstalled,
    /// Details: `keep_days`, or null for forever.
    RetentionChanged,
}

impl Action {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Added => "added",
            Self::Edited => "edited",
            Self::Renamed => "renamed",
            Self::Moved => "moved",
            Self::Trashed => "trashed",
            Self::Restored => "restored",
            Self::Deleted => "deleted",
            Self::VersionRestored => "version_restored",
            Self::DescriptionChanged => "description_changed",
            Self::Shared => "shared",
            Self::ShareChanged => "share_changed",
            Self::Unshared => "unshared",
            Self::SignedIn => "signed_in",
            Self::SignInFailed => "sign_in_failed",
            Self::SignedOut => "signed_out",
            Self::SessionEnded => "session_ended",
            Self::PasswordChanged => "password_changed",
            Self::AppPasswordCreated => "app_password_created",
            Self::AppPasswordRemoved => "app_password_removed",
            Self::AccountCreated => "account_created",
            Self::AccountChanged => "account_changed",
            Self::AccountDeleted => "account_deleted",
            Self::ModuleChanged => "module_changed",
            Self::BackupChanged => "backup_changed",
            Self::BackupRestored => "backup_restored",
            Self::UpdateInstalled => "update_installed",
            Self::RetentionChanged => "retention_changed",
        }
    }

    const ALL: [Self; 27] = [
        Self::Added,
        Self::Edited,
        Self::Renamed,
        Self::Moved,
        Self::Trashed,
        Self::Restored,
        Self::Deleted,
        Self::VersionRestored,
        Self::DescriptionChanged,
        Self::Shared,
        Self::ShareChanged,
        Self::Unshared,
        Self::SignedIn,
        Self::SignInFailed,
        Self::SignedOut,
        Self::SessionEnded,
        Self::PasswordChanged,
        Self::AppPasswordCreated,
        Self::AppPasswordRemoved,
        Self::AccountCreated,
        Self::AccountChanged,
        Self::AccountDeleted,
        Self::ModuleChanged,
        Self::BackupChanged,
        Self::BackupRestored,
        Self::UpdateInstalled,
        Self::RetentionChanged,
    ];

    fn from_column(row: &Row<'_>, index: usize) -> rusqlite::Result<Self> {
        let text = row.get_ref(index)?.as_str()?;
        Self::ALL
            .into_iter()
            .find(|action| action.as_str() == text)
            .ok_or_else(|| unknown(index, "action", text))
    }

    /// A change to a file or folder, which its owner hears about when
    /// someone else makes it.
    fn changes_files(self) -> bool {
        matches!(
            self,
            Self::Added
                | Self::Edited
                | Self::Renamed
                | Self::Moved
                | Self::Trashed
                | Self::Restored
                | Self::VersionRestored
                | Self::DescriptionChanged
        )
    }
}

fn unknown(index: usize, what: &str, text: &str) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        index,
        rusqlite::types::Type::Text,
        format!("unknown {what} {text:?}").into(),
    )
}

/// An event to record.
#[derive(Clone, Debug, PartialEq)]
pub struct NewEvent {
    pub action: Action,
    pub item: Option<ItemId>,
    /// The item's owner.
    pub owner: Option<UserId>,
    pub other: Option<UserId>,
    /// `Value::Null` for none.
    pub details: Value,
}

impl NewEvent {
    /// An event with no item and nobody else, such as a sign-out.
    pub fn new(action: Action) -> Self {
        Self {
            action,
            item: None,
            owner: None,
            other: None,
            details: Value::Null,
        }
    }

    /// An event about one of `owner`'s items.
    pub fn about(action: Action, item: ItemId, owner: UserId) -> Self {
        Self {
            item: Some(item),
            owner: Some(owner),
            ..Self::new(action)
        }
    }

    pub fn other(self, other: UserId) -> Self {
        Self {
            other: Some(other),
            ..self
        }
    }

    pub fn details(self, details: Value) -> Self {
        Self { details, ..self }
    }
}

/// A recorded event.
#[derive(Clone, Debug, PartialEq)]
pub struct Event {
    /// Later events have higher numbers.
    pub id: i64,
    pub at: String,
    pub actor: Option<UserId>,
    pub via: Via,
    pub action: Action,
    pub item: Option<ItemId>,
    pub owner: Option<UserId>,
    pub other: Option<UserId>,
    pub details: Value,
}

/// An event someone was notified about.
#[derive(Clone, Debug, PartialEq)]
pub struct Notice {
    pub event: Event,
    /// Whether they've opened the list since it arrived.
    pub seen: bool,
    /// Whether they've selected it.
    pub read: bool,
}

/// What [`Events::prune`] deleted.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Pruned {
    pub events: usize,
    pub notices: usize,
}

/// The event log. Cloning it is cheap.
#[derive(Clone)]
pub struct Events {
    db: Database,
}

impl Events {
    /// Starts the event log, creating its tables first.
    pub async fn open(db: Database) -> Result<Self, Error> {
        migrate(&db).await?;
        Ok(Self { db })
    }

    /// Records an event that a module or the server made happen, right after
    /// the change. Core's own changes record theirs in the same transaction.
    pub async fn record(&self, actor: Actor, event: NewEvent) -> Result<(), Error> {
        self.db
            .write(move |tx| record(tx, actor, &event).map_err(Error::from))
            .await
    }

    /// An item's events, and for a folder everything inside it, newest
    /// first, as `reader` may see them. Pass the lowest `id` from one page
    /// as `before` to get the next.
    ///
    /// Anyone who can open the item sees its events, including those from
    /// before they had access. Only the owner sees what's in the trash,
    /// what was deleted for good, and who it's shared with, apart from
    /// sharing with the reader themselves.
    pub async fn activity(
        &self,
        reader: UserId,
        id: ItemId,
        before: Option<i64>,
        limit: usize,
    ) -> Result<Vec<Event>, Error> {
        self.db
            .read(move |conn| {
                let access = storage::access(conn, reader, id)?.ok_or(Error::NotFound)?;
                let owner = access == Access::Owner;
                let item = storage::item_parent(conn, id)?.ok_or(Error::NotFound)?;
                // Everything in someone's Drive is theirs, so their own
                // events are quicker to find than walking every folder.
                let inside = if item.is_none() {
                    "SELECT item_id FROM events_log WHERE owner_id = ?3"
                } else {
                    "WITH RECURSIVE inside (id) AS (
                         SELECT ?1
                         UNION ALL
                         SELECT i.id FROM storage_items i JOIN inside ON i.parent_id = inside.id
                         WHERE ?2 OR i.trashed_at IS NULL
                     )
                     SELECT id FROM inside"
                };
                let events = conn
                    .prepare(&format!(
                        "SELECT {EVENT_COLUMNS} FROM events_log e
                         WHERE e.item_id IN ({inside})
                           AND (?2 OR (e.action NOT IN ({SHARE_ACTIONS}, 'deleted')
                                       OR e.other_id = ?3))
                           AND (?4 IS NULL OR e.id < ?4)
                         ORDER BY e.id DESC LIMIT ?5"
                    ))?
                    .query_map(
                        (id.as_uuid(), owner, reader.as_uuid(), before, limit as i64),
                        event_from_row,
                    )?
                    .collect::<Result<_, _>>()?;
                Ok(events)
            })
            .await
    }

    /// `user`'s latest notifications, newest first. Those about items they
    /// can no longer open are left out.
    pub async fn notifications(&self, user: UserId, limit: usize) -> Result<Vec<Notice>, Error> {
        self.db
            .read(move |conn| {
                let mut statement = conn.prepare(&format!(
                    "SELECT {EVENT_COLUMNS}, n.seen_at IS NOT NULL, n.read_at IS NOT NULL
                     FROM events_notices n JOIN events_log e ON e.id = n.event_id
                     WHERE n.user_id = ?1 ORDER BY e.id DESC LIMIT ?2"
                ))?;
                let rows = statement.query_map((user.as_uuid(), limit as i64), |row| {
                    Ok(Notice {
                        event: event_from_row(row)?,
                        seen: row.get(EVENT_COLUMN_COUNT)?,
                        read: row.get(EVENT_COLUMN_COUNT + 1)?,
                    })
                })?;
                let mut notices = Vec::new();
                for notice in rows {
                    let notice = notice?;
                    if let Some(item) = notice.event.item
                        && storage::access(conn, user, item)?.is_none()
                    {
                        continue;
                    }
                    notices.push(notice);
                }
                Ok(notices)
            })
            .await
    }

    /// Marks `user`'s notifications up to and including event `up_to` as
    /// seen, such as when they open the list.
    pub async fn mark_seen(&self, user: UserId, up_to: i64) -> Result<(), Error> {
        self.db
            .write(move |tx| {
                tx.execute(
                    concat!(
                        "UPDATE events_notices SET seen_at = ",
                        sql_now!(),
                        " WHERE user_id = ?1 AND event_id <= ?2 AND seen_at IS NULL"
                    ),
                    (user.as_uuid(), up_to),
                )?;
                Ok(())
            })
            .await
    }

    /// Marks some of `user`'s notifications read, such as a group they
    /// selected. Reading one also counts as seeing it.
    pub async fn mark_read(&self, user: UserId, events: Vec<i64>) -> Result<(), Error> {
        self.db
            .write(move |tx| {
                let mut statement = tx.prepare(concat!(
                    "UPDATE events_notices SET read_at = coalesce(read_at, ",
                    sql_now!(),
                    "), seen_at = coalesce(seen_at, ",
                    sql_now!(),
                    ") WHERE user_id = ?1 AND event_id = ?2"
                ))?;
                for event in events {
                    statement.execute((user.as_uuid(), event))?;
                }
                Ok(())
            })
            .await
    }

    /// Removes some of `user`'s notifications, for them only. The events
    /// stay in the log.
    pub async fn remove_notifications(&self, user: UserId, events: Vec<i64>) -> Result<(), Error> {
        self.db
            .write(move |tx| {
                let mut statement =
                    tx.prepare("DELETE FROM events_notices WHERE user_id = ?1 AND event_id = ?2")?;
                for event in events {
                    statement.execute((user.as_uuid(), event))?;
                }
                Ok(())
            })
            .await
    }

    /// Removes all of `user`'s notifications up to and including event
    /// `up_to`, so any that arrive while they're looking stay.
    pub async fn clear_notifications(&self, user: UserId, up_to: i64) -> Result<(), Error> {
        self.db
            .write(move |tx| {
                tx.execute(
                    "DELETE FROM events_notices WHERE user_id = ?1 AND event_id <= ?2",
                    (user.as_uuid(), up_to),
                )?;
                Ok(())
            })
            .await
    }

    /// `user`'s own activity log, newest first: their sign-ins, failed
    /// sign-ins, changes to their password and app passwords, and what they
    /// shared and what others shared with them.
    pub async fn account_log(
        &self,
        user: UserId,
        before: Option<i64>,
        limit: usize,
    ) -> Result<Vec<Event>, Error> {
        self.db
            .read(move |conn| {
                let events = conn
                    .prepare(&format!(
                        "SELECT {EVENT_COLUMNS} FROM events_log e
                         WHERE ((e.actor_id = ?1
                                 AND e.action IN ({ACCOUNT_ACTIONS}, {SHARE_ACTIONS}))
                                OR (e.other_id = ?1
                                    AND e.action IN ({ACCOUNT_ACTIONS}, {SHARE_ACTIONS})))
                           AND (?2 IS NULL OR e.id < ?2)
                         ORDER BY e.id DESC LIMIT ?3"
                    ))?
                    .query_map((user.as_uuid(), before, limit as i64), event_from_row)?
                    .collect::<Result<_, _>>()?;
                Ok(events)
            })
            .await
    }

    /// The admin log, newest first: every admin action, and failed sign-ins
    /// for every account. It shows nobody's file activity.
    pub async fn admin_log(&self, before: Option<i64>, limit: usize) -> Result<Vec<Event>, Error> {
        self.db
            .read(move |conn| {
                let events = conn
                    .prepare(&format!(
                        "SELECT {EVENT_COLUMNS} FROM events_log e
                         WHERE e.action IN ({ADMIN_ACTIONS}) AND (?1 IS NULL OR e.id < ?1)
                         ORDER BY e.id DESC LIMIT ?2"
                    ))?
                    .query_map((before, limit as i64), event_from_row)?
                    .collect::<Result<_, _>>()?;
                Ok(events)
            })
            .await
    }

    /// How many days events are kept, or `None` for forever.
    pub async fn keep_days(&self) -> Result<Option<u32>, Error> {
        self.db
            .read(|conn| {
                Ok(
                    conn.query_row("SELECT keep_days FROM events_settings", [], |row| {
                        row.get(0)
                    })?,
                )
            })
            .await
    }

    /// Sets how long events are kept, to one of [`KEEP_CHOICES`]. Events
    /// older than that go the next time [`Events::prune`] runs.
    pub async fn set_keep_days(&self, actor: Actor, days: Option<u32>) -> Result<(), Error> {
        if !KEEP_CHOICES.contains(&days) {
            return Err(Error::InvalidRetention);
        }
        self.db
            .write(move |tx| {
                let changed = tx.execute(
                    "UPDATE events_settings SET keep_days = ?1 WHERE keep_days IS NOT ?1",
                    [days],
                )?;
                if changed > 0 {
                    record(
                        tx,
                        actor,
                        &NewEvent::new(Action::RetentionChanged)
                            .details(serde_json::json!({ "keep_days": days })),
                    )?;
                }
                Ok(())
            })
            .await
    }

    /// Deletes events older than the setting allows, and notifications
    /// read more than 90 days ago, a batch at a time.
    pub async fn prune(&self) -> Result<Pruned, Error> {
        let mut pruned = Pruned::default();
        if let Some(days) = self.keep_days().await? {
            let cutoff = format!("-{days} days");
            loop {
                let cutoff = cutoff.clone();
                // Events are in time order by ID, apart from merged edits,
                // so the oldest come first.
                let deleted = self
                    .db
                    .write(move |tx| {
                        tx.execute(
                            "DELETE FROM events_log WHERE id IN (
                                 SELECT id FROM events_log
                                 WHERE at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?1)
                                 ORDER BY id LIMIT ?2)",
                            (&cutoff, BATCH),
                        )
                    })
                    .await
                    .map_err(Error::from)?;
                pruned.events += deleted;
                if (deleted as i64) < BATCH {
                    break;
                }
            }
        }
        loop {
            let deleted = self.prune_read_notices().await?;
            pruned.notices += deleted;
            if (deleted as i64) < BATCH {
                break;
            }
        }
        Ok(pruned)
    }

    async fn prune_read_notices(&self) -> Result<usize, Error> {
        self.db
            .write(|tx| {
                Ok(tx.execute(
                    &format!(
                        "DELETE FROM events_notices WHERE (user_id, event_id) IN (
                             SELECT user_id, event_id FROM events_notices
                             WHERE read_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '{KEEP_READ_NOTICES}')
                             LIMIT ?1)"
                    ),
                    [BATCH],
                )?)
            })
            .await
    }

    /// Deletes expired events once a day until `stopped` completes.
    pub async fn maintain(self, stopped: impl Future<Output = ()>) {
        let mut stopped = std::pin::pin!(stopped);
        loop {
            match self.prune().await {
                Ok(pruned) => tracing::debug!(?pruned, "pruned the event log"),
                Err(err) => tracing::warn!(%err, "couldn't prune the event log"),
            }
            if tokio::time::timeout(PRUNE_EVERY, &mut stopped)
                .await
                .is_ok()
            {
                return;
            }
        }
    }
}

/// Creates or updates the event log's tables. Identity and storage call it
/// too, since they write events and may start first.
pub(crate) async fn migrate(db: &Database) -> Result<(), db::Error> {
    db.migrate(OWNER, MIGRATIONS).await
}

const EVENT_COLUMNS: &str = "e.id, e.at, e.actor_id, e.via, e.action, e.item_id, e.owner_id, \
    e.other_id, e.details";
const EVENT_COLUMN_COUNT: usize = 9;

fn event_from_row(row: &Row<'_>) -> rusqlite::Result<Event> {
    let user = |index| -> rusqlite::Result<Option<UserId>> {
        Ok(row
            .get::<_, Option<uuid::Uuid>>(index)?
            .map(UserId::from_uuid))
    };
    let details: Option<String> = row.get(8)?;
    Ok(Event {
        id: row.get(0)?,
        at: row.get(1)?,
        actor: user(2)?,
        via: Via::from_column(row, 3)?,
        action: Action::from_column(row, 4)?,
        item: row.get::<_, Option<uuid::Uuid>>(5)?.map(ItemId::from_uuid),
        owner: user(6)?,
        other: user(7)?,
        details: details
            .and_then(|details| serde_json::from_str(&details).ok())
            .unwrap_or(Value::Null),
    })
}

/// Records an event inside the transaction making the change, and notifies
/// whoever should hear about it.
///
/// Someone hears when an item is shared with them or their access changes,
/// but not when it's taken away. An owner hears when someone else changes
/// their files.
pub(crate) fn record(conn: &Connection, actor: Actor, event: &NewEvent) -> rusqlite::Result<()> {
    if let (Some(user), Some(item)) = (actor.user, event.item) {
        if event.action == Action::Edited && merge_edit(conn, user, item)? {
            return Ok(());
        }
        if event.action == Action::Restored && undo_trash(conn, user, item)? {
            return Ok(());
        }
    }
    let details = match &event.details {
        Value::Null => None,
        details => Some(details.to_string()),
    };
    conn.execute(
        concat!(
            "INSERT INTO events_log
             (at, actor_id, via, action, item_id, owner_id, other_id, details)
             VALUES (",
            sql_now!(),
            ", ?1, ?2, ?3, ?4, ?5, ?6, ?7)"
        ),
        (
            actor.user.map(|user| user.as_uuid()),
            actor.via.as_str(),
            event.action.as_str(),
            event.item.map(|item| item.as_uuid()),
            event.owner.map(|owner| owner.as_uuid()),
            event.other.map(|other| other.as_uuid()),
            details,
        ),
    )?;
    let id = conn.last_insert_rowid();
    let notify = match event.action {
        Action::Shared | Action::ShareChanged => {
            event.other.filter(|&other| Some(other) != actor.user)
        }
        action if action.changes_files() => match (actor.user, event.owner) {
            (Some(user), Some(owner)) if user != owner => Some(owner),
            _ => None,
        },
        _ => None,
    };
    if let Some(user) = notify {
        conn.execute(
            "INSERT INTO events_notices (user_id, event_id) VALUES (?1, ?2)",
            (user.as_uuid(), id),
        )?;
    }
    Ok(())
}

/// Folds an edit into the same person's last addition or edit of the file,
/// if it was recent, by moving that event's time up. A desktop app saving
/// every few seconds, or File Explorer saving a new file empty and then
/// again with its contents, makes one event.
fn merge_edit(conn: &Connection, user: UserId, item: ItemId) -> rusqlite::Result<bool> {
    let recent: Option<i64> = conn
        .query_row(
            &format!(
                "SELECT id FROM events_log
                 WHERE item_id = ?1 AND actor_id = ?2 AND action IN ('added', 'edited')
                   AND at >= strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '{MERGE_WITHIN}')
                   AND id = (SELECT max(id) FROM events_log WHERE item_id = ?1)"
            ),
            (item.as_uuid(), user.as_uuid()),
            |row| row.get(0),
        )
        .optional()?;
    let Some(id) = recent else {
        return Ok(false);
    };
    conn.execute(
        concat!("UPDATE events_log SET at = ", sql_now!(), " WHERE id = ?1"),
        [id],
    )?;
    Ok(true)
}

/// Takes back the same person's recent move of the item to the trash, so
/// putting something back right away, such as with **Undo**, leaves no
/// trace. File Explorer deletes a folder one item at a time and Drive puts
/// them back together, which this keeps out of the log too.
fn undo_trash(conn: &Connection, user: UserId, item: ItemId) -> rusqlite::Result<bool> {
    let undone = conn.execute(
        &format!(
            "DELETE FROM events_log
             WHERE id = (SELECT max(id) FROM events_log WHERE item_id = ?1)
               AND actor_id = ?2 AND action = 'trashed'
               AND at >= strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '{MERGE_WITHIN}')"
        ),
        (item.as_uuid(), user.as_uuid()),
    )?;
    Ok(undone > 0)
}

/// Deletes the activity of an item and everything inside it, which are
/// being deleted for good. Sharing events stay, since they're part of
/// people's activity logs, and lose the item's name along with the item.
pub(crate) fn forget_items(conn: &Connection, id: ItemId) -> rusqlite::Result<()> {
    conn.execute(
        &format!(
            "WITH RECURSIVE down (id) AS (
                 SELECT ?1
                 UNION ALL
                 SELECT i.id FROM storage_items i JOIN down ON i.parent_id = down.id
             )
             DELETE FROM events_log
             WHERE item_id IN (SELECT id FROM down) AND action NOT IN ({SHARE_ACTIONS})"
        ),
        [id.as_uuid()],
    )?;
    Ok(())
}

/// Deletes someone's own activity log and notifications, for deleting their
/// account. Their events in other people's history stay.
pub(crate) fn forget_person(conn: &Connection, user: UserId) -> rusqlite::Result<()> {
    conn.execute(
        &format!(
            "DELETE FROM events_log
             WHERE (actor_id = ?1 OR other_id = ?1) AND action IN ({ACCOUNT_ACTIONS})"
        ),
        [user.as_uuid()],
    )?;
    conn.execute(
        "DELETE FROM events_notices WHERE user_id = ?1",
        [user.as_uuid()],
    )?;
    Ok(())
}

/// Gives the events about one person's items to another, when the first
/// person's files are given to them.
pub(crate) fn change_owner(conn: &Connection, from: UserId, to: UserId) -> rusqlite::Result<()> {
    conn.execute(
        "UPDATE events_log SET owner_id = ?2 WHERE owner_id = ?1",
        (from.as_uuid(), to.as_uuid()),
    )?;
    Ok(())
}

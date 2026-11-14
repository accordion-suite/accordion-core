//! The co-editing service's tables: each document's state as of its last
//! save, and every edit accepted since.

use accordion_types::ItemId;
use rusqlite::OptionalExtension;

use crate::db::{self, Database, Migration};
use crate::sql_now;
use crate::storage::ContentHash;

const OWNER: &str = "coedit";

static MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    name: "Create document states and the edit log",
    sql: "
        -- Each document's yrs state when its file was last written or read,
        -- so the next people to open it carry on from the same history, and
        -- the hash of that file. Unsaved is 1 when the state has edits the
        -- file doesn't, kept when the file changed outside the editor.
        CREATE TABLE coedit_states (
            item_id BLOB PRIMARY KEY CHECK (length(item_id) = 16),
            format TEXT NOT NULL,
            file_hash BLOB NOT NULL CHECK (length(file_hash) = 32),
            state BLOB NOT NULL,
            unsaved INTEGER NOT NULL CHECK (unsaved IN (0, 1)),
            saved_at TEXT NOT NULL
        ) STRICT, WITHOUT ROWID;

        -- Edits accepted since the last save, in order. Each is written
        -- before it's passed on, so a crash loses nothing anyone saw.
        CREATE TABLE coedit_updates (
            item_id BLOB NOT NULL CHECK (length(item_id) = 16),
            seq INTEGER NOT NULL,
            data BLOB NOT NULL,
            PRIMARY KEY (item_id, seq)
        ) STRICT, WITHOUT ROWID;
    ",
}];

/// What's kept for a document between sessions.
pub(super) struct Kept {
    pub state: Option<KeptState>,
    /// Edits since the state, oldest first.
    pub updates: Vec<Vec<u8>>,
    /// The number the next edit gets.
    pub next_seq: i64,
}

pub(super) struct KeptState {
    pub format: String,
    pub file_hash: ContentHash,
    pub state: Vec<u8>,
    /// The state has edits the file doesn't.
    pub unsaved: bool,
}

pub(super) async fn migrate(db: &Database) -> Result<(), db::Error> {
    db.migrate(OWNER, MIGRATIONS).await
}

/// Forgets documents whose files are gone for good.
pub(super) async fn prune(db: &Database) -> Result<(), db::Error> {
    db.write(|tx| {
        tx.execute(
            "DELETE FROM coedit_states
             WHERE item_id NOT IN (SELECT id FROM storage_items)",
            [],
        )?;
        tx.execute(
            "DELETE FROM coedit_updates
             WHERE item_id NOT IN (SELECT id FROM storage_items)",
            [],
        )?;
        Ok(())
    })
    .await
}

pub(super) async fn load(db: &Database, item: ItemId) -> Result<Kept, db::Error> {
    db.read(move |conn| {
        let state = conn
            .query_row(
                "SELECT format, file_hash, state, unsaved FROM coedit_states
                 WHERE item_id = ?1",
                [item.as_uuid()],
                |row| {
                    let hash: Vec<u8> = row.get(1)?;
                    Ok(KeptState {
                        format: row.get(0)?,
                        file_hash: ContentHash(hash.try_into().unwrap_or([0; 32])),
                        state: row.get(2)?,
                        unsaved: row.get(3)?,
                    })
                },
            )
            .optional()?;
        let mut updates = Vec::new();
        let mut last = 0;
        let mut statement =
            conn.prepare("SELECT seq, data FROM coedit_updates WHERE item_id = ?1 ORDER BY seq")?;
        let mut rows = statement.query([item.as_uuid()])?;
        while let Some(row) = rows.next()? {
            last = row.get(0)?;
            updates.push(row.get(1)?);
        }
        Ok(Kept {
            state,
            updates,
            next_seq: last + 1,
        })
    })
    .await
}

pub(super) async fn append(
    db: &Database,
    item: ItemId,
    seq: i64,
    update: Vec<u8>,
) -> Result<(), db::Error> {
    db.write(move |tx| {
        tx.execute(
            "INSERT INTO coedit_updates (item_id, seq, data) VALUES (?1, ?2, ?3)",
            (item.as_uuid(), seq, update),
        )?;
        Ok(())
    })
    .await
}

/// Keeps the document's `state`, which takes in the file with `file_hash`,
/// and lets go of the edits before `next_seq`, which the state holds.
/// `unsaved` when the state has edits the file doesn't.
pub(super) async fn keep(
    db: &Database,
    item: ItemId,
    format: &'static str,
    file_hash: ContentHash,
    state: Vec<u8>,
    unsaved: bool,
    next_seq: i64,
) -> Result<(), db::Error> {
    db.write(move |tx| {
        tx.execute(
            concat!(
                "INSERT INTO coedit_states (item_id, format, file_hash, state, unsaved, saved_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ",
                sql_now!(),
                ")
                 ON CONFLICT (item_id) DO UPDATE SET
                     format = excluded.format, file_hash = excluded.file_hash,
                     state = excluded.state, unsaved = excluded.unsaved,
                     saved_at = excluded.saved_at"
            ),
            (item.as_uuid(), format, &file_hash.0[..], state, unsaved),
        )?;
        tx.execute(
            "DELETE FROM coedit_updates WHERE item_id = ?1 AND seq < ?2",
            (item.as_uuid(), next_seq),
        )?;
        Ok(())
    })
    .await
}

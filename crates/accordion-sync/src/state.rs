//! What the device knows: each item as it was when both sides last agreed,
//! and the cursor for the list of changes. It's kept in SQLite on the
//! device, outside the synced folder.
//!
//! Comparing each side with this tells who changed what. A file whose
//! contents differ from `sha256` here changed on that side.

use std::path::Path;
use std::sync::{Mutex, MutexGuard};

use rusqlite::{Connection, OptionalExtension, Row, params};

/// One item as it was at the last sync.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Known {
    pub id: String,
    /// `None` for a root: the person's top folder, or an item shared with
    /// them on its own.
    pub parent: Option<String>,
    pub name: String,
    pub folder: bool,
    /// The contents both sides had, for a file.
    pub sha256: Option<String>,
    pub access: String,
    /// Where it is in the synced folder, such as `My Drive/Recipes/soup.md`.
    pub path: String,
    /// The local file's identity and its look at the last sync, so a
    /// rename shows as the same file, and an unchanged file isn't hashed
    /// again.
    pub inode: u64,
    pub mtime: i64,
    pub size: u64,
}

/// The connection sits behind a lock, so an engine can move between
/// threads while it waits for the server. Only its own engine uses it.
pub struct State {
    conn: Mutex<Connection>,
}

impl State {
    pub fn open(path: &Path) -> rusqlite::Result<Self> {
        let conn = Connection::open(path)?;
        conn.pragma_update(None, "journal_mode", "wal")?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS items (
                 id TEXT PRIMARY KEY,
                 parent TEXT,
                 name TEXT NOT NULL,
                 folder INTEGER NOT NULL,
                 sha256 TEXT,
                 access TEXT NOT NULL,
                 path TEXT NOT NULL UNIQUE,
                 inode INTEGER NOT NULL,
                 mtime INTEGER NOT NULL,
                 size INTEGER NOT NULL
             ) STRICT;
             CREATE INDEX IF NOT EXISTS items_inode ON items (inode);
             CREATE INDEX IF NOT EXISTS items_parent ON items (parent);
             CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT;",
        )?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    fn conn(&self) -> MutexGuard<'_, Connection> {
        self.conn
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn cursor(&self) -> rusqlite::Result<Option<String>> {
        self.meta("cursor")
    }

    pub fn set_cursor(&self, cursor: Option<&str>) -> rusqlite::Result<()> {
        match cursor {
            Some(cursor) => self.set_meta("cursor", cursor),
            None => {
                self.conn()
                    .execute("DELETE FROM meta WHERE key = 'cursor'", [])?;
                Ok(())
            }
        }
    }

    pub fn meta(&self, key: &str) -> rusqlite::Result<Option<String>> {
        self.conn()
            .query_row("SELECT value FROM meta WHERE key = ?1", [key], |row| {
                row.get(0)
            })
            .optional()
    }

    pub fn set_meta(&self, key: &str, value: &str) -> rusqlite::Result<()> {
        self.conn().execute(
            "INSERT INTO meta (key, value) VALUES (?1, ?2)
             ON CONFLICT (key) DO UPDATE SET value = excluded.value",
            [key, value],
        )?;
        Ok(())
    }

    pub fn get(&self, id: &str) -> rusqlite::Result<Option<Known>> {
        self.conn()
            .query_row(&format!("{SELECT} WHERE id = ?1"), [id], from_row)
            .optional()
    }

    pub fn at_path(&self, path: &str) -> rusqlite::Result<Option<Known>> {
        self.conn()
            .query_row(&format!("{SELECT} WHERE path = ?1"), [path], from_row)
            .optional()
    }

    /// The items whose folder on the server is `parent`.
    pub fn children(&self, parent: &str) -> rusqlite::Result<Vec<Known>> {
        self.conn()
            .prepare(&format!("{SELECT} WHERE parent = ?1"))?
            .query_map([parent], from_row)?
            .collect()
    }

    /// The item at `path`, or one whose path differs only in case, as
    /// Windows and macOS see them. SQLite folds only ASCII letters.
    pub fn at_path_any_case(&self, path: &str) -> rusqlite::Result<Option<Known>> {
        self.conn()
            .query_row(
                &format!("{SELECT} WHERE path = ?1 COLLATE NOCASE"),
                [path],
                from_row,
            )
            .optional()
    }

    pub fn all(&self) -> rusqlite::Result<Vec<Known>> {
        self.conn()
            .prepare(&format!("{SELECT} ORDER BY path"))?
            .query_map([], from_row)?
            .collect()
    }

    /// Everything under `path`, not counting it, deepest first.
    pub fn inside(&self, path: &str) -> rusqlite::Result<Vec<Known>> {
        let prefix = format!("{path}/");
        self.conn()
            .prepare(&format!(
                "{SELECT} WHERE substr(path, 1, length(?1)) = ?1 ORDER BY length(path) DESC"
            ))?
            .query_map([prefix], from_row)?
            .collect()
    }

    /// Records an item, replacing what was known about it. Another item
    /// recorded at the same path is forgotten, since only one thing can be
    /// there.
    pub fn put(&self, known: &Known) -> rusqlite::Result<()> {
        self.conn().execute(
            "DELETE FROM items WHERE path = ?1 AND id != ?2",
            [&known.path, &known.id],
        )?;
        self.conn().execute(
            "INSERT INTO items (id, parent, name, folder, sha256, access, path, inode, mtime, size)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
             ON CONFLICT (id) DO UPDATE SET parent = excluded.parent, name = excluded.name,
                 folder = excluded.folder, sha256 = excluded.sha256, access = excluded.access,
                 path = excluded.path, inode = excluded.inode, mtime = excluded.mtime,
                 size = excluded.size",
            params![
                known.id,
                known.parent,
                known.name,
                known.folder,
                known.sha256,
                known.access,
                known.path,
                i64::try_from(known.inode).unwrap_or(i64::MAX),
                known.mtime,
                i64::try_from(known.size).unwrap_or(i64::MAX),
            ],
        )?;
        Ok(())
    }

    /// Moves everything under `from` to under `to`, after a folder moved.
    pub fn move_inside(&self, from: &str, to: &str) -> rusqlite::Result<()> {
        // The rest of each path, after `from`, starts with its slash.
        self.conn().execute(
            "UPDATE items SET path = ?2 || substr(path, length(?1) + 1)
             WHERE substr(path, 1, length(?1) + 1) = ?1 || '/'",
            [from, to],
        )?;
        Ok(())
    }

    pub fn forget(&self, id: &str) -> rusqlite::Result<()> {
        self.conn()
            .execute("DELETE FROM items WHERE id = ?1", [id])?;
        Ok(())
    }

    /// Forgets every item, for starting over.
    pub fn forget_all(&self) -> rusqlite::Result<()> {
        self.conn().execute("DELETE FROM items", [])?;
        Ok(())
    }
}

const SELECT: &str =
    "SELECT id, parent, name, folder, sha256, access, path, inode, mtime, size FROM items";

fn from_row(row: &Row<'_>) -> rusqlite::Result<Known> {
    Ok(Known {
        id: row.get(0)?,
        parent: row.get(1)?,
        name: row.get(2)?,
        folder: row.get(3)?,
        sha256: row.get(4)?,
        access: row.get(5)?,
        path: row.get(6)?,
        inode: row.get::<_, i64>(7)?.unsigned_abs(),
        mtime: row.get(8)?,
        size: row.get::<_, i64>(9)?.unsigned_abs(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn known(id: &str, path: &str) -> Known {
        Known {
            id: id.into(),
            parent: None,
            name: path.rsplit('/').next().unwrap().into(),
            folder: true,
            sha256: None,
            access: "owner".into(),
            path: path.into(),
            inode: 1,
            mtime: 0,
            size: 0,
        }
    }

    #[test]
    fn moving_a_folder_moves_whats_inside() {
        let dir = tempfile::tempdir().unwrap();
        let state = State::open(&dir.path().join("state.db")).unwrap();
        state.put(&known("a", "My Drive/A")).unwrap();
        state.put(&known("b", "My Drive/A/B")).unwrap();
        state.put(&known("c", "My Drive/A/B/c.txt")).unwrap();
        state.put(&known("d", "My Drive/AB")).unwrap();
        state.move_inside("My Drive/A", "My Drive/Z").unwrap();
        assert_eq!(state.get("c").unwrap().unwrap().path, "My Drive/Z/B/c.txt");
        assert_eq!(
            state.get("d").unwrap().unwrap().path,
            "My Drive/AB",
            "only what's inside"
        );
        let inside: Vec<String> = state
            .inside("My Drive/Z")
            .unwrap()
            .into_iter()
            .map(|k| k.path)
            .collect();
        assert_eq!(inside, ["My Drive/Z/B/c.txt", "My Drive/Z/B"]);
    }
}

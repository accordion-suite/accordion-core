//! The metadata database.
//!
//! Accordion keeps metadata in one SQLite file, in WAL mode with every commit
//! flushed to disk (decision 2 in the `accordion` repo's `docs/decisions`).
//!
//! All writes go through one writer thread, so tasks queue in the server
//! instead of waiting on SQLite's lock. Reads run on a small pool of read-only
//! connections, and WAL mode lets them run while a write is in progress.
//!
//! Each owner (core's identity service, or a module) creates its tables with
//! its own [`Migration`]s. Table names start with the owner's name, such as
//! `identity_users` or `drive_files`, so owners never collide.

use std::panic::{self, AssertUnwindSafe};
use std::path::Path;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;

use rusqlite::{Connection, OpenFlags, Transaction, TransactionBehavior};
use tokio::sync::oneshot;

pub use rusqlite;

/// A SQLite expression for the current time, optionally shifted by a
/// modifier such as `"-30 days"`.
///
/// Every timestamp column uses this format: ISO 8601 in UTC with
/// milliseconds. Text in one fixed format sorts and compares correctly.
///
/// ```
/// use accordion_core::sql_now;
/// const SQL: &str = concat!("SELECT ", sql_now!("-1 hour"));
/// assert_eq!(SQL, "SELECT strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-1 hour')");
/// ```
#[macro_export]
macro_rules! sql_now {
    () => {
        "strftime('%Y-%m-%dT%H:%M:%fZ', 'now')"
    };
    ($modifier:literal) => {
        concat!("strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '", $modifier, "')")
    };
}

/// Read connections. The Raspberry Pi 5 has four cores, and reads are short.
const READERS: usize = 4;

/// How long a connection waits for a lock held outside the writer thread,
/// such as by a backup, before it gives up.
const BUSY_TIMEOUT_MS: u32 = 5_000;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),

    #[error(
        "SQLite couldn't turn on write-ahead logging for the database (it reported \
         journal mode \"{0}\"). Check that the database is on a local disk."
    )]
    NoWal(String),

    #[error(
        "The {owner} migrations must be numbered 1, 2, 3, and so on with no gaps, \
         but number {found} is in place of {expected}."
    )]
    MigrationOrder {
        owner: &'static str,
        expected: u32,
        found: u32,
    },

    #[error(
        "The database has {owner} schema version {found}, but this version of \
         Accordion knows only up to {known}. It was probably written by a newer \
         version of Accordion. Restore the snapshot taken before the update, or \
         install the newer version again."
    )]
    NewerSchema {
        owner: &'static str,
        found: u32,
        known: u32,
    },

    #[error(
        "The database's {owner} migration {version} is named \"{applied}\", but this \
         version of Accordion calls it \"{expected}\". A migration was changed after \
         it shipped."
    )]
    MigrationChanged {
        owner: &'static str,
        version: u32,
        applied: String,
        expected: &'static str,
    },
}

/// One step in an owner's schema history.
///
/// Never edit or remove a migration after it ships. Add a new one instead.
pub struct Migration {
    /// Starts at 1 and goes up by one with each migration.
    pub version: u32,
    /// A short description, stored in the database to catch edited history.
    pub name: &'static str,
    /// Runs inside a transaction, so it mustn't contain `BEGIN` or `COMMIT`.
    pub sql: &'static str,
}

type Job = Box<dyn FnOnce(&mut Connection) + Send>;

/// A handle to the metadata database. Cloning it is cheap, and every clone
/// shares the same connections.
#[derive(Clone)]
pub struct Database {
    writer: Sender<Job>,
    readers: Sender<Job>,
}

impl Database {
    /// Opens the database at `path`, creating it if it doesn't exist.
    ///
    /// Call this once at startup. It blocks while it opens the connections.
    pub fn open(path: &Path) -> Result<Self, Error> {
        let writer = Connection::open(path)?;
        let mode: String =
            writer.pragma_update_and_check(None, "journal_mode", "wal", |row| row.get(0))?;
        if !mode.eq_ignore_ascii_case("wal") {
            return Err(Error::NoWal(mode));
        }
        // FULL also syncs the WAL on every commit, so a power cut never loses
        // a committed write. NORMAL would be faster but can lose the last few.
        writer.pragma_update(None, "synchronous", "FULL")?;
        writer.pragma_update(None, "foreign_keys", true)?;
        writer.busy_timeout(std::time::Duration::from_millis(BUSY_TIMEOUT_MS.into()))?;
        writer.execute_batch(
            "CREATE TABLE IF NOT EXISTS accordion_migrations (
                owner TEXT NOT NULL,
                version INTEGER NOT NULL,
                name TEXT NOT NULL,
                applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                PRIMARY KEY (owner, version)
            ) STRICT, WITHOUT ROWID;",
        )?;

        let (writer_jobs, receiver) = mpsc::channel();
        spawn_worker("accordion-db-writer".into(), writer, move || {
            receiver.recv().ok()
        });

        let (reader_jobs, receiver) = mpsc::channel::<Job>();
        let receiver = Arc::new(Mutex::new(receiver));
        for i in 0..READERS {
            let reader = Connection::open_with_flags(
                path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )?;
            reader.busy_timeout(std::time::Duration::from_millis(BUSY_TIMEOUT_MS.into()))?;
            let receiver = Arc::clone(&receiver);
            spawn_worker(format!("accordion-db-reader-{i}"), reader, move || {
                next_job(&receiver)
            });
        }

        Ok(Self {
            writer: writer_jobs,
            readers: reader_jobs,
        })
    }

    /// Runs `f` in a write transaction, and commits if it returns `Ok`.
    ///
    /// Writes run one at a time, so keep `f` short: no network calls, file
    /// I/O, or password hashing. Group related writes into one call rather
    /// than one call per row, and commit bulk work in batches.
    pub async fn write<T, E, F>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce(&Transaction<'_>) -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: From<rusqlite::Error> + Send + 'static,
    {
        run(&self.writer, move |conn| {
            // IMMEDIATE takes the write lock up front, so a transaction never
            // fails halfway because something outside the server, such as a
            // backup, started writing.
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let value = f(&tx)?;
            tx.commit()?;
            Ok(value)
        })
        .await
    }

    /// Runs `f` on a read-only connection.
    ///
    /// Everything `f` reads comes from one consistent snapshot, even if a
    /// write commits partway through.
    pub async fn read<T, E, F>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce(&Connection) -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: From<rusqlite::Error> + Send + 'static,
    {
        run(&self.readers, move |conn| {
            let tx = conn.transaction()?;
            let value = f(&tx)?;
            tx.commit()?;
            Ok(value)
        })
        .await
    }

    /// Brings `owner`'s tables up to date by running any of `migrations` that
    /// haven't run yet.
    ///
    /// All pending migrations run in one transaction, so a failure leaves the
    /// schema as it was.
    pub async fn migrate(
        &self,
        owner: &'static str,
        migrations: &'static [Migration],
    ) -> Result<(), Error> {
        for (i, migration) in migrations.iter().enumerate() {
            let expected = i as u32 + 1;
            if migration.version != expected {
                return Err(Error::MigrationOrder {
                    owner,
                    expected,
                    found: migration.version,
                });
            }
        }
        let known = migrations.len() as u32;

        self.write(move |tx| {
            let applied: Vec<(u32, String)> = tx
                .prepare("SELECT version, name FROM accordion_migrations WHERE owner = ?1 ORDER BY version")?
                .query_map([owner], |row| Ok((row.get(0)?, row.get(1)?)))?
                .collect::<Result<_, _>>()?;

            if let Some(&(found, _)) = applied.last()
                && found > known
            {
                return Err(Error::NewerSchema { owner, found, known });
            }
            for ((version, applied), migration) in applied.iter().zip(migrations) {
                if applied != migration.name {
                    return Err(Error::MigrationChanged {
                        owner,
                        version: *version,
                        applied: applied.clone(),
                        expected: migration.name,
                    });
                }
            }

            for migration in &migrations[applied.len()..] {
                tx.execute_batch(migration.sql)?;
                tx.execute(
                    "INSERT INTO accordion_migrations (owner, version, name) VALUES (?1, ?2, ?3)",
                    (owner, migration.version, migration.name),
                )?;
            }
            Ok(())
        })
        .await
    }

    /// The latest migration that has run for `owner`, or 0 if none has.
    pub async fn schema_version(&self, owner: &'static str) -> Result<u32, Error> {
        self.read(move |conn| {
            let version: Option<u32> = conn.query_row(
                "SELECT max(version) FROM accordion_migrations WHERE owner = ?1",
                [owner],
                |row| row.get(0),
            )?;
            Ok(version.unwrap_or(0))
        })
        .await
    }
}

fn next_job(receiver: &Mutex<Receiver<Job>>) -> Option<Job> {
    receiver
        .lock()
        .expect("reader queue lock poisoned")
        .recv()
        .ok()
}

fn spawn_worker(
    name: String,
    mut conn: Connection,
    mut next: impl FnMut() -> Option<Job> + Send + 'static,
) {
    thread::Builder::new()
        .name(name)
        .spawn(move || {
            // The loop ends when every `Database` handle has been dropped.
            while let Some(job) = next() {
                // A panic fails only the job that caused it: its transaction
                // rolls back, its caller is told, and the connection stays
                // open for the next job.
                let _ = panic::catch_unwind(AssertUnwindSafe(|| job(&mut conn)));
            }
        })
        .expect("couldn't start a database thread");
}

async fn run<T: Send + 'static>(
    jobs: &Sender<Job>,
    f: impl FnOnce(&mut Connection) -> T + Send + 'static,
) -> T {
    let (reply, result) = oneshot::channel();
    let job: Job = Box::new(move |conn| {
        let _ = reply.send(f(conn));
    });
    jobs.send(job).expect("database thread stopped");
    // The reply is dropped unsent only if the job panicked. Panicking here too
    // carries that bug to the caller's task, as if the job had run inline.
    result.await.expect("database job panicked")
}

#[cfg(test)]
mod tests {
    use super::*;

    static PETS: &[Migration] = &[
        Migration {
            version: 1,
            name: "Create pets",
            sql: "CREATE TABLE pets_pets (name TEXT NOT NULL) STRICT;",
        },
        Migration {
            version: 2,
            name: "Add species",
            sql: "ALTER TABLE pets_pets ADD COLUMN species TEXT;",
        },
    ];

    fn open() -> (tempfile::TempDir, Database) {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir.path().join("accordion.db")).unwrap();
        (dir, db)
    }

    #[tokio::test]
    async fn migrations_run_once_in_order() {
        let (_dir, db) = open();
        assert_eq!(db.schema_version("pets").await.unwrap(), 0);

        db.migrate("pets", &PETS[..1]).await.unwrap();
        assert_eq!(db.schema_version("pets").await.unwrap(), 1);

        db.migrate("pets", PETS).await.unwrap();
        db.migrate("pets", PETS).await.unwrap();
        assert_eq!(db.schema_version("pets").await.unwrap(), 2);

        db.write(|tx| tx.execute("INSERT INTO pets_pets VALUES ('Rex', 'dog')", []))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn refuses_a_schema_from_a_newer_version() {
        let (_dir, db) = open();
        db.migrate("pets", PETS).await.unwrap();
        let err = db.migrate("pets", &PETS[..1]).await.unwrap_err();
        assert!(matches!(
            err,
            Error::NewerSchema {
                found: 2,
                known: 1,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn refuses_edited_history() {
        static EDITED: &[Migration] = &[Migration {
            version: 1,
            name: "Create pets with owners",
            sql: "",
        }];
        let (_dir, db) = open();
        db.migrate("pets", &PETS[..1]).await.unwrap();
        let err = db.migrate("pets", EDITED).await.unwrap_err();
        assert!(matches!(err, Error::MigrationChanged { version: 1, .. }));
    }

    #[tokio::test]
    async fn refuses_gaps_in_numbering() {
        static GAP: &[Migration] = &[Migration {
            version: 2,
            name: "Oops",
            sql: "",
        }];
        let (_dir, db) = open();
        let err = db.migrate("pets", GAP).await.unwrap_err();
        assert!(matches!(
            err,
            Error::MigrationOrder {
                expected: 1,
                found: 2,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn a_failed_migration_changes_nothing() {
        static BROKEN: &[Migration] = &[
            Migration {
                version: 1,
                name: "Create pets",
                sql: "CREATE TABLE pets_pets (name TEXT NOT NULL) STRICT;",
            },
            Migration {
                version: 2,
                name: "Broken",
                sql: "THIS IS NOT SQL;",
            },
        ];
        let (_dir, db) = open();
        assert!(db.migrate("pets", BROKEN).await.is_err());
        assert_eq!(db.schema_version("pets").await.unwrap(), 0);
        let exists: bool = db
            .read(|conn| {
                conn.query_row(
                    "SELECT EXISTS (SELECT 1 FROM sqlite_schema WHERE name = 'pets_pets')",
                    [],
                    |r| r.get(0),
                )
            })
            .await
            .unwrap();
        assert!(!exists);
    }

    #[tokio::test]
    async fn a_failed_write_rolls_back() {
        let (_dir, db) = open();
        db.migrate("pets", PETS).await.unwrap();
        let result: Result<(), rusqlite::Error> = db
            .write(|tx| {
                tx.execute("INSERT INTO pets_pets (name) VALUES ('Rex')", [])?;
                tx.execute("INSERT INTO pets_pets (name) VALUES (NULL)", [])?;
                Ok(())
            })
            .await;
        assert!(result.is_err());
        let count: u32 = db
            .read(|conn| conn.query_row("SELECT count(*) FROM pets_pets", [], |r| r.get(0)))
            .await
            .unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn readers_cant_write() {
        let (_dir, db) = open();
        db.migrate("pets", PETS).await.unwrap();
        let result = db
            .read(|conn| conn.execute("INSERT INTO pets_pets (name) VALUES ('Rex')", []))
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn a_panicking_job_leaves_the_writer_working() {
        let (_dir, db) = open();
        db.migrate("pets", PETS).await.unwrap();

        let db2 = db.clone();
        let panicked = tokio::spawn(async move {
            db2.write(|tx| -> rusqlite::Result<()> {
                tx.execute("INSERT INTO pets_pets (name) VALUES ('Rex')", [])?;
                panic!("bug in a module");
            })
            .await
        })
        .await;
        assert!(panicked.unwrap_err().is_panic());

        db.write(|tx| tx.execute("INSERT INTO pets_pets (name) VALUES ('Fido')", []))
            .await
            .unwrap();
        let names: Vec<String> = db
            .read(|conn| {
                conn.prepare("SELECT name FROM pets_pets")?
                    .query_map([], |r| r.get(0))?
                    .collect()
            })
            .await
            .unwrap();
        assert_eq!(names, ["Fido"]);
    }

    #[test]
    fn opens_in_wal_mode() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("accordion.db");
        drop(Database::open(&path).unwrap());
        let conn = Connection::open(&path).unwrap();
        let mode: String = conn
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .unwrap();
        assert_eq!(mode, "wal");
    }
}

//! Backups: encrypted copies of everything Accordion keeps, in restic
//! repositories on disks and servers the owner chooses (decision 8 in the
//! `accordion` repo's `docs/decisions`).
//!
//! A backup copies the database with SQLite's online backup API, holds
//! storage's contents so upkeep can't remove any, and has restic save the
//! copy, `storage/blobs/`, and `gateway/` as one snapshot in each target.
//! restic runs as a separate process, so a problem in a backup can't stop
//! the server.
//!
//! Every repository is locked with the recovery key. The owner keeps a copy
//! of it off the box, and `backup/recovery-key` in the data folder keeps
//! another, so backups run unattended. `backup/` also holds restic's cache,
//! and the database copy while a backup runs.
//!
//! [`Backup::maintain`] runs the schedule: a backup every night, a weekly
//! prune and check that reads an eighth of each repository's data, and a
//! monthly restore test.

mod restic;

use std::ffi::OsString;
use std::fmt;
use std::future::Future;
use std::io::{self, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use accordion_types::{BackupRunId, BackupTargetId};
use rusqlite::{Connection, OptionalExtension, Row};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, MutexGuard, Notify, watch};

use crate::db::{self, Database, Migration};
use crate::storage::{Blob, ContentHash, Storage};
use crate::{random, sql_now, time};
use restic::{Failure, Restic};

const OWNER: &str = "backup";

static MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "Create targets, runs, and settings",
        sql: "
        -- Where backups go. Each target is its own restic repository.
        CREATE TABLE backup_targets (
            id BLOB PRIMARY KEY CHECK (length(id) = 16),
            name TEXT NOT NULL,
            kind TEXT NOT NULL CHECK (kind IN ('folder')),
            -- Where the repository is, such as a folder on a USB disk.
            location TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL
        ) STRICT, WITHOUT ROWID;

        -- Each finished backup, check, and restore test.
        CREATE TABLE backup_runs (
            id BLOB PRIMARY KEY CHECK (length(id) = 16),
            target_id BLOB NOT NULL REFERENCES backup_targets (id) ON DELETE CASCADE,
            kind TEXT NOT NULL CHECK (kind IN ('backup', 'check', 'restore_test')),
            started_at TEXT NOT NULL,
            finished_at TEXT NOT NULL,
            ok INTEGER NOT NULL CHECK (ok IN (0, 1)),
            -- The restic snapshot a backup made.
            snapshot TEXT,
            bytes_added INTEGER CHECK (bytes_added >= 0),
            -- What went wrong, written for the admin.
            problem TEXT
        ) STRICT, WITHOUT ROWID;

        CREATE INDEX backup_runs_latest ON backup_runs (target_id, kind, finished_at);

        -- Settings for every target. There's only ever one row.
        CREATE TABLE backup_settings (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            -- When the owner showed they'd saved the recovery key.
            key_confirmed_at TEXT
        ) STRICT;
        INSERT INTO backup_settings (id) VALUES (1);
    ",
    },
    Migration {
        version: 2,
        name: "Add the backup hour and weekly upkeep",
        sql: "
        -- The hour of the night backups start, in the server's time zone.
        ALTER TABLE backup_settings ADD COLUMN hour INTEGER NOT NULL DEFAULT 3
            CHECK (hour BETWEEN 0 AND 23);
        -- When the target last had its expired data removed.
        ALTER TABLE backup_targets ADD COLUMN pruned_at TEXT;
        -- Which eighth of the target's data the next weekly check reads.
        ALTER TABLE backup_targets ADD COLUMN next_part INTEGER NOT NULL DEFAULT 1
            CHECK (next_part BETWEEN 1 AND 8);
    ",
    },
];

/// Letters in a recovery key: about 141 bits.
const KEY_LEN: usize = 30;

/// Snapshots kept in each repository, as decision 8 sets out.
const KEEP: [(&str, &str); 3] = [
    ("--keep-daily", "7"),
    ("--keep-weekly", "4"),
    ("--keep-monthly", "12"),
];

/// Stored contents a restore test brings back and rehashes.
const TEST_SAMPLE: usize = 20;

/// Parts a weekly check splits each repository's data into, so all of it is
/// read back about every two months.
const PARTS: u32 = 8;

/// How long after a missed backup the server starts one, if it was off at
/// backup time. Long enough for everything else to start first.
const CATCH_UP: Duration = Duration::from_secs(10 * 60);

/// Marks Accordion's snapshots, so they're grouped together however the
/// box's hostname or data folder changes.
const TAG: &str = "accordion";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Db(#[from] db::Error),

    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),

    #[error("Accordion couldn't read or write its backup files: {0}")]
    Io(#[from] io::Error),

    #[error("Create a recovery key before you set up backups.")]
    NoRecoveryKey,

    #[error(
        "There's already a recovery key. Every backup is locked with it, so it \
         can't be replaced."
    )]
    KeyExists,

    #[error("That isn't the recovery key. Check what you wrote down, then try again.")]
    KeyMismatch,

    #[error("Add a place for backups to go, such as a USB disk, then try again.")]
    NoTargets,

    #[error("A backup is already running. Try again when it finishes.")]
    Busy,

    #[error("Give the backup location a name of up to 100 characters.")]
    BadName,

    #[error("Enter the folder's full path, starting with /.")]
    NotAbsolute,

    #[error(
        "Backups can't go inside Accordion's own data folder, because losing \
         that disk would lose the backups too. Choose a folder on another disk."
    )]
    InsideData,

    #[error("{} doesn't exist. If it's on a USB disk, check that the disk is plugged in.", .0.display())]
    NoSuchFolder(PathBuf),

    #[error(
        "There's already a backup in {0}, locked with a different recovery key. \
         Choose another folder."
    )]
    WrongKey(String),

    #[error("That backup location is already set up.")]
    TargetExists,

    #[error("{0}")]
    Restic(String),

    #[error("Accordion couldn't find that backup location.")]
    NotFound,

    #[error("Choose an hour from 0 to 23.")]
    BadHour,
}

/// The password that locks every backup. Shown to the owner in groups of
/// five, such as `abcde fghij …`.
#[derive(Clone, PartialEq, Eq)]
pub struct RecoveryKey(String);

impl RecoveryKey {
    /// The key in groups of five letters, which reads better off a screen
    /// or a sheet of paper.
    pub fn grouped(&self) -> String {
        random::grouped(&self.0, 5)
    }

    /// The letters with no spaces, as restic expects them.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for RecoveryKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RecoveryKey(..)")
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Target {
    pub id: BackupTargetId,
    pub name: String,
    /// A folder path, for a folder target.
    pub location: String,
    pub created_at: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunKind {
    Backup,
    Check,
    RestoreTest,
}

impl RunKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Backup => "backup",
            Self::Check => "check",
            Self::RestoreTest => "restore_test",
        }
    }

    fn parse(text: &str) -> Option<Self> {
        match text {
            "backup" => Some(Self::Backup),
            "check" => Some(Self::Check),
            "restore_test" => Some(Self::RestoreTest),
            _ => None,
        }
    }
}

/// One finished backup, check, or restore test of one target.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Run {
    pub id: BackupRunId,
    pub target: BackupTargetId,
    pub kind: RunKind,
    pub started_at: String,
    pub finished_at: String,
    pub ok: bool,
    /// The snapshot a backup made.
    pub snapshot: Option<String>,
    /// New data a backup added to the repository, after deduplication.
    pub bytes_added: Option<u64>,
    /// What went wrong, written for the admin.
    pub problem: Option<String>,
}

/// How much of the stored data a check reads back and verifies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckDepth {
    /// The repository's structure only. Quick.
    Structure,
    /// Also one part of the data, such as `(3, 8)` for the third eighth.
    Data { part: u32, of: u32 },
}

/// How backups are doing, for the health page and the admin's backup page.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BackupStatus {
    pub key: KeyState,
    /// The hour of the night backups start, in the server's time zone.
    pub hour: u8,
    /// A backup, check, or restore test is running.
    pub running: bool,
    pub targets: Vec<TargetStatus>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum KeyState {
    /// Backups haven't been set up.
    #[default]
    None,
    /// The owner hasn't shown they saved a copy.
    Unconfirmed,
    Confirmed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TargetStatus {
    pub target: Target,
    /// The newest backup, whether or not it worked.
    pub last_backup: Option<Outcome>,
    /// When the newest backup that worked finished.
    pub last_good_backup: Option<SystemTime>,
    pub last_check: Option<Outcome>,
    pub last_restore_test: Option<Outcome>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Outcome {
    pub finished_at: SystemTime,
    pub ok: bool,
    pub problem: Option<String>,
}

/// A handle to the backup service. Cloning it is cheap.
#[derive(Clone)]
pub struct Backup {
    inner: Arc<Inner>,
}

struct Inner {
    db: Database,
    storage: Storage,
    data_dir: PathBuf,
    dir: PathBuf,
    restic: Restic,
    /// Held while restic runs, so only one backup or test runs at a time.
    running: Mutex<()>,
    status: watch::Sender<BackupStatus>,
    /// Wakes [`Backup::maintain`] when the backup hour changes.
    rescheduled: Notify,
}

/// Holds the `running` lock, and shows a run in progress in the status
/// until it's dropped.
struct Busy<'a> {
    _lock: MutexGuard<'a, ()>,
    status: &'a watch::Sender<BackupStatus>,
}

impl Drop for Busy<'_> {
    fn drop(&mut self) {
        self.status.send_modify(|status| status.running = false);
    }
}

impl Backup {
    /// Starts the backup service, creating its tables and `backup/` in
    /// `data_dir`. `restic` is the restic program to run.
    pub async fn open(
        db: Database,
        storage: Storage,
        data_dir: &Path,
        restic: PathBuf,
    ) -> Result<Self, Error> {
        db.migrate(OWNER, MIGRATIONS).await?;
        let dir = data_dir.join("backup");
        for folder in [&dir, &dir.join("cache"), &dir.join("temp")] {
            std::fs::create_dir_all(folder)?;
        }
        let backup = Self {
            inner: Arc::new(Inner {
                db,
                storage,
                data_dir: data_dir.to_owned(),
                restic: Restic {
                    program: restic,
                    key_file: dir.join("recovery-key"),
                    cache: dir.join("cache"),
                    temp: dir.join("temp"),
                },
                dir,
                running: Mutex::new(()),
                status: watch::Sender::new(BackupStatus::default()),
                rescheduled: Notify::new(),
            }),
        };
        backup.refresh().await?;
        Ok(backup)
    }

    /// How backups are doing. It changes as they run.
    pub fn status(&self) -> watch::Receiver<BackupStatus> {
        self.inner.status.subscribe()
    }

    /// Waits for the `running` lock.
    async fn busy(&self) -> Busy<'_> {
        let lock = self.inner.running.lock().await;
        self.mark_busy(lock)
    }

    fn mark_busy<'a>(&'a self, lock: MutexGuard<'a, ()>) -> Busy<'a> {
        self.inner
            .status
            .send_modify(|status| status.running = true);
        Busy {
            _lock: lock,
            status: &self.inner.status,
        }
    }

    /// Reads the status again from the database and `backup/`.
    async fn refresh(&self) -> Result<(), Error> {
        let has_key = tokio::fs::try_exists(&self.inner.restic.key_file).await?;
        let (confirmed, hour, targets) = self
            .inner
            .db
            .read(|conn| {
                let (confirmed, hour): (Option<String>, u8) = conn.query_row(
                    "SELECT key_confirmed_at, hour FROM backup_settings WHERE id = 1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?;
                let targets: Vec<Target> = conn
                    .prepare(&format!("{TARGET_SELECT} ORDER BY created_at, id"))?
                    .query_map([], target_from_row)?
                    .collect::<Result<_, _>>()?;
                let mut statuses = Vec::with_capacity(targets.len());
                for target in targets {
                    let latest = |kind: RunKind| latest_outcome(conn, target.id, kind);
                    statuses.push(TargetStatus {
                        last_backup: latest(RunKind::Backup)?,
                        last_good_backup: conn
                            .query_row(
                                "SELECT max(finished_at) FROM backup_runs
                                 WHERE target_id = ?1 AND kind = 'backup' AND ok = 1",
                                [target.id.as_uuid()],
                                |row| row.get::<_, Option<String>>(0),
                            )?
                            .as_deref()
                            .and_then(time::parse),
                        last_check: latest(RunKind::Check)?,
                        last_restore_test: latest(RunKind::RestoreTest)?,
                        target,
                    });
                }
                Ok::<_, rusqlite::Error>((confirmed.is_some(), hour, statuses))
            })
            .await?;
        self.inner.status.send_modify(|status| {
            status.key = match (has_key, confirmed) {
                (false, _) => KeyState::None,
                (true, false) => KeyState::Unconfirmed,
                (true, true) => KeyState::Confirmed,
            };
            status.hour = hour;
            status.targets = targets;
        });
        Ok(())
    }

    /// Changes the hour backups start, in the server's time zone.
    pub async fn set_hour(&self, hour: u8) -> Result<(), Error> {
        if hour > 23 {
            return Err(Error::BadHour);
        }
        self.inner
            .db
            .write(move |tx| {
                tx.execute("UPDATE backup_settings SET hour = ?1 WHERE id = 1", [hour])
            })
            .await?;
        self.refresh().await?;
        self.inner.rescheduled.notify_one();
        Ok(())
    }

    /// Runs the schedule until `stopped` completes: a backup each night at
    /// the chosen hour, then any weekly upkeep and monthly restore tests that
    /// are due. If the server was off when a backup was due, one starts soon
    /// after the server does.
    pub async fn maintain(self, stopped: impl Future<Output = ()>) {
        tokio::pin!(stopped);
        let mut wait = match self.missed_a_backup().await {
            Ok(true) => CATCH_UP,
            Ok(false) => self.until_next_backup(),
            Err(err) => {
                tracing::warn!(%err, "couldn't read the backup schedule");
                self.until_next_backup()
            }
        };
        loop {
            tokio::select! {
                () = &mut stopped => return,
                () = self.inner.rescheduled.notified() => {
                    wait = self.until_next_backup();
                    continue;
                }
                () = tokio::time::sleep(wait) => {}
            }
            self.nightly().await;
            wait = self.until_next_backup();
        }
    }

    /// How long until the next backup hour, in the server's time zone.
    fn until_next_backup(&self) -> Duration {
        let hour = self.inner.status.borrow().hour;
        let now = jiff::Zoned::now();
        let next = |date: jiff::civil::Date| {
            date.at(hour as i8, 0, 0, 0)
                .to_zoned(now.time_zone().clone())
        };
        let today = next(now.date());
        let next = match today {
            Ok(today) if today > now => Ok(today),
            _ => now.date().tomorrow().and_then(next),
        };
        next.ok()
            .and_then(|next| Duration::try_from(next.duration_since(&now)).ok())
            // A day, if the time zone has no such hour.
            .unwrap_or(Duration::from_secs(24 * 60 * 60))
    }

    /// Whether there's somewhere to back up to, and no backup has started
    /// there in the last day.
    async fn missed_a_backup(&self) -> Result<bool, Error> {
        if self.inner.status.borrow().key == KeyState::None {
            return Ok(false);
        }
        Ok(self
            .inner
            .db
            .read(|conn| {
                conn.query_row(
                    concat!(
                        "SELECT EXISTS (SELECT 1 FROM backup_targets t
                         WHERE NOT EXISTS (SELECT 1 FROM backup_runs r
                                           WHERE r.target_id = t.id AND r.kind = 'backup'
                                             AND r.finished_at > ",
                        sql_now!("-1 day"),
                        "))"
                    ),
                    [],
                    |row| row.get(0),
                )
            })
            .await?)
    }

    /// The night's work: a backup, then weekly upkeep and monthly restore
    /// tests where they're due. Problems are recorded, not returned.
    pub async fn nightly(&self) {
        match self.back_up().await {
            Ok(_) | Err(Error::NoRecoveryKey | Error::NoTargets) => {}
            Err(err) => tracing::warn!(%err, "the nightly backup failed"),
        }
        let due = match self.due().await {
            Ok(due) => due,
            Err(err) => {
                tracing::warn!(%err, "couldn't read which backup upkeep is due");
                return;
            }
        };
        for (target, weekly, restore_test) in due {
            if let Some(part) = weekly
                && let Err(err) = self.weekly(&target, part).await
            {
                tracing::warn!(target = %target.name, %err, "weekly backup upkeep failed");
            }
            if restore_test && let Err(err) = self.test_restore(&target).await {
                tracing::warn!(target = %target.name, %err, "the restore test failed to run");
            }
        }
    }

    /// For each target with a backup that worked: the part of its data the
    /// weekly check reads, if weekly upkeep is due, and whether a monthly
    /// restore test is due.
    async fn due(&self) -> Result<Vec<(Target, Option<u32>, bool)>, Error> {
        Ok(self
            .inner
            .db
            .read(|conn| {
                conn.prepare(concat!(
                    "SELECT t.id, t.name, t.location, t.created_at,
                            CASE WHEN t.pruned_at IS NULL OR t.pruned_at < ",
                    sql_now!("-7 days"),
                    " THEN t.next_part END,
                            NOT EXISTS (SELECT 1 FROM backup_runs r
                                        WHERE r.target_id = t.id AND r.kind = 'restore_test'
                                          AND r.finished_at > ",
                    sql_now!("-30 days"),
                    ")
                     FROM backup_targets t
                     WHERE EXISTS (SELECT 1 FROM backup_runs r
                                   WHERE r.target_id = t.id AND r.kind = 'backup' AND r.ok = 1)
                     ORDER BY t.created_at, t.id"
                ))?
                .query_map([], |row| {
                    Ok((target_from_row(row)?, row.get(4)?, row.get(5)?))
                })?
                .collect::<Result<_, _>>()
            })
            .await?)
    }

    /// Removes expired data from a target, reads back one part of what's
    /// left, and moves on to the next part for next week.
    async fn weekly(&self, target: &Target, part: u32) -> Result<(), Error> {
        self.prune(target).await?;
        let run = self
            .check(target, CheckDepth::Data { part, of: PARTS })
            .await?;
        let next = part % PARTS + 1;
        let id = target.id;
        self.inner
            .db
            .write(move |tx| {
                tx.execute(
                    concat!(
                        "UPDATE backup_targets SET pruned_at = ",
                        sql_now!(),
                        ", next_part = ?2 WHERE id = ?1"
                    ),
                    (id.as_uuid(), next),
                )
            })
            .await?;
        if !run.ok {
            tracing::warn!(target = %target.name, problem = ?run.problem, "the weekly check failed");
        }
        Ok(())
    }

    /// Creates the recovery key, the first time backups are set up. Show it
    /// to the owner so they can keep a copy.
    pub async fn create_recovery_key(&self) -> Result<RecoveryKey, Error> {
        let path = self.inner.restic.key_file.clone();
        let key = RecoveryKey(random::letters(KEY_LEN));
        let letters = key.0.clone();
        tokio::task::spawn_blocking(move || {
            // create_new, so an existing key is never overwritten.
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&path)
                .map_err(|err| match err.kind() {
                    io::ErrorKind::AlreadyExists => Error::KeyExists,
                    _ => Error::Io(err),
                })?;
            file.write_all(letters.as_bytes())?;
            file.sync_all()?;
            Ok::<_, Error>(())
        })
        .await
        .expect("writing the recovery key panicked")?;
        self.inner
            .db
            .write(|tx| {
                tx.execute(
                    "UPDATE backup_settings SET key_confirmed_at = NULL WHERE id = 1",
                    [],
                )
            })
            .await?;
        self.refresh().await?;
        Ok(key)
    }

    /// The recovery key, if there is one, to show the owner again.
    pub async fn recovery_key(&self) -> Result<Option<RecoveryKey>, Error> {
        match tokio::fs::read_to_string(&self.inner.restic.key_file).await {
            Ok(letters) => Ok(Some(RecoveryKey(letters.trim().to_owned()))),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    /// Records that the owner has saved the recovery key, once they type it
    /// back correctly. Spaces, hyphens, and case don't matter.
    pub async fn confirm_recovery_key(&self, typed: &str) -> Result<(), Error> {
        let key = self.recovery_key().await?.ok_or(Error::NoRecoveryKey)?;
        if random::parse_letters(typed, KEY_LEN).as_deref() != Some(key.as_str()) {
            return Err(Error::KeyMismatch);
        }
        self.inner
            .db
            .write(|tx| {
                tx.execute(
                    concat!(
                        "UPDATE backup_settings SET key_confirmed_at = ",
                        sql_now!(),
                        " WHERE id = 1"
                    ),
                    [],
                )
            })
            .await?;
        self.refresh().await?;
        Ok(())
    }

    /// When the owner confirmed they'd saved the recovery key, if they have.
    pub async fn key_confirmed_at(&self) -> Result<Option<String>, Error> {
        Ok(self
            .inner
            .db
            .read(|conn| {
                conn.query_row(
                    "SELECT key_confirmed_at FROM backup_settings WHERE id = 1",
                    [],
                    |row| row.get(0),
                )
            })
            .await?)
    }

    /// Adds a folder, such as one on a USB disk, as a place for backups.
    /// Sets up a new repository there, or uses the one already there if the
    /// recovery key opens it.
    pub async fn add_folder(&self, name: &str, path: &Path) -> Result<Target, Error> {
        let name = name.trim().to_owned();
        if name.is_empty() || name.chars().count() > 100 {
            return Err(Error::BadName);
        }
        if !path.is_absolute() {
            return Err(Error::NotAbsolute);
        }
        if self.recovery_key().await?.is_none() {
            return Err(Error::NoRecoveryKey);
        }
        let parent = path.parent().ok_or(Error::InsideData)?;
        let parent = tokio::fs::canonicalize(parent)
            .await
            .map_err(|_| Error::NoSuchFolder(parent.to_owned()))?;
        let data_dir = tokio::fs::canonicalize(&self.inner.data_dir).await?;
        let path = parent.join(path.file_name().ok_or(Error::InsideData)?);
        if path.starts_with(&data_dir) || data_dir.starts_with(&path) {
            return Err(Error::InsideData);
        }
        let location = path
            .to_str()
            .ok_or(Error::Restic(
                "Accordion can only use folders whose path is valid UTF-8.".into(),
            ))?
            .to_owned();

        let _busy = self.busy().await;
        let restic = &self.inner.restic;
        match restic.run(&location, None, &args(["cat", "config"])).await {
            Ok(_) => {}
            Err(Failure::NoRepository) => {
                restic
                    .run(&location, None, &args(["init", "--json"]))
                    .await
                    .map_err(|failure| Error::Restic(describe(&failure, &location)))?;
            }
            Err(Failure::WrongKey) => return Err(Error::WrongKey(location)),
            Err(failure) => return Err(Error::Restic(describe(&failure, &location))),
        }

        let id = BackupTargetId::generate();
        let target = self
            .inner
            .db
            .write(move |tx| {
                let taken: bool = tx.query_row(
                    "SELECT EXISTS (SELECT 1 FROM backup_targets WHERE location = ?1)",
                    [&location],
                    |row| row.get(0),
                )?;
                if taken {
                    return Err(Error::TargetExists);
                }
                tx.execute(
                    concat!(
                        "INSERT INTO backup_targets (id, name, kind, location, created_at)
                         VALUES (?1, ?2, 'folder', ?3, ",
                        sql_now!(),
                        ")"
                    ),
                    (id.as_uuid(), &name, &location),
                )?;
                target(tx, id)?.ok_or(Error::NotFound)
            })
            .await?;
        self.refresh().await?;
        Ok(target)
    }

    /// Every place backups go, oldest first.
    pub async fn targets(&self) -> Result<Vec<Target>, Error> {
        Ok(self
            .inner
            .db
            .read(|conn| {
                conn.prepare(&format!("{TARGET_SELECT} ORDER BY created_at, id"))?
                    .query_map([], target_from_row)?
                    .collect::<Result<_, _>>()
            })
            .await?)
    }

    /// Stops backing up to a target, and forgets its history. The backups
    /// already there stay, so they can still be restored.
    pub async fn remove_target(&self, id: BackupTargetId) -> Result<(), Error> {
        let removed = self
            .inner
            .db
            .write(move |tx| tx.execute("DELETE FROM backup_targets WHERE id = ?1", [id.as_uuid()]))
            .await?;
        if removed == 0 {
            return Err(Error::NotFound);
        }
        self.refresh().await?;
        Ok(())
    }

    /// Backs up to every target in turn, removes snapshots that are past
    /// keeping, and checks each repository's structure. A target that fails
    /// doesn't stop the others. Returns each backup and check.
    pub async fn back_up(&self) -> Result<Vec<Run>, Error> {
        let _busy = self.mark_busy(self.inner.running.try_lock().map_err(|_| Error::Busy)?);
        if self.recovery_key().await?.is_none() {
            return Err(Error::NoRecoveryKey);
        }
        let targets = self.targets().await?;
        if targets.is_empty() {
            return Err(Error::NoTargets);
        }

        // Held before the copy, so every stored file the copy refers to stays
        // on disk until restic has read it.
        let hold = self.inner.storage.hold_contents().await;
        let copy = self.inner.dir.join("accordion.db");
        self.inner.db.copy_to(&copy).await?;
        let mut paths = vec![
            OsString::from("backup/accordion.db"),
            OsString::from("storage/blobs"),
        ];
        if tokio::fs::try_exists(self.inner.data_dir.join("gateway")).await? {
            paths.push("gateway".into());
        }

        let mut runs = Vec::new();
        for target in &targets {
            let run = self.back_up_to(target, &paths).await;
            let backed_up = run.ok;
            runs.push(self.record(run).await?);
            if backed_up {
                let run = self.check_target(target, CheckDepth::Structure).await;
                runs.push(self.record(run).await?);
            }
        }
        drop(hold);
        tokio::fs::remove_file(&copy).await?;
        Ok(runs)
    }

    async fn back_up_to(&self, target: &Target, paths: &[OsString]) -> Run {
        let started_at = now();
        let mut command = args(["backup", "--json", "--host", TAG, "--tag", TAG]);
        command.extend(paths.iter().cloned());
        let restic = &self.inner.restic;
        let result = restic
            .run(&target.location, Some(&self.inner.data_dir), &command)
            .await;
        let mut run = Run::new(target.id, RunKind::Backup, started_at);
        let stdout = match result {
            Ok(stdout) => stdout,
            Err(failure) => return run.failed(describe(&failure, &target.location)),
        };
        let summary = restic::message(&stdout, "summary").unwrap_or_default();
        run.snapshot = summary["snapshot_id"].as_str().map(str::to_owned);
        run.bytes_added = summary["data_added"].as_u64();
        if run.snapshot.is_none() {
            return run.failed("restic finished without saying which snapshot it made.".into());
        }
        let mut forget = args(["forget", "--json", "--group-by", "host,tags", "--tag", TAG]);
        for (flag, count) in KEEP {
            forget.extend(args([flag, count]));
        }
        if let Err(failure) = restic.run(&target.location, None, &forget).await {
            // The backup itself worked, so this is only worth a warning.
            tracing::warn!(
                target = %target.name,
                problem = describe(&failure, &target.location),
                "couldn't remove old snapshots"
            );
        }
        run.finish()
    }

    /// Removes the data that only expired snapshots used. It takes a while,
    /// so it runs weekly rather than after every backup.
    pub async fn prune(&self, target: &Target) -> Result<(), Error> {
        let _busy = self.busy().await;
        self.inner
            .restic
            .run(&target.location, None, &args(["prune"]))
            .await
            .map_err(|failure| Error::Restic(describe(&failure, &target.location)))?;
        Ok(())
    }

    /// Checks a repository, and reads back part of its data if `depth` asks.
    pub async fn check(&self, target: &Target, depth: CheckDepth) -> Result<Run, Error> {
        let _busy = self.busy().await;
        let run = self.check_target(target, depth).await;
        self.record(run).await
    }

    async fn check_target(&self, target: &Target, depth: CheckDepth) -> Run {
        let started_at = now();
        let mut command = args(["check"]);
        if let CheckDepth::Data { part, of } = depth {
            command.push(format!("--read-data-subset={part}/{of}").into());
        }
        let run = Run::new(target.id, RunKind::Check, started_at);
        match self
            .inner
            .restic
            .run(&target.location, None, &command)
            .await
        {
            Ok(_) => run.finish(),
            Err(failure) => run.failed(describe(&failure, &target.location)),
        }
    }

    /// Restores the newest database copy and a sample of stored files from
    /// a target into a temporary folder, and checks them: the database with
    /// SQLite's integrity check, and each file against its SHA-256 hash.
    pub async fn test_restore(&self, target: &Target) -> Result<Run, Error> {
        let _busy = self.busy().await;
        let started_at = now();
        let place = self.inner.dir.join("restore-test");
        let _ = tokio::fs::remove_dir_all(&place).await;
        let result = self.restore_sample(target, &place).await;
        let _ = tokio::fs::remove_dir_all(&place).await;
        let run = Run::new(target.id, RunKind::RestoreTest, started_at);
        let run = match result {
            Ok(()) => run.finish(),
            Err(problem) => run.failed(problem),
        };
        self.record(run).await
    }

    async fn restore_sample(&self, target: &Target, place: &Path) -> Result<(), String> {
        let snapshot = self
            .last_snapshot(target.id)
            .await
            .map_err(|err| err.to_string())?
            .ok_or("There's no backup in this location yet.")?;
        let restic = &self.inner.restic;
        let restore = |includes: Vec<String>| {
            let mut command = args(["restore", &snapshot, "--target"]);
            command.push(place.into());
            for include in includes {
                command.extend(args(["--include", &include]));
            }
            async move {
                restic
                    .run(&target.location, None, &command)
                    .await
                    .map_err(|failure| describe(&failure, &target.location))
            }
        };

        restore(vec!["/backup/accordion.db".into()]).await?;
        let database = place.join("backup").join("accordion.db");
        let sample = tokio::task::spawn_blocking(move || {
            let conn = Connection::open(&database)?;
            let check: String = conn.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
            if check != "ok" {
                return Ok(Err(format!(
                    "The database in the backup failed SQLite's integrity check: {check}"
                )));
            }
            Ok(Ok(crate::storage::sample_contents(&conn, TEST_SAMPLE)?))
        })
        .await
        .expect("checking the restored database panicked")
        .map_err(|err: rusqlite::Error| {
            format!("Accordion couldn't read the database in the backup: {err}")
        })??;
        if sample.is_empty() {
            return Ok(());
        }

        let include = |blob: &Blob| {
            Path::new("/storage")
                .join(Storage::contents_path(blob.hash))
                .to_string_lossy()
                .into_owned()
        };
        restore(sample.iter().map(include).collect()).await?;
        let storage = place.join("storage");
        tokio::task::spawn_blocking(move || {
            for blob in sample {
                let path = storage.join(Storage::contents_path(blob.hash));
                let intact = std::fs::read(&path).is_ok_and(|contents| {
                    contents.len() as u64 == blob.size
                        && ContentHash(Sha256::digest(&contents).into()) == blob.hash
                });
                if !intact {
                    return Err(format!(
                        "A stored file came back from the backup damaged or missing \
                         (contents {}).",
                        blob.hash.to_hex()
                    ));
                }
            }
            Ok(())
        })
        .await
        .expect("checking restored files panicked")
    }

    /// The most recent runs, newest first.
    pub async fn runs(&self, limit: usize) -> Result<Vec<Run>, Error> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        Ok(self
            .inner
            .db
            .read(move |conn| {
                conn.prepare(&format!(
                    "{RUN_SELECT} ORDER BY finished_at DESC, id DESC LIMIT ?1"
                ))?
                .query_map([limit], run_from_row)?
                .collect::<Result<_, _>>()
            })
            .await?)
    }

    /// The snapshot from a target's newest successful backup.
    async fn last_snapshot(&self, target: BackupTargetId) -> Result<Option<String>, Error> {
        Ok(self
            .inner
            .db
            .read(move |conn| {
                conn.query_row(
                    "SELECT snapshot FROM backup_runs
                     WHERE target_id = ?1 AND kind = 'backup' AND ok = 1
                     ORDER BY finished_at DESC LIMIT 1",
                    [target.as_uuid()],
                    |row| row.get(0),
                )
                .optional()
            })
            .await?)
    }

    async fn record(&self, run: Run) -> Result<Run, Error> {
        if let Some(problem) = &run.problem {
            tracing::warn!(kind = run.kind.as_str(), %problem, "backup run failed");
        }
        let saved = run.clone();
        self.inner
            .db
            .write(move |tx| {
                tx.execute(
                    "INSERT INTO backup_runs (id, target_id, kind, started_at, finished_at,
                                              ok, snapshot, bytes_added, problem)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                    (
                        saved.id.as_uuid(),
                        saved.target.as_uuid(),
                        saved.kind.as_str(),
                        &saved.started_at,
                        &saved.finished_at,
                        saved.ok,
                        &saved.snapshot,
                        saved
                            .bytes_added
                            .map(|bytes| i64::try_from(bytes).unwrap_or(i64::MAX)),
                        &saved.problem,
                    ),
                )
            })
            .await?;
        self.refresh().await?;
        Ok(run)
    }
}

impl Run {
    fn new(target: BackupTargetId, kind: RunKind, started_at: String) -> Self {
        Self {
            id: BackupRunId::generate(),
            target,
            kind,
            started_at,
            finished_at: String::new(),
            ok: false,
            snapshot: None,
            bytes_added: None,
            problem: None,
        }
    }

    fn finish(mut self) -> Self {
        self.finished_at = now();
        self.ok = true;
        self
    }

    fn failed(mut self, problem: String) -> Self {
        self.finished_at = now();
        self.problem = Some(problem);
        self
    }
}

/// What a restic failure means for the admin, at `location`.
fn describe(failure: &Failure, location: &str) -> String {
    match failure {
        Failure::Start(err) => format!(
            "Accordion couldn't start restic, the program that makes backups: {err}. \
             Reinstall Accordion, then try again."
        ),
        Failure::NoRepository => format!(
            "There's no backup in {location}. If it's on a USB disk, check that the \
             disk is plugged in, then try again."
        ),
        Failure::Locked => format!(
            "Another program is using the backup in {location}. Try again in a few \
             minutes."
        ),
        Failure::WrongKey => {
            format!("The backup in {location} is locked with a different recovery key.")
        }
        Failure::Failed { code, message } => {
            let code =
                code.map_or_else(|| "stopped".to_owned(), |code| format!("exit code {code}"));
            format!("restic couldn't finish ({code}): {message}")
        }
    }
}

fn args<const N: usize>(list: [&str; N]) -> Vec<OsString> {
    list.iter().map(OsString::from).collect()
}

/// The current time, in the same format as the database's timestamps.
fn now() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    time::format(now)
}

const TARGET_SELECT: &str = "SELECT id, name, location, created_at FROM backup_targets";

fn target(conn: &Connection, id: BackupTargetId) -> rusqlite::Result<Option<Target>> {
    conn.query_row(
        &format!("{TARGET_SELECT} WHERE id = ?1"),
        [id.as_uuid()],
        target_from_row,
    )
    .optional()
}

fn target_from_row(row: &Row<'_>) -> rusqlite::Result<Target> {
    Ok(Target {
        id: BackupTargetId::from_uuid(row.get(0)?),
        name: row.get(1)?,
        location: row.get(2)?,
        created_at: row.get(3)?,
    })
}

const RUN_SELECT: &str = "SELECT id, target_id, kind, started_at, finished_at, ok, snapshot, \
                          bytes_added, problem FROM backup_runs";

fn run_from_row(row: &Row<'_>) -> rusqlite::Result<Run> {
    let kind: String = row.get(2)?;
    Ok(Run {
        id: BackupRunId::from_uuid(row.get(0)?),
        target: BackupTargetId::from_uuid(row.get(1)?),
        kind: RunKind::parse(&kind).ok_or_else(|| {
            rusqlite::Error::FromSqlConversionFailure(
                2,
                rusqlite::types::Type::Text,
                format!("unknown backup run kind {kind:?}").into(),
            )
        })?,
        started_at: row.get(3)?,
        finished_at: row.get(4)?,
        ok: row.get(5)?,
        snapshot: row.get(6)?,
        bytes_added: row
            .get::<_, Option<i64>>(7)?
            .map(|bytes| u64::try_from(bytes).unwrap_or_default()),
        problem: row.get(8)?,
    })
}

/// The newest run of `kind` on a target.
fn latest_outcome(
    conn: &Connection,
    target: BackupTargetId,
    kind: RunKind,
) -> rusqlite::Result<Option<Outcome>> {
    conn.query_row(
        "SELECT finished_at, ok, problem FROM backup_runs
         WHERE target_id = ?1 AND kind = ?2
         ORDER BY finished_at DESC, id DESC LIMIT 1",
        (target.as_uuid(), kind.as_str()),
        |row| {
            Ok(Outcome {
                finished_at: row
                    .get::<_, String>(0)
                    .map(|text| time::parse(&text).unwrap_or(SystemTime::UNIX_EPOCH))?,
                ok: row.get(1)?,
                problem: row.get(2)?,
            })
        },
    )
    .optional()
}

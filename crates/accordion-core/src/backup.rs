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
//! A target is a folder, such as one on a USB disk, an S3 bucket at a
//! storage provider, a folder on another computer over SFTP, or a REST
//! server on another computer. A bucket's access key, Accordion's SSH key,
//! each SFTP server's host key, and each REST server's password and pinned
//! certificate are kept in the database, so a restored server goes on
//! backing up to them. They're read only when restic runs, so they never
//! reach the status pages show.
//!
//! A REST server in append-only mode won't let Accordion delete anything,
//! so a server taken over by an attacker can't delete its backups there
//! (decision 10). Accordion finds out when the target is added, and never
//! removes old snapshots from one.
//!
//! Every repository is locked with the recovery key. The owner keeps a copy
//! of it off the box, and `backup/recovery-key` in the data folder keeps
//! another, so backups run unattended. `backup/` also holds restic's cache,
//! and the database copy while a backup runs.
//!
//! [`Backup::maintain`] runs the schedule: a backup every night, a weekly
//! prune and check that reads an eighth of each repository's data, and a
//! monthly restore test.
//!
//! Restoring to a fresh server happens in two parts. [`Backup::restore`]
//! brings a snapshot back into `restore/` in the data folder while the server
//! runs. Then the server starts again, and [`apply_restore`] moves the
//! restored data into place before anything opens the database.

mod rest;
mod restic;

use std::ffi::OsString;
use std::fmt;
use std::future::Future;
use std::io::{self, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use accordion_types::{BackupRunId, BackupTargetId};
use rusqlite::{Connection, OptionalExtension, Row};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, Notify, OwnedMutexGuard, watch};

use crate::db::{self, Database, Migration};
use crate::storage::{Blob, ContentHash, Storage};
use crate::{random, sql_now, time};
use rest::Problem;
pub use rest::RestServer;
use restic::{Failure, Keys, Login, Repository, Restic, SshProblem};

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
    Migration {
        version: 3,
        name: "Add S3 buckets as targets",
        // SQLite can't change a CHECK constraint in place, so both tables are
        // rebuilt. Runs are copied first, so dropping the old targets can't
        // delete them.
        sql: "
        CREATE TABLE backup_targets_new (
            id BLOB PRIMARY KEY CHECK (length(id) = 16),
            name TEXT NOT NULL,
            kind TEXT NOT NULL CHECK (kind IN ('folder', 's3')),
            -- In restic's form: a folder's path, or s3:https://host/bucket.
            location TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL,
            pruned_at TEXT,
            next_part INTEGER NOT NULL DEFAULT 1 CHECK (next_part BETWEEN 1 AND 8),
            -- A bucket's access key. Folders don't have one.
            access_key_id TEXT,
            secret_access_key TEXT,
            CHECK ((kind = 's3') = (access_key_id IS NOT NULL AND secret_access_key IS NOT NULL))
        ) STRICT, WITHOUT ROWID;
        INSERT INTO backup_targets_new (id, name, kind, location, created_at, pruned_at, next_part)
            SELECT id, name, kind, location, created_at, pruned_at, next_part FROM backup_targets;

        CREATE TABLE backup_runs_new (
            id BLOB PRIMARY KEY CHECK (length(id) = 16),
            target_id BLOB NOT NULL REFERENCES backup_targets_new (id) ON DELETE CASCADE,
            kind TEXT NOT NULL CHECK (kind IN ('backup', 'check', 'restore_test')),
            started_at TEXT NOT NULL,
            finished_at TEXT NOT NULL,
            ok INTEGER NOT NULL CHECK (ok IN (0, 1)),
            snapshot TEXT,
            bytes_added INTEGER CHECK (bytes_added >= 0),
            problem TEXT
        ) STRICT, WITHOUT ROWID;
        INSERT INTO backup_runs_new SELECT * FROM backup_runs;

        DROP TABLE backup_runs;
        DROP TABLE backup_targets;
        -- Renaming also updates the runs' reference to the targets.
        ALTER TABLE backup_targets_new RENAME TO backup_targets;
        ALTER TABLE backup_runs_new RENAME TO backup_runs;
        CREATE INDEX backup_runs_latest ON backup_runs (target_id, kind, finished_at);
    ",
    },
    Migration {
        version: 4,
        name: "Add SFTP folders as targets",
        // Rebuilt like version 3, for the new kind.
        sql: "
        CREATE TABLE backup_targets_new (
            id BLOB PRIMARY KEY CHECK (length(id) = 16),
            name TEXT NOT NULL,
            kind TEXT NOT NULL CHECK (kind IN ('folder', 's3', 'sftp')),
            -- In restic's form: a folder's path, s3:https://host/bucket, or
            -- sftp:user@host:folder.
            location TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL,
            pruned_at TEXT,
            next_part INTEGER NOT NULL DEFAULT 1 CHECK (next_part BETWEEN 1 AND 8),
            -- A bucket's access key.
            access_key_id TEXT,
            secret_access_key TEXT,
            -- An SFTP server's port, and its host key in known_hosts form,
            -- as it was when the target was added.
            ssh_port INTEGER CHECK (ssh_port BETWEEN 1 AND 65535),
            host_key TEXT,
            CHECK ((kind = 's3') = (access_key_id IS NOT NULL AND secret_access_key IS NOT NULL)),
            CHECK ((kind = 'sftp') = (ssh_port IS NOT NULL AND host_key IS NOT NULL))
        ) STRICT, WITHOUT ROWID;
        INSERT INTO backup_targets_new (id, name, kind, location, created_at, pruned_at,
                                        next_part, access_key_id, secret_access_key)
            SELECT id, name, kind, location, created_at, pruned_at, next_part,
                   access_key_id, secret_access_key
            FROM backup_targets;

        CREATE TABLE backup_runs_new (
            id BLOB PRIMARY KEY CHECK (length(id) = 16),
            target_id BLOB NOT NULL REFERENCES backup_targets_new (id) ON DELETE CASCADE,
            kind TEXT NOT NULL CHECK (kind IN ('backup', 'check', 'restore_test')),
            started_at TEXT NOT NULL,
            finished_at TEXT NOT NULL,
            ok INTEGER NOT NULL CHECK (ok IN (0, 1)),
            snapshot TEXT,
            bytes_added INTEGER CHECK (bytes_added >= 0),
            problem TEXT
        ) STRICT, WITHOUT ROWID;
        INSERT INTO backup_runs_new SELECT * FROM backup_runs;

        DROP TABLE backup_runs;
        DROP TABLE backup_targets;
        ALTER TABLE backup_targets_new RENAME TO backup_targets;
        ALTER TABLE backup_runs_new RENAME TO backup_runs;
        CREATE INDEX backup_runs_latest ON backup_runs (target_id, kind, finished_at);

        -- Accordion's SSH key, which SFTP servers let in. Created the first
        -- time it's needed, in OpenSSH's formats.
        ALTER TABLE backup_settings ADD COLUMN ssh_private_key TEXT;
        ALTER TABLE backup_settings ADD COLUMN ssh_public_key TEXT;
    ",
    },
    Migration {
        version: 5,
        name: "Add REST servers as targets, and append-only targets",
        // Rebuilt like version 3, for the new kind.
        sql: "
        CREATE TABLE backup_targets_new (
            id BLOB PRIMARY KEY CHECK (length(id) = 16),
            name TEXT NOT NULL,
            kind TEXT NOT NULL CHECK (kind IN ('folder', 's3', 'sftp', 'rest')),
            -- In restic's form: a folder's path, s3:https://host/bucket,
            -- sftp:user@host:folder, or rest:https://host:port/path.
            location TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL,
            pruned_at TEXT,
            next_part INTEGER NOT NULL DEFAULT 1 CHECK (next_part BETWEEN 1 AND 8),
            -- A bucket's access key.
            access_key_id TEXT,
            secret_access_key TEXT,
            -- An SFTP server's port, and its host key in known_hosts form,
            -- as it was when the target was added.
            ssh_port INTEGER CHECK (ssh_port BETWEEN 1 AND 65535),
            host_key TEXT,
            -- A REST server's username and password, both empty if it
            -- doesn't ask, and the certificate pinned when the target was
            -- added, if the system didn't trust the server's.
            rest_username TEXT,
            rest_password TEXT,
            certificate TEXT,
            -- The target refuses deletes, so Accordion never removes old
            -- snapshots from it.
            append_only INTEGER NOT NULL DEFAULT 0 CHECK (append_only IN (0, 1)),
            -- For an append-only target, measured at weekly upkeep: the
            -- space the repository uses, and its oldest snapshot.
            stored_bytes INTEGER CHECK (stored_bytes >= 0),
            oldest_backup_at TEXT,
            CHECK ((kind = 's3') = (access_key_id IS NOT NULL AND secret_access_key IS NOT NULL)),
            CHECK ((kind = 'sftp') = (ssh_port IS NOT NULL AND host_key IS NOT NULL)),
            CHECK ((kind = 'rest') = (rest_username IS NOT NULL AND rest_password IS NOT NULL)),
            CHECK (kind = 'rest' OR certificate IS NULL)
        ) STRICT, WITHOUT ROWID;
        INSERT INTO backup_targets_new (id, name, kind, location, created_at, pruned_at,
                                        next_part, access_key_id, secret_access_key,
                                        ssh_port, host_key)
            SELECT id, name, kind, location, created_at, pruned_at, next_part,
                   access_key_id, secret_access_key, ssh_port, host_key
            FROM backup_targets;

        CREATE TABLE backup_runs_new (
            id BLOB PRIMARY KEY CHECK (length(id) = 16),
            target_id BLOB NOT NULL REFERENCES backup_targets_new (id) ON DELETE CASCADE,
            kind TEXT NOT NULL CHECK (kind IN ('backup', 'check', 'restore_test')),
            started_at TEXT NOT NULL,
            finished_at TEXT NOT NULL,
            ok INTEGER NOT NULL CHECK (ok IN (0, 1)),
            snapshot TEXT,
            bytes_added INTEGER CHECK (bytes_added >= 0),
            problem TEXT
        ) STRICT, WITHOUT ROWID;
        INSERT INTO backup_runs_new SELECT * FROM backup_runs;

        DROP TABLE backup_runs;
        DROP TABLE backup_targets;
        ALTER TABLE backup_targets_new RENAME TO backup_targets;
        ALTER TABLE backup_runs_new RENAME TO backup_runs;
        CREATE INDEX backup_runs_latest ON backup_runs (target_id, kind, finished_at);
    ",
    },
];

/// Where ssh finds Accordion's SSH key and each SFTP server's host key. restic
/// runs in the data folder, and these paths are relative to it, because
/// restic doesn't honor quotes in its ssh command.
const SSH_DIR: &str = "backup/ssh";
const SSH_KEY: &str = "backup/ssh/id_ed25519";
/// The host key of a server that's being added or restored from, before
/// it's recorded.
const NEW_HOST_KEY: &str = "backup/ssh/new.known_hosts";
/// Where restic finds each REST server's pinned certificate.
const CERT_DIR: &str = "backup/certs";
/// The certificate of a REST server that's being added or restored from,
/// before it's recorded.
const NEW_CERT: &str = "backup/certs/new.pem";

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

/// How long a new location has to answer while the admin waits. A computer
/// at home answers in milliseconds, and a storage provider in well under a
/// second.
const ANSWER_TIME: Duration = Duration::from_secs(10);

/// Marks Accordion's snapshots, so they're grouped together however the
/// box's hostname or data folder changes.
const TAG: &str = "accordion";
/// Marks the empty snapshot that tests whether a REST server is
/// append-only. It's never listed for restoring.
const PROBE_TAG: &str = "accordion-append-only-test";

/// Where a restore waits in the data folder until the server starts again.
const RESTORE_DIR: &str = "restore";
/// Written once a restore is complete and checked. Without it, the restore
/// stopped partway, and the next start deletes it.
const RESTORE_READY: &str = "ready";

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
         Choose another folder or bucket."
    )]
    WrongKey(String),

    #[error(
        "Enter the S3 address from your storage provider, starting with https://, \
         such as https://s3.us-west-004.backblazeb2.com."
    )]
    BadEndpoint,

    #[error(
        "Bucket names are 3 to 63 lowercase letters, numbers, periods, and hyphens. \
         Check the name your storage provider shows."
    )]
    BadBucket,

    #[error("Enter the key ID and the secret key from your storage provider.")]
    NoKeys,

    #[error(
        "{0} didn't accept the key ID and secret key. Check them, and check that the \
         key is allowed to read and write the bucket."
    )]
    KeysRefused(String),

    #[error(
        "There's no bucket named {bucket} at {host}. Check the name, or create the \
         bucket at your storage provider first."
    )]
    NoBucket { bucket: String, host: String },

    /// `problem` is already in plain words, from `why_unreachable`.
    #[error("Accordion couldn't reach {host}: {problem}. {reach}")]
    Unreachable {
        host: String,
        problem: String,
        reach: Reach,
    },

    #[error(
        "Enter the other computer's name or address, such as nas.local or 192.168.1.50. \
         Add :port at the end if its SSH port isn't 22."
    )]
    BadServer,

    #[error("Enter the username Accordion signs in to the other computer with.")]
    BadUsername,

    #[error(
        "Enter the folder on the other computer, such as /volume1/backups/accordion, or \
         backups/accordion for one inside the user's home folder."
    )]
    BadRemoteFolder,

    #[error(
        "Accordion needs OpenSSH's ssh and ssh-keygen programs for backups over SFTP. \
         Install the openssh-client package, then try again."
    )]
    NoSsh,

    #[error(
        "{host} didn't accept Accordion's SSH key. Add the key shown on this page to \
         ~/.ssh/authorized_keys for {user} on {host}, then try again."
    )]
    SshKeyRefused { host: String, user: String },

    #[error(
        "{0} didn't identify itself the way it did before, so Accordion didn't send it \
         anything. If you reinstalled or replaced that computer, remove this location \
         and add it again. If you didn't, someone may be pretending to be it."
    )]
    HostKeyChanged(String),

    #[error(
        "Enter the REST server's address, starting with https://, such as \
         https://nas.local:8000/accordion. Put the username and password in their own boxes."
    )]
    BadRestAddress,

    #[error(
        "Enter both the username and the password from the REST server's password file, \
         or leave both empty if the server doesn't ask for them."
    )]
    RestLoginIncomplete,

    #[error(
        "{0} didn't accept the username and password. Check them against the REST \
         server's password file, then try again."
    )]
    RestLoginRefused(String),

    #[error(
        "{0}'s certificate is for a different name. Enter the address the certificate \
         was made for, or make a new certificate that includes this one."
    )]
    CertificateWrongName(String),

    #[error(
        "{0}'s certificate has expired, or its date is in the future. Make a new \
         certificate for the REST server, or check its clock, then try again."
    )]
    CertificateExpired(String),

    #[error("{0} isn't using HTTPS. Turn on TLS in the REST server, then try again.")]
    NotHttps(String),

    #[error(
        "{0} showed a different certificate from the one it had before, so Accordion \
         didn't send it anything. If you gave it a new certificate, remove this location \
         and add it again. If you didn't, someone may be pretending to be it."
    )]
    CertificateChanged(String),

    #[error("That backup location is already set up.")]
    TargetExists,

    #[error("{0}")]
    Restic(String),

    #[error("Accordion couldn't find that backup location.")]
    NotFound,

    #[error("Choose an hour from 0 to 23.")]
    BadHour,

    #[error("A recovery key is 30 letters. Check what you wrote down, then try again.")]
    BadKey,

    #[error(
        "The recovery key doesn't open the backup in {0}. Check what you wrote down, \
         then try again."
    )]
    KeyDoesntOpen(String),

    #[error(
        "There are no Accordion backups in {0}. Check that it's the folder or bucket \
         you chose when you set up backups."
    )]
    NoSnapshots(String),

    #[error("Accordion couldn't find that backup. Choose one from the list.")]
    NoSuchSnapshot,

    #[error(
        "The backup came back without Accordion's database, so it can't be \
         restored. Choose an earlier backup."
    )]
    NoDatabase,

    #[error(
        "The database in the backup failed SQLite's check (\"{0}\"), so it can't be \
         restored. Choose an earlier backup."
    )]
    DamagedDatabase(String),
}

/// Where a target is reached, which decides what to check when it can't be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reach {
    /// A storage provider's bucket.
    Internet,
    /// Another computer, usually at home, over SFTP or a REST server.
    Network,
}

impl fmt::Display for Reach {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Reach::Internet => {
                "Check the address, and that this server is connected to the internet, then \
                 try again."
            }
            Reach::Network => {
                "Check the address, and that the other computer is turned on and connected \
                 to the network, then try again."
            }
        })
    }
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
    pub kind: TargetKind,
    /// In restic's form, such as a folder's path or
    /// `s3:https://host/bucket`.
    pub location: String,
    pub created_at: String,
    /// The target refuses deletes, so a compromised server can't delete the
    /// backups there, and Accordion never removes old ones.
    pub append_only: bool,
}

impl Target {
    /// The host and bucket name, for a bucket.
    pub fn bucket(&self) -> Option<(&str, &str)> {
        match self.kind {
            TargetKind::S3 => bucket_parts(&self.location),
            TargetKind::Folder | TargetKind::Sftp | TargetKind::Rest => None,
        }
    }

    /// The username, host, and folder, for a folder over SFTP.
    pub fn sftp(&self) -> Option<(&str, &str, &str)> {
        match self.kind {
            TargetKind::Sftp => sftp_parts(&self.location),
            TargetKind::Folder | TargetKind::S3 | TargetKind::Rest => None,
        }
    }

    /// The address, such as `https://nas.local:8000/accordion`, for a REST
    /// server.
    pub fn rest(&self) -> Option<&str> {
        match self.kind {
            TargetKind::Rest => self.location.strip_prefix("rest:"),
            TargetKind::Folder | TargetKind::S3 | TargetKind::Sftp => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TargetKind {
    Folder,
    S3,
    Sftp,
    Rest,
}

impl TargetKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Folder => "folder",
            Self::S3 => "s3",
            Self::Sftp => "sftp",
            Self::Rest => "rest",
        }
    }
}

/// Where backups go, or where to restore from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Place {
    /// A folder's full path, such as one on a USB disk.
    Folder(PathBuf),
    S3(S3Bucket),
    Sftp(SftpFolder),
    Rest(RestServer),
}

/// A folder on another computer, reached over SFTP, as the owner types it
/// in. Accordion signs in with its own SSH key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SftpFolder {
    /// A name or address, with `:port` if SSH isn't on port 22.
    pub server: String,
    pub username: String,
    /// A full path, or one inside the user's home folder.
    pub folder: String,
}

/// A checked [`SftpFolder`].
struct SftpServer {
    user: String,
    host: String,
    port: u16,
    folder: String,
}

impl SftpServer {
    fn location(&self) -> String {
        format!("sftp:{}@{}:{}", self.user, self.host, self.folder)
    }

    /// The command restic connects with. ssh reads only Accordion's key and
    /// the given host keys, never asks anything, and with `accept-new`
    /// records the key of a server it hasn't seen.
    fn ssh(&self, known_hosts: &str, check: &str) -> String {
        format!(
            "ssh -F /dev/null -i {SSH_KEY} -o IdentitiesOnly=yes -o BatchMode=yes \
             -o LogLevel=ERROR -o ConnectTimeout=30 -o ServerAliveInterval=60 \
             -o UserKnownHostsFile={known_hosts} -o StrictHostKeyChecking={check} \
             -p {} {}@{} -s sftp",
            self.port, self.user, self.host
        )
    }
}

/// An S3 bucket at a storage provider, as the owner types it in.
#[derive(Clone, PartialEq, Eq)]
pub struct S3Bucket {
    /// The provider's S3 address, such as
    /// `https://s3.us-west-004.backblazeb2.com`.
    pub endpoint: String,
    pub bucket: String,
    pub key_id: String,
    pub secret_key: String,
}

impl fmt::Debug for S3Bucket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Bucket")
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .field("key_id", &self.key_id)
            .finish_non_exhaustive()
    }
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

/// A backup in a location, for the owner to choose from when restoring.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    /// restic's ID for it.
    pub id: String,
    pub time: SystemTime,
    /// How much it holds, before deduplication, if restic recorded it.
    pub size: Option<u64>,
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
    /// How the last night's work went, since the server started. Updates
    /// install right after it (decision 12 in the suite's docs).
    pub last_nightly: Option<NightlyRun>,
}

/// One night's backup, weekly upkeep, and restore tests, once they're all
/// done.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NightlyRun {
    pub finished_at: SystemTime,
    pub backup: NightlyBackup,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NightlyBackup {
    /// Every location got a backup.
    Worked,
    /// At least one location didn't, or the backup couldn't start.
    Failed,
    /// Backups are off, or there's nowhere for them to go.
    Off,
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
    /// For an append-only target, the space its backups use, as of the
    /// last weekly upkeep.
    pub stored_bytes: Option<u64>,
    /// For an append-only target, its oldest backup, as of the last weekly
    /// upkeep.
    pub oldest_backup: Option<SystemTime>,
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
    running: Arc<Mutex<()>>,
    status: watch::Sender<BackupStatus>,
    /// Wakes [`Backup::maintain`] when the backup hour changes.
    rescheduled: Notify,
}

/// Holds the `running` lock, and shows a run in progress in the status
/// until it's dropped.
/// Owned, so a run started in the background can hold it.
struct Busy {
    _lock: OwnedMutexGuard<()>,
    backup: Backup,
}

impl Drop for Busy {
    fn drop(&mut self) {
        self.backup
            .inner
            .status
            .send_modify(|status| status.running = false);
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
        // restic runs inside the data folder, so every path it's given must
        // be a full path.
        let data_dir = &std::fs::canonicalize(data_dir)?;
        let dir = data_dir.join("backup");
        for folder in [&dir, &dir.join("cache"), &dir.join("temp")] {
            std::fs::create_dir_all(folder)?;
        }
        for private in [SSH_DIR, CERT_DIR] {
            let private = data_dir.join(private);
            std::fs::create_dir_all(&private)?;
            std::fs::set_permissions(&private, std::fs::Permissions::from_mode(0o700))?;
        }
        let backup = Self {
            inner: Arc::new(Inner {
                db,
                storage,
                data_dir: data_dir.to_owned(),
                restic: Restic {
                    program: restic,
                    home: data_dir.to_owned(),
                    key_file: dir.join("recovery-key"),
                    cache: dir.join("cache"),
                    temp: dir.join("temp"),
                },
                dir,
                running: Arc::new(Mutex::new(())),
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
    async fn busy(&self) -> Busy {
        let lock = Arc::clone(&self.inner.running).lock_owned().await;
        self.mark_busy(lock)
    }

    /// The `running` lock if nothing else holds it.
    fn try_busy(&self) -> Result<Busy, Error> {
        let lock = Arc::clone(&self.inner.running)
            .try_lock_owned()
            .map_err(|_| Error::Busy)?;
        Ok(self.mark_busy(lock))
    }

    fn mark_busy(&self, lock: OwnedMutexGuard<()>) -> Busy {
        self.inner
            .status
            .send_modify(|status| status.running = true);
        Busy {
            _lock: lock,
            backup: self.clone(),
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
                    .prepare(&format!(
                        "SELECT {TARGET_COLUMNS} FROM backup_targets t ORDER BY t.created_at, t.id"
                    ))?
                    .query_map([], target_from_row)?
                    .collect::<Result<_, _>>()?;
                let mut statuses = Vec::with_capacity(targets.len());
                for target in targets {
                    let latest = |kind: RunKind| latest_outcome(conn, target.id, kind);
                    let (stored_bytes, oldest_backup): (Option<i64>, Option<String>) = conn
                        .query_row(
                            "SELECT stored_bytes, oldest_backup_at FROM backup_targets
                             WHERE id = ?1",
                            [target.id.as_uuid()],
                            |row| Ok((row.get(0)?, row.get(1)?)),
                        )?;
                    statuses.push(TargetStatus {
                        stored_bytes: stored_bytes.and_then(|bytes| u64::try_from(bytes).ok()),
                        oldest_backup: oldest_backup.as_deref().and_then(time::parse),
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
            // Stopping partway stops restic, which leaves each repository
            // usable. Nothing is recorded, so the next start catches up.
            tokio::select! {
                () = &mut stopped => return,
                () = self.nightly() => {}
            }
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
        let backup = match self.back_up().await {
            Ok(runs) if runs.iter().all(|run| run.ok) => NightlyBackup::Worked,
            Ok(_) => NightlyBackup::Failed,
            Err(Error::NoRecoveryKey | Error::NoTargets) => NightlyBackup::Off,
            Err(err) => {
                tracing::warn!(%err, "the nightly backup failed");
                NightlyBackup::Failed
            }
        };
        self.upkeep().await;
        self.inner.status.send_modify(|status| {
            status.last_nightly = Some(NightlyRun {
                finished_at: SystemTime::now(),
                backup,
            });
        });
    }

    /// Weekly upkeep and monthly restore tests, where they're due.
    async fn upkeep(&self) {
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
                conn.prepare(&format!(
                    "SELECT {TARGET_COLUMNS},
                            CASE WHEN t.pruned_at IS NULL OR t.pruned_at < {week_ago}
                                 THEN t.next_part END,
                            NOT EXISTS (SELECT 1 FROM backup_runs r
                                        WHERE r.target_id = t.id AND r.kind = 'restore_test'
                                          AND r.finished_at > {month_ago})
                     FROM backup_targets t
                     WHERE EXISTS (SELECT 1 FROM backup_runs r
                                   WHERE r.target_id = t.id AND r.kind = 'backup' AND r.ok = 1)
                     ORDER BY t.created_at, t.id",
                    week_ago = sql_now!("-7 days"),
                    month_ago = sql_now!("-30 days"),
                ))?
                .query_map([], |row| {
                    Ok((target_from_row(row)?, row.get(6)?, row.get(7)?))
                })?
                .collect::<Result<_, _>>()
            })
            .await?)
    }

    /// Removes expired data from a target, reads back one part of what's
    /// left, and moves on to the next part for next week.
    async fn weekly(&self, target: &Target, part: u32) -> Result<(), Error> {
        self.prune(target).await?;
        // Read again: pruning may have found the target refuses deletes.
        let target = &self.target(target.id).await?;
        if target.append_only
            && let Err(err) = self.measure(target).await
        {
            tracing::warn!(target = %target.name, %err, "couldn't measure an append-only target");
        }
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

    /// Adds a place for backups: a folder, such as one on a USB disk, an S3
    /// bucket, a folder over SFTP, or a REST server. Sets up a new repository
    /// there, or uses the one already there if the recovery key opens it.
    /// For a REST server, it also finds out whether the server is
    /// append-only.
    pub async fn add_target(&self, name: &str, place: &Place) -> Result<Target, Error> {
        let name = name.trim().to_owned();
        if name.is_empty() || name.chars().count() > 100 {
            return Err(Error::BadName);
        }
        let (kind, repository, port) = match place {
            Place::Folder(path) => (TargetKind::Folder, self.new_folder(path).await?, None),
            Place::S3(bucket) => (TargetKind::S3, s3_repository(bucket)?, None),
            Place::Sftp(folder) => {
                let (repository, port) = self.new_sftp(folder).await?;
                (TargetKind::Sftp, repository, Some(port))
            }
            Place::Rest(server) => (TargetKind::Rest, rest_repository(server)?, None),
        };
        if self.recovery_key().await?.is_none() {
            return Err(Error::NoRecoveryKey);
        }
        check_answers(place, ANSWER_TIME).await?;

        let _busy = self.busy().await;
        let restic = &self.inner.restic;
        let mut repository = repository;
        let certificate = match kind {
            TargetKind::Rest => self.trust_rest(&mut repository, restic).await?,
            TargetKind::Folder | TargetKind::S3 | TargetKind::Sftp => None,
        };
        match restic
            .run_once(&repository, None, &args(["cat", "config"]))
            .await
        {
            Ok(_) => {}
            Err(Failure::NoRepository) => {
                restic
                    .run_once(&repository, None, &args(["init", "--json"]))
                    .await
                    .map_err(|failure| setup_failure(&failure, &repository))?;
            }
            Err(Failure::WrongKey) => return Err(Error::WrongKey(place_name(&repository))),
            Err(failure) => return Err(setup_failure(&failure, &repository)),
        }
        // ssh recorded the server's host key on the way, to check from now on.
        let host_key = match kind {
            TargetKind::Sftp => Some(self.new_host_key().await?),
            TargetKind::Folder | TargetKind::S3 | TargetKind::Rest => None,
        };
        let append_only = match kind {
            TargetKind::Rest => self.refuses_deletes(&repository).await?,
            TargetKind::Folder | TargetKind::S3 | TargetKind::Sftp => false,
        };

        let id = BackupTargetId::generate();
        let target = self
            .inner
            .db
            .write(move |tx| {
                let taken: bool = tx.query_row(
                    "SELECT EXISTS (SELECT 1 FROM backup_targets WHERE location = ?1)",
                    [&repository.location],
                    |row| row.get(0),
                )?;
                if taken {
                    return Err(Error::TargetExists);
                }
                let keys = repository.keys.as_ref();
                let login = repository.login.as_ref();
                tx.execute(
                    concat!(
                        "INSERT INTO backup_targets (id, name, kind, location, created_at,
                                                     access_key_id, secret_access_key,
                                                     ssh_port, host_key, rest_username,
                                                     rest_password, certificate, append_only)
                         VALUES (?1, ?2, ?3, ?4, ",
                        sql_now!(),
                        ", ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)"
                    ),
                    rusqlite::params![
                        id.as_uuid(),
                        &name,
                        kind.as_str(),
                        &repository.location,
                        keys.map(|keys| &keys.id),
                        keys.map(|keys| &keys.secret),
                        port,
                        &host_key,
                        login.map(|login| &login.username),
                        login.map(|login| &login.password),
                        &certificate,
                        append_only,
                    ],
                )?;
                target(tx, id)?.ok_or(Error::NotFound)
            })
            .await?;
        ignore_missing(tokio::fs::remove_file(self.inner.data_dir.join(NEW_CERT)).await)?;
        self.refresh().await?;
        Ok(target)
    }

    /// Checks a folder for new backups: a full path, on another disk than
    /// the data folder, in a folder that exists. The folder itself can be
    /// new.
    async fn new_folder(&self, path: &Path) -> Result<Repository, Error> {
        if !path.is_absolute() {
            return Err(Error::NotAbsolute);
        }
        let parent = path.parent().ok_or(Error::InsideData)?;
        let parent = tokio::fs::canonicalize(parent)
            .await
            .map_err(|_| Error::NoSuchFolder(parent.to_owned()))?;
        let data_dir = &self.inner.data_dir;
        let path = parent.join(path.file_name().ok_or(Error::InsideData)?);
        if path.starts_with(data_dir) || data_dir.starts_with(&path) {
            return Err(Error::InsideData);
        }
        folder_repository(&path)
    }

    /// Checks a folder over SFTP, for adding or restoring from. ssh records
    /// the server's host key in [`NEW_HOST_KEY`] when it first connects.
    async fn new_sftp(&self, folder: &SftpFolder) -> Result<(Repository, u16), Error> {
        let server = sftp_server(folder)?;
        self.ready_ssh_key().await?;
        ignore_missing(tokio::fs::remove_file(self.inner.data_dir.join(NEW_HOST_KEY)).await)?;
        let repository = Repository {
            ssh: Some(server.ssh(NEW_HOST_KEY, "accept-new")),
            ..Repository::at(server.location())
        };
        Ok((repository, server.port))
    }

    /// Trusts a REST server's certificate the first time Accordion connects,
    /// if the system doesn't trust it already. Points `repository` at the
    /// certificate, and returns it in PEM form to record.
    async fn trust_rest(
        &self,
        repository: &mut Repository,
        restic: &Restic,
    ) -> Result<Option<String>, Error> {
        let failure = match restic
            .run_once(repository, None, &args(["cat", "config"]))
            .await
        {
            Err(failure) if rest::problem(&failure) == Some(Problem::Untrusted) => failure,
            // Anything else comes up again when the repository is opened.
            _ => return Ok(None),
        };
        let address = rest::location_address(&repository.location).ok_or(Error::BadRestAddress)?;
        let certificate = rest::certificate(&address.host, address.port)
            .await
            .map_err(|problem| Error::Unreachable {
                host: address.host.clone(),
                problem: why_unreachable(&problem),
                reach: Reach::Network,
            })?;
        tracing::info!(
            host = %address.host,
            fingerprint = %rest::fingerprint(&certificate),
            after = ?failure,
            "trusting a REST server's certificate"
        );
        let pem = rest::pem(&certificate);
        self.write_private(NEW_CERT, pem.clone()).await?;
        repository.cacert = Some(self.inner.data_dir.join(NEW_CERT));
        Ok(Some(pem))
    }

    /// Whether a repository refuses deletes, as an append-only REST server
    /// does. Backs up an empty folder with its own tag, then tries to remove
    /// that snapshot. On an append-only server, the empty snapshot stays.
    async fn refuses_deletes(&self, repository: &Repository) -> Result<bool, Error> {
        let empty = self.inner.dir.join("append-only-test");
        ignore_missing(tokio::fs::remove_dir_all(&empty).await)?;
        tokio::fs::create_dir(&empty).await?;
        let restic = &self.inner.restic;
        let mut command = args(["backup", "--json", "--host", TAG, "--tag", PROBE_TAG]);
        command.push(empty.clone().into());
        let result = restic.run_once(repository, None, &command).await;
        ignore_missing(tokio::fs::remove_dir_all(&empty).await)?;
        let stdout = result.map_err(|failure| setup_failure(&failure, repository))?;
        let snapshot = restic::message(&stdout, "summary")
            .and_then(|summary| summary["snapshot_id"].as_str().map(str::to_owned))
            .ok_or_else(|| {
                Error::Restic("restic finished without saying which snapshot it made.".into())
            })?;
        match restic
            .run_once(repository, None, &args(["forget", &snapshot]))
            .await
        {
            Ok(_) => Ok(false),
            Err(failure) if rest::problem(&failure) == Some(Problem::Forbidden) => Ok(true),
            Err(failure) => Err(setup_failure(&failure, repository)),
        }
    }

    /// The host key ssh recorded for a new server.
    async fn new_host_key(&self) -> Result<String, Error> {
        let path = self.inner.data_dir.join(NEW_HOST_KEY);
        let host_key = tokio::fs::read_to_string(&path).await?;
        tokio::fs::remove_file(&path).await?;
        if host_key.trim().is_empty() {
            return Err(Error::Restic(
                "ssh connected without recording the server's host key.".into(),
            ));
        }
        Ok(host_key)
    }

    /// Accordion's SSH public key, for the owner to add to
    /// `~/.ssh/authorized_keys` on an SFTP server. Creates the key the first
    /// time.
    pub async fn ssh_public_key(&self) -> Result<String, Error> {
        if let Some((_, public)) = self.ssh_key().await? {
            return Ok(public);
        }
        let new = self.inner.data_dir.join(SSH_DIR).join("new-key");
        let new_public = new.with_extension("pub");
        for path in [&new, &new_public] {
            ignore_missing(tokio::fs::remove_file(path).await)?;
        }
        let mut command = tokio::process::Command::new("ssh-keygen");
        command.env_clear();
        if let Some(path) = std::env::var_os("PATH") {
            command.env("PATH", path);
        }
        let output = command
            .args([
                "-q",
                "-t",
                "ed25519",
                "-N",
                "",
                "-C",
                "accordion-backups",
                "-f",
            ])
            .arg(&new)
            .stdin(std::process::Stdio::null())
            .output()
            .await
            .map_err(|err| match err.kind() {
                io::ErrorKind::NotFound => Error::NoSsh,
                _ => Error::Io(err),
            })?;
        if !output.status.success() {
            return Err(Error::Restic(format!(
                "ssh-keygen couldn't create Accordion's SSH key: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        let private = tokio::fs::read_to_string(&new).await?;
        let public = tokio::fs::read_to_string(&new_public).await?;
        for path in [&new, &new_public] {
            tokio::fs::remove_file(path).await?;
        }
        let public = public.trim().to_owned();
        // If two requests raced, the first key is kept.
        self.inner
            .db
            .write(move |tx| {
                tx.execute(
                    "UPDATE backup_settings SET ssh_private_key = ?1, ssh_public_key = ?2
                     WHERE id = 1 AND ssh_private_key IS NULL",
                    (&private, &public),
                )
            })
            .await?;
        let (_, public) = self.ssh_key().await?.ok_or(Error::NotFound)?;
        Ok(public)
    }

    async fn ssh_key(&self) -> Result<Option<(String, String)>, Error> {
        Ok(self
            .inner
            .db
            .read(|conn| {
                conn.query_row(
                    "SELECT ssh_private_key, ssh_public_key FROM backup_settings WHERE id = 1",
                    [],
                    |row| {
                        Ok(match (row.get(0)?, row.get(1)?) {
                            (Some(private), Some(public)) => Some((private, public)),
                            _ => None,
                        })
                    },
                )
            })
            .await?)
    }

    /// Puts Accordion's SSH key where ssh reads it, creating the key if
    /// there isn't one. It's written each time, so a restored server's key
    /// is the one used.
    async fn ready_ssh_key(&self) -> Result<(), Error> {
        self.ssh_public_key().await?;
        let (private, _) = self.ssh_key().await?.ok_or(Error::NotFound)?;
        self.write_private(SSH_KEY, private).await
    }

    /// Writes a file in the data folder that only Accordion can read.
    async fn write_private(&self, path: &str, contents: String) -> Result<(), Error> {
        let path = self.inner.data_dir.join(path);
        tokio::task::spawn_blocking(move || {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o600)
                .open(&path)?;
            // ssh refuses a key that others can read.
            file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
            file.write_all(contents.as_bytes())?;
            file.sync_all()
        })
        .await
        .expect("writing a private file panicked")?;
        Ok(())
    }

    async fn target(&self, id: BackupTargetId) -> Result<Target, Error> {
        self.inner
            .db
            .read(move |conn| target(conn, id))
            .await?
            .ok_or(Error::NotFound)
    }

    /// Every place backups go, oldest first.
    pub async fn targets(&self) -> Result<Vec<Target>, Error> {
        Ok(self
            .inner
            .db
            .read(|conn| {
                conn.prepare(&format!(
                    "SELECT {TARGET_COLUMNS} FROM backup_targets t ORDER BY t.created_at, t.id"
                ))?
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
        let busy = self.try_busy()?;
        self.back_up_holding(busy).await
    }

    /// Starts [`Backup::back_up`] in the background. The status shows it
    /// running before this returns, so a page that's watching never sees it
    /// idle in between.
    pub fn start_back_up(&self) -> Result<(), Error> {
        let busy = self.try_busy()?;
        let backup = self.clone();
        tokio::spawn(async move {
            if let Err(err) = backup.back_up_holding(busy).await {
                tracing::warn!(%err, "a backup started by hand failed");
            }
        });
        Ok(())
    }

    async fn back_up_holding(&self, _busy: Busy) -> Result<Vec<Run>, Error> {
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
        let mut run = Run::new(target.id, RunKind::Backup, started_at);
        let repository = match self.repository(target).await {
            Ok(repository) => repository,
            Err(err) => return run.failed(err.to_string()),
        };
        let result = restic
            .run(&repository, Some(&self.inner.data_dir), &command)
            .await;
        let stdout = match result {
            Ok(stdout) => stdout,
            Err(failure) => return run.failed(describe(&failure, &repository)),
        };
        let summary = restic::message(&stdout, "summary").unwrap_or_default();
        run.snapshot = summary["snapshot_id"].as_str().map(str::to_owned);
        run.bytes_added = summary["data_added"].as_u64();
        if run.snapshot.is_none() {
            return run.failed("restic finished without saying which snapshot it made.".into());
        }
        // An append-only target keeps everything.
        if target.append_only {
            return run.finish();
        }
        let mut forget = args(["forget", "--json", "--group-by", "host,tags", "--tag", TAG]);
        for (flag, count) in KEEP {
            forget.extend(args([flag, count]));
        }
        match restic.run(&repository, None, &forget).await {
            Ok(_) => {}
            Err(failure) if refused_delete(&failure, &repository) => {
                self.became_append_only(target).await;
            }
            Err(failure) => {
                // The backup itself worked, so this is only worth a warning.
                tracing::warn!(
                    target = %target.name,
                    problem = describe(&failure, &repository),
                    "couldn't remove old snapshots"
                );
            }
        }
        run.finish()
    }

    /// Records that a target now refuses deletes, such as a REST server
    /// switched to append-only after it was added.
    async fn became_append_only(&self, target: &Target) {
        let id = target.id;
        let marked = self
            .inner
            .db
            .write(move |tx| {
                tx.execute(
                    "UPDATE backup_targets SET append_only = 1 WHERE id = ?1",
                    [id.as_uuid()],
                )
            })
            .await;
        match marked {
            Ok(_) => tracing::info!(
                target = %target.name,
                "the target refused to delete old snapshots, so it's treated as append-only now"
            ),
            Err(err) => {
                tracing::warn!(target = %target.name, %err, "couldn't record an append-only target")
            }
        }
        if let Err(err) = self.refresh().await {
            tracing::warn!(%err, "couldn't refresh the backup status");
        }
    }

    /// Removes the data that only expired snapshots used. It takes a while,
    /// so it runs weekly rather than after every backup. An append-only
    /// target keeps everything, so there's nothing to do there.
    pub async fn prune(&self, target: &Target) -> Result<(), Error> {
        if target.append_only {
            return Ok(());
        }
        let _busy = self.busy().await;
        let repository = self.repository(target).await?;
        match self
            .inner
            .restic
            .run(&repository, None, &args(["prune"]))
            .await
        {
            Ok(_) => Ok(()),
            Err(failure) if refused_delete(&failure, &repository) => {
                self.became_append_only(target).await;
                Ok(())
            }
            Err(failure) => Err(Error::Restic(describe(&failure, &repository))),
        }
    }

    /// Measures an append-only target, which only grows: the space its
    /// backups use, and its oldest backup.
    async fn measure(&self, target: &Target) -> Result<(), Error> {
        let _busy = self.busy().await;
        let repository = self.repository(target).await?;
        let restic = &self.inner.restic;
        let failed = |failure| Error::Restic(describe(&failure, &repository));
        let stats = restic
            .run(
                &repository,
                None,
                &args(["stats", "--json", "--mode", "raw-data"]),
            )
            .await
            .map_err(failed)?;
        let stored_bytes = serde_json::from_str::<serde_json::Value>(stats.trim())
            .ok()
            .and_then(|stats| stats["total_size"].as_u64())
            .and_then(|bytes| i64::try_from(bytes).ok());
        let snapshots = restic
            .run(
                &repository,
                None,
                &args(["snapshots", "--json", "--tag", TAG]),
            )
            .await
            .map_err(failed)?;
        let oldest = parse_snapshots(&snapshots)
            .and_then(|snapshots| snapshots.into_iter().map(|snapshot| snapshot.time).min())
            .and_then(|time| time.duration_since(SystemTime::UNIX_EPOCH).ok())
            .map(time::format);
        let id = target.id;
        self.inner
            .db
            .write(move |tx| {
                tx.execute(
                    "UPDATE backup_targets SET stored_bytes = ?2, oldest_backup_at = ?3
                     WHERE id = ?1",
                    (id.as_uuid(), stored_bytes, &oldest),
                )
            })
            .await?;
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
        let repository = match self.repository(target).await {
            Ok(repository) => repository,
            Err(err) => return run.failed(err.to_string()),
        };
        match self.inner.restic.run(&repository, None, &command).await {
            Ok(_) => run.finish(),
            Err(failure) => run.failed(describe(&failure, &repository)),
        }
    }

    /// Restores the newest database copy and a sample of stored files from
    /// a target into a temporary folder, and checks them: the database with
    /// SQLite's integrity check, and each file against its SHA-256 hash.
    pub async fn test_restore(&self, target: &Target) -> Result<Run, Error> {
        let busy = self.busy().await;
        self.test_restore_holding(target, busy).await
    }

    /// Starts [`Backup::test_restore`] in the background, like
    /// [`Backup::start_back_up`].
    pub fn start_test_restore(&self, target: Target) -> Result<(), Error> {
        let busy = self.try_busy()?;
        let backup = self.clone();
        tokio::spawn(async move {
            if let Err(err) = backup.test_restore_holding(&target, busy).await {
                tracing::warn!(%err, "a restore test started by hand failed to run");
            }
        });
        Ok(())
    }

    async fn test_restore_holding(&self, target: &Target, _busy: Busy) -> Result<Run, Error> {
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
        let repository = &self
            .repository(target)
            .await
            .map_err(|err| err.to_string())?;
        let restic = &self.inner.restic;
        let restore = |includes: Vec<String>| {
            let mut command = args(["restore", &snapshot, "--target"]);
            command.push(place.into());
            for include in includes {
                command.extend(args(["--include", &include]));
            }
            async move {
                restic
                    .run(repository, None, &command)
                    .await
                    .map_err(|failure| describe(&failure, repository))
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

    /// The Accordion backups in `place`, newest first, if `key` opens them.
    /// For restoring to a fresh server.
    pub async fn find_snapshots(&self, place: &Place, key: &str) -> Result<Vec<Snapshot>, Error> {
        let _busy = self.busy().await;
        let (repository, restic) = self.prepare_restore(place, key).await?;
        let result = restic
            .run_once(
                &repository,
                None,
                &args(["snapshots", "--json", "--tag", TAG]),
            )
            .await;
        // Nothing is restored yet, so only the typed key is in there.
        let _ = tokio::fs::remove_dir_all(self.restore_dir()).await;
        let stdout = result.map_err(|failure| restore_failure(&failure, &repository))?;
        let mut snapshots = parse_snapshots(&stdout).ok_or_else(|| {
            Error::Restic("restic listed the backups in a way Accordion didn't expect.".into())
        })?;
        if snapshots.is_empty() {
            return Err(Error::NoSnapshots(place_name(&repository)));
        }
        snapshots.sort_by_key(|snapshot| std::cmp::Reverse(snapshot.time));
        Ok(snapshots)
    }

    /// Restores `snapshot` from `place` into `restore/` in the
    /// data folder, and checks its database. `progress` hears how much is
    /// done, from 0 to 1. The restored data waits there until the server
    /// starts again, when [`apply_restore`] moves it into place. Returns the
    /// folder holding it, such as for reading the server's restored name.
    ///
    /// Only the server's setup should call this, before there's anything to
    /// lose: the restore replaces everything.
    pub async fn restore(
        &self,
        place: &Place,
        key: &str,
        snapshot: &str,
        progress: impl FnMut(f64) + Send,
    ) -> Result<PathBuf, Error> {
        // An ID from the list, so it can't be mistaken for one of restic's
        // options.
        if snapshot.len() < 8 || !snapshot.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(Error::NoSuchSnapshot);
        }
        let _busy = self.busy().await;
        let (repository, restic) = self.prepare_restore(place, key).await?;
        let result = self
            .restore_snapshot(&restic, &repository, snapshot, progress)
            .await;
        if result.is_err() {
            let _ = tokio::fs::remove_dir_all(self.restore_dir()).await;
        }
        result
    }

    async fn restore_snapshot(
        &self,
        restic: &Restic,
        repository: &Repository,
        snapshot: &str,
        mut progress: impl FnMut(f64) + Send,
    ) -> Result<PathBuf, Error> {
        let dir = self.restore_dir();
        let place = dir.join("snapshot");
        let mut command = args(["restore", snapshot, "--json", "--target"]);
        command.push(place.clone().into());
        restic
            .run_streaming(repository, None, &command, |line| {
                if let Ok(status) = serde_json::from_str::<serde_json::Value>(line)
                    && status["message_type"] == "status"
                    && let Some(done) = status["percent_done"].as_f64()
                {
                    progress(done.clamp(0.0, 1.0));
                }
            })
            .await
            .map_err(|failure| match failure {
                Failure::Failed { message, .. } if message.contains("no matching ID found") => {
                    Error::NoSuchSnapshot
                }
                failure => restore_failure(&failure, repository),
            })?;

        let database = place.join("backup").join("accordion.db");
        tokio::task::spawn_blocking(move || {
            if !database.is_file() {
                return Err(Error::NoDatabase);
            }
            let conn = Connection::open(&database)?;
            let check: String = conn.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
            if check != "ok" {
                return Err(Error::DamagedDatabase(check));
            }
            Ok(())
        })
        .await
        .expect("checking the restored database panicked")?;

        let ready = dir.join(RESTORE_READY);
        tokio::task::spawn_blocking(move || std::fs::File::create(ready)?.sync_all())
            .await
            .expect("marking the restore ready panicked")?;
        Ok(place)
    }

    fn restore_dir(&self) -> PathBuf {
        self.inner.data_dir.join(RESTORE_DIR)
    }

    /// Checks the place and key for a restore, and starts an empty
    /// `restore/` holding the key, which restic reads it from. Returns the
    /// repository and a restic that uses that key.
    async fn prepare_restore(
        &self,
        place: &Place,
        key: &str,
    ) -> Result<(Repository, Restic), Error> {
        let letters = random::parse_letters(key, KEY_LEN).ok_or(Error::BadKey)?;
        let repository = match place {
            Place::Folder(path) => {
                if !path.is_absolute() {
                    return Err(Error::NotAbsolute);
                }
                let path = tokio::fs::canonicalize(path)
                    .await
                    .map_err(|_| Error::NoSuchFolder(path.to_owned()))?;
                folder_repository(&path)?
            }
            Place::S3(bucket) => s3_repository(bucket)?,
            Place::Sftp(folder) => self.new_sftp(folder).await?.0,
            Place::Rest(server) => rest_repository(server)?,
        };
        check_answers(place, ANSWER_TIME).await?;
        let dir = self.restore_dir();
        let key_file = dir.join("recovery-key");
        let path = key_file.clone();
        tokio::task::spawn_blocking(move || {
            ignore_missing(std::fs::remove_dir_all(&dir))?;
            std::fs::create_dir(&dir)?;
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&path)?;
            file.write_all(letters.as_bytes())?;
            file.sync_all()
        })
        .await
        .expect("preparing the restore panicked")?;
        let restic = Restic {
            program: self.inner.restic.program.clone(),
            home: self.inner.data_dir.clone(),
            key_file,
            cache: self.inner.restic.cache.clone(),
            temp: self.inner.restic.temp.clone(),
        };
        let mut repository = repository;
        if let Place::Rest(_) = place {
            // The restored database has the certificate for later backups.
            self.trust_rest(&mut repository, &restic).await?;
        }
        Ok((repository, restic))
    }

    /// The repository for a target, with its keys if it's a bucket, or how to
    /// connect if it's over SFTP.
    async fn repository(&self, target: &Target) -> Result<Repository, Error> {
        match target.kind {
            TargetKind::Sftp => return self.sftp_repository(target).await,
            TargetKind::Rest => return self.rest_target_repository(target).await,
            TargetKind::Folder | TargetKind::S3 => {}
        }
        let keys = match target.kind {
            TargetKind::Folder | TargetKind::Sftp | TargetKind::Rest => None,
            TargetKind::S3 => {
                let id = target.id;
                self.inner
                    .db
                    .read(move |conn| {
                        conn.query_row(
                            "SELECT access_key_id, secret_access_key FROM backup_targets
                             WHERE id = ?1",
                            [id.as_uuid()],
                            |row| {
                                Ok(Keys {
                                    id: row.get(0)?,
                                    secret: row.get(1)?,
                                })
                            },
                        )
                        .optional()
                    })
                    .await?
            }
        };
        Ok(Repository {
            keys,
            ..Repository::at(target.location.clone())
        })
    }

    /// Connects to a REST target with its username and password, trusting
    /// only the certificate pinned when it was added, if there is one.
    async fn rest_target_repository(&self, target: &Target) -> Result<Repository, Error> {
        let id = target.id;
        let (login, certificate) = self
            .inner
            .db
            .read(move |conn| {
                conn.query_row(
                    "SELECT rest_username, rest_password, certificate FROM backup_targets
                     WHERE id = ?1",
                    [id.as_uuid()],
                    |row| {
                        Ok((
                            Login {
                                username: row.get(0)?,
                                password: row.get(1)?,
                            },
                            row.get::<_, Option<String>>(2)?,
                        ))
                    },
                )
            })
            .await?;
        let cacert = match certificate {
            Some(certificate) => {
                let path = format!("{CERT_DIR}/{id}.pem");
                self.write_private(&path, certificate).await?;
                Some(self.inner.data_dir.join(path))
            }
            None => None,
        };
        Ok(Repository {
            login: Some(login),
            cacert,
            ..Repository::at(target.location.clone())
        })
    }

    /// Connects to an SFTP target only if it still has the host key it had
    /// when it was added.
    async fn sftp_repository(&self, target: &Target) -> Result<Repository, Error> {
        let id = target.id;
        let (port, host_key): (u16, String) = self
            .inner
            .db
            .read(move |conn| {
                conn.query_row(
                    "SELECT ssh_port, host_key FROM backup_targets WHERE id = ?1",
                    [id.as_uuid()],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
            })
            .await?;
        let (user, host, folder) = sftp_parts(&target.location).ok_or(Error::NotFound)?;
        let server = SftpServer {
            user: user.to_owned(),
            host: host.to_owned(),
            port,
            folder: folder.to_owned(),
        };
        self.ready_ssh_key().await?;
        let known_hosts = format!("{SSH_DIR}/{id}.known_hosts");
        self.write_private(&known_hosts, host_key).await?;
        Ok(Repository {
            ssh: Some(server.ssh(&known_hosts, "yes")),
            ..Repository::at(target.location.clone())
        })
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

/// What a restic failure means for the admin.
fn describe(failure: &Failure, repository: &Repository) -> String {
    let place = place_name(repository);
    if repository.login.is_some()
        && let Some(problem) = rest::problem(failure)
        && let Some(error) = rest_error(&problem, repository)
    {
        return error.to_string();
    }
    match failure {
        Failure::Start(err) => format!(
            "Accordion couldn't start restic, the program that makes backups: {err}. \
             Reinstall Accordion, then try again."
        ),
        Failure::NoRepository if repository.keys.is_some() => format!(
            "There's no backup in {place}. Check that the bucket still exists at your \
             storage provider, then try again."
        ),
        Failure::NoRepository if repository.ssh.is_some() || repository.login.is_some() => {
            format!(
                "There's no backup in {place}. Check that the folder is still there, then \
                 try again."
            )
        }
        Failure::NoRepository => format!(
            "There's no backup in {place}. If it's on a USB disk, check that the \
             disk is plugged in, then try again."
        ),
        Failure::Locked => format!(
            "Another program is using the backup in {place}. Try again in a few \
             minutes."
        ),
        Failure::WrongKey => {
            format!("The backup in {place} is locked with a different recovery key.")
        }
        Failure::Retrying(problem) => format!(
            "Accordion couldn't reach {place}: {}. {}",
            why_unreachable(problem),
            if repository.keys.is_some() {
                Reach::Internet
            } else {
                Reach::Network
            }
        ),
        Failure::Ssh(problem) => {
            let (user, host, _) = sftp_parts(&repository.location).unwrap_or(("", "", ""));
            match problem {
                SshProblem::KeyRefused => format!(
                    "{host} didn't accept Accordion's SSH key. Check that the key from the \
                     backups page is still in ~/.ssh/authorized_keys for {user} on {host}."
                ),
                SshProblem::HostKeyChanged => Error::HostKeyChanged(host.to_owned()).to_string(),
                SshProblem::Unreachable(problem) => Error::Unreachable {
                    host: host.to_owned(),
                    problem: why_unreachable(problem),
                    reach: Reach::Network,
                }
                .to_string(),
                SshProblem::Other(problem) => {
                    format!("Accordion couldn't connect to {host} over SFTP: {problem}")
                }
            }
        }
        Failure::Failed { code, message, .. } => {
            let code =
                code.map_or_else(|| "stopped".to_owned(), |code| format!("exit code {code}"));
            format!("restic couldn't finish ({code}): {message}")
        }
    }
}

/// What a restic failure means while the owner sets up a target or looks
/// for backups, with the common mistakes in a bucket's details spelled out.
fn setup_failure(failure: &Failure, repository: &Repository) -> Error {
    if repository.login.is_some() {
        if let Some(problem) = rest::problem(failure)
            && let Some(error) = rest_error(&problem, repository)
        {
            return error;
        }
        if let (Failure::Retrying(problem), Some(address)) =
            (failure, rest::location_address(&repository.location))
        {
            return Error::Unreachable {
                host: address.host,
                problem: why_unreachable(problem),
                reach: Reach::Network,
            };
        }
        return Error::Restic(describe(failure, repository));
    }
    if let Failure::Ssh(problem) = failure
        && let Some((user, host, _)) = sftp_parts(&repository.location)
    {
        let host = host.to_owned();
        return match problem {
            SshProblem::KeyRefused => Error::SshKeyRefused {
                host,
                user: user.to_owned(),
            },
            SshProblem::HostKeyChanged => Error::HostKeyChanged(host),
            SshProblem::Unreachable(problem) => Error::Unreachable {
                host,
                problem: why_unreachable(problem),
                reach: Reach::Network,
            },
            SshProblem::Other(_) => Error::Restic(describe(failure, repository)),
        };
    }
    let Failure::Retrying(problem) = failure else {
        return Error::Restic(describe(failure, repository));
    };
    let Some((host, bucket)) = bucket_parts(&repository.location) else {
        return Error::Restic(describe(failure, repository));
    };
    let lower = problem.to_ascii_lowercase();
    if [
        "signature",
        "access denied",
        "accessdenied",
        "access key",
        "forbidden",
    ]
    .iter()
    .any(|phrase| lower.contains(phrase))
    {
        Error::KeysRefused(host.to_owned())
    } else if lower.contains("bucket does not exist") || lower.contains("nosuchbucket") {
        Error::NoBucket {
            bucket: bucket.to_owned(),
            host: host.to_owned(),
        }
    } else {
        Error::Unreachable {
            host: host.to_owned(),
            problem: why_unreachable(problem),
            reach: Reach::Internet,
        }
    }
}

/// Checks that a location on another machine answers at all, before restic
/// tries it. restic waits 30 seconds for each connection and says nothing
/// meanwhile, and adding a location can take it three tries, so someone
/// waiting on the page would see nothing happen for a minute or more.
async fn check_answers(place: &Place, within: Duration) -> Result<(), Error> {
    let Some((shown, host, port, reach)) = place_address(place) else {
        return Ok(());
    };
    // Through a proxy, as some servers reach the internet, the bucket's
    // own address isn't reachable directly, so leave the check to restic.
    let proxied = ["HTTPS_PROXY", "https_proxy", "ALL_PROXY", "all_proxy"]
        .iter()
        .any(|name| std::env::var_os(name).is_some_and(|value| !value.is_empty()));
    if reach == Reach::Internet && proxied {
        return Ok(());
    }
    let problem = match tokio::time::timeout(
        within,
        tokio::net::TcpStream::connect((host.as_str(), port)),
    )
    .await
    {
        Ok(Ok(_)) => return Ok(()),
        Ok(Err(err)) => why_unreachable(&err.to_string()),
        Err(_) => why_unreachable("didn't answer"),
    };
    Err(Error::Unreachable {
        host: shown,
        problem,
        reach,
    })
}

/// Where a place on another machine is: its name as messages show it, the
/// host and port to connect to, and whether it's on the internet. `None`
/// for a folder on this server, or an address that doesn't parse, which
/// gets its own error later.
fn place_address(place: &Place) -> Option<(String, String, u16, Reach)> {
    match place {
        Place::Folder(_) => None,
        Place::Rest(server) => {
            let address = rest::address(&server.address)?;
            let host = address.host.trim_start_matches('[').trim_end_matches(']');
            Some((
                address.host.clone(),
                host.to_owned(),
                address.port,
                Reach::Network,
            ))
        }
        Place::Sftp(folder) => {
            let server = sftp_server(folder).ok()?;
            Some((
                server.host.clone(),
                server.host,
                server.port,
                Reach::Network,
            ))
        }
        Place::S3(bucket) => {
            let endpoint = s3_endpoint(&bucket.endpoint)?;
            let (scheme, shown) = endpoint.split_once("://")?;
            let (host, port) = match shown.rsplit_once(':') {
                Some((host, port)) => (host, port.parse().ok()?),
                None => (shown, if scheme == "http" { 80 } else { 443 }),
            };
            Some((shown.to_owned(), host.to_owned(), port, Reach::Internet))
        }
    }
}

/// Why a connection failed, in plain words. restic, ssh, and rustls each
/// describe it their own way, often with the whole request in front, such
/// as `Head "https://user:***@nas:8000/config": dial tcp …: i/o timeout`.
fn why_unreachable(problem: &str) -> String {
    let lower = problem.to_ascii_lowercase();
    let has = |phrases: &[&str]| phrases.iter().any(|phrase| lower.contains(phrase));
    let reason = if has(&["timeout", "timed out", "deadline exceeded", "didn't answer"]) {
        "it didn't answer"
    } else if has(&["connection refused"]) {
        "nothing is listening on that port"
    } else if has(&[
        "no such host",
        "could not resolve hostname",
        "name or service not known",
        "temporary failure in name resolution",
    ]) {
        "no computer on the network has that name"
    } else if has(&["no route to host", "network is unreachable"]) {
        "this server has no network route to it"
    } else if has(&[
        "connection reset",
        "connection closed",
        "broken pipe",
        "eof",
    ]) {
        "it closed the connection"
    } else {
        // Drop the request, which repeats the address and can name the
        // username, and keep the reason after it.
        return problem
            .rsplit_once("\": ")
            .map_or(problem, |(_, reason)| reason)
            .trim()
            .trim_end_matches('.')
            .to_owned();
    };
    reason.to_owned()
}

/// What a restic failure means while restoring.
fn restore_failure(failure: &Failure, repository: &Repository) -> Error {
    match failure {
        Failure::WrongKey => Error::KeyDoesntOpen(place_name(repository)),
        failure => setup_failure(failure, repository),
    }
}

/// How messages name a repository: a folder's path, "the bucket b at
/// host", or "the folder f on host".
fn place_name(repository: &Repository) -> String {
    if repository.ssh.is_some()
        && let Some((_, host, folder)) = sftp_parts(&repository.location)
    {
        return format!("the folder {folder} on {host}");
    }
    if repository.login.is_some()
        && let Some(address) = rest::location_address(&repository.location)
    {
        return format!("the REST server at {}", address.url);
    }
    match bucket_parts(&repository.location) {
        Some((host, bucket)) if repository.keys.is_some() => {
            format!("the bucket {bucket} at {host}")
        }
        _ => repository.location.clone(),
    }
}

/// The username, host, and folder in an `sftp:` location.
fn sftp_parts(location: &str) -> Option<(&str, &str, &str)> {
    let (user, rest) = location.strip_prefix("sftp:")?.split_once('@')?;
    let (host, folder) = rest.split_once(':')?;
    Some((user, host, folder))
}

/// Checks what the owner typed for a folder over SFTP. Nothing in it can
/// start with a hyphen, which ssh would read as an option.
fn sftp_server(folder: &SftpFolder) -> Result<SftpServer, Error> {
    let server = folder.server.trim().to_ascii_lowercase();
    let (host, port) = match server.rsplit_once(':') {
        Some((host, port)) => (host, port.parse::<u16>().ok().filter(|&port| port > 0)),
        None => (server.as_str(), Some(22)),
    };
    let host_ok = !host.is_empty()
        && host.len() <= 253
        && host.split('.').all(|label| {
            !label.is_empty()
                && !label.starts_with('-')
                && label
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        });
    let port = port.filter(|_| host_ok).ok_or(Error::BadServer)?;
    let user = folder.username.trim();
    let user_ok = (1..=32).contains(&user.len())
        && !user.starts_with('-')
        && user
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
    if !user_ok {
        return Err(Error::BadUsername);
    }
    let path = folder.folder.trim();
    let path_ok = !path.is_empty()
        && path.len() <= 4096
        && !path.starts_with('-')
        && !path.chars().any(char::is_control);
    if !path_ok {
        return Err(Error::BadRemoteFolder);
    }
    Ok(SftpServer {
        user: user.to_owned(),
        host: host.to_owned(),
        port,
        folder: path.to_owned(),
    })
}

/// The host and bucket in an `s3:` location.
fn bucket_parts(location: &str) -> Option<(&str, &str)> {
    let rest = location.strip_prefix("s3:")?;
    let rest = rest.split_once("://").map_or(rest, |(_, rest)| rest);
    rest.split_once('/')
}

fn folder_repository(path: &Path) -> Result<Repository, Error> {
    let location = path
        .to_str()
        .ok_or(Error::Restic(
            "Accordion can only use folders whose path is valid UTF-8.".into(),
        ))?
        .to_owned();
    Ok(Repository::at(location))
}

/// Checks what the owner typed for a bucket, and puts it in restic's form.
fn s3_repository(bucket: &S3Bucket) -> Result<Repository, Error> {
    let endpoint = s3_endpoint(&bucket.endpoint).ok_or(Error::BadEndpoint)?;
    let name = bucket.bucket.trim();
    if !is_bucket_name(name) {
        return Err(Error::BadBucket);
    }
    let (id, secret) = (bucket.key_id.trim(), bucket.secret_key.trim());
    if id.is_empty() || secret.is_empty() {
        return Err(Error::NoKeys);
    }
    Ok(Repository {
        keys: Some(Keys {
            id: id.to_owned(),
            secret: secret.to_owned(),
        }),
        ..Repository::at(format!("s3:{endpoint}/{name}"))
    })
}

/// Checks what the owner typed for a REST server, and puts it in restic's
/// form. A username needs a password, and the other way around.
fn rest_repository(server: &RestServer) -> Result<Repository, Error> {
    let address = rest::address(&server.address).ok_or(Error::BadRestAddress)?;
    let username = server.username.trim();
    if username.is_empty() != server.password.is_empty() {
        return Err(Error::RestLoginIncomplete);
    }
    Ok(Repository {
        login: Some(Login {
            username: username.to_owned(),
            password: server.password.clone(),
        }),
        ..Repository::at(format!("rest:{}", address.url))
    })
}

/// Whether a failure is a REST server refusing a delete, as an append-only
/// one does.
fn refused_delete(failure: &Failure, repository: &Repository) -> bool {
    repository.login.is_some() && rest::problem(failure) == Some(Problem::Forbidden)
}

/// What a REST server's problem means, as an error to show.
fn rest_error(problem: &Problem, repository: &Repository) -> Option<Error> {
    let host = rest::location_address(&repository.location)?.host;
    Some(match problem {
        Problem::Untrusted if repository.cacert.is_some() => Error::CertificateChanged(host),
        // Accordion pins an untrusted certificate before it gets this far.
        Problem::Untrusted | Problem::Forbidden => return None,
        Problem::WrongName => Error::CertificateWrongName(host),
        Problem::Expired => Error::CertificateExpired(host),
        Problem::NotHttps => Error::NotHttps(host),
        Problem::LoginRefused => Error::RestLoginRefused(host),
    })
}

/// A provider's S3 address as `https://host`, with a port if it has one.
/// Without a scheme, it's HTTPS. Plain HTTP is only for a server on this
/// machine, such as one for testing, because anyone on the network could
/// read what it sends.
fn s3_endpoint(typed: &str) -> Option<String> {
    let typed = typed.trim().trim_end_matches('/').to_ascii_lowercase();
    let (scheme, host) = match typed.split_once("://") {
        Some((scheme, host)) => (scheme, host),
        None => ("https", typed.as_str()),
    };
    let (name, port) = match host.rsplit_once(':') {
        Some((name, port)) => (name, Some(port)),
        None => (host, None),
    };
    let name_ok = !name.is_empty()
        && name.len() <= 253
        && name.split('.').all(|label| {
            !label.is_empty()
                && label
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        });
    let port_ok = port.is_none_or(|port| port.parse::<u16>().is_ok_and(|port| port > 0));
    let local = name == "localhost" || name == "127.0.0.1";
    let scheme_ok = scheme == "https" || (scheme == "http" && local);
    (name_ok && port_ok && scheme_ok).then(|| format!("{scheme}://{host}"))
}

/// S3's rules for bucket names: 3 to 63 lowercase letters, numbers, periods,
/// and hyphens, starting and ending with a letter or number.
fn is_bucket_name(name: &str) -> bool {
    let edge = |b: Option<u8>| b.is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit());
    (3..=63).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'.' || b == b'-')
        && edge(name.bytes().next())
        && edge(name.bytes().last())
}

/// The snapshots in the output of `restic snapshots --json`.
fn parse_snapshots(stdout: &str) -> Option<Vec<Snapshot>> {
    let list: Vec<serde_json::Value> = serde_json::from_str(stdout.trim()).ok()?;
    list.iter()
        .map(|snapshot| {
            let time: jiff::Timestamp = snapshot["time"].as_str()?.parse().ok()?;
            Some(Snapshot {
                id: snapshot["id"].as_str()?.to_owned(),
                time: SystemTime::from(time),
                size: snapshot["summary"]["total_bytes_processed"].as_u64(),
            })
        })
        .collect()
}

/// Moves a finished restore into place: the database, stored contents, the
/// gateway's settings, and the recovery key. Call it when the server starts,
/// before anything opens the database. Returns whether there was one to
/// move. A restore that stopped partway is deleted instead.
pub fn apply_restore(data_dir: &Path) -> io::Result<bool> {
    let dir = data_dir.join(RESTORE_DIR);
    if !dir.exists() {
        return Ok(false);
    }
    if !dir.join(RESTORE_READY).exists() {
        std::fs::remove_dir_all(&dir)?;
        return Ok(false);
    }
    // Each step moves something only if it's still waiting in `restore/`, so
    // after a power cut partway through, the next start carries on.
    let restored = dir.join("snapshot");
    let database = restored.join("backup").join("accordion.db");
    if database.exists() {
        for name in ["accordion.db", "accordion.db-wal", "accordion.db-shm"] {
            ignore_missing(std::fs::remove_file(data_dir.join(name)))?;
        }
        std::fs::rename(&database, data_dir.join("accordion.db"))?;
    }
    for folder in ["storage/blobs", "gateway"] {
        let from = restored.join(folder);
        if from.exists() {
            let to = data_dir.join(folder);
            ignore_missing(std::fs::remove_dir_all(&to))?;
            if let Some(parent) = to.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::rename(&from, &to)?;
        }
    }
    let key = dir.join("recovery-key");
    if key.exists() {
        let backup = data_dir.join("backup");
        std::fs::create_dir_all(&backup)?;
        std::fs::rename(&key, backup.join("recovery-key"))?;
    }
    std::fs::remove_dir_all(&dir)?;
    Ok(true)
}

/// Treats removing something that isn't there as done.
fn ignore_missing(result: io::Result<()>) -> io::Result<()> {
    match result {
        Err(err) if err.kind() != io::ErrorKind::NotFound => Err(err),
        _ => Ok(()),
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

const TARGET_COLUMNS: &str = "t.id, t.name, t.kind, t.location, t.created_at, t.append_only";

fn target(conn: &Connection, id: BackupTargetId) -> rusqlite::Result<Option<Target>> {
    conn.query_row(
        &format!("SELECT {TARGET_COLUMNS} FROM backup_targets t WHERE t.id = ?1"),
        [id.as_uuid()],
        target_from_row,
    )
    .optional()
}

/// A target from the first six columns of `row`, as in [`TARGET_COLUMNS`].
fn target_from_row(row: &Row<'_>) -> rusqlite::Result<Target> {
    let kind: String = row.get(2)?;
    Ok(Target {
        id: BackupTargetId::from_uuid(row.get(0)?),
        name: row.get(1)?,
        kind: match kind.as_str() {
            "s3" => TargetKind::S3,
            "sftp" => TargetKind::Sftp,
            "rest" => TargetKind::Rest,
            _ => TargetKind::Folder,
        },
        location: row.get(3)?,
        created_at: row.get(4)?,
        append_only: row.get(5)?,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn adding_buckets_keeps_existing_targets_and_their_history() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir.path().join("accordion.db")).unwrap();
        db.migrate(OWNER, &MIGRATIONS[..2]).await.unwrap();
        db.write(|tx| {
            tx.execute_batch(
                "INSERT INTO backup_targets (id, name, kind, location, created_at, next_part)
                 VALUES (x'00000000000000000000000000000001', 'USB disk', 'folder',
                         '/media/usb/backups', '2026-10-01T03:00:00.000Z', 3);
                 INSERT INTO backup_runs (id, target_id, kind, started_at, finished_at, ok)
                 VALUES (x'00000000000000000000000000000002',
                         x'00000000000000000000000000000001', 'backup',
                         '2026-10-01T03:00:00.000Z', '2026-10-01T03:05:00.000Z', 1);",
            )
        })
        .await
        .unwrap();

        db.migrate(OWNER, MIGRATIONS).await.unwrap();
        let (target, next_part, runs) = db
            .read(|conn| {
                let target = target(conn, BackupTargetId::from_uuid(uuid::Uuid::from_u128(1)))?;
                let next_part: u32 =
                    conn.query_row("SELECT next_part FROM backup_targets", [], |row| row.get(0))?;
                let runs: u32 =
                    conn.query_row("SELECT count(*) FROM backup_runs", [], |row| row.get(0))?;
                Ok::<_, rusqlite::Error>((target, next_part, runs))
            })
            .await
            .unwrap();
        let target = target.unwrap();
        assert_eq!(target.kind, TargetKind::Folder);
        assert_eq!(target.location, "/media/usb/backups");
        assert_eq!((next_part, runs), (3, 1));

        // Runs still go with their target.
        db.write(|tx| tx.execute("DELETE FROM backup_targets", []))
            .await
            .unwrap();
        let runs: u32 = db
            .read(|conn| conn.query_row("SELECT count(*) FROM backup_runs", [], |row| row.get(0)))
            .await
            .unwrap();
        assert_eq!(runs, 0);
    }

    #[tokio::test]
    async fn adding_sftp_keeps_buckets_and_their_keys() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir.path().join("accordion.db")).unwrap();
        db.migrate(OWNER, &MIGRATIONS[..3]).await.unwrap();
        db.write(|tx| {
            tx.execute_batch(
                "INSERT INTO backup_targets (id, name, kind, location, created_at,
                                             access_key_id, secret_access_key)
                 VALUES (x'00000000000000000000000000000001', 'Backblaze', 's3',
                         's3:https://s3.example.com/backups', '2026-10-01T03:00:00.000Z',
                         'id', 'secret');",
            )
        })
        .await
        .unwrap();
        db.migrate(OWNER, MIGRATIONS).await.unwrap();
        let keys: (String, String) = db
            .read(|conn| {
                conn.query_row(
                    "SELECT access_key_id, secret_access_key FROM backup_targets",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
            })
            .await
            .unwrap();
        assert_eq!(keys, ("id".into(), "secret".into()));
    }

    #[test]
    fn reads_s3_addresses() {
        for (typed, endpoint) in [
            (
                "https://s3.us-west-004.backblazeb2.com",
                "https://s3.us-west-004.backblazeb2.com",
            ),
            ("S3.Example.com/", "https://s3.example.com"),
            ("https://s3.example.com:8443", "https://s3.example.com:8443"),
            ("http://127.0.0.1:9000", "http://127.0.0.1:9000"),
        ] {
            assert_eq!(s3_endpoint(typed).as_deref(), Some(endpoint), "{typed}");
        }
        for typed in [
            "",
            "https://",
            "http://s3.example.com",
            "https://s3.example.com/bucket",
            "https://s3..example.com",
            "https://s3.example.com:0",
            "https://user@s3.example.com",
        ] {
            assert_eq!(s3_endpoint(typed), None, "{typed}");
        }
    }

    #[test]
    fn names_buckets_in_messages() {
        let repository = s3_repository(&S3Bucket {
            endpoint: "https://s3.example.com".into(),
            bucket: "backups".into(),
            key_id: "id".into(),
            secret_key: "secret".into(),
        })
        .unwrap();
        assert_eq!(
            place_name(&repository),
            "the bucket backups at s3.example.com"
        );
        let folder = folder_repository(Path::new("/media/usb/backups")).unwrap();
        assert_eq!(place_name(&folder), "/media/usb/backups");
        let server = sftp_server(&SftpFolder {
            server: "NAS.local:2222".into(),
            username: "accordion".into(),
            folder: "/volume1/backups".into(),
        })
        .unwrap();
        assert_eq!(server.port, 2222);
        let sftp = Repository {
            ssh: Some(server.ssh("known_hosts", "yes")),
            ..Repository::at(server.location())
        };
        assert_eq!(
            place_name(&sftp),
            "the folder /volume1/backups on nas.local"
        );
        let rest = rest_repository(&RestServer {
            address: "https://NAS.local:8000/accordion/".into(),
            username: "household".into(),
            password: "hunter2".into(),
        })
        .unwrap();
        assert_eq!(rest.location, "rest:https://nas.local:8000/accordion");
        assert_eq!(
            place_name(&rest),
            "the REST server at https://nas.local:8000/accordion"
        );
    }

    #[test]
    fn checks_rest_servers() {
        let server = |address: &str, username: &str, password: &str| {
            rest_repository(&RestServer {
                address: address.into(),
                username: username.into(),
                password: password.into(),
            })
        };
        // A server that doesn't ask for a username and password.
        let open = server("https://nas.local:8000", "", "").unwrap();
        assert_eq!(open.login.unwrap().username, "");
        assert!(matches!(
            server("https://nas.local", "household", ""),
            Err(Error::RestLoginIncomplete)
        ));
        assert!(matches!(
            server("http://nas.local", "household", "hunter2"),
            Err(Error::BadRestAddress)
        ));
        assert_eq!(
            format!(
                "{:?}",
                RestServer {
                    address: "https://nas.local".into(),
                    username: "household".into(),
                    password: "hunter2".into(),
                }
            ),
            "RestServer { address: \"https://nas.local\", username: \"household\", .. }"
        );

        // A refused delete only means append-only on a REST server.
        let forbidden = Failure::Failed {
            code: Some(3),
            message: String::new(),
            output: "Remove(<snapshot/97895728d8>) failed: unexpected HTTP response (403)".into(),
        };
        assert!(refused_delete(
            &forbidden,
            &server("https://nas.local", "", "").unwrap()
        ));
        assert!(!refused_delete(
            &forbidden,
            &Repository::at("/media/usb".into())
        ));

        // Messages name the server and say what to do.
        let rest = server("https://nas.local:8000/accordion", "household", "hunter2").unwrap();
        let refused = Failure::Retrying("unexpected HTTP response (401): 401 Unauthorized".into());
        assert!(matches!(
            setup_failure(&refused, &rest),
            Error::RestLoginRefused(host) if host == "nas.local"
        ));
        let untrusted = Failure::Retrying("x509: certificate signed by unknown authority".into());
        let pinned = Repository {
            cacert: Some("cert.pem".into()),
            ..rest.clone()
        };
        assert!(describe(&untrusted, &pinned).contains("showed a different certificate"));
        let off =
            Failure::Retrying("dial tcp 192.168.1.50:8000: connect: connection refused".into());
        assert!(matches!(
            setup_failure(&off, &rest),
            Error::Unreachable { host, .. } if host == "nas.local"
        ));
    }

    #[test]
    fn finds_where_a_place_is() {
        let rest = Place::Rest(RestServer {
            address: "https://[fd00::50]:8000/accordion".into(),
            username: String::new(),
            password: String::new(),
        });
        assert_eq!(
            place_address(&rest),
            Some(("[fd00::50]".into(), "fd00::50".into(), 8000, Reach::Network))
        );
        let sftp = Place::Sftp(SftpFolder {
            server: "NAS.local".into(),
            username: "backup".into(),
            folder: "accordion".into(),
        });
        assert_eq!(
            place_address(&sftp),
            Some(("nas.local".into(), "nas.local".into(), 22, Reach::Network))
        );
        let bucket = |endpoint: &str| {
            place_address(&Place::S3(S3Bucket {
                endpoint: endpoint.into(),
                bucket: "family".into(),
                key_id: "id".into(),
                secret_key: "secret".into(),
            }))
        };
        assert_eq!(
            bucket("s3.us-west-004.backblazeb2.com"),
            Some((
                "s3.us-west-004.backblazeb2.com".into(),
                "s3.us-west-004.backblazeb2.com".into(),
                443,
                Reach::Internet
            ))
        );
        assert_eq!(
            bucket("http://localhost:9000"),
            Some((
                "localhost:9000".into(),
                "localhost".into(),
                9000,
                Reach::Internet
            ))
        );
        assert_eq!(place_address(&Place::Folder("/media/usb".into())), None);
    }

    #[tokio::test]
    async fn checks_that_a_place_answers() {
        let at = |address: String| {
            Place::Rest(RestServer {
                address,
                username: String::new(),
                password: String::new(),
            })
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let open = at(format!("https://127.0.0.1:{port}/accordion"));
        check_answers(&open, Duration::from_secs(5)).await.unwrap();
        drop(listener);

        // Not the listener's port, which another test can take as soon as
        // it's free.
        let closed = at("https://127.0.0.1:1/accordion".into());
        let closed = check_answers(&closed, Duration::from_secs(5)).await;
        assert!(
            matches!(&closed, Err(Error::Unreachable { problem, .. })
                if problem == "nothing is listening on that port"),
            "{closed:?}"
        );

        // A documentation address, which no network routes, so the
        // connection either hangs or fails at once.
        let started = std::time::Instant::now();
        let nowhere = at("https://192.0.2.1:8444/accordion".into());
        let silent = check_answers(&nowhere, Duration::from_millis(300)).await;
        assert!(
            matches!(&silent, Err(Error::Unreachable { problem, reach: Reach::Network, .. })
                if problem == "it didn't answer"
                    || problem == "this server has no network route to it"),
            "{silent:?}"
        );
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn says_why_a_target_is_unreachable() {
        // From the first try at a REST server on a Raspberry Pi.
        let timeout = "Head \"https://accordion-test:***@192.168.1.57:8444/accordion/config\": \
                       dial tcp 192.168.1.57:8444: i/o timeout";
        assert_eq!(why_unreachable(timeout), "it didn't answer");
        let error = Error::Unreachable {
            host: "192.168.1.57".into(),
            problem: why_unreachable(timeout),
            reach: Reach::Network,
        };
        assert_eq!(
            error.to_string(),
            "Accordion couldn't reach 192.168.1.57: it didn't answer. Check the address, and \
             that the other computer is turned on and connected to the network, then try again."
        );
        for (problem, reason) in [
            (
                "connect to host nas port 22: Connection refused",
                "nothing is listening on that port",
            ),
            (
                "ssh: Could not resolve hostname nas: Name or service not known",
                "no computer on the network has that name",
            ),
            (
                "Get \"https://nas.local:8000/config\": dial tcp: lookup nas.local: no such host",
                "no computer on the network has that name",
            ),
            (
                "connect to host 10.0.0.9 port 22: No route to host",
                "this server has no network route to it",
            ),
            ("it didn't answer", "it didn't answer"),
            (
                "Head \"https://u:***@nas:8000/config\": something new went wrong.",
                "something new went wrong",
            ),
        ] {
            assert_eq!(why_unreachable(problem), reason, "{problem}");
        }
    }
}

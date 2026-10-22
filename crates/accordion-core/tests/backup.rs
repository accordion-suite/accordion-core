//! Backups with the real restic program. Set `ACCORDION_RESTIC` to its path,
//! or put it on `PATH`.

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

use accordion_core::backup::{self, Backup, CheckDepth, Error, KeyState, RunKind};
use accordion_core::db::Database;
use accordion_core::identity::{Identity, NewUser};
use accordion_core::storage::Storage;
use accordion_types::Role;

struct Server {
    data: tempfile::TempDir,
    /// Somewhere outside the data folder, like a USB disk.
    disk: tempfile::TempDir,
    storage: Storage,
    backup: Backup,
}

fn restic() -> PathBuf {
    if let Some(path) = std::env::var_os("ACCORDION_RESTIC") {
        return path.into();
    }
    std::env::var_os("PATH")
        .and_then(|paths| {
            std::env::split_paths(&paths)
                .map(|dir| dir.join("restic"))
                .find(|path| path.is_file())
        })
        .expect("These tests run restic. Install restic 0.19.1, or set ACCORDION_RESTIC.")
}

async fn start() -> Server {
    let data = tempfile::tempdir().unwrap();
    let disk = tempfile::tempdir().unwrap();
    let db = Database::open(&data.path().join("accordion.db")).unwrap();
    let identity = Identity::new(db.clone()).await.unwrap();
    let storage = Storage::open(db.clone(), identity.clone(), &data.path().join("storage"))
        .await
        .unwrap();
    let alex = identity
        .create_user(NewUser {
            username: "alex".into(),
            display_name: "Alex Kim".into(),
            role: Role::Owner,
            password: "correct horse".into(),
        })
        .await
        .unwrap();
    let top = storage.top_folder(alex.id).await.unwrap();
    for (name, contents) in [("notes.md", "shopping list"), ("photo.jpg", "pixels")] {
        storage
            .write_file(top, name, contents.as_bytes())
            .await
            .unwrap();
    }
    let backup = Backup::open(db, storage.clone(), data.path(), restic())
        .await
        .unwrap();
    Server {
        data,
        disk,
        storage,
        backup,
    }
}

impl Server {
    fn usb(&self) -> PathBuf {
        self.disk.path().join("accordion-backups")
    }

    /// Runs restic directly on `repository`, as an admin restoring by hand
    /// would.
    fn restic(&self, repository: &Path, args: &[&str]) -> String {
        let output = Command::new(restic())
            .env("RESTIC_REPOSITORY", repository)
            .env(
                "RESTIC_PASSWORD_FILE",
                self.data.path().join("backup/recovery-key"),
            )
            .env("RESTIC_CACHE_DIR", self.disk.path().join("cache"))
            .args(args)
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        String::from_utf8(output.stdout).unwrap()
    }
}

#[tokio::test]
async fn creates_the_recovery_key_once() {
    let server = start().await;
    assert_eq!(server.backup.recovery_key().await.unwrap(), None);
    let key = server.backup.create_recovery_key().await.unwrap();
    assert_eq!(key.as_str().len(), 30);
    assert!(key.as_str().bytes().all(|b| b.is_ascii_lowercase()));
    assert_eq!(key.grouped().split(' ').count(), 6);
    assert_eq!(
        server.backup.recovery_key().await.unwrap(),
        Some(key.clone())
    );
    let mode = std::fs::metadata(server.data.path().join("backup/recovery-key"))
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o600);

    // Every backup is locked with it, so it can't be replaced.
    let err = server.backup.create_recovery_key().await.unwrap_err();
    assert!(matches!(err, Error::KeyExists));

    assert_eq!(server.backup.key_confirmed_at().await.unwrap(), None);
    let err = server
        .backup
        .confirm_recovery_key("abcde fghij klmno pqrst uvwxy zabcd")
        .await
        .unwrap_err();
    assert!(matches!(err, Error::KeyMismatch));
    server
        .backup
        .confirm_recovery_key(&key.grouped().to_uppercase())
        .await
        .unwrap();
    assert!(server.backup.key_confirmed_at().await.unwrap().is_some());
}

#[tokio::test]
async fn needs_a_key_and_somewhere_to_go() {
    let server = start().await;
    let usb = server.usb();
    let err = server
        .backup
        .add_folder("USB disk", &usb)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NoRecoveryKey));
    let err = server.backup.back_up().await.unwrap_err();
    assert!(matches!(err, Error::NoRecoveryKey));

    server.backup.create_recovery_key().await.unwrap();
    let err = server.backup.back_up().await.unwrap_err();
    assert!(matches!(err, Error::NoTargets));

    let err = server
        .backup
        .add_folder("USB disk", Path::new("backups"))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NotAbsolute));
    let err = server
        .backup
        .add_folder("Same disk", &server.data.path().join("storage/backups"))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InsideData));
    let err = server
        .backup
        .add_folder("Unplugged", &server.disk.path().join("missing/backups"))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NoSuchFolder(_)));
    let err = server.backup.add_folder(" ", &usb).await.unwrap_err();
    assert!(matches!(err, Error::BadName));
}

#[tokio::test]
async fn backs_up_everything_and_passes_a_restore_test() {
    let server = start().await;
    std::fs::create_dir(server.data.path().join("gateway")).unwrap();
    std::fs::write(server.data.path().join("gateway/settings.json"), "{}").unwrap();
    server.backup.create_recovery_key().await.unwrap();
    let target = server
        .backup
        .add_folder("USB disk", &server.usb())
        .await
        .unwrap();
    assert_eq!(
        server.backup.targets().await.unwrap(),
        std::slice::from_ref(&target)
    );

    let runs = server.backup.back_up().await.unwrap();
    let kinds: Vec<(RunKind, bool)> = runs.iter().map(|run| (run.kind, run.ok)).collect();
    assert_eq!(kinds, [(RunKind::Backup, true), (RunKind::Check, true)]);
    assert!(runs[0].snapshot.is_some());
    assert!(runs[0].bytes_added.unwrap() > 0);
    // The database copy is gone once restic has it.
    assert!(!server.data.path().join("backup/accordion.db").exists());

    // The snapshot holds a data folder, which restic can restore by hand.
    let listing = server.restic(&server.usb(), &["ls", "latest"]);
    for path in [
        "/backup/accordion.db",
        "/gateway/settings.json",
        "/storage/blobs/sha256",
    ] {
        assert!(
            listing.lines().any(|line| line == path),
            "{path}: {listing}"
        );
    }
    assert!(!listing.contains("/storage/files"), "{listing}");

    let run = server.backup.test_restore(&target).await.unwrap();
    assert!(run.ok, "{:?}", run.problem);
    assert!(!server.data.path().join("backup/restore-test").exists());

    let run = server
        .backup
        .check(&target, CheckDepth::Data { part: 1, of: 8 })
        .await
        .unwrap();
    assert!(run.ok, "{:?}", run.problem);
    server.backup.prune(&target).await.unwrap();

    let recorded = server.backup.runs(10).await.unwrap();
    assert_eq!(recorded.len(), 4);
    assert_eq!(recorded[0].kind, RunKind::Check);
}

#[tokio::test]
async fn finds_damage_in_a_backup() {
    let server = start().await;
    server.backup.create_recovery_key().await.unwrap();
    let target = server
        .backup
        .add_folder("USB disk", &server.usb())
        .await
        .unwrap();
    server.backup.back_up().await.unwrap();

    // Flip a byte in every pack of data, as a failing disk might.
    for pack in files_in(&server.usb().join("data")) {
        let mut bytes = std::fs::read(&pack).unwrap();
        let middle = bytes.len() / 2;
        bytes[middle] ^= 0xff;
        // restic stores packs read-only.
        std::fs::set_permissions(&pack, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::write(&pack, bytes).unwrap();
    }
    let run = server.backup.test_restore(&target).await.unwrap();
    assert!(!run.ok);
    assert!(run.problem.is_some());
    let run = server
        .backup
        .check(&target, CheckDepth::Data { part: 1, of: 1 })
        .await
        .unwrap();
    assert!(!run.ok);
}

#[tokio::test]
async fn reports_a_missing_disk_without_stopping_other_targets() {
    let server = start().await;
    server.backup.create_recovery_key().await.unwrap();
    server
        .backup
        .add_folder("USB disk", &server.usb())
        .await
        .unwrap();
    server
        .backup
        .add_folder("Second disk", &server.disk.path().join("second"))
        .await
        .unwrap();
    std::fs::rename(server.usb(), server.disk.path().join("unplugged")).unwrap();

    let runs = server.backup.back_up().await.unwrap();
    let kinds: Vec<(RunKind, bool)> = runs.iter().map(|run| (run.kind, run.ok)).collect();
    assert_eq!(
        kinds,
        [
            (RunKind::Backup, false),
            (RunKind::Backup, true),
            (RunKind::Check, true)
        ]
    );
    let problem = runs[0].problem.as_deref().unwrap();
    assert!(problem.contains("plugged in"), "{problem}");
    // Storage carries on as usual afterwards.
    assert_eq!(server.storage.clean_up().await.unwrap().contents, 0);
}

#[tokio::test]
async fn uses_an_existing_backup_only_with_its_key() {
    let server = start().await;
    server.backup.create_recovery_key().await.unwrap();
    let target = server
        .backup
        .add_folder("USB disk", &server.usb())
        .await
        .unwrap();
    server.backup.back_up().await.unwrap();
    let err = server
        .backup
        .add_folder("Again", &server.usb())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::TargetExists));

    // Removing a target leaves its backups, so it can be added back.
    server.backup.remove_target(target.id).await.unwrap();
    assert!(server.backup.targets().await.unwrap().is_empty());
    server
        .backup
        .add_folder("USB disk", &server.usb())
        .await
        .unwrap();

    // Another server has its own key, which doesn't open this backup.
    let other = start().await;
    other.backup.create_recovery_key().await.unwrap();
    let err = other
        .backup
        .add_folder("Borrowed disk", &server.usb())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::WrongKey(_)));
}

#[tokio::test]
async fn restores_everything_to_a_new_server() {
    let server = start().await;
    std::fs::create_dir(server.data.path().join("gateway")).unwrap();
    std::fs::write(server.data.path().join("gateway/settings.json"), "{}").unwrap();
    let key = server.backup.create_recovery_key().await.unwrap();
    server
        .backup
        .add_folder("USB disk", &server.usb())
        .await
        .unwrap();
    server.backup.back_up().await.unwrap();

    // A fresh server, whose data folder holds an empty database and nothing
    // else worth keeping. Its services use their own folder, because a
    // database must be closed before a restore replaces it, and the server
    // does that by starting again.
    let data = tempfile::tempdir().unwrap();
    let services = tempfile::tempdir().unwrap();
    let db = Database::open(&services.path().join("accordion.db")).unwrap();
    let identity = Identity::new(db.clone()).await.unwrap();
    let storage = Storage::open(db.clone(), identity, &services.path().join("storage"))
        .await
        .unwrap();
    let fresh = Backup::open(db, storage, data.path(), restic())
        .await
        .unwrap();
    for (path, contents) in [
        ("accordion.db", "empty"),
        ("accordion.db-wal", "empty"),
        ("storage/blobs/leftover", "old"),
        ("gateway/settings.json", "old"),
    ] {
        let path = data.path().join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, contents).unwrap();
    }

    let usb = server.usb();
    let err = fresh.find_snapshots(&usb, "abcde").await.unwrap_err();
    assert!(matches!(err, Error::BadKey));
    let wrong = "abcde fghij klmno pqrst uvwxy zabcd";
    let err = fresh.find_snapshots(&usb, wrong).await.unwrap_err();
    assert!(matches!(err, Error::KeyDoesntOpen(_)), "{err}");
    let err = fresh
        .find_snapshots(&server.disk.path().join("missing"), key.as_str())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NoSuchFolder(_)));
    let err = fresh
        .find_snapshots(server.disk.path(), key.as_str())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("There's no backup in"), "{err}");

    // The key is accepted however it's typed.
    let snapshots = fresh
        .find_snapshots(&usb, &key.grouped().to_uppercase())
        .await
        .unwrap();
    assert_eq!(snapshots.len(), 1);
    assert!(snapshots[0].size.unwrap() > 0);
    // Listing leaves nothing behind, including the typed key.
    assert!(!data.path().join("restore").exists());

    let err = fresh
        .restore(&usb, key.as_str(), "--help", |_| {})
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NoSuchSnapshot));
    let err = fresh
        .restore(&usb, key.as_str(), "0123456789abcdef", |_| {})
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NoSuchSnapshot), "{err}");
    assert!(!data.path().join("restore").exists());

    let restored = fresh
        .restore(&usb, key.as_str(), &snapshots[0].id, |done| {
            assert!((0.0..=1.0).contains(&done));
        })
        .await
        .unwrap();
    assert!(restored.join("gateway/settings.json").is_file());
    drop(fresh);

    assert!(backup::apply_restore(data.path()).unwrap());
    assert!(!data.path().join("restore").exists());
    assert!(!data.path().join("accordion.db-wal").exists());
    assert!(!data.path().join("storage/blobs/leftover").exists());
    let read = |path: &str| std::fs::read_to_string(data.path().join(path)).unwrap();
    assert_eq!(read("gateway/settings.json"), "{}");
    assert_eq!(read("backup/recovery-key"), key.as_str());

    // The restored server has the same people and files.
    let db = Database::open(&data.path().join("accordion.db")).unwrap();
    let identity = Identity::new(db.clone()).await.unwrap();
    let alex = identity.user_by_username("alex").await.unwrap().unwrap();
    let storage = Storage::open(db, identity, &data.path().join("storage"))
        .await
        .unwrap();
    let top = storage.top_folder(alex.id).await.unwrap();
    let notes = storage
        .find(top, vec!["notes.md".into()])
        .await
        .unwrap()
        .unwrap();
    let mut contents = String::new();
    tokio::io::AsyncReadExt::read_to_string(
        &mut storage.open_file(notes.id).await.unwrap(),
        &mut contents,
    )
    .await
    .unwrap();
    assert_eq!(contents, "shopping list");

    // Nothing more to move the next time the server starts.
    assert!(!backup::apply_restore(data.path()).unwrap());
}

#[test]
fn deletes_a_restore_that_stopped_partway() {
    let data = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(data.path().join("restore/snapshot/backup")).unwrap();
    std::fs::write(
        data.path().join("restore/snapshot/backup/accordion.db"),
        "half",
    )
    .unwrap();
    std::fs::write(data.path().join("accordion.db"), "current").unwrap();
    assert!(!backup::apply_restore(data.path()).unwrap());
    assert!(!data.path().join("restore").exists());
    assert_eq!(
        std::fs::read_to_string(data.path().join("accordion.db")).unwrap(),
        "current"
    );
}

fn files_in(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            files.extend(files_in(&path));
        } else {
            files.push(path);
        }
    }
    files
}

#[tokio::test]
async fn reports_how_backups_are_doing() {
    let server = start().await;
    let status = server.backup.status();
    assert_eq!(status.borrow().key, KeyState::None);
    assert_eq!(status.borrow().hour, 3);

    let key = server.backup.create_recovery_key().await.unwrap();
    assert_eq!(status.borrow().key, KeyState::Unconfirmed);
    server
        .backup
        .confirm_recovery_key(key.as_str())
        .await
        .unwrap();
    assert_eq!(status.borrow().key, KeyState::Confirmed);

    let target = server
        .backup
        .add_folder("USB disk", &server.usb())
        .await
        .unwrap();
    {
        let status = status.borrow();
        assert_eq!(status.targets.len(), 1);
        assert_eq!(status.targets[0].target, target);
        assert_eq!(status.targets[0].last_backup, None);
    }
    server.backup.back_up().await.unwrap();
    let status = status.borrow().clone();
    assert!(!status.running);
    let usb = &status.targets[0];
    assert!(usb.last_backup.as_ref().unwrap().ok);
    assert!(usb.last_good_backup.is_some());
    assert!(usb.last_check.as_ref().unwrap().ok);
    assert_eq!(usb.last_restore_test, None);

    let err = server.backup.set_hour(24).await.unwrap_err();
    assert!(matches!(err, Error::BadHour));
    server.backup.set_hour(5).await.unwrap();
    assert_eq!(server.backup.status().borrow().hour, 5);
}

#[tokio::test]
async fn runs_weekly_and_monthly_work_when_its_due() {
    let server = start().await;
    server.backup.create_recovery_key().await.unwrap();
    server
        .backup
        .add_folder("USB disk", &server.usb())
        .await
        .unwrap();

    // The first night: a backup and its check, then the weekly check of
    // the first eighth of the data, then the first restore test.
    server.backup.nightly().await;
    let mut kinds: Vec<RunKind> = server
        .backup
        .runs(10)
        .await
        .unwrap()
        .iter()
        .map(|run| run.kind)
        .collect();
    kinds.reverse();
    assert_eq!(
        kinds,
        [
            RunKind::Backup,
            RunKind::Check,
            RunKind::Check,
            RunKind::RestoreTest
        ]
    );
    assert!(
        server
            .backup
            .runs(10)
            .await
            .unwrap()
            .iter()
            .all(|run| run.ok)
    );

    // The next night, only the backup and its check are due.
    server.backup.nightly().await;
    assert_eq!(server.backup.runs(10).await.unwrap().len(), 6);
}

#[tokio::test(start_paused = true)]
async fn catches_up_on_a_missed_backup() {
    let server = start().await;
    server.backup.create_recovery_key().await.unwrap();
    server
        .backup
        .add_folder("USB disk", &server.usb())
        .await
        .unwrap();
    // The server was off at backup time, so one starts 10 minutes after it
    // does, not at the next backup hour. Time is paused, so it jumps ahead
    // to each timer.
    let mut status = server.backup.status();
    let backed_up = async move {
        let _ = status
            .wait_for(|status| status.targets[0].last_backup.is_some())
            .await;
    };
    let started = tokio::time::Instant::now();
    server.backup.clone().maintain(backed_up).await;
    assert_eq!(started.elapsed().as_secs() / 60, 10);
    assert!(
        server.backup.status().borrow().targets[0]
            .last_good_backup
            .is_some()
    );
}

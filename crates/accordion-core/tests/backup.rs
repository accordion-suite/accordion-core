//! Backups with the real restic program. Set `ACCORDION_RESTIC` to its path,
//! or put it on `PATH`.

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

use accordion_core::backup::{Backup, CheckDepth, Error, RunKind};
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

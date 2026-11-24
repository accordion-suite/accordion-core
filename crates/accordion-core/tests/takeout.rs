//! Reading Takeout's archives in the sandbox (decision 34), with archives
//! made here in the shapes the real exports had: `.zip` and `.tgz` parts,
//! a note's picture in another part, and entries that try to leave their
//! folder.

mod workers;

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use accordion_core::db::Database;
use accordion_core::events::{Action, Events};
use accordion_core::identity::{Identity, NewUser};
use accordion_core::storage::{ItemKind, Storage};
use accordion_core::takeout::imports::{self, BoxFuture, Import, State};
use accordion_core::takeout::{
    Error, FolderFound, Importer, Importing, Imports, ItemError, Take, Takeout, Tools, Unpacked,
    escape,
};
use accordion_types::{ItemId, Role, UserId};
use flate2::Compression;
use flate2::write::GzEncoder;
use tokio::io::AsyncReadExt;
use tokio::sync::oneshot;
use zip::write::SimpleFileOptions;

const LAUNCHER: &str = env!("CARGO_BIN_EXE_accordion-core-sandbox");

fn start(dir: &Path) -> Takeout {
    let scratch = dir.join("scratch");
    std::fs::create_dir_all(&scratch).unwrap();
    Takeout::new(
        Tools {
            workers: workers::workers(dir),
            ..Tools::installed(LAUNCHER)
        },
        scratch,
    )
}

fn note(title: &str, created: u64, attachments: &[&str]) -> Vec<u8> {
    let attachments: Vec<_> = attachments
        .iter()
        .map(|name| serde_json::json!({ "filePath": name, "mimetype": "image/png" }))
        .collect();
    serde_json::json!({
        "title": title,
        "textContent": "Some text",
        "createdTimestampUsec": created,
        "attachments": attachments,
    })
    .to_string()
    .into_bytes()
}

/// A `.zip` part, with a link and a path that leaves its folder thrown in.
fn zip_part(path: &Path) {
    let mut zip = zip::ZipWriter::new(std::fs::File::create(path).unwrap());
    let options = SimpleFileOptions::default();
    let files: [(&str, Vec<u8>); 5] = [
        (
            "Takeout/Keep/Groceries.json",
            note("Groceries", 1_000, &["photo.png"]),
        ),
        ("Takeout/Keep/Plans.json", note("Plans", 2_000, &[])),
        ("Takeout/Keep/Labels.txt", b"Home\n".to_vec()),
        ("Takeout/archive_browser.html", b"<html></html>".to_vec()),
        ("Takeout/../../evil.txt", b"outside".to_vec()),
    ];
    for (name, contents) in files {
        zip.start_file(name, options).unwrap();
        zip.write_all(&contents).unwrap();
    }
    zip.add_directory("Takeout/Mail/", options).unwrap();
    zip.add_symlink("Takeout/Keep/link", "/etc/passwd", options)
        .unwrap();
    zip.finish().unwrap();
}

/// A `.tgz` part, with the note's picture, a folder no product knows, a
/// link, and a raw name that leaves its folder.
fn tgz_part(path: &Path) {
    let gz = GzEncoder::new(std::fs::File::create(path).unwrap(), Compression::fast());
    let mut tar = tar::Builder::new(gz);
    let mut add = |name: &str, contents: &[u8]| {
        let mut header = tar::Header::new_gnu();
        header.set_size(contents.len() as u64);
        header.set_mode(0o644);
        header.set_entry_type(tar::EntryType::Regular);
        tar.append_data(&mut header, name, contents).unwrap();
    };
    add("Takeout/Keep/photo.png", &[7; 3000]);
    add("Takeout/Mail/All mail.mbox", &[b'm'; 500]);
    add("Takeout/Mail/Settings/filters.xml", b"<filters/>");

    let mut link = tar::Header::new_gnu();
    link.set_entry_type(tar::EntryType::Symlink);
    link.set_size(0);
    tar.append_link(&mut link, "Takeout/Keep/sneaky", "/etc/passwd")
        .unwrap();

    // Raw, since the tar crate refuses to write `..` itself.
    let mut evil = tar::Header::new_gnu();
    evil.as_gnu_mut().unwrap().name[..17].copy_from_slice(b"../../escaped.txt");
    evil.set_size(7);
    evil.set_mode(0o644);
    evil.set_entry_type(tar::EntryType::Regular);
    evil.set_cksum();
    tar.append(&evil, &b"outside"[..]).unwrap();
    tar.into_inner().unwrap().finish().unwrap();
}

fn parts(dir: &Path) -> Vec<PathBuf> {
    let zip = dir.join("takeout-1.zip");
    let tgz = dir.join("takeout-2.tgz");
    zip_part(&zip);
    tgz_part(&tgz);
    vec![zip, tgz]
}

#[tokio::test]
async fn plans_an_export_from_its_parts() {
    let dir = tempfile::tempdir().unwrap();
    let takeout = start(dir.path());
    let plan = takeout.plan(&["notes"], &parts(dir.path())).await.unwrap();

    assert_eq!(
        plan.parts
            .iter()
            .map(|part| part.format.as_str())
            .collect::<Vec<_>>(),
        ["zip", "tgz"]
    );
    assert_eq!(
        plan.folders,
        [
            // The note's picture is in the other part, and still counts.
            FolderFound {
                folder: "Keep".into(),
                product: Some("notes".into()),
                items: 2,
                bytes: plan.folders[0].bytes,
            },
            FolderFound {
                folder: "Mail".into(),
                product: None,
                items: 2,
                bytes: 510,
            },
        ]
    );
    assert!(plan.folders[0].bytes > 3000);
    let skipped: BTreeMap<_, _> = plan
        .skipped
        .iter()
        .map(|skipped| (skipped.path.as_str(), skipped.why.as_str()))
        .collect();
    assert_eq!(
        skipped,
        BTreeMap::from([
            ("../../escaped.txt", "its path leaves its folder"),
            ("Takeout/../../evil.txt", "its path leaves its folder"),
            ("Takeout/Keep/link", "it's a link"),
            ("Takeout/Keep/sneaky", "it's a link or a device"),
            (
                "Takeout/archive_browser.html",
                "it isn't in a product's folder"
            ),
        ])
    );
    assert_eq!(plan.skipped_count, 5);
}

#[tokio::test]
async fn unpacks_in_batches_and_never_outside_the_job() {
    let dir = tempfile::tempdir().unwrap();
    let takeout = start(dir.path());
    let parts = parts(dir.path());
    let take = [Take::Product("notes".into()), Take::Other("Mail".into())];

    // A small budget gives one item at a time, since each batch takes at
    // least one item.
    let mut items: Vec<Unpacked> = Vec::new();
    let mut contents = Vec::new();
    let mut from = Some(0);
    let mut batches = 0;
    while let Some(start) = from {
        let batch = takeout
            .unpack(&["notes"], &parts, &take, start, 100)
            .await
            .unwrap();
        batches += 1;
        for item in &batch.items {
            for file in &item.files {
                contents.push((file.path.clone(), std::fs::read(&file.file).unwrap()));
            }
        }
        items.extend(batch.items.iter().cloned());
        from = batch.next;
    }
    assert_eq!(batches, 4);
    assert_eq!(
        items
            .iter()
            .map(|item| (
                item.folder.as_str(),
                item.product.as_deref(),
                item.key.as_str()
            ))
            .collect::<Vec<_>>(),
        [
            ("Keep", Some("notes"), "1000 Groceries"),
            ("Keep", Some("notes"), "2000 Plans"),
            ("Mail", None, "All mail.mbox"),
            ("Mail", None, "Settings/filters.xml"),
        ]
    );
    let groceries = &items[0];
    assert_eq!(
        groceries
            .files
            .iter()
            .map(|file| (file.role.as_str(), file.path.as_str(), file.size))
            .collect::<Vec<_>>(),
        [
            (
                "note",
                "Takeout/Keep/Groceries.json",
                groceries.files[0].size
            ),
            ("attachment", "Takeout/Keep/photo.png", 3000),
        ]
    );
    let contents: BTreeMap<_, _> = contents.into_iter().collect();
    assert_eq!(contents["Takeout/Keep/photo.png"], vec![7; 3000]);
    assert_eq!(contents["Takeout/Mail/Settings/filters.xml"], b"<filters/>");

    // One big budget gives everything at once.
    let batch = takeout
        .unpack(&["notes"], &parts, &take, 0, 1 << 30)
        .await
        .unwrap();
    assert_eq!(batch.items.len(), 4);
    assert_eq!(batch.next, None);

    // Taking only the product leaves the other folder out.
    let batch = takeout
        .unpack(
            &["notes"],
            &parts,
            &[Take::Product("notes".into())],
            0,
            1 << 30,
        )
        .await
        .unwrap();
    assert_eq!(batch.items.len(), 2);

    // Nothing was written outside a job's folder.
    let escaped = |name: &str| {
        walk(dir.path())
            .into_iter()
            .any(|path| path.file_name().is_some_and(|file| file == name))
    };
    assert!(!escaped("evil.txt") && !escaped("escaped.txt"));
    assert!(!dir.path().parent().unwrap().join("escaped.txt").exists());
}

#[tokio::test]
async fn says_why_it_cant_read_a_part() {
    let dir = tempfile::tempdir().unwrap();
    let takeout = start(dir.path());
    let text = dir.path().join("notes.txt");
    std::fs::write(&text, "not an archive").unwrap();
    let err = takeout.plan(&["notes"], &[text]).await.unwrap_err();
    assert!(
        matches!(&err, Error::Unreadable(message) if message == "Part 0 isn't a .zip or .tgz archive."),
        "{err:?}"
    );
}

fn walk(dir: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    for entry in std::fs::read_dir(dir).unwrap().flatten() {
        let path = entry.path();
        if entry.file_type().unwrap().is_dir() {
            found.extend(walk(&path));
        } else {
            found.push(path);
        }
    }
    found
}

/// Plans a real export, for checking the reader against one by hand. Run
/// with `ACCORDION_TEST_TAKEOUT` set to the parts' paths, separated by `:`.
#[tokio::test]
#[ignore = "needs a real Takeout export"]
async fn plans_a_real_export() {
    let parts: Vec<PathBuf> = std::env::var("ACCORDION_TEST_TAKEOUT")
        .unwrap()
        .split(':')
        .map(PathBuf::from)
        .collect();
    let dir = tempfile::tempdir().unwrap();
    let plan = start(dir.path()).plan(&["notes"], &parts).await.unwrap();
    println!("{plan:#?}");
}

/// A third part, with a note that can't be converted, a second note
/// titled "Plans", and a title too long for a file name.
fn more_part(path: &Path) {
    let mut zip = zip::ZipWriter::new(std::fs::File::create(path).unwrap());
    let options = SimpleFileOptions::default();
    let long = "Long ".repeat(60);
    let files: [(&str, Vec<u8>); 3] = [
        ("Takeout/Keep/Broken.json", note("Broken", 3_000, &[])),
        (
            "Takeout/Keep/Plans(1).json",
            note("Plans", 4_000, &["gone.png"]),
        ),
        ("Takeout/Keep/Long.json", note(&long, 5_000, &[])),
    ];
    for (name, contents) in files {
        zip.start_file(name, options).unwrap();
        zip.write_all(&contents).unwrap();
    }
    zip.finish().unwrap();
}

/// Stores the test product's notes as files in **Imported notes**, as a
/// module's importer would.
#[derive(Default)]
struct NotesImporter {
    /// Fires once this many notes are stored, to stop the server.
    stop_after: Mutex<Option<(usize, oneshot::Sender<()>)>>,
    stored: Mutex<usize>,
}

impl Importer for NotesImporter {
    fn product(&self) -> &'static str {
        "notes"
    }

    fn app(&self) -> &'static str {
        "Notes"
    }

    fn brought(&self, count: u64) -> String {
        format!("{count} notes from Keep, in Notes")
    }

    fn label(&self, item: &Unpacked) -> String {
        escape(item.fields["title"].as_str().unwrap_or_default())
    }

    fn store<'a>(
        &'a self,
        item: &'a Unpacked,
        importing: &'a Importing,
    ) -> BoxFuture<'a, Result<Option<ItemId>, ItemError>> {
        Box::pin(async move {
            let title = item.fields["title"].as_str().unwrap_or_default();
            if title == "Broken" {
                return Err(ItemError::NotConverted("it's broken".to_owned()));
            }
            let top = importing.storage().top_folder(importing.person()).await?;
            let folder = importing.folder(top, "Imported notes").await?;
            let note = item.files.iter().find(|file| file.role == "note").unwrap();
            let stored = importing
                .create_file_from(folder, &format!("{title}.md"), note, None)
                .await?;
            for file in item.files.iter().filter(|file| file.role == "attachment") {
                let name = file.path.rsplit('/').next().unwrap();
                importing.create_file_from(folder, name, file, None).await?;
            }
            for missing in item.fields["missing"].as_array().into_iter().flatten() {
                importing.report(
                    "Missing pictures",
                    format!("{}: {}", escape(title), escape(missing.as_str().unwrap())),
                );
            }
            let mut stored_count = self.stored.lock().unwrap();
            *stored_count += 1;
            let mut stop = self.stop_after.lock().unwrap();
            if stop
                .as_ref()
                .is_some_and(|(after, _)| *stored_count >= *after)
            {
                let _ = stop.take().unwrap().1.send(());
            }
            Ok(Some(stored.id))
        })
    }
}

struct Server {
    _dir: tempfile::TempDir,
    storage: Storage,
    events: Events,
    imports: Imports,
    person: UserId,
    parts: Vec<ItemId>,
}

async fn server(batch_bytes: u64) -> Server {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(&dir.path().join("accordion.db")).unwrap();
    let identity = Identity::new(db.clone()).await.unwrap();
    let storage = Storage::open(db.clone(), identity.clone(), &dir.path().join("storage"))
        .await
        .unwrap();
    let events = Events::open(db.clone()).await.unwrap();
    let imports = Imports::open(db, storage.clone(), events.clone(), start(dir.path()))
        .await
        .unwrap()
        .with_batch_bytes(batch_bytes);
    let person = identity
        .create_user(NewUser {
            username: "sam".into(),
            display_name: "Sam".into(),
            role: Role::User,
            password: "correct horse".into(),
        })
        .await
        .unwrap()
        .id;

    let mut paths = parts(dir.path());
    let more = dir.path().join("takeout-3.zip");
    more_part(&more);
    paths.push(more);
    let top = storage.top_folder(person).await.unwrap();
    let mut parts = Vec::new();
    for path in paths {
        let name = path.file_name().unwrap().to_str().unwrap().to_owned();
        let file = tokio::fs::File::open(&path).await.unwrap();
        parts.push(storage.create_file(top, &name, file).await.unwrap().id);
    }
    Server {
        _dir: dir,
        storage,
        events,
        imports,
        person,
        parts,
    }
}

impl Server {
    /// Runs imports until `stop` fires, or the import ends.
    async fn run(&self, stop: Option<oneshot::Receiver<()>>) -> Import {
        let (ended, end) = oneshot::channel::<()>();
        let runner = tokio::spawn(self.imports.clone().run(async move {
            match stop {
                Some(stop) => {
                    tokio::select! {
                        _ = stop => {}
                        _ = end => {}
                    }
                }
                None => {
                    let _ = end.await;
                }
            }
        }));
        let latest = loop {
            let latest = self.imports.list(self.person, 1).await.unwrap().remove(0);
            if !matches!(latest.state, State::Running | State::Canceling) || runner.is_finished() {
                break latest;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        let _ = ended.send(());
        tokio::time::timeout(Duration::from_secs(60), runner)
            .await
            .unwrap()
            .unwrap();
        latest
    }

    async fn names(&self, path: &[&str]) -> Vec<String> {
        let top = self.storage.top_folder(self.person).await.unwrap();
        let folder = self
            .storage
            .find(top, path.iter().map(|name| (*name).to_owned()).collect())
            .await
            .unwrap()
            .unwrap();
        self.storage
            .children(folder.id)
            .await
            .unwrap()
            .into_iter()
            .map(|item| item.name)
            .collect()
    }

    async fn text(&self, id: ItemId) -> String {
        let mut text = String::new();
        self.storage
            .open_file(id)
            .await
            .unwrap()
            .read_to_string(&mut text)
            .await
            .unwrap();
        text
    }

    fn take(&self) -> [Take; 2] {
        [Take::Product("notes".into()), Take::Other("Mail".into())]
    }
}

#[tokio::test]
async fn imports_an_export_and_reports_what_came_over() {
    let server = server(imports::BATCH_BYTES).await;
    let _registered = server
        .imports
        .add_importer(Arc::new(NotesImporter::default()));
    let id = server
        .imports
        .start(server.person, &server.parts, &server.take())
        .await
        .unwrap();
    let import = server.run(None).await;
    assert_eq!((import.id, import.state), (id, State::Done));
    assert_eq!((import.items, import.done), (Some(7), 7));

    let long = format!("{}.md", "Long ".repeat(60));
    let mut notes = server.names(&["Imported notes"]).await;
    notes.sort();
    let shortened = notes
        .iter()
        .find(|name| name.starts_with("Long"))
        .unwrap()
        .clone();
    assert!(shortened.len() == 255 && shortened.ends_with(".md") && long.len() > 255);
    assert_eq!(
        notes,
        [
            "Groceries.md".to_owned(),
            shortened.clone(),
            "Plans (2).md".to_owned(),
            "Plans.md".to_owned(),
            "photo.png".to_owned(),
        ]
    );
    // Gmail's files and the note that couldn't be converted are kept.
    assert_eq!(
        server
            .names(&["Google Takeout", "Unconverted", "Mail"])
            .await,
        ["All mail.mbox", "Settings"]
    );
    assert_eq!(
        server
            .names(&["Google Takeout", "Unconverted", "Mail", "Settings"])
            .await,
        ["filters.xml"]
    );
    assert_eq!(
        server
            .names(&["Google Takeout", "Unconverted", "Keep"])
            .await,
        ["Broken.json"]
    );

    let report = server.text(import.report.unwrap()).await;
    for line in [
        "# Import from Google Takeout",
        "- 4 notes from Keep, in Notes\n",
        "- 2 files from Mail, kept in **Unconverted**\n",
        "Files Accordion didn't convert are in **Google Takeout** > **Unconverted**",
        "## Not converted\n\n- Broken: it's broken\n",
        "## Missing pictures\n\n- Plans: gone.png\n",
        "## Names that were shortened\n\n- `Long Long",
        "## Left out of the archives\n",
        "- `Takeout/Keep/link`: it's a link\n",
        "- `../../escaped.txt`: its path leaves its folder\n",
    ] {
        assert!(report.contains(line), "{line:?} isn't in:\n{report}");
    }
    let report_name = server.names(&["Google Takeout"]).await;
    assert!(
        report_name
            .iter()
            .any(|name| name.starts_with("Takeout import ") && name.ends_with(".md")),
        "{report_name:?}"
    );

    // The bell rings for the person who imported, with the report.
    let notices = server
        .events
        .notifications(server.person, 10)
        .await
        .unwrap();
    let notice = notices
        .iter()
        .find(|notice| notice.event.action == Action::ImportFinished)
        .unwrap();
    assert_eq!(notice.event.item, import.report);
    assert_eq!(notice.event.details["state"], "done");
    assert_eq!(notice.event.details["brought"], 6);

    // Importing again brings nothing twice.
    server
        .imports
        .start(server.person, &server.parts, &server.take())
        .await
        .unwrap();
    let again = server.run(None).await;
    assert_eq!(again.state, State::Done);
    assert_eq!(server.names(&["Imported notes"]).await.len(), 5);
    let report = server.text(again.report.unwrap()).await;
    assert!(report.contains("Nothing new came over."), "{report}");
    assert!(
        report.contains("7 items were brought by an earlier import, so they were skipped."),
        "{report}"
    );
    assert!(report.contains("## Brought by an earlier import\n\n- Groceries\n"));
    let reports = server.names(&["Google Takeout"]).await;
    assert_eq!(
        reports
            .iter()
            .filter(|name| name.starts_with("Takeout import "))
            .count(),
        2,
        "{reports:?}"
    );
}

#[tokio::test]
async fn picks_up_where_it_stopped() {
    // Everything in one batch, stopped after the first note, so the batch
    // starts again and the stored note is passed over.
    let server = server(imports::BATCH_BYTES).await;
    let (stop, stopped) = oneshot::channel();
    let importer = Arc::new(NotesImporter {
        stop_after: Mutex::new(Some((1, stop))),
        ..NotesImporter::default()
    });
    let _registered = server.imports.add_importer(importer.clone());
    server
        .imports
        .start(server.person, &server.parts, &server.take())
        .await
        .unwrap();
    let paused = server.run(Some(stopped)).await;
    assert_eq!(paused.state, State::Running);
    assert_eq!(paused.done, 1);

    let import = server.run(None).await;
    assert_eq!(import.state, State::Done);
    assert_eq!(import.done, 7);
    assert_eq!(*importer.stored.lock().unwrap(), 4);
    let report = server.text(import.report.unwrap()).await;
    assert!(
        report.contains("- 4 notes from Keep, in Notes\n"),
        "{report}"
    );
    assert!(!report.contains("earlier import"), "{report}");

    // One item a batch, stopped between batches.
    let server = server_with_one_item_batches().await;
    let (stop, stopped) = oneshot::channel();
    let importer = Arc::new(NotesImporter {
        stop_after: Mutex::new(Some((2, stop))),
        ..NotesImporter::default()
    });
    let _registered = server.imports.add_importer(importer.clone());
    server
        .imports
        .start(server.person, &server.parts, &server.take())
        .await
        .unwrap();
    server.run(Some(stopped)).await;
    let import = server.run(None).await;
    assert_eq!((import.state, import.done), (State::Done, 7));
    assert_eq!(*importer.stored.lock().unwrap(), 4);
    assert_eq!(server.names(&["Imported notes"]).await.len(), 5);
}

async fn server_with_one_item_batches() -> Server {
    server(1).await
}

#[tokio::test]
async fn cancels_and_still_reports() {
    let server = server(imports::BATCH_BYTES).await;
    let _registered = server
        .imports
        .add_importer(Arc::new(NotesImporter::default()));
    let id = server
        .imports
        .start(server.person, &server.parts, &server.take())
        .await
        .unwrap();
    server.imports.cancel(server.person, id).await.unwrap();
    assert!(matches!(
        server.imports.cancel(server.person, id).await,
        Err(imports::Error::NotRunning)
    ));
    let import = server.run(None).await;
    assert_eq!(import.state, State::Canceled);
    let report = server.text(import.report.unwrap()).await;
    assert!(
        report.contains("You canceled it before it finished."),
        "{report}"
    );
}

#[tokio::test]
async fn refuses_an_import_with_no_room() {
    let server = server(imports::BATCH_BYTES).await;
    let _registered = server
        .imports
        .add_importer(Arc::new(NotesImporter::default()));
    let used = server.storage.usage(server.person).await.unwrap().used;
    server
        .storage
        .set_quota(server.person, Some(used + 2000))
        .await
        .unwrap();
    server
        .imports
        .start(server.person, &server.parts, &server.take())
        .await
        .unwrap();
    let import = server.run(None).await;
    assert_eq!(import.state, State::Failed);
    let error = import.error.unwrap();
    assert!(error.contains("There isn't enough space"), "{error}");
    let top = server.storage.top_folder(server.person).await.unwrap();
    assert!(
        server
            .storage
            .find(top, vec!["Imported notes".into()])
            .await
            .unwrap()
            .is_none()
    );
    let report = server.text(import.report.unwrap()).await;
    assert!(report.contains("It stopped before it finished. There isn't enough space"));
}

#[tokio::test]
async fn checks_what_it_is_asked_to_import() {
    let server = server(imports::BATCH_BYTES).await;
    let imports = &server.imports;
    let person = server.person;

    // Nothing converts Keep until Notes adds its importer.
    assert!(matches!(
        imports.start(person, &server.parts, &server.take()).await,
        Err(imports::Error::UnknownProduct(product)) if product == "notes"
    ));
    let plan = imports.plan(person, &server.parts).await.unwrap();
    assert!(plan.folders.iter().all(|folder| folder.product.is_none()));
    let registered = imports.add_importer(Arc::new(NotesImporter::default()));
    assert_eq!(imports.products(), ["notes"]);
    let plan = imports.plan(person, &server.parts).await.unwrap();
    assert_eq!(plan.folders[0].product.as_deref(), Some("notes"));
    assert_eq!(plan.folders[0].items, 5);

    assert!(matches!(
        imports.start(person, &server.parts, &[]).await,
        Err(imports::Error::NothingChosen)
    ));
    assert!(matches!(
        imports.start(person, &[], &server.take()).await,
        Err(imports::Error::NoParts)
    ));
    let top = server.storage.top_folder(person).await.unwrap();
    assert!(matches!(
        imports.start(person, &[top], &server.take()).await,
        Err(imports::Error::PartNotAFile)
    ));
    let other = UserId::generate();
    assert!(matches!(
        imports.start(other, &server.parts, &server.take()).await,
        Err(imports::Error::PartNotFound)
    ));
    imports
        .start(person, &server.parts, &server.take())
        .await
        .unwrap();
    assert!(matches!(
        imports.start(person, &server.parts, &server.take()).await,
        Err(imports::Error::Busy)
    ));

    // Turning Notes off stops the import, which says why.
    drop(registered);
    assert!(imports.products().is_empty());
    let import = server.run(None).await;
    assert_eq!(import.state, State::Failed);
    assert!(
        import
            .error
            .unwrap()
            .contains("The app that does may have been turned off")
    );
    let kind = server
        .storage
        .item(import.report.unwrap())
        .await
        .unwrap()
        .unwrap()
        .kind;
    assert!(matches!(kind, ItemKind::File { .. }));
}

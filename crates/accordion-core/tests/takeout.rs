//! Reading Takeout's archives in the sandbox (decision 34), with archives
//! made here in the shapes the real exports had: `.zip` and `.tgz` parts,
//! a note's picture in another part, and entries that try to leave their
//! folder.

mod workers;

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};

use accordion_core::takeout::{Error, FolderFound, Take, Takeout, Tools, Unpacked};
use flate2::Compression;
use flate2::write::GzEncoder;
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
    let plan = takeout.plan(&parts(dir.path())).await.unwrap();

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
        let batch = takeout.unpack(&parts, &take, start, 100).await.unwrap();
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
    let batch = takeout.unpack(&parts, &take, 0, 1 << 30).await.unwrap();
    assert_eq!(batch.items.len(), 4);
    assert_eq!(batch.next, None);

    // Taking only the product leaves the other folder out.
    let batch = takeout
        .unpack(&parts, &[Take::Product("notes".into())], 0, 1 << 30)
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
    let err = takeout.plan(&[text]).await.unwrap_err();
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
    let plan = start(dir.path()).plan(&parts).await.unwrap();
    println!("{plan:#?}");
}

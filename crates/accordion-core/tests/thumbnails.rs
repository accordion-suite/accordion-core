//! Makes thumbnails with the real programs in the sandbox. The tests need
//! libvips (libvips-tools, with libheif's AV1 encoder for test images),
//! poppler-utils, and a kernel with Landlock, as CI has.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use accordion_core::db::Database;
use accordion_core::identity::{Identity, NewUser};
use accordion_core::storage::{ContentHash, ItemKind, Storage};
use accordion_core::thumbnails::{Thumbnails, Tools};
use accordion_types::{ItemId, Role};

struct Server {
    dir: tempfile::TempDir,
    storage: Storage,
    thumbnails: Thumbnails,
    top: ItemId,
}

async fn start_with(tools: impl FnOnce(Tools) -> Tools) -> Server {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(&dir.path().join("accordion.db")).unwrap();
    let identity = Identity::new(db.clone()).await.unwrap();
    let storage = Storage::open(db.clone(), identity.clone(), &dir.path().join("storage"))
        .await
        .unwrap();
    let user = identity
        .create_user(NewUser {
            username: "alex".into(),
            display_name: "Alex".into(),
            role: Role::Owner,
            password: "correct horse".into(),
        })
        .await
        .unwrap();
    let top = storage.top_folder(user.id).await.unwrap();
    let tools = tools(Tools::installed(env!(
        "CARGO_BIN_EXE_accordion-core-sandbox"
    )));
    let thumbnails = Thumbnails::open(db, storage.clone(), dir.path(), tools)
        .await
        .unwrap();
    Server {
        dir,
        storage,
        thumbnails,
        top,
    }
}

async fn start() -> Server {
    start_with(|tools| tools).await
}

impl Server {
    /// Saves `contents` as a file, and returns its contents' hash.
    async fn save(&self, name: &str, contents: &[u8]) -> ContentHash {
        let item = self
            .storage
            .write_file(self.top, name, contents)
            .await
            .unwrap();
        let ItemKind::File { blob, .. } = item.kind else {
            panic!("{name} isn't a file");
        };
        blob.hash
    }

    /// Makes an image with libvips, outside the sandbox, such as
    /// "photo.jpg" or "photo.avif".
    fn image(&self, name: &str, width: u32, height: u32) -> Vec<u8> {
        let path = self.dir.path().join(name);
        let made = Command::new("vips")
            .args(["xyz", path.to_str().unwrap()])
            .args([width.to_string(), height.to_string()])
            .status()
            .unwrap();
        assert!(made.success(), "libvips couldn't make {name}");
        std::fs::read(path).unwrap()
    }
}

/// The width and height of an image, and its metadata fields, from libvips.
fn describe(path: &Path) -> (u32, u32, String) {
    let output = Command::new("vipsheader")
        .arg("-a")
        .arg(path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8(output.stdout).unwrap();
    let field = |name: &str| {
        text.lines()
            .find_map(|line| line.strip_prefix(&format!("{name}: ")))
            .unwrap()
            .parse()
            .unwrap()
    };
    (field("width"), field("height"), text)
}

fn thumbnail(server: &Server, hash: ContentHash) -> PathBuf {
    server
        .thumbnails
        .find(hash)
        .expect("there should be a thumbnail")
}

/// A JPEG with a camera's make in its metadata, as phones write, and where
/// it was taken.
fn with_metadata(jpeg: &[u8]) -> Vec<u8> {
    // A TIFF header and one directory with two entries: the camera's make,
    // and a pointer to the GPS directory with the latitude's hemisphere.
    let mut tiff = Vec::new();
    tiff.extend_from_slice(b"II*\0\x08\0\0\0");
    tiff.extend_from_slice(&2u16.to_le_bytes());
    // Make: ASCII, 6 bytes, stored after both directories, at offset 56.
    tiff.extend_from_slice(&[0x0f, 0x01, 2, 0, 6, 0, 0, 0, 56, 0, 0, 0]);
    // GPSInfo: LONG, 1, the GPS directory, which follows this one at 38.
    tiff.extend_from_slice(&[0x25, 0x88, 4, 0, 1, 0, 0, 0, 38, 0, 0, 0]);
    tiff.extend_from_slice(&0u32.to_le_bytes());
    // The GPS directory: GPSLatitudeRef, ASCII, "S" in place.
    tiff.extend_from_slice(&1u16.to_le_bytes());
    tiff.extend_from_slice(&[0x01, 0x00, 2, 0, 2, 0, 0, 0, b'S', 0, 0, 0]);
    tiff.extend_from_slice(&0u32.to_le_bytes());
    tiff.extend_from_slice(b"Phone\0");
    let mut segment = b"Exif\0\0".to_vec();
    segment.extend_from_slice(&tiff);
    let mut out = jpeg[..2].to_vec();
    out.extend_from_slice(&[0xff, 0xe1]);
    out.extend_from_slice(&((segment.len() + 2) as u16).to_be_bytes());
    out.extend_from_slice(&segment);
    out.extend_from_slice(&jpeg[2..]);
    out
}

/// A one-page PDF with a filled square, written by hand.
fn pdf() -> Vec<u8> {
    let objects = [
        "<< /Type /Catalog /Pages 2 0 R >>",
        "<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R >>",
        "<< /Length 24 >>\nstream\n100 100 400 400 re f\nendstream",
    ];
    let mut pdf = b"%PDF-1.4\n".to_vec();
    let mut offsets = Vec::new();
    for (index, object) in objects.iter().enumerate() {
        offsets.push(pdf.len());
        pdf.extend_from_slice(format!("{} 0 obj\n{object}\nendobj\n", index + 1).as_bytes());
    }
    let xref = pdf.len();
    pdf.extend_from_slice(
        format!("xref\n0 {}\n0000000000 65535 f \n", objects.len() + 1).as_bytes(),
    );
    for offset in offsets {
        pdf.extend_from_slice(format!("{offset:010} 00000 n \n").as_bytes());
    }
    pdf.extend_from_slice(
        format!(
            "trailer\n<< /Size {} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n",
            objects.len() + 1
        )
        .as_bytes(),
    );
    pdf
}

#[tokio::test]
async fn makes_thumbnails_no_larger_than_480_pixels() {
    let server = start().await;
    for (name, width, height, expected) in [
        ("wide.jpg", 4000, 3000, (480, 360)),
        ("tall.png", 1500, 2000, (360, 480)),
        ("small.webp", 200, 100, (200, 100)),
        ("photo.avif", 1200, 900, (480, 360)),
    ] {
        let hash = server.save(name, &server.image(name, width, height)).await;
        assert!(server.thumbnails.make_now(hash).await.unwrap(), "{name}");
        let path = thumbnail(&server, hash);
        assert_eq!(&std::fs::read(&path).unwrap()[8..12], b"WEBP", "{name}");
        let (width, height, _) = describe(&path);
        assert_eq!((width, height), expected, "{name}");
    }
}

#[tokio::test]
async fn shows_a_pdfs_first_page() {
    let server = start().await;
    let hash = server.save("letter.pdf", &pdf()).await;
    assert!(server.thumbnails.make_now(hash).await.unwrap());
    let (width, height, _) = describe(&thumbnail(&server, hash));
    // US Letter, 8.5 by 11 inches.
    assert_eq!((width, height), (371, 480));
}

#[tokio::test]
async fn keeps_no_metadata_such_as_where_a_photo_was_taken() {
    let server = start().await;
    let original = server.dir.path().join("original.jpg");
    let jpeg = with_metadata(&server.image("plain.jpg", 800, 600));
    std::fs::write(&original, &jpeg).unwrap();
    let (_, _, fields) = describe(&original);
    assert!(fields.contains("GPSLatitudeRef"), "{fields}");

    let hash = server.save("photo.jpg", &jpeg).await;
    assert!(server.thumbnails.make_now(hash).await.unwrap());
    let (_, _, fields) = describe(&thumbnail(&server, hash));
    assert!(!fields.contains("exif"), "{fields}");
}

#[tokio::test]
async fn makes_none_for_other_files_and_huge_heic_and_avif() {
    let server = start().await;
    let notes = server.save("notes.txt", b"Pick up the bread.\n").await;
    assert!(!server.thumbnails.make_now(notes).await.unwrap());
    assert!(server.thumbnails.find(notes).is_none());

    // 25 megapixels, over the limit, since these are decoded whole.
    let huge = server
        .save("huge.avif", &server.image("huge.avif", 5000, 5000))
        .await;
    assert!(!server.thumbnails.make_now(huge).await.unwrap());

    // A JPEG with nothing after its first bytes.
    let broken = server
        .save("broken.jpg", b"\xff\xd8\xff\xe0 not really")
        .await;
    assert!(!server.thumbnails.make_now(broken).await.unwrap());
}

#[tokio::test]
async fn makes_thumbnails_in_the_background_as_files_arrive() {
    let server = start().await;
    let old = server
        .save("old.png", &server.image("old.png", 640, 480))
        .await;
    let broken = server
        .save("broken.jpg", b"\xff\xd8\xff\xe0 not really")
        .await;
    let mut status = server.thumbnails.status();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let worker = tokio::spawn(server.thumbnails.clone().run(async {
        let _ = stopped.await;
    }));

    // Files that were there first, then a file saved while it waits.
    tokio::time::timeout(Duration::from_secs(30), async {
        while server.thumbnails.find(old).is_none() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the worker should make the existing file's thumbnail");
    let new = server
        .save("new.jpg", &server.image("new.jpg", 640, 480))
        .await;
    tokio::time::timeout(Duration::from_secs(30), async {
        while server.thumbnails.find(new).is_none() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the worker should make the new file's thumbnail");
    tokio::time::timeout(
        Duration::from_secs(30),
        status.wait_for(|status| status.waiting == 0 && status.failed == 1),
    )
    .await
    .expect("the status should count the broken file")
    .unwrap();
    assert!(server.thumbnails.find(broken).is_none());

    stop.send(()).unwrap();
    worker.await.unwrap();
}

#[tokio::test]
async fn says_why_without_the_programs() {
    let server = start_with(|tools| Tools {
        vips: "/nonexistent/vips".into(),
        ..tools
    })
    .await;
    let reason = server
        .thumbnails
        .status()
        .borrow()
        .unavailable
        .clone()
        .unwrap();
    assert!(reason.contains("libvips-tools"), "{reason}");
}

#[tokio::test]
async fn makes_thumbnails_again_when_the_cache_loses_them() {
    // As after a restore: the database says there's a thumbnail, but backups
    // leave the cache out.
    let server = start().await;
    let hash = server
        .save("photo.png", &server.image("photo.png", 640, 480))
        .await;
    assert!(server.thumbnails.make_now(hash).await.unwrap());
    std::fs::remove_file(thumbnail(&server, hash)).unwrap();
    assert!(server.thumbnails.find(hash).is_none());

    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let worker = tokio::spawn(server.thumbnails.clone().run(async {
        let _ = stopped.await;
    }));
    tokio::time::timeout(Duration::from_secs(30), async {
        while server.thumbnails.find(hash).is_none() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the worker should make the thumbnail again");
    stop.send(()).unwrap();
    worker.await.unwrap();
}

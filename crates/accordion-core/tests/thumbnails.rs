//! Makes thumbnails with the real programs in the sandbox. The tests need
//! libvips (libvips-tools, with libheif's AV1 encoder for test images),
//! poppler-utils, ffmpeg, LibreOffice's Writer, Calc, and Impress,
//! libraw-bin, librsvg2-bin, djvulibre-bin, and a kernel with Landlock, as
//! CI has.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use accordion_core::db::Database;
use accordion_core::identity::{Identity, NewUser};
use accordion_core::storage::{ContentHash, ItemKind, Storage};
use accordion_core::thumbnails::{
    PREVIEW_PAGES, PreviewSettings, ThumbnailKind, Thumbnails, Tools, VideoConversion,
};
use accordion_types::{ItemId, Role};
use rusqlite::OptionalExtension;

struct Server {
    dir: tempfile::TempDir,
    db: Database,
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
    let thumbnails = Thumbnails::open(db.clone(), storage.clone(), dir.path(), tools)
        .await
        .unwrap();
    Server {
        dir,
        db,
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

    /// Makes a two-second video with ffmpeg, outside the sandbox, from its
    /// moving test pattern, with `options` for the encoder and format.
    fn video(&self, name: &str, options: &[&str]) -> Vec<u8> {
        let path = self.dir.path().join(name);
        let made = Command::new("ffmpeg")
            .args(["-v", "error", "-y", "-f", "lavfi"])
            .args(["-i", "testsrc2=size=640x360:rate=30:duration=2"])
            .args(options)
            .arg(&path)
            .status()
            .unwrap();
        assert!(made.success(), "ffmpeg couldn't make {name}");
        std::fs::read(path).unwrap()
    }

    /// Makes a document with LibreOffice, outside the sandbox, from
    /// `source`, a file LibreOffice reads, such as "letter.fodt". `to` is
    /// the extension to save as, such as "docx".
    fn document(&self, source: &str, contents: &str, to: &str) -> Vec<u8> {
        let dir = self.dir.path().join("documents");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(source), contents).unwrap();
        let profile = format!(
            "-env:UserInstallation=file://{}",
            self.dir.path().join("libreoffice").display()
        );
        let made = Command::new("soffice")
            .args([&profile, "--headless", "--norestore", "--convert-to", to])
            .arg("--outdir")
            .arg(&dir)
            .arg(dir.join(source))
            .output()
            .unwrap();
        let (stem, _) = source.rsplit_once('.').unwrap();
        let path = dir.join(format!("{stem}.{to}"));
        assert!(
            made.status.success() && path.is_file(),
            "LibreOffice couldn't make {stem}.{to}: {}",
            String::from_utf8_lossy(&made.stderr)
        );
        std::fs::read(path).unwrap()
    }
}

/// A two-page letter, as a flat OpenDocument file LibreOffice reads.
const LETTER: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<office:document xmlns:office="urn:oasis:names:tc:opendocument:xmlns:office:1.0"
  xmlns:text="urn:oasis:names:tc:opendocument:xmlns:text:1.0"
  xmlns:fo="urn:oasis:names:tc:opendocument:xmlns:xsl-fo-compatible:1.0"
  xmlns:style="urn:oasis:names:tc:opendocument:xmlns:style:1.0"
  office:version="1.3" office:mimetype="application/vnd.oasis.opendocument.text">
 <office:automatic-styles>
  <style:style style:name="P1" style:family="paragraph">
   <style:paragraph-properties fo:break-before="page"/>
  </style:style>
 </office:automatic-styles>
 <office:body><office:text>
  <text:h text:outline-level="1">Dear Sam</text:h>
  <text:p>The garden needs watering on Tuesdays.</text:p>
  <text:p text:style-name="P1">Love, Alex</text:p>
 </office:text></office:body>
</office:document>"#;

/// Two slides, as a flat OpenDocument file LibreOffice reads.
const SLIDES: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<office:document xmlns:office="urn:oasis:names:tc:opendocument:xmlns:office:1.0"
  xmlns:draw="urn:oasis:names:tc:opendocument:xmlns:drawing:1.0"
  xmlns:text="urn:oasis:names:tc:opendocument:xmlns:text:1.0"
  xmlns:svg="urn:oasis:names:tc:opendocument:xmlns:svg-compatible:1.0"
  office:version="1.3" office:mimetype="application/vnd.oasis.opendocument.presentation">
 <office:body><office:presentation>
  <draw:page draw:name="One">
   <draw:frame svg:x="2cm" svg:y="2cm" svg:width="20cm" svg:height="3cm">
    <draw:text-box><text:p>Family trip</text:p></draw:text-box>
   </draw:frame>
  </draw:page>
  <draw:page draw:name="Two">
   <draw:frame svg:x="2cm" svg:y="2cm" svg:width="20cm" svg:height="3cm">
    <draw:text-box><text:p>The lake</text:p></draw:text-box>
   </draw:frame>
  </draw:page>
 </office:presentation></office:body>
</office:document>"#;

/// How many pages a PDF has, from poppler.
fn pages(pdf: &Path) -> u32 {
    let output = Command::new("pdfinfo").arg(pdf).output().unwrap();
    String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix("Pages:"))
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}

/// Starts the background worker, and returns how to stop it.
fn start_worker(
    thumbnails: &Thumbnails,
) -> (
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let worker = tokio::spawn(thumbnails.clone().run(async {
        let _ = stopped.await;
    }));
    (stop, worker)
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

/// The picture made of the contents with `hash`.
fn thumbnail(server: &Server, hash: ContentHash) -> PathBuf {
    let found = server
        .thumbnails
        .find(hash)
        .expect("there should be a thumbnail");
    assert_eq!(found.kind, ThumbnailKind::Picture);
    found.path
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
async fn keeps_a_text_files_first_lines() {
    let server = start().await;
    let mut list = String::from("\u{feff}Shopping\r\n");
    for n in 1..=1000 {
        list.push_str(&format!("{n}. Bread\r\n"));
    }
    let hash = server.save("list.txt", list.as_bytes()).await;
    assert!(server.thumbnails.make_now(hash).await.unwrap());
    let found = server.thumbnails.find(hash).unwrap();
    assert_eq!(found.kind, ThumbnailKind::Text);
    assert_eq!(found.kind.content_type(), "text/plain; charset=utf-8");
    let text = std::fs::read_to_string(found.path).unwrap();
    assert!(text.starts_with("Shopping\n1. Bread\n2. Bread\n"), "{text}");
    assert_eq!(text.lines().count(), 40);
}

#[tokio::test]
async fn makes_none_for_other_files_and_huge_heic_and_avif() {
    let server = start().await;
    // The start of a ZIP file, such as a Word document.
    let zip = server.save("letter.docx", b"PK\x03\x04\x14\0\x06\0").await;
    assert!(!server.thumbnails.make_now(zip).await.unwrap());
    assert!(server.thumbnails.find(zip).is_none());

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

#[tokio::test]
async fn shows_a_frame_from_each_kind_of_video() {
    let server = start().await;
    let audio = ["-f", "lavfi", "-i", "sine=duration=2"];
    for (name, options) in [
        (
            "iphone.mov",
            vec![
                "-c:v",
                "libx265",
                "-preset",
                "ultrafast",
                "-x265-params",
                "log-level=error",
                "-tag:v",
                "hvc1",
            ],
        ),
        (
            "android.mp4",
            vec!["-c:v", "libx264", "-preset", "ultrafast"],
        ),
        (
            "download.webm",
            [
                &audio[..],
                &[
                    "-c:v",
                    "libvpx-vp9",
                    "-deadline",
                    "realtime",
                    "-c:a",
                    "libopus",
                ],
            ]
            .concat(),
        ),
        ("old.avi", vec!["-c:v", "mpeg4"]),
        (
            // A camcorder's, with 192-byte packets and AC-3 sound.
            "camcorder.mts",
            [
                &audio[..],
                &[
                    "-c:v",
                    "libx264",
                    "-c:a",
                    "ac3",
                    "-f",
                    "mpegts",
                    "-mpegts_m2ts_mode",
                    "1",
                ],
            ]
            .concat(),
        ),
    ] {
        let hash = server.save(name, &server.video(name, &options)).await;
        assert!(server.thumbnails.make_now(hash).await.unwrap(), "{name}");
        let (width, height, _) = describe(&thumbnail(&server, hash));
        assert_eq!((width, height), (480, 270), "{name}");
    }

    // A video already smaller stays its size.
    let small = server.video(
        "small.mp4",
        &[
            "-vf",
            "scale=200:100",
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
        ],
    );
    let hash = server.save("small.mp4", &small).await;
    assert!(server.thumbnails.make_now(hash).await.unwrap());
    let (width, height, _) = describe(&thumbnail(&server, hash));
    assert_eq!((width, height), (200, 100));
}

#[tokio::test]
async fn turns_portrait_videos_upright() {
    // Phones store portrait video sideways, with a flag to turn it.
    let server = start().await;
    server.video("sideways.mp4", &["-c:v", "libx264", "-preset", "ultrafast"]);
    let path = server.dir.path().join("portrait.mov");
    let made = Command::new("ffmpeg")
        .args(["-v", "error", "-y", "-display_rotation", "90", "-i"])
        .arg(server.dir.path().join("sideways.mp4"))
        .args(["-c", "copy"])
        .arg(&path)
        .status()
        .unwrap();
    assert!(made.success());
    let hash = server
        .save("portrait.mov", &std::fs::read(path).unwrap())
        .await;
    assert!(server.thumbnails.make_now(hash).await.unwrap());
    let (width, height, _) = describe(&thumbnail(&server, hash));
    assert_eq!((width, height), (270, 480));
}

#[tokio::test]
async fn tone_maps_hdr_video_within_its_limits() {
    // 10-bit HLG, as iPhones record HDR. Tone mapping a whole frame this
    // size would pass the memory limit, so this checks it's done after
    // shrinking.
    let server = start().await;
    let hdr = server.video(
        "hdr.mov",
        &[
            "-vf",
            "scale=3840:2160,format=yuv420p10le",
            "-c:v",
            "libx265",
            "-preset",
            "ultrafast",
            "-x265-params",
            "log-level=error:colorprim=bt2020:transfer=arib-std-b67:colormatrix=bt2020nc",
            "-color_primaries",
            "bt2020",
            "-color_trc",
            "arib-std-b67",
            "-colorspace",
            "bt2020nc",
        ],
    );
    let hash = server.save("hdr.mov", &hdr).await;
    assert!(server.thumbnails.make_now(hash).await.unwrap());
    let (width, height, _) = describe(&thumbnail(&server, hash));
    assert_eq!((width, height), (480, 270));
}

#[tokio::test]
async fn makes_none_for_songs_and_fails_cut_off_videos() {
    let server = start().await;
    let song = server.dir.path().join("song.m4a");
    let made = Command::new("ffmpeg")
        .args([
            "-v",
            "error",
            "-y",
            "-f",
            "lavfi",
            "-i",
            "sine=duration=2",
            "-c:a",
            "aac",
        ])
        .arg(&song)
        .status()
        .unwrap();
    assert!(made.success());
    // Saved as .mp4, so it gets as far as ffprobe.
    let song = server.save("song.mp4", &std::fs::read(song).unwrap()).await;
    let video = server.video("whole.mov", &["-c:v", "libx264", "-preset", "ultrafast"]);
    let cut_off = server.save("cut-off.mov", &video[..video.len() / 2]).await;

    let mut status = server.thumbnails.status();
    let (stop, worker) = start_worker(&server.thumbnails);
    // Both are in the worker's first batch, so it counts them together.
    let done = tokio::time::timeout(
        Duration::from_secs(60),
        status.wait_for(|status| status.waiting == 0 && status.failed > 0),
    )
    .await
    .expect("the worker should try both")
    .unwrap()
    .clone();
    stop.send(()).unwrap();
    worker.await.unwrap();
    assert!(server.thumbnails.find(song).is_none());
    assert!(server.thumbnails.find(cut_off).is_none());
    // A song has no video, which isn't a failure.
    assert_eq!(done.failed, 1);
}

#[tokio::test]
async fn shows_videos_once_ffmpeg_is_installed() {
    let server = start_with(|tools| Tools {
        ffmpeg: "/nonexistent/ffmpeg".into(),
        ..tools
    })
    .await;
    let reason = server.thumbnails.status().borrow().videos.clone().unwrap();
    assert!(reason.contains("ffmpeg"), "{reason}");
    let hash = server
        .save(
            "clip.mp4",
            &server.video("clip.mp4", &["-c:v", "libx264", "-preset", "ultrafast"]),
        )
        .await;
    assert!(!server.thumbnails.make_now(hash).await.unwrap());
    // Photos still work.
    let photo = server
        .save("photo.png", &server.image("photo.png", 64, 48))
        .await;
    assert!(server.thumbnails.make_now(photo).await.unwrap());

    // As if the admin installed it: the same data, with ffmpeg.
    let thumbnails = Thumbnails::open(
        server.db.clone(),
        server.storage.clone(),
        server.dir.path(),
        Tools::installed(env!("CARGO_BIN_EXE_accordion-core-sandbox")),
    )
    .await
    .unwrap();
    assert_eq!(thumbnails.status().borrow().videos, None);
    let (stop, worker) = start_worker(&thumbnails);
    tokio::time::timeout(Duration::from_secs(30), async {
        while thumbnails.find(hash).is_none() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the worker should try the video again");
    stop.send(()).unwrap();
    worker.await.unwrap();
}

#[tokio::test]
async fn shows_the_first_page_of_each_kind_of_document() {
    let server = start().await;
    let budget = "Month,Food,Rent\nJanuary,420,1200\nFebruary,395,1200\n";
    for (source, contents, formats) in [
        (
            "letter.fodt",
            LETTER,
            ["docx", "doc", "odt", "rtf"].as_slice(),
        ),
        ("budget.csv", budget, &["xlsx", "xls", "ods"]),
        ("slides.fodp", SLIDES, &["pptx", "ppt", "odp"]),
    ] {
        for to in formats {
            let (stem, _) = source.rsplit_once('.').unwrap();
            let name = format!("{stem}.{to}");
            let hash = server
                .save(&name, &server.document(source, contents, to))
                .await;
            assert!(server.thumbnails.make_now(hash).await.unwrap(), "{name}");
            let (width, height, _) = describe(&thumbnail(&server, hash));
            assert!(width.max(height) <= 480, "{name}: {width}x{height}");
            let preview = server.thumbnails.preview(hash).expect(&name);
            assert!(!preview.partial, "{name}");
            let expected = if stem == "budget" { 1 } else { 2 };
            assert_eq!(pages(&preview.path), expected, "{name}");
        }
    }
}

#[tokio::test]
async fn stops_a_long_spreadsheets_preview_at_50_pages() {
    let server = start().await;
    let rows: String = (1..=5000)
        .map(|row| format!("{row},Groceries,{}\n", row % 97))
        .collect();
    let hash = server
        .save("ledger.xlsx", &server.document("ledger.csv", &rows, "xlsx"))
        .await;
    assert!(server.thumbnails.make_now(hash).await.unwrap());
    let preview = server.thumbnails.preview(hash).unwrap();
    assert!(preview.partial);
    assert_eq!(pages(&preview.path), PREVIEW_PAGES);
}

#[tokio::test]
async fn reads_a_document_only_as_what_its_name_says() {
    let server = start().await;
    // An OpenDocument file named as Word's is read only as Word's, which
    // fails.
    let odt = server.document("letter.fodt", LETTER, "odt");
    let hash = server.save("letter.docx", &odt).await;
    assert!(!server.thumbnails.make_now(hash).await.unwrap());
    assert!(server.thumbnails.preview(hash).is_none());

    // A web page named as a document never reaches LibreOffice, and shows
    // as text, as it is.
    let hash = server
        .save("invoice.docx", b"<html><body>Pay now</body></html>")
        .await;
    assert!(server.thumbnails.make_now(hash).await.unwrap());
    assert_eq!(
        server.thumbnails.find(hash).unwrap().kind,
        ThumbnailKind::Text
    );
    assert!(server.thumbnails.preview(hash).is_none());
}

#[tokio::test]
async fn shows_documents_once_libreoffice_is_installed() {
    let server = start_with(|tools| Tools {
        soffice: "/nonexistent/soffice.bin".into(),
        ..tools
    })
    .await;
    let reason = server
        .thumbnails
        .status()
        .borrow()
        .documents
        .clone()
        .unwrap();
    assert!(reason.contains("LibreOffice"), "{reason}");
    let hash = server
        .save(
            "letter.docx",
            &server.document("letter.fodt", LETTER, "docx"),
        )
        .await;
    assert!(!server.thumbnails.make_now(hash).await.unwrap());

    // As if the admin installed it: the same data, with LibreOffice.
    let thumbnails = Thumbnails::open(
        server.db.clone(),
        server.storage.clone(),
        server.dir.path(),
        Tools::installed(env!("CARGO_BIN_EXE_accordion-core-sandbox")),
    )
    .await
    .unwrap();
    assert_eq!(thumbnails.status().borrow().documents, None);
    let (stop, worker) = start_worker(&thumbnails);
    tokio::time::timeout(Duration::from_secs(60), async {
        while thumbnails.preview(hash).is_none() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the worker should try the document again");
    assert!(thumbnails.find(hash).is_some());
    stop.send(()).unwrap();
    worker.await.unwrap();
}

#[tokio::test]
async fn tries_failed_documents_again_when_the_server_starts() {
    // As when the server starts while apt is still installing LibreOffice:
    // its program is there, but it finishes without making a PDF.
    let half_installed = tempfile::tempdir().unwrap();
    let soffice = half_installed.path().join("program").join("soffice.bin");
    std::fs::create_dir_all(soffice.parent().unwrap()).unwrap();
    std::fs::copy("/usr/bin/true", &soffice).unwrap();
    let server = start_with(|tools| Tools {
        soffice: soffice.clone(),
        ..tools
    })
    .await;
    let hash = server
        .save(
            "letter.docx",
            &server.document("letter.fodt", LETTER, "docx"),
        )
        .await;
    assert!(!server.thumbnails.make_now(hash).await.unwrap());
    assert_eq!(server.thumbnails.status().borrow().documents, None);

    // The same data after a restart, with LibreOffice installed.
    let thumbnails = Thumbnails::open(
        server.db.clone(),
        server.storage.clone(),
        server.dir.path(),
        Tools::installed(env!("CARGO_BIN_EXE_accordion-core-sandbox")),
    )
    .await
    .unwrap();
    let (stop, worker) = start_worker(&thumbnails);
    tokio::time::timeout(Duration::from_secs(60), async {
        while thumbnails.preview(hash).is_none() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the worker should try the document again");
    stop.send(()).unwrap();
    worker.await.unwrap();
}

#[tokio::test]
async fn makes_a_large_preview_of_pictures_the_browser_cant_show() {
    let server = start().await;
    let tiff = server
        .save("scan.tif", &server.image("scan.tif", 4000, 3000))
        .await;
    // Netpbm goes through ffmpeg, as Photoshop and Targa files do.
    let netpbm = server
        .save("frame.ppm", &server.image("frame.ppm", 3000, 2000))
        .await;
    let small = server
        .save("small.ppm", &server.image("small.ppm", 800, 600))
        .await;
    let png = server
        .save("photo.png", &server.image("photo.png", 4000, 3000))
        .await;
    let (stop, worker) = start_worker(&server.thumbnails);

    tokio::time::timeout(Duration::from_secs(60), async {
        while [tiff, netpbm, small]
            .iter()
            .any(|hash| server.thumbnails.picture(*hash).is_none())
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the worker should make each preview");
    let (width, height, _) = describe(&server.thumbnails.picture(tiff).unwrap());
    assert_eq!((width, height), (2560, 1920));
    let (width, height, _) = describe(&server.thumbnails.picture(netpbm).unwrap());
    assert_eq!((width, height), (2560, 1707));
    // A picture already smaller keeps its size.
    let (width, height, _) = describe(&server.thumbnails.picture(small).unwrap());
    assert_eq!((width, height), (800, 600));
    // The browser shows a PNG itself.
    assert!(server.thumbnails.picture(png).is_none());

    stop.send(()).unwrap();
    worker.await.unwrap();
}

/// A JPEG with no metadata, as cameras keep in their raw photos, made with
/// libvips outside the sandbox.
fn plain_jpeg(server: &Server, name: &str, width: u32, height: u32) -> Vec<u8> {
    let path = server.dir.path().join(name);
    let made = Command::new("vips")
        .args(["xyz", &format!("{}[strip]", path.display())])
        .args([width.to_string(), height.to_string()])
        .status()
        .unwrap();
    assert!(made.success(), "libvips couldn't make {name}");
    std::fs::read(path).unwrap()
}

/// A camera raw photo LibRaw reads: a DNG with `jpeg`, `width` by `height`,
/// as the preview the camera keeps, the photo's `orientation`, and a
/// 64-pixel raw picture of nothing. Every value fits in its directory
/// entry, so the file is the header, two directories, the raw picture, and
/// the JPEG.
fn dng(jpeg: &[u8], width: u32, height: u32, orientation: u16) -> Vec<u8> {
    const SIDE: u32 = 64;
    const BYTE: u16 = 1;
    const ASCII: u16 = 2;
    const SHORT: u16 = 3;
    const LONG: u16 = 4;
    let ascii = |text: &[u8; 4]| u32::from_le_bytes(*text);
    let raw = vec![0; (SIDE * SIDE * 2) as usize];
    let size = |entries: u32| 2 + 12 * entries + 4;
    let first = 8;
    let second = first + size(15);
    let raw_at = second + size(12);
    let jpeg_at = raw_at + raw.len() as u32;
    let preview: [(u16, u16, u32, u32); 15] = [
        // A preview, as JPEG in YCbCr.
        (254, LONG, 1, 1),
        (256, LONG, 1, width),
        (257, LONG, 1, height),
        (258, SHORT, 1, 8),
        (259, SHORT, 1, 7),
        (262, SHORT, 1, 6),
        (271, ASCII, 4, ascii(b"Test")),
        (272, ASCII, 4, ascii(b"Cam\0")),
        (273, LONG, 1, jpeg_at),
        (274, SHORT, 1, u32::from(orientation)),
        (277, SHORT, 1, 3),
        (279, LONG, 1, jpeg.len() as u32),
        (330, LONG, 1, second),
        // DNG 1.4, and the camera's name.
        (50706, BYTE, 4, u32::from_le_bytes([1, 4, 0, 0])),
        (50708, ASCII, 4, ascii(b"Cam\0")),
    ];
    let sensor: [(u16, u16, u32, u32); 12] = [
        // The full picture, 16 bits a pixel, through a 2×2 color filter.
        (254, LONG, 1, 0),
        (256, LONG, 1, SIDE),
        (257, LONG, 1, SIDE),
        (258, SHORT, 1, 16),
        (259, SHORT, 1, 1),
        (262, SHORT, 1, 32803),
        (273, LONG, 1, raw_at),
        (277, SHORT, 1, 1),
        (278, LONG, 1, SIDE),
        (279, LONG, 1, raw.len() as u32),
        (33421, SHORT, 2, 2 | (2 << 16)),
        (33422, BYTE, 4, u32::from_le_bytes([0, 1, 1, 2])),
    ];
    let mut file = b"II*\0".to_vec();
    file.extend(first.to_le_bytes());
    for directory in [&preview[..], &sensor[..]] {
        file.extend((directory.len() as u16).to_le_bytes());
        for (tag, kind, count, value) in directory {
            file.extend(tag.to_le_bytes());
            file.extend(kind.to_le_bytes());
            file.extend(count.to_le_bytes());
            file.extend(value.to_le_bytes());
        }
        file.extend(0u32.to_le_bytes());
    }
    file.extend(raw);
    file.extend(jpeg);
    file
}

/// Waits for the worker to make a preview of each of `hashes`.
async fn wait_for_pictures(thumbnails: &Thumbnails, hashes: &[ContentHash]) {
    tokio::time::timeout(Duration::from_secs(60), async {
        while hashes
            .iter()
            .any(|hash| thumbnails.picture(*hash).is_none())
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the worker should make each preview");
}

#[tokio::test]
async fn shows_the_jpeg_a_camera_keeps_in_a_raw_photo() {
    let server = start().await;
    // Kept as the sensor saw it, on its side, for a photo taken upright.
    let sideways = plain_jpeg(&server, "sideways.jpg", 3000, 2000);
    let sideways = server
        .save("DSC_0001.dng", &dng(&sideways, 3000, 2000, 6))
        .await;
    // Already turned by the camera, with the same orientation.
    let turned = plain_jpeg(&server, "turned.jpg", 2000, 3000);
    let turned = server
        .save("DSC_0002.DNG", &dng(&turned, 2000, 3000, 6))
        .await;
    for hash in [sideways, turned] {
        assert!(server.thumbnails.make_now(hash).await.unwrap());
        let (width, height, _) = describe(&thumbnail(&server, hash));
        assert_eq!((width, height), (320, 480));
    }

    let (stop, worker) = start_worker(&server.thumbnails);
    wait_for_pictures(&server.thumbnails, &[sideways, turned]).await;
    for hash in [sideways, turned] {
        let (width, height, _) = describe(&server.thumbnails.picture(hash).unwrap());
        assert_eq!((width, height), (1707, 2560));
    }
    stop.send(()).unwrap();
    worker.await.unwrap();
}

/// A drawing with a script and a picture from another file, neither of
/// which the worker runs or reads.
const DRAWING: &str = r##"<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="200" height="100" viewBox="0 0 200 100">
 <script>alert("never runs")</script>
 <rect width="200" height="100" fill="#2a6"/>
 <image href="file:///etc/hostname" width="10" height="10"/>
 <text x="10" y="60" font-size="30">Garden</text>
</svg>"##;

#[tokio::test]
async fn draws_svg_drawings_as_pictures() {
    let server = start().await;
    let drawing = server.save("garden.svg", DRAWING.as_bytes()).await;
    let path = server.dir.path().join("garden.svg");
    std::fs::write(&path, DRAWING).unwrap();
    let compressed = Command::new("gzip").arg("-c").arg(&path).output().unwrap();
    assert!(compressed.status.success());
    let compressed = server.save("garden.svgz", &compressed.stdout).await;
    for hash in [drawing, compressed] {
        assert!(server.thumbnails.make_now(hash).await.unwrap());
        let (width, height, _) = describe(&thumbnail(&server, hash));
        assert_eq!((width, height), (480, 240));
    }

    // Drawn at the preview's size, not enlarged from a small picture.
    let (stop, worker) = start_worker(&server.thumbnails);
    wait_for_pictures(&server.thumbnails, &[drawing, compressed]).await;
    for hash in [drawing, compressed] {
        let (width, height, _) = describe(&server.thumbnails.picture(hash).unwrap());
        assert_eq!((width, height), (2560, 1280));
    }
    stop.send(()).unwrap();
    worker.await.unwrap();
}

/// Makes a DjVu book of `pages` pages, each with a black bar, with
/// djvulibre outside the sandbox.
fn book(server: &Server, pages: usize) -> Vec<u8> {
    let dir = server.dir.path().join("book");
    std::fs::create_dir_all(&dir).unwrap();
    let (width, height) = (850, 1100);
    let mut bitmap = format!("P4\n{width} {height}\n").into_bytes();
    for y in 0..height {
        let ink = if (100..200).contains(&y) { 0xff } else { 0 };
        bitmap.extend(std::iter::repeat_n(ink, (width as usize).div_ceil(8)));
    }
    let pbm = dir.join("page.pbm");
    std::fs::write(&pbm, bitmap).unwrap();
    let parts: Vec<PathBuf> = (0..pages)
        .map(|page| {
            let part = dir.join(format!("{page}.djvu"));
            let made = Command::new("cjb2").arg(&pbm).arg(&part).status().unwrap();
            assert!(made.success(), "djvulibre couldn't make a page");
            part
        })
        .collect();
    let path = dir.join("book.djvu");
    let made = Command::new("djvm")
        .arg("-c")
        .arg(&path)
        .args(&parts)
        .status()
        .unwrap();
    assert!(made.success(), "djvulibre couldn't make the book");
    std::fs::read(path).unwrap()
}

#[tokio::test]
async fn shows_a_djvu_books_pages() {
    let server = start().await;
    let hash = server.save("Manual.djvu", &book(&server, 2)).await;
    assert!(server.thumbnails.make_now(hash).await.unwrap());
    let (width, height, _) = describe(&thumbnail(&server, hash));
    assert_eq!(height, 480);
    assert!(width < height, "{width}x{height}");
    let preview = server.thumbnails.preview(hash).unwrap();
    assert!(!preview.partial);
    assert_eq!(pages(&preview.path), 2);
}

#[tokio::test]
async fn shows_raw_photos_drawings_and_books_once_their_programs_are_installed() {
    let server = start_with(|tools| Tools {
        simple_dcraw: "/nonexistent/simple_dcraw".into(),
        rsvg_convert: "/nonexistent/rsvg-convert".into(),
        ddjvu: "/nonexistent/ddjvu".into(),
        ..tools
    })
    .await;
    let reason = server
        .thumbnails
        .status()
        .borrow()
        .pictures
        .clone()
        .unwrap();
    assert!(
        reason.contains("libraw-bin, librsvg2-bin, and djvulibre-bin"),
        "{reason}"
    );
    let drawing = server.save("garden.svg", DRAWING.as_bytes()).await;
    let manual = server.save("Manual.djvu", &book(&server, 1)).await;
    let jpeg = plain_jpeg(&server, "photo.jpg", 300, 200);
    let photo = server.save("DSC_0003.dng", &dng(&jpeg, 300, 200, 1)).await;
    for hash in [drawing, manual, photo] {
        assert!(!server.thumbnails.make_now(hash).await.unwrap());
    }

    // As if the admin installed them: the same data, with the programs.
    let thumbnails = Thumbnails::open(
        server.db.clone(),
        server.storage.clone(),
        server.dir.path(),
        Tools::installed(env!("CARGO_BIN_EXE_accordion-core-sandbox")),
    )
    .await
    .unwrap();
    assert_eq!(thumbnails.status().borrow().pictures, None);
    let (stop, worker) = start_worker(&thumbnails);
    tokio::time::timeout(Duration::from_secs(30), async {
        while [drawing, manual, photo]
            .iter()
            .any(|hash| thumbnails.find(*hash).is_none())
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the worker should try each file again");
    stop.send(()).unwrap();
    worker.await.unwrap();
}

/// An MNG animation that starts with `png`, as some programs save a still
/// picture: its header, the PNG's chunks as they are, and its end.
fn mng(png: &[u8]) -> Vec<u8> {
    let chunk = |name: &[u8; 4], data: &[u8]| {
        let mut chunk = (data.len() as u32).to_be_bytes().to_vec();
        chunk.extend_from_slice(name);
        chunk.extend_from_slice(data);
        // The checksum, which MNG readers check but the worker doesn't.
        chunk.extend_from_slice(&[0; 4]);
        chunk
    };
    let mut file = b"\x8aMNG\r\n\x1a\n".to_vec();
    file.extend(chunk(b"MHDR", &[0; 28]));
    file.extend_from_slice(&png[8..]);
    file.extend(chunk(b"MEND", b""));
    file
}

#[tokio::test]
async fn shows_hdr_fits_and_mng_pictures() {
    let server = start().await;
    // Radiance pictures are color, so this one has three bands.
    let path = server.dir.path().join("sky.hdr");
    let made = Command::new("vips")
        .args([
            "black",
            path.to_str().unwrap(),
            "3000",
            "2000",
            "--bands",
            "3",
        ])
        .status()
        .unwrap();
    assert!(made.success(), "libvips couldn't make sky.hdr");
    let hdr = server.save("sky.hdr", &std::fs::read(path).unwrap()).await;
    let fits = server
        .save("m31.fits", &server.image("m31.fits", 3000, 2000))
        .await;
    let animation = mng(&server.image("frame.png", 3000, 2000));
    let animation = server.save("logo.mng", &animation).await;
    for hash in [hdr, fits, animation] {
        assert!(server.thumbnails.make_now(hash).await.unwrap());
        let (width, height, _) = describe(&thumbnail(&server, hash));
        assert_eq!((width, height), (480, 320));
    }

    let (stop, worker) = start_worker(&server.thumbnails);
    wait_for_pictures(&server.thumbnails, &[hdr, fits, animation]).await;
    for hash in [hdr, fits, animation] {
        let (width, height, _) = describe(&server.thumbnails.picture(hash).unwrap());
        assert_eq!((width, height), (2560, 1707));
    }
    stop.send(()).unwrap();
    worker.await.unwrap();
}

impl Server {
    /// Makes a three-second sound with ffmpeg, outside the sandbox, with
    /// `options` for the encoder and format, and `cover` as its cover art
    /// if there's one.
    fn sound(&self, name: &str, options: &[&str], cover: Option<&Path>) -> Vec<u8> {
        let path = self.dir.path().join(name);
        let mut command = Command::new("ffmpeg");
        command
            .args(["-v", "error", "-y", "-f", "lavfi"])
            .args(["-i", "sine=frequency=440:duration=3"]);
        if let Some(cover) = cover {
            command
                .arg("-i")
                .arg(cover)
                .args(["-map", "0:a", "-map", "1:v", "-c:v", "copy"])
                .args(["-disposition:v", "attached_pic"]);
        }
        let made = command.args(options).arg(&path).status().unwrap();
        assert!(made.success(), "ffmpeg couldn't make {name}");
        std::fs::read(path).unwrap()
    }

    /// Why the contents with `hash` have no preview.
    async fn preview_reason(&self, hash: ContentHash) -> Option<String> {
        self.db
            .read(move |conn| {
                conn.query_row(
                    "SELECT preview_reason FROM thumbnails WHERE hash = ?1",
                    [&hash.0[..]],
                    |row| row.get(0),
                )
            })
            .await
            .unwrap()
    }

    /// Waits until the worker has decided every preview.
    async fn wait_for_previews(&self) {
        tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                let waiting: i64 = self
                    .db
                    .read(|conn| {
                        conn.query_row(
                            "SELECT count(*) FROM storage_blobs b
                             WHERE NOT EXISTS (SELECT 1 FROM thumbnails t
                                               WHERE t.hash = b.hash AND t.preview IS NOT NULL)",
                            [],
                            |row| row.get(0),
                        )
                    })
                    .await
                    .unwrap();
                if waiting == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the worker should decide each preview");
    }
}

/// Three seconds of a tone in one of SoX's own formats, from SoX.
fn sox(dir: &Path, format: &str) -> Vec<u8> {
    let path = dir.join(format!("tone.{format}"));
    let made = Command::new("sox")
        .args(["-V1", "-n", "-c", "1"])
        .arg(&path)
        .args(["synth", "3", "sine", "440"])
        .status()
        .unwrap();
    assert!(made.success(), "SoX couldn't make a .{format}");
    std::fs::read(path).unwrap()
}

/// What ffprobe says about a converted sound: its codec, its channels, and
/// how long it is, in seconds.
fn listen(path: &Path) -> (String, u64, f64) {
    let probed = Command::new("ffprobe")
        .args(["-v", "error", "-select_streams", "a:0"])
        .args([
            "-show_entries",
            "stream=codec_name,channels:format=duration",
        ])
        .args(["-of", "default=noprint_wrappers=1"])
        .arg(path)
        .output()
        .unwrap();
    let text = String::from_utf8(probed.stdout).unwrap();
    let field = |name: &str| {
        text.lines()
            .find_map(|line| line.strip_prefix(&format!("{name}=")))
            .unwrap_or_else(|| panic!("ffprobe didn't say the {name}: {text}"))
            .to_owned()
    };
    (
        field("codec_name"),
        field("channels").parse().unwrap(),
        field("duration").parse().unwrap(),
    )
}

/// A tracker module: one instrument, a square wave, played once.
fn module() -> Vec<u8> {
    let mut module = b"tune".to_vec();
    module.resize(20, 0);
    for instrument in 0..31 {
        let mut header = [0; 30];
        if instrument == 0 {
            header[..6].copy_from_slice(b"square");
            // 16 words long, at full volume, looping all of it.
            header[22..30].copy_from_slice(&[0, 16, 0, 64, 0, 0, 0, 16]);
        } else {
            header[29] = 1;
        }
        module.extend(header);
    }
    // One pattern in the song, then its 64 rows of four channels.
    module.extend([1, 127]);
    module.extend([0; 128]);
    module.extend(b"M.K.");
    let mut pattern = [0; 64 * 4 * 4];
    pattern[..4].copy_from_slice(&[0x01, 0xac, 0x10, 0x00]);
    module.extend(pattern);
    module.extend([100; 16]);
    module.extend([156; 16]);
    module
}

#[tokio::test]
async fn shows_a_songs_cover_art() {
    let server = start().await;
    let cover = server.dir.path().join("cover.jpg");
    std::fs::write(&cover, server.image("cover.jpg", 600, 400)).unwrap();
    let mp3 = server.sound("song.mp3", &["-c:a", "libmp3lame"], Some(&cover));
    let flac = server.sound("song.flac", &["-c:a", "flac"], Some(&cover));
    let m4a = server.sound("song.m4a", &["-c:a", "aac"], Some(&cover));
    let plain = server.sound("plain.mp3", &["-c:a", "libmp3lame"], None);
    for (name, contents) in [("Song.mp3", &mp3), ("Song.flac", &flac), ("Song.m4a", &m4a)] {
        let hash = server.save(name, contents).await;
        assert!(server.thumbnails.make_now(hash).await.unwrap(), "{name}");
        let (width, height, _) = describe(&thumbnail(&server, hash));
        assert_eq!((width, height), (480, 320), "{name}");
    }
    let plain = server.save("Plain.mp3", &plain).await;
    assert!(!server.thumbnails.make_now(plain).await.unwrap());
    assert_eq!(server.thumbnails.status().borrow().failed, 0);
}

#[tokio::test]
async fn converts_sound_browsers_cant_play_to_aac() {
    let server = start().await;
    let cover = server.dir.path().join("cover.jpg");
    std::fs::write(&cover, server.image("cover.jpg", 600, 400)).unwrap();
    let converted = [
        server
            .save(
                "Song.wma",
                &server.sound("song.wma", &["-c:a", "wmav2"], None),
            )
            .await,
        server
            .save("Song.aiff", &server.sound("song.aiff", &[], None))
            .await,
        // Apple Lossless has the same name as AAC, but most browsers don't
        // play it.
        server
            .save(
                "Lossless.m4a",
                &server.sound("lossless.m4a", &["-c:a", "alac"], Some(&cover)),
            )
            .await,
        server
            .save("Old.mp2", &server.sound("old.mp2", &["-c:a", "mp2"], None))
            .await,
        server
            .save(
                "Film.ac3",
                &server.sound("film.ac3", &["-ac", "6", "-c:a", "ac3"], None),
            )
            .await,
        server
            .save(
                "Ringtone.mka",
                &server.sound("ringtone.mka", &["-c:a", "libvorbis"], None),
            )
            .await,
        server.save("Tune.mod", &module()).await,
        // Stereo Speex doesn't say which channel is which, which AAC needs.
        server
            .save(
                "Speech.spx",
                &server.sound(
                    "speech.spx",
                    &["-ac", "2", "-c:a", "libspeex", "-f", "ogg"],
                    None,
                ),
            )
            .await,
    ];
    let played = [
        server
            .save(
                "Song.mp3",
                &server.sound("song.mp3", &["-c:a", "libmp3lame"], None),
            )
            .await,
        server
            .save("Take.wav", &server.sound("take.wav", &[], None))
            .await,
    ];

    let (stop, worker) = start_worker(&server.thumbnails);
    server.wait_for_previews().await;
    stop.send(()).unwrap();
    worker.await.unwrap();
    for hash in converted {
        let Some(sound) = server.thumbnails.sound(hash) else {
            panic!(
                "no converted sound: {:?}",
                server.preview_reason(hash).await
            );
        };
        let (codec, channels, seconds) = listen(&sound);
        assert_eq!(codec, "aac");
        // Surround sound is mixed down.
        assert!(channels <= 2, "{channels} channels");
        assert!(seconds > 2.0, "{seconds} seconds");
    }
    for hash in played {
        assert!(server.thumbnails.sound(hash).is_none());
    }
    // Apple Lossless keeps its cover art too.
    assert!(server.thumbnails.find(converted[2]).is_some());
    assert_eq!(server.thumbnails.status().borrow().failed, 0);
}

#[tokio::test]
async fn plays_older_sound_once_sox_is_installed() {
    let server = start_with(|tools| Tools {
        sox: "/nonexistent/sox".into(),
        ..tools
    })
    .await;
    let reason = server
        .thumbnails
        .status()
        .borrow()
        .pictures
        .clone()
        .unwrap();
    assert!(reason.contains("Install sox with apt"), "{reason}");
    let mut sounds = Vec::new();
    for format in [
        "cdda", "cvsd", "cvu", "dvms", "fssd", "gsrt", "sndr", "sndt", "txw",
    ] {
        let tone = sox(server.dir.path(), format);
        sounds.push((format, server.save(&format!("Tone.{format}"), &tone).await));
    }
    for format in [
        "paf", "fap", "smp", "wve", "htk", "vox", "cvs", "vms", "sou",
    ] {
        let tone = sox(server.dir.path(), format);
        sounds.push((format, server.save(&format!("Tone.{format}"), &tone).await));
    }
    // A SoundTool file as it's often named, known by its first bytes.
    let snd = sox(server.dir.path(), "sndt");
    sounds.push(("snd", server.save("Tone.snd", &snd).await));
    // Sound Designer II, as it is without the resource fork a Mac keeps.
    let sd2 = server.dir.path().join("tone.sd2");
    let made = Command::new("sox")
        .args([
            "-V1",
            "-n",
            "-c",
            "2",
            "-r",
            "44100",
            "-t",
            "raw",
            "-e",
            "signed-integer",
        ])
        .args(["-b", "16", "-B"])
        .arg(&sd2)
        .args(["synth", "3", "sine", "440"])
        .status()
        .unwrap();
    assert!(made.success());
    let sd2 = std::fs::read(sd2).unwrap();
    sounds.push(("sd2", server.save("Take.sd2", &sd2).await));
    let (stop, worker) = start_worker(&server.thumbnails);
    server.wait_for_previews().await;
    stop.send(()).unwrap();
    worker.await.unwrap();
    for (format, hash) in &sounds {
        assert_eq!(
            server.preview_reason(*hash).await.as_deref(),
            Some("SoX isn't installed"),
            "{format}"
        );
    }

    // As if the admin installed it.
    let thumbnails = Thumbnails::open(
        server.db.clone(),
        server.storage.clone(),
        server.dir.path(),
        Tools::installed(env!("CARGO_BIN_EXE_accordion-core-sandbox")),
    )
    .await
    .unwrap();
    let (stop, worker) = start_worker(&thumbnails);
    tokio::time::timeout(Duration::from_secs(60), async {
        while sounds
            .iter()
            .any(|(_, hash)| thumbnails.sound(*hash).is_none())
            && thumbnails.status().borrow().failed == 0
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the worker should convert each sound");
    stop.send(()).unwrap();
    worker.await.unwrap();
    for (format, hash) in sounds {
        let Some(sound) = thumbnails.sound(hash) else {
            panic!(
                ".{format} wasn't converted: {:?}",
                server.preview_reason(hash).await
            );
        };
        let (codec, channels, seconds) = listen(&sound);
        assert_eq!(codec, "aac", "{format}");
        let stereo = ["cdda", "sd2"].contains(&format);
        assert_eq!(channels, if stereo { 2 } else { 1 }, "{format}");
        // Formats without a header play at the rate SoX assumes, which
        // isn't the tone's.
        if !["cvsd", "cvs", "cvu", "fssd", "sou", "vox"].contains(&format) {
            assert!((seconds - 3.0).abs() < 0.2, ".{format}: {seconds} seconds");
        }
        // They can't have cover art.
        assert!(thumbnails.find(hash).is_none(), "{format}");
    }
}

/// What ffprobe says about a video's preview: its picture's codec, width,
/// and height, and its sound's codec, if it has sound.
fn watch(path: &Path) -> (String, u32, u32, Option<String>) {
    let probed = Command::new("ffprobe")
        .args(["-v", "error"])
        .args(["-show_entries", "stream=codec_type,codec_name,width,height"])
        .args(["-of", "json"])
        .arg(path)
        .output()
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&probed.stdout).unwrap();
    let streams = json["streams"].as_array().unwrap();
    let picture = streams
        .iter()
        .find(|stream| stream["codec_type"] == "video")
        .expect("a picture");
    let sound = streams
        .iter()
        .find(|stream| stream["codec_type"] == "audio")
        .map(|stream| stream["codec_name"].as_str().unwrap().to_owned());
    (
        picture["codec_name"].as_str().unwrap().to_owned(),
        picture["width"].as_u64().unwrap() as u32,
        picture["height"].as_u64().unwrap() as u32,
        sound,
    )
}

impl Server {
    /// A video's preview state: video, waiting, none, or failed.
    async fn preview_state(&self, hash: ContentHash) -> Option<String> {
        self.db
            .read(move |conn| {
                conn.query_row(
                    "SELECT preview FROM thumbnails WHERE hash = ?1",
                    [&hash.0[..]],
                    |row| row.get(0),
                )
                .optional()
            })
            .await
            .unwrap()
            .flatten()
    }

    /// Waits until each video's preview is made, or decided against.
    async fn wait_for_videos(&self, hashes: &[ContentHash]) {
        tokio::time::timeout(Duration::from_secs(120), async {
            loop {
                let mut done = true;
                for hash in hashes {
                    let state = self.preview_state(*hash).await;
                    done &= !matches!(state.as_deref(), None | Some("waiting"));
                }
                if done {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the worker should make each video's preview");
    }

    /// Waits until the worker has decided how each video's preview is made.
    async fn wait_for_plans(&self, hashes: &[ContentHash]) {
        tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                let mut done = true;
                for hash in hashes {
                    done &= self.preview_state(*hash).await.is_some();
                }
                if done {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the worker should decide each video");
    }
}

const SOUND: [&str; 4] = ["-f", "lavfi", "-i", "sine=duration=2"];

#[tokio::test]
async fn repackages_or_converts_videos_browsers_cant_play() {
    let server = start().await;
    let settings = PreviewSettings {
        convert_videos: Some(VideoConversion::Upload),
        space_percent: 10,
    };
    server.thumbnails.set_settings(settings).await.unwrap();
    assert_eq!(server.thumbnails.settings().await.unwrap(), settings);
    let h264 = ["-c:v", "libx264", "-preset", "ultrafast"];
    let video = |name: &str, options: &[&[&str]]| server.video(name, &options.concat());
    let film = server
        .save(
            "Film.mkv",
            &video("film.mkv", &[&SOUND, &h264, &["-c:a", "aac"]]),
        )
        .await;
    let camcorder = server
        .save(
            "00001.MTS",
            &video(
                "camcorder.mts",
                &[&SOUND, &h264, &["-c:a", "ac3", "-f", "mpegts"]],
            ),
        )
        .await;
    let talk = server
        .save(
            "Talk.mkv",
            &video(
                "talk.mkv",
                &[
                    &SOUND,
                    &["-c:v", "libvpx-vp9", "-deadline", "realtime"],
                    &["-c:a", "libopus"],
                ],
            ),
        )
        .await;
    let old = server
        .save(
            "Old.avi",
            &video(
                "old.avi",
                &[&SOUND, &["-c:v", "mpeg4", "-c:a", "libmp3lame"]],
            ),
        )
        .await;
    let iphone = server
        .save(
            "IMG_0001.MOV",
            &video(
                "iphone.mov",
                &[
                    &SOUND,
                    &[
                        "-vf",
                        "transpose=1",
                        "-c:v",
                        "libx265",
                        "-preset",
                        "ultrafast",
                    ],
                    &[
                        "-x265-params",
                        "log-level=error",
                        "-tag:v",
                        "hvc1",
                        "-c:a",
                        "aac",
                    ],
                ],
            ),
        )
        .await;
    let large = server
        .save(
            "Large.avi",
            &video("large.avi", &[&["-vf", "scale=2560:1440", "-c:v", "mpeg4"]]),
        )
        .await;
    let plays = server
        .save(
            "Android.mp4",
            &video("android.mp4", &[&SOUND, &h264, &["-c:a", "aac"]]),
        )
        .await;

    let (stop, worker) = start_worker(&server.thumbnails);
    let all = [film, camcorder, talk, old, iphone, large, plays];
    server.wait_for_videos(&all).await;
    stop.send(()).unwrap();
    worker.await.unwrap();

    let expect = [
        (film, "video/mp4", "h264", 640, 360, Some("aac")),
        (camcorder, "video/mp4", "h264", 640, 360, Some("aac")),
        (talk, "video/webm", "vp9", 640, 360, Some("opus")),
        // Browsers play MP3 in MP4, so it's copied.
        (old, "video/mp4", "h264", 640, 360, Some("mp3")),
        // Turned upright, as the phone recorded it.
        (iphone, "video/mp4", "h264", 360, 640, Some("aac")),
        // Shrunk to 1080p.
        (large, "video/mp4", "h264", 1920, 1080, None),
    ];
    for (hash, kind, codec, width, height, sound) in expect {
        let Some((path, found)) = server.thumbnails.video(hash) else {
            panic!(
                "no preview for {kind} {codec}: {:?}",
                server.preview_reason(hash).await
            );
        };
        assert_eq!(found, kind);
        assert_eq!(
            watch(&path),
            (codec.into(), width, height, sound.map(str::to_owned)),
            "{codec} {width}x{height}"
        );
    }
    assert!(server.thumbnails.video(plays).is_none());
    assert_eq!(
        server.preview_reason(plays).await.as_deref(),
        Some("the browser plays it")
    );
    assert!(server.thumbnails.space().await.unwrap().used > 0);
    assert_eq!(server.thumbnails.status().borrow().failed, 0);
}

#[tokio::test]
async fn converts_a_video_when_someone_opens_it_or_never() {
    let server = start().await;
    server
        .thumbnails
        .set_settings(PreviewSettings {
            convert_videos: Some(VideoConversion::Opened),
            space_percent: 10,
        })
        .await
        .unwrap();
    let old = server
        .save("Old.avi", &server.video("old.avi", &["-c:v", "mpeg4"]))
        .await;
    // Repackaging is quick, so it happens after upload anyway.
    let film = server
        .save(
            "Film.mkv",
            &server.video("film.mkv", &["-c:v", "libx264", "-preset", "ultrafast"]),
        )
        .await;
    let (stop, worker) = start_worker(&server.thumbnails);
    server.wait_for_videos(&[film]).await;
    assert!(server.thumbnails.video(film).is_some());
    assert_eq!(server.preview_state(old).await.as_deref(), Some("waiting"));
    assert!(server.thumbnails.video_waiting(old));
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(server.thumbnails.video(old).is_none());

    assert!(server.thumbnails.want(old).await.unwrap());
    server.wait_for_videos(&[old]).await;
    assert!(server.thumbnails.video(old).is_some());

    // On a server that never converts, a video that needs it waits for
    // nothing, and opening it doesn't start it.
    server
        .thumbnails
        .set_settings(PreviewSettings {
            convert_videos: Some(VideoConversion::Never),
            space_percent: 10,
        })
        .await
        .unwrap();
    let never = server
        .save(
            "Never.avi",
            &server.video("never.avi", &["-c:v", "mpeg4", "-t", "1"]),
        )
        .await;
    server.wait_for_plans(&[never]).await;
    assert!(!server.thumbnails.video_waiting(never));
    assert!(!server.thumbnails.want(never).await.unwrap());
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(server.thumbnails.video(never).is_none());
    stop.send(()).unwrap();
    worker.await.unwrap();
}

#[tokio::test]
async fn makes_a_lost_video_preview_again_when_someone_opens_it() {
    let server = start().await;
    server
        .thumbnails
        .set_settings(PreviewSettings {
            convert_videos: Some(VideoConversion::Upload),
            space_percent: 10,
        })
        .await
        .unwrap();
    let film = server
        .save(
            "Film.mkv",
            &server.video("film.mkv", &["-c:v", "libx264", "-preset", "ultrafast"]),
        )
        .await;
    let (stop, worker) = start_worker(&server.thumbnails);
    server.wait_for_videos(&[film]).await;
    stop.send(()).unwrap();
    worker.await.unwrap();
    let (path, _) = server.thumbnails.video(film).unwrap();
    std::fs::remove_file(path).unwrap();

    // The worker notices when it starts, as the cache limit would leave
    // it, but doesn't make it again until someone opens it.
    let (stop, worker) = start_worker(&server.thumbnails);
    tokio::time::timeout(Duration::from_secs(30), async {
        while !server.thumbnails.video_waiting(film) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the worker should notice the lost preview");
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(server.thumbnails.video(film).is_none());
    assert!(server.thumbnails.want(film).await.unwrap());
    server.wait_for_videos(&[film]).await;
    assert!(server.thumbnails.video(film).is_some());
    assert!(!server.thumbnails.video_waiting(film));
    stop.send(()).unwrap();
    worker.await.unwrap();
}

#[tokio::test]
async fn converts_4k_hdr_phone_video_within_its_limits() {
    let server = start().await;
    server
        .thumbnails
        .set_settings(PreviewSettings {
            convert_videos: Some(VideoConversion::Upload),
            space_percent: 10,
        })
        .await
        .unwrap();
    let path = server.dir.path().join("hdr.mov");
    let made = Command::new("ffmpeg")
        .args(["-v", "error", "-y", "-f", "lavfi"])
        .args(["-i", "testsrc2=size=3840x2160:rate=30:duration=1"])
        .args([
            "-pix_fmt",
            "yuv420p10le",
            "-c:v",
            "libx265",
            "-preset",
            "ultrafast",
        ])
        .args([
            "-x265-params",
            "log-level=error:colorprim=bt2020:transfer=arib-std-b67:colormatrix=bt2020nc",
        ])
        .args(["-tag:v", "hvc1"])
        .arg(&path)
        .status()
        .unwrap();
    assert!(made.success());
    let hash = server
        .save("IMG_0003.MOV", &std::fs::read(path).unwrap())
        .await;
    let (stop, worker) = start_worker(&server.thumbnails);
    server.wait_for_videos(&[hash]).await;
    stop.send(()).unwrap();
    worker.await.unwrap();
    let Some((path, _)) = server.thumbnails.video(hash) else {
        panic!("no preview: {:?}", server.preview_reason(hash).await);
    };
    assert_eq!(watch(&path), ("h264".into(), 1920, 1080, None));
}

#[tokio::test]
async fn converts_videos_from_cameras_tv_and_without_a_container() {
    let server = start().await;
    server
        .thumbnails
        .set_settings(PreviewSettings {
            convert_videos: Some(VideoConversion::Upload),
            space_percent: 10,
        })
        .await
        .unwrap();
    let mpeg2 = ["-c:v", "mpeg2video"];
    let videos = [
        (
            "Camera.mxf",
            "camera.mxf",
            [&mpeg2[..], &["-f", "mxf"]].concat(),
        ),
        (
            "Recording.wtv",
            "recording.wtv",
            [&mpeg2[..], &["-f", "wtv"]].concat(),
        ),
        (
            "Tape.dv",
            "tape.dv",
            vec![
                "-vf",
                "scale=720:480",
                "-r",
                "30000/1001",
                "-pix_fmt",
                "yuv411p",
                "-c:v",
                "dvvideo",
                "-f",
                "dv",
            ],
        ),
        (
            "Camera.mjpeg",
            "camera.mjpeg",
            vec!["-c:v", "mjpeg", "-f", "mjpeg"],
        ),
        (
            "Clip.hevc",
            "clip.hevc",
            vec![
                "-c:v",
                "libx265",
                "-preset",
                "ultrafast",
                "-x265-params",
                "log-level=error",
                "-f",
                "hevc",
            ],
        ),
        (
            "Clip.h264",
            "clip.h264",
            vec!["-c:v", "libx264", "-preset", "ultrafast", "-f", "h264"],
        ),
    ];
    let mut hashes = Vec::new();
    for (name, made, options) in &videos {
        hashes.push((*name, server.save(name, &server.video(made, options)).await));
    }
    let (stop, worker) = start_worker(&server.thumbnails);
    let all: Vec<ContentHash> = hashes.iter().map(|(_, hash)| *hash).collect();
    server.wait_for_videos(&all).await;
    tokio::time::timeout(Duration::from_secs(30), async {
        while all
            .iter()
            .any(|hash| server.thumbnails.find(*hash).is_none())
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("each video should get a thumbnail");
    stop.send(()).unwrap();
    worker.await.unwrap();
    for (name, hash) in hashes {
        let Some((path, kind)) = server.thumbnails.video(hash) else {
            panic!(
                "no preview of {name}: {:?}",
                server.preview_reason(hash).await
            );
        };
        assert_eq!(kind, "video/mp4", "{name}");
        assert_eq!(watch(&path).0, "h264", "{name}");
    }
}

/// An EPUB 3 with `cover` as its cover picture, and a table of contents.
fn epub(cover: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut zip = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    let stored =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    zip.start_file("mimetype", stored).unwrap();
    zip.write_all(b"application/epub+zip").unwrap();
    let files: [(&str, &[u8]); 4] = [
        (
            "META-INF/container.xml",
            br#"<container><rootfiles><rootfile full-path="OEBPS/book.opf"/></rootfiles></container>"#,
        ),
        (
            "OEBPS/book.opf",
            br#"<package version="3.0" xmlns:dc="http://purl.org/dc/elements/1.1/">
  <metadata><dc:title>Persuasion</dc:title><dc:creator>Jane Austen</dc:creator>
    <dc:description>&lt;p&gt;Her last novel.&lt;/p&gt;&lt;script&gt;alert(1)&lt;/script&gt;</dc:description></metadata>
  <manifest>
    <item id="nav" href="nav.xhtml" media-type="application/xhtml+xml" properties="nav"/>
    <item id="cover" href="cover.jpg" media-type="image/jpeg" properties="cover-image"/>
  </manifest>
</package>"#,
        ),
        (
            "OEBPS/nav.xhtml",
            br#"<html><body><nav><ol><li><a href="1.xhtml">Chapter 1</a></li>
<li><a href="2.xhtml">Chapter 2</a></li></ol></nav></body></html>"#,
        ),
        ("OEBPS/cover.jpg", cover),
    ];
    for (path, contents) in files {
        zip.start_file(path, zip::write::SimpleFileOptions::default())
            .unwrap();
        zip.write_all(contents).unwrap();
    }
    zip.finish().unwrap().into_inner()
}

#[tokio::test]
async fn shows_an_e_books_cover_and_keeps_its_details() {
    let server = start().await;
    let book = server
        .save(
            "Persuasion.epub",
            &epub(&server.image("cover.jpg", 600, 900)),
        )
        .await;
    // A FictionBook without a cover keeps its icon, but has details.
    let fb2 = server
        .save(
            "Poems.fb2",
            br#"<?xml version="1.0" encoding="utf-8"?>
<FictionBook><description><title-info><author><first-name>Anna</first-name>
<last-name>Akhmatova</last-name></author><book-title>Poems</book-title></title-info></description>
<body><section><title><p>Evening</p></title></section></body></FictionBook>"#,
        )
        .await;
    // An e-book with nothing in it has neither.
    let empty = server
        .save(
            "Empty.fb2",
            br#"<?xml version="1.0"?><FictionBook></FictionBook>"#,
        )
        .await;
    assert!(server.thumbnails.make_now(book).await.unwrap());
    let (width, height, _) = describe(&thumbnail(&server, book));
    assert_eq!((width, height), (320, 480));
    assert!(!server.thumbnails.make_now(fb2).await.unwrap());
    assert!(!server.thumbnails.make_now(empty).await.unwrap());

    let (stop, worker) = start_worker(&server.thumbnails);
    server.wait_for_previews().await;
    stop.send(()).unwrap();
    worker.await.unwrap();
    let details = |hash| {
        let path = server.thumbnails.book(hash).expect("the book's details");
        serde_json::from_slice::<serde_json::Value>(&std::fs::read(path).unwrap()).unwrap()
    };
    assert_eq!(
        details(book),
        serde_json::json!({
            "title": "Persuasion",
            "authors": ["Jane Austen"],
            "description": "Her last novel.",
            "contents": [
                { "title": "Chapter 1", "depth": 0 },
                { "title": "Chapter 2", "depth": 0 },
            ],
        })
    );
    assert_eq!(
        details(fb2)["authors"],
        serde_json::json!(["Anna Akhmatova"])
    );
    assert_eq!(details(fb2)["contents"][0]["title"], "Evening");
    assert_eq!(server.preview_state(book).await.as_deref(), Some("book"));
    assert!(server.thumbnails.book(empty).is_none());
    assert_eq!(server.preview_state(empty).await.as_deref(), Some("none"));
    assert_eq!(server.thumbnails.status().borrow().failed, 0);
}

/// A TrueType font from the system's packages: Liberation, as CI installs
/// with LibreOffice, or DejaVu.
fn truetype() -> Vec<u8> {
    [
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ]
    .iter()
    .find_map(|path| std::fs::read(path).ok())
    .expect("the tests need fonts-liberation2 or fonts-dejavu-core")
}

/// A TrueType font's tables: each one's tag, and its data.
fn tables(font: &[u8]) -> Vec<([u8; 4], Vec<u8>)> {
    let u16_at = |at: usize| usize::from(u16::from_be_bytes([font[at], font[at + 1]]));
    let u32_at = |at: usize| u32::from_be_bytes(font[at..at + 4].try_into().unwrap()) as usize;
    (0..u16_at(4))
        .map(|index| {
            let entry = 12 + index * 16;
            let (offset, length) = (u32_at(entry + 8), u32_at(entry + 12));
            (
                font[entry..entry + 4].try_into().unwrap(),
                font[offset..offset + length].to_vec(),
            )
        })
        .collect()
}

/// `font` as a WOFF web font, with its tables stored as they are, which
/// WOFF allows.
fn woff(font: &[u8]) -> Vec<u8> {
    let tables = tables(font);
    let mut data = Vec::new();
    let mut directory = Vec::new();
    let start = 44 + 20 * tables.len();
    for (tag, table) in &tables {
        directory.extend(tag);
        directory.extend(((start + data.len()) as u32).to_be_bytes());
        directory.extend((table.len() as u32).to_be_bytes());
        directory.extend((table.len() as u32).to_be_bytes());
        directory.extend([0; 4]);
        data.extend(table);
        data.resize(data.len().next_multiple_of(4), 0);
    }
    let mut woff = b"wOFF".to_vec();
    woff.extend(&font[0..4]);
    woff.extend(((start + data.len()) as u32).to_be_bytes());
    woff.extend((tables.len() as u16).to_be_bytes());
    woff.extend([0; 2]);
    woff.extend((font.len() as u32).to_be_bytes());
    woff.extend([0, 1, 0, 0]);
    woff.extend([0; 20]);
    woff.extend(directory);
    woff.extend(data);
    woff
}

/// `font` alone in a TrueType collection, whose header comes first, so
/// every table moves.
fn collection(font: &[u8]) -> Vec<u8> {
    const HEADER: usize = 16;
    let mut moved = font.to_vec();
    let count = usize::from(u16::from_be_bytes([font[4], font[5]]));
    for index in 0..count {
        let at = 12 + index * 16 + 8;
        let offset = u32::from_be_bytes(font[at..at + 4].try_into().unwrap());
        moved[at..at + 4].copy_from_slice(&(offset + HEADER as u32).to_be_bytes());
    }
    let mut ttc = b"ttcf\0\x01\0\0\0\0\0\x01".to_vec();
    ttc.extend((HEADER as u32).to_be_bytes());
    ttc.extend(moved);
    ttc
}

#[tokio::test]
async fn shows_sample_text_in_each_kind_of_font() {
    let server = start().await;
    let ttf = truetype();
    let fonts = [
        ("Sans.ttf", ttf.clone()),
        ("Sans.woff", woff(&ttf)),
        ("Sans.ttc", collection(&ttf)),
    ];
    let mut hashes = Vec::new();
    for (name, contents) in &fonts {
        let hash = server.save(name, contents).await;
        assert!(server.thumbnails.make_now(hash).await.unwrap(), "{name}");
        let (width, height, _) = describe(&thumbnail(&server, hash));
        assert_eq!((width, height), (480, 360), "{name}");
        hashes.push(hash);
    }
    let (stop, worker) = start_worker(&server.thumbnails);
    server.wait_for_previews().await;
    stop.send(()).unwrap();
    worker.await.unwrap();
    // The browser draws TrueType and WOFF itself, but not collections.
    assert!(server.thumbnails.picture(hashes[0]).is_none());
    assert!(server.thumbnails.picture(hashes[1]).is_none());
    let page = server
        .thumbnails
        .picture(hashes[2])
        .expect("a page of sample text in the collection's font");
    assert_eq!(describe(&page).0, 1920);

    // A broken font fails, rather than showing another font's letters.
    let mut broken = ttf[..64].to_vec();
    broken.resize(4096, 0);
    let broken = server.save("Broken.ttf", &broken).await;
    assert!(!server.thumbnails.make_now(broken).await.unwrap());
}

#[tokio::test]
async fn shows_a_comic_books_cover() {
    use std::io::Write;
    let server = start().await;
    let comic = |info: Option<&str>| {
        let mut zip = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
        let options = zip::write::SimpleFileOptions::default();
        for (page, (width, height)) in [(1, (800, 1200)), (2, (1200, 800))] {
            zip.start_file(format!("Nemo/page{page}.jpg"), options)
                .unwrap();
            zip.write_all(&server.image(&format!("page{page}.jpg"), width, height))
                .unwrap();
        }
        if let Some(info) = info {
            zip.start_file("ComicInfo.xml", options).unwrap();
            zip.write_all(info.as_bytes()).unwrap();
        }
        zip.finish().unwrap().into_inner()
    };
    let plain = server.save("Nemo.cbz", &comic(None)).await;
    let described = server
        .save(
            "Nemo 2.cbz",
            &comic(Some("<ComicInfo><Title>Slumberland</Title></ComicInfo>")),
        )
        .await;
    for hash in [plain, described] {
        assert!(server.thumbnails.make_now(hash).await.unwrap());
        let (width, height, _) = describe(&thumbnail(&server, hash));
        assert_eq!((width, height), (320, 480));
    }

    let (stop, worker) = start_worker(&server.thumbnails);
    server.wait_for_previews().await;
    stop.send(()).unwrap();
    worker.await.unwrap();
    // Without details, its cover shows large.
    let cover = server.thumbnails.picture(plain).expect("the cover, large");
    assert_eq!(describe(&cover).1, 1200);
    assert!(server.thumbnails.book(plain).is_none());
    let details = server.thumbnails.book(described).expect("its details");
    let details: serde_json::Value =
        serde_json::from_slice(&std::fs::read(details).unwrap()).unwrap();
    assert_eq!(details["title"], "Slumberland");
}

//! Thumbnails: a small picture of each photo, PDF, video, and Office
//! document that's stored, and the first lines of each text file, for
//! Drive's grid now and Photos, Mail, and Docs later (decisions 14, 15, and
//! 17 in the `accordion` repo's `docs/decisions`).
//!
//! A picture is a WebP, at most 480 pixels on its longer side. Text is the
//! file's first lines as UTF-8, which apps draw themselves, so it follows
//! their light or dark look. Each is named by the hash of the contents it
//! shows, so copies and unchanged versions share one. They're kept in
//! `cache/thumbnails/` in the data folder, which backups leave out, because
//! they can be made again. After a restore, the worker finds them missing
//! and makes them again.
//!
//! [`Thumbnails::run`] makes them in the background, one at a time, newest
//! files first. The decoding happens in a sandboxed worker
//! ([`accordion_sandbox`]): libvips for images, poppler's `pdftoppm` for a
//! PDF's first page, ffmpeg for a frame from a video, and LibreOffice for an
//! Office document. ffmpeg and LibreOffice are optional: without them,
//! videos and documents have none, and they get another try once they're
//! installed. The server only reads a file's first bytes, to decide
//! whether there's anything to decode, and to take a text file's first lines,
//! which needs no decoder. Thumbnails keep no metadata from the file, such as
//! where a photo was taken.
//!
//! LibreOffice turns each document into a PDF of its first pages, which is
//! the document's preview, kept beside its thumbnail. The thumbnail is the
//! PDF's first page.
//!
//! A file that fails isn't tried again until [`WORKER`] changes, such as
//! when an update makes thumbnails of more kinds of file.

use std::ffi::OsString;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use accordion_sandbox::{Job, Limits, Sandbox};
use rusqlite::{OptionalExtension, Row};
use tokio::sync::watch;

use crate::db::{self, Database, Migration};
use crate::live::{self, Hint};
use crate::sql_now;
use crate::storage::{ContentHash, Storage, blobs};

const OWNER: &str = "thumbnails";

static MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    name: "Create thumbnails",
    sql: "
    -- What happened to each stored file's contents. A file whose contents
    -- have no row is waiting for its thumbnail.
    CREATE TABLE thumbnails (
        hash BLOB PRIMARY KEY CHECK (length(hash) = 32),
        -- ready: the thumbnail is in the cache. none: it's a kind of file
        -- without thumbnails, or too large. failed: the worker couldn't
        -- make one.
        state TEXT NOT NULL CHECK (state IN ('ready', 'none', 'failed')),
        -- The worker that decided, so a newer one tries again.
        worker INTEGER NOT NULL,
        -- Why there's none, or what went wrong.
        reason TEXT,
        made_at TEXT NOT NULL
    ) STRICT, WITHOUT ROWID;

    CREATE INDEX thumbnails_failed ON thumbnails (state) WHERE state = 'failed';
",
}];

/// The version of what makes thumbnails. Raise it when it can make
/// thumbnails of files it couldn't before, so they get another try.
pub const WORKER: u32 = 6;

/// The longer side of a thumbnail, in pixels.
const SIZE: u32 = 480;

/// Larger files get no thumbnail.
const MAX_FILE: u64 = 200 * 1024 * 1024;

/// HEIC and AVIF files are decoded whole, so larger ones get no thumbnail.
const MAX_HEIF_PIXELS: u64 = 24_000_000;

/// Larger documents get no thumbnail. LibreOffice reads the whole file.
const MAX_DOCUMENT: u64 = 100 * 1024 * 1024;

/// The most pages a document's preview has, so a long spreadsheet can't
/// make a huge PDF.
pub const PREVIEW_PAGES: u32 = 50;

/// How much of a file is read to tell what it is, and the most a text
/// thumbnail keeps.
const HEAD: usize = 4096;

/// The most lines a text thumbnail keeps. A card shows fewer.
const TEXT_LINES: usize = 40;

/// What a worker may use for each thumbnail.
const LIMITS: Limits = Limits {
    memory: 1024 * 1024 * 1024,
    cpu: Duration::from_secs(20),
    time: Duration::from_secs(30),
    output: 10 * 1024 * 1024,
};

/// What LibreOffice may use for each document, which is more than other
/// workers, for long documents on a Raspberry Pi.
const DOCUMENT_LIMITS: Limits = Limits {
    memory: 1024 * 1024 * 1024,
    cpu: Duration::from_secs(60),
    time: Duration::from_secs(90),
    output: 1024 * 1024,
};

/// What LibreOffice reads besides programs and libraries: its settings,
/// which Debian keeps in `/etc`, the fonts, random numbers, without which it
/// stops, and the list of accounts, which has no passwords. The service
/// can't make memory both writable and runnable, so LibreOffice maps a file
/// in its home folder twice instead, and it finds its home folder through
/// the list. Without it, it tries `/tmp`, which it can't write to, and stops.
const DOCUMENT_READS: [&str; 5] = [
    "/etc/libreoffice",
    "/etc/fonts",
    "/var/cache/fontconfig",
    "/dev/urandom",
    "/etc/passwd",
];

/// Where LibreOffice makes the socket it checks for another copy of itself
/// with. It's always here, whatever `TMPDIR` says. The service has its own,
/// through systemd's `PrivateTmp`.
const SOCKETS: &str = "/tmp";

/// Settings for every document: macros never run, and nothing a document
/// links to or embeds is fetched or updated. A conversion isn't meant to do
/// either anyway, but these don't rely on that. Writer and Calc number their
/// "never" differently.
const DOCUMENT_SETTINGS: &str = r#"<item oor:path="/org.openoffice.Office.Common/Security/Scripting"><prop oor:name="MacroSecurityLevel" oor:op="fuse"><value>3</value></prop></item>
<item oor:path="/org.openoffice.Office.Common/Security/Scripting"><prop oor:name="DisableMacrosExecution" oor:op="fuse"><value>true</value></prop></item>
<item oor:path="/org.openoffice.Office.Common/Security/Scripting"><prop oor:name="DisableActiveContent" oor:op="fuse"><value>true</value></prop></item>
<item oor:path="/org.openoffice.Office.Common/Security/Scripting"><prop oor:name="BlockUntrustedRefererLinks" oor:op="fuse"><value>true</value></prop></item>
<item oor:path="/org.openoffice.Office.Writer/Content/Update"><prop oor:name="Link" oor:op="fuse"><value>2</value></prop></item>
<item oor:path="/org.openoffice.Office.Calc/Content/Update"><prop oor:name="Link" oor:op="fuse"><value>1</value></prop></item>
"#;

/// The decoders ffmpeg may use for video: the codecs families' videos have,
/// including older ones from DVDs, Windows Media, and Flash.
const VIDEO_DECODERS: &str = "h264,hevc,vp8,vp9,av1,libdav1d,mpeg4,mpeg1video,mpeg2video,mjpeg,\
                              prores,msmpeg4v1,msmpeg4v2,msmpeg4,wmv1,wmv2,wmv3,vc1,flv,\
                              vp6f,theora";

/// ffprobe opens a decoder for every track, and stops at one it may not use,
/// so it may also use the decoders for the audio in those videos. ffmpeg
/// skips the audio.
const AUDIO_DECODERS: &str = "aac,mp3float,mp3,opus,libopus,vorbis,libvorbis,ac3,eac3,\
                              flac,alac,pcm_s16le,pcm_s16be,pcm_s24le,pcm_s24be,pcm_s32le,\
                              pcm_f32le,pcm_bluray,mp2,mp2float,wmav1,wmav2";

/// Turns an HDR frame's colors into ordinary ones. It runs after the frame
/// is shrunk, since a whole 4K frame takes more memory than the limit.
const TONE_MAP: &str = "zscale=t=linear:npl=100,format=gbrpf32le,zscale=p=bt709,\
                        tonemap=hable:desat=0,zscale=t=bt709:m=bt709:r=tv,format=yuv420p";

/// Why a video has no thumbnail when ffmpeg isn't installed. Videos with
/// this reason get another try once it is.
const NO_FFMPEG: &str = "ffmpeg isn't installed";

/// How the reason starts when a document fails, so failed documents can get
/// another try when LibreOffice changes.
const DOCUMENT_FAILED: &str = "Document: ";

/// Why a document has no thumbnail when LibreOffice isn't installed.
/// Documents with this reason get another try once it is.
const NO_OFFICE: &str = "LibreOffice isn't installed";

/// How many waiting files each look at the database fetches.
const BATCH: usize = 20;

/// How often the cache loses the thumbnails of contents that are gone, and
/// how long the worker waits for a new file before looking anyway.
const TIDY_EVERY: Duration = Duration::from_secs(60 * 60);

/// The programs that make thumbnails.
#[derive(Clone, Debug)]
pub struct Tools {
    /// The program that answers `sandbox`: the server itself.
    pub launcher: PathBuf,
    pub vips: PathBuf,
    pub vipsheader: PathBuf,
    pub pdftoppm: PathBuf,
    pub pdfinfo: PathBuf,
    /// Optional: without ffmpeg, videos have no thumbnails.
    pub ffmpeg: PathBuf,
    pub ffprobe: PathBuf,
    /// LibreOffice's own program, rather than the `soffice` script that
    /// starts it. Optional: without it, documents have no thumbnails.
    pub soffice: PathBuf,
}

impl Tools {
    /// The programs from the system's packages, with `launcher` as the
    /// sandbox.
    pub fn installed(launcher: impl Into<PathBuf>) -> Self {
        Self {
            launcher: launcher.into(),
            vips: "/usr/bin/vips".into(),
            vipsheader: "/usr/bin/vipsheader".into(),
            pdftoppm: "/usr/bin/pdftoppm".into(),
            pdfinfo: "/usr/bin/pdfinfo".into(),
            ffmpeg: "/usr/bin/ffmpeg".into(),
            ffprobe: "/usr/bin/ffprobe".into(),
            soffice: "/usr/lib/libreoffice/program/soffice.bin".into(),
        }
    }

    fn has_ffmpeg(&self) -> bool {
        self.ffmpeg.is_file() && self.ffprobe.is_file()
    }

    fn has_office(&self) -> bool {
        self.soffice.is_file()
    }
}

/// A thumbnail in the cache.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Thumbnail {
    pub kind: ThumbnailKind,
    pub path: PathBuf,
}

/// What a thumbnail is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ThumbnailKind {
    /// A WebP picture.
    Picture,
    /// A text file's first lines, as UTF-8 with `\n` between lines.
    Text,
}

impl ThumbnailKind {
    /// The thumbnail's media type, for sending it.
    pub fn content_type(self) -> &'static str {
        match self {
            Self::Picture => "image/webp",
            Self::Text => "text/plain; charset=utf-8",
        }
    }

    fn extension(self) -> &'static str {
        match self {
            Self::Picture => "webp",
            Self::Text => "txt",
        }
    }
}

/// A document's preview: a PDF of its first pages.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Preview {
    pub path: PathBuf,
    /// Whether the PDF stops at [`PREVIEW_PAGES`], so the document may go
    /// on past it.
    pub partial: bool,
}

/// How thumbnails are doing, for the health page.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ThumbnailStatus {
    /// Why no thumbnails are made, such as a missing program.
    pub unavailable: Option<String>,
    /// Why videos have no thumbnails, such as ffmpeg not being installed.
    pub videos: Option<String>,
    /// Why Office documents have no thumbnails, such as LibreOffice not
    /// being installed.
    pub documents: Option<String>,
    /// Files waiting for a thumbnail.
    pub waiting: u64,
    /// Files the worker couldn't make a thumbnail of.
    pub failed: u64,
    /// The last error from the database or the cache, if the last round had
    /// one.
    pub last_error: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Db(#[from] db::Error),

    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),

    #[error("Accordion couldn't read or write its thumbnails: {0}")]
    Io(#[from] io::Error),
}

/// The thumbnail service. Cloning it is cheap.
#[derive(Clone)]
pub struct Thumbnails {
    inner: Arc<Inner>,
}

struct Inner {
    db: Database,
    storage: Storage,
    dir: PathBuf,
    tools: Tools,
    sandbox: Sandbox,
    status: watch::Sender<ThumbnailStatus>,
    /// LibreOffice's settings, made the first time a document needs them.
    /// Holding the lock also keeps to one document at a time, so two large
    /// ones can't use the memory together.
    office: tokio::sync::Mutex<Option<tempfile::TempDir>>,
    /// LibreOffice's installation as the worker last saw it, from
    /// [`office_stamp`]. `None` until the first look after the server
    /// starts.
    office_seen: std::sync::Mutex<Option<OfficeStamp>>,
}

/// What became of one file.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Ready,
    None(&'static str),
    Failed(String),
}

/// The kinds of file with thumbnails, by their first bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    /// JPEG, PNG, GIF, WebP, or TIFF.
    Image,
    /// A picture ffmpeg decodes, such as a BMP or a Photoshop file, because
    /// libvips reads them only with decoders it doesn't trust with hostile
    /// files.
    Still(Still),
    /// HEIC or AVIF, which are decoded whole.
    Heif,
    Pdf,
    Video(Container),
    /// UTF-8 text, such as notes, Markdown, CSV, or code.
    Text,
    Document(&'static Format),
}

/// An Office format, with the one reader LibreOffice may use for it, so a
/// disguised file can't reach its other readers.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Format {
    pub(crate) extension: &'static str,
    /// How every file in the format starts.
    pub(crate) signature: &'static [u8],
    /// LibreOffice's reader.
    filter: &'static str,
    /// LibreOffice's PDF writer for that kind of document.
    export: &'static str,
}

pub(crate) const ZIP: &[u8] = b"PK\x03\x04";
/// Office 97–2003 files are in Microsoft's compound file format.
const COMPOUND: &[u8] = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1";

/// The Office formats families have. Apple's are left out, since
/// LibreOffice reads only some of them.
pub(crate) static FORMATS: [Format; 11] = [
    Format {
        extension: "docx",
        signature: ZIP,
        filter: "MS Word 2007 XML",
        export: "writer_pdf_Export",
    },
    Format {
        extension: "doc",
        signature: COMPOUND,
        filter: "MS Word 97",
        export: "writer_pdf_Export",
    },
    Format {
        extension: "odt",
        signature: ZIP,
        filter: "writer8",
        export: "writer_pdf_Export",
    },
    Format {
        extension: "ott",
        signature: ZIP,
        filter: "writer8_template",
        export: "writer_pdf_Export",
    },
    Format {
        extension: "rtf",
        signature: b"{\\rtf",
        filter: "Rich Text Format",
        export: "writer_pdf_Export",
    },
    Format {
        extension: "xlsx",
        signature: ZIP,
        filter: "Calc MS Excel 2007 XML",
        export: "calc_pdf_Export",
    },
    Format {
        extension: "xls",
        signature: COMPOUND,
        filter: "MS Excel 97",
        export: "calc_pdf_Export",
    },
    Format {
        extension: "ods",
        signature: ZIP,
        filter: "calc8",
        export: "calc_pdf_Export",
    },
    Format {
        extension: "pptx",
        signature: ZIP,
        filter: "Impress MS PowerPoint 2007 XML",
        export: "impress_pdf_Export",
    },
    Format {
        extension: "ppt",
        signature: COMPOUND,
        filter: "MS PowerPoint 97",
        export: "impress_pdf_Export",
    },
    Format {
        extension: "odp",
        signature: ZIP,
        filter: "impress8",
        export: "impress_pdf_Export",
    },
];

/// The Office format of a file named `name` that starts with `head`, if
/// it's one families have and it starts the way the format does.
pub(crate) fn format(name: &str, head: &[u8]) -> Option<&'static Format> {
    let (_, extension) = name.rsplit_once('.')?;
    FORMATS.iter().find(|format| {
        extension.eq_ignore_ascii_case(format.extension) && head.starts_with(format.signature)
    })
}

/// The video formats families have. ffmpeg reads each with only the one
/// demuxer, so it never tries its others.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Container {
    /// QuickTime and MP4: `.mov`, `.mp4`, `.m4v`, and `.3gp`.
    Mov,
    /// `.mkv` and `.webm`.
    Matroska,
    Avi,
    /// MPEG transport streams, such as a camcorder's `.mts` files.
    MpegTs,
    /// MPEG program streams: `.mpg`, `.mpeg`, and a DVD's `.vob` files.
    MpegPs,
    /// MPEG video on its own, without a container, as some `.mpg` and
    /// `.m2v` files are.
    MpegVideo,
    /// Windows Media: `.wmv` and `.asf`.
    Asf,
    /// Flash video: `.flv`.
    Flv,
    /// Ogg with Theora video: `.ogv`.
    Ogg,
}

impl Container {
    fn format(self) -> &'static str {
        match self {
            Self::Mov => "mov",
            Self::Matroska => "matroska",
            Self::Avi => "avi",
            Self::MpegTs => "mpegts",
            Self::MpegPs => "mpeg",
            Self::MpegVideo => "mpegvideo",
            Self::Asf => "asf",
            Self::Flv => "flv",
            Self::Ogg => "ogg",
        }
    }
}

/// The pictures ffmpeg decodes, each with only the one reader and decoder
/// it needs (decision 26).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Still {
    Bmp,
    /// Photoshop's flattened picture, which every PSD keeps.
    Psd,
    /// JPEG 2000, in a JP2 file or a bare codestream.
    Jpeg2000,
    /// DirectDraw Surface, the textures games use.
    Dds,
    /// OpenEXR, the HDR pictures film and 3D work use.
    Exr,
    SunRaster,
    /// A Kodak Photo CD scan.
    PhotoCd,
    Sgi,
    /// An X Window dump.
    Xwd,
    Pcx,
    /// PBM, PGM, PPM, and PAM.
    Netpbm,
    /// Portable FloatMap, an HDR picture.
    Pfm,
    /// Windows icons and cursors, which hold BMP or PNG pictures.
    Icon,
    /// Targa, which has no signature, so it's known by its name.
    Targa,
    /// Wireless bitmap, known by its name for the same reason.
    Wbmp,
    /// Macintosh PICT, known by its name.
    Pict,
}

impl Still {
    /// ffmpeg's arguments that pick the reader, and the decoders it may use.
    fn reader(self) -> (&'static [&'static str], &'static str) {
        match self {
            Self::Bmp => (&["-f", "bmp_pipe"], "bmp"),
            Self::Psd => (&["-f", "psd_pipe"], "psd"),
            Self::Jpeg2000 => (&["-f", "j2k_pipe"], "jpeg2000"),
            Self::Dds => (&["-f", "dds_pipe"], "dds"),
            Self::Exr => (&["-f", "exr_pipe"], "exr"),
            Self::SunRaster => (&["-f", "sunrast_pipe"], "sunrast"),
            Self::PhotoCd => (&["-f", "photocd_pipe"], "photocd"),
            Self::Sgi => (&["-f", "sgi_pipe"], "sgi"),
            Self::Xwd => (&["-f", "xwd_pipe"], "xwd"),
            Self::Pcx => (&["-f", "pcx_pipe"], "pcx"),
            Self::Netpbm => (&["-f", "ppm_pipe"], "pbm,pgm,ppm,pam"),
            Self::Pfm => (&["-f", "pfm_pipe"], "pfm"),
            Self::Icon => (&["-f", "ico"], "bmp,png"),
            Self::Pict => (&["-f", "qdraw_pipe"], "qdraw"),
            // ffmpeg reads these as one picture, by the decoder it's told,
            // and never treats the name as a pattern for other files.
            Self::Targa => (
                &["-f", "image2", "-pattern_type", "none", "-c:v", "targa"],
                "targa",
            ),
            Self::Wbmp => (
                &["-f", "image2", "-pattern_type", "none", "-c:v", "wbmp"],
                "wbmp",
            ),
        }
    }
}

/// What ffprobe says about a video.
#[derive(Debug, PartialEq)]
struct Probe {
    /// Whether it has a video track at all.
    video: bool,
    seconds: Option<f64>,
    /// Whether the video is HDR, as HLG or PQ.
    hdr: bool,
}

impl Thumbnails {
    /// Starts the service, with its cache in `cache/thumbnails/` under
    /// `data_dir`. Without the sandbox or the programs, it starts anyway and
    /// makes no thumbnails, and [`Thumbnails::status`] says why.
    pub async fn open(
        db: Database,
        storage: Storage,
        data_dir: &Path,
        tools: Tools,
    ) -> Result<Self, Error> {
        db.migrate(OWNER, MIGRATIONS).await?;
        let dir = data_dir.join("cache").join("thumbnails");
        std::fs::create_dir_all(dir.join("partial"))?;
        // Thumbnails and scratch folders from a run that stopped partway.
        for entry in std::fs::read_dir(dir.join("partial"))? {
            let entry = entry?;
            let _ = if entry.file_type()?.is_dir() {
                std::fs::remove_dir_all(entry.path())
            } else {
                std::fs::remove_file(entry.path())
            };
        }
        // A newer worker may manage what an older one couldn't.
        db.write(|tx| {
            tx.execute(
                "DELETE FROM thumbnails WHERE state != 'ready' AND worker < ?1",
                [WORKER],
            )
        })
        .await?;
        let unavailable = unavailable(&tools);
        let videos = videos_unavailable(&tools);
        let documents = documents_unavailable(&tools);
        if let Some(reason) = &unavailable {
            tracing::warn!("no thumbnails: {reason}");
        }
        Ok(Self {
            inner: Arc::new(Inner {
                db,
                storage,
                dir,
                sandbox: Sandbox::new(&tools.launcher),
                tools,
                status: watch::Sender::new(ThumbnailStatus {
                    unavailable,
                    videos,
                    documents,
                    ..ThumbnailStatus::default()
                }),
                office: tokio::sync::Mutex::new(None),
                office_seen: std::sync::Mutex::new(None),
            }),
        })
    }

    pub fn status(&self) -> watch::Receiver<ThumbnailStatus> {
        self.inner.status.subscribe()
    }

    /// The thumbnail of the contents with `hash`, if there is one yet.
    pub fn find(&self, hash: ContentHash) -> Option<Thumbnail> {
        [ThumbnailKind::Picture, ThumbnailKind::Text]
            .into_iter()
            .map(|kind| Thumbnail {
                kind,
                path: self.path(hash, kind),
            })
            .find(|thumbnail| thumbnail.path.is_file())
    }

    /// The preview of the document with `hash`, if there is one yet.
    pub fn preview(&self, hash: ContentHash) -> Option<Preview> {
        [false, true]
            .into_iter()
            .map(|partial| Preview {
                path: self.preview_path(hash, partial),
                partial,
            })
            .find(|preview| preview.path.is_file())
    }

    fn path(&self, hash: ContentHash, kind: ThumbnailKind) -> PathBuf {
        self.cached(hash, kind.extension())
    }

    fn preview_path(&self, hash: ContentHash, partial: bool) -> PathBuf {
        self.cached(hash, if partial { "part.pdf" } else { "pdf" })
    }

    fn cached(&self, hash: ContentHash, extension: &str) -> PathBuf {
        self.inner
            .dir
            .join(blobs::shard(hash))
            .with_extension(extension)
    }

    /// Makes thumbnails until `stopped` finishes: the newest waiting files
    /// first, then each new file as it's saved.
    pub async fn run(self, stopped: impl Future<Output = ()>) {
        let mut stopped = std::pin::pin!(stopped);
        if self.inner.status.borrow().unavailable.is_some() {
            stopped.await;
            return;
        }
        let mut saved = self.inner.storage.saved();
        let mut tidied_at: Option<tokio::time::Instant> = None;
        loop {
            // Before looking, so a file saved after the look wakes the
            // worker below.
            saved.mark_unchanged();
            if tidied_at.is_none_or(|at| at.elapsed() >= TIDY_EVERY) {
                let result = self.tidy().await;
                self.note(result.err());
                tidied_at = Some(tokio::time::Instant::now());
            }
            let waiting = match self.waiting(BATCH).await {
                Ok(waiting) => waiting,
                Err(err) => {
                    self.note(Some(err));
                    Vec::new()
                }
            };
            for hash in &waiting {
                let outcome = tokio::select! {
                    () = &mut stopped => return,
                    outcome = self.make(*hash) => outcome,
                };
                let result = self.record(*hash, outcome).await;
                self.note(result.err());
            }
            let result = self.count().await;
            self.note(result.err());
            if waiting.is_empty() {
                tokio::select! {
                    () = &mut stopped => return,
                    _ = saved.changed() => {}
                    () = tokio::time::sleep(TIDY_EVERY) => {}
                }
            }
        }
    }

    /// Makes the thumbnail of the contents with `hash` now, whether or not
    /// it's waiting, and records how it went. Returns whether there's a
    /// thumbnail.
    pub async fn make_now(&self, hash: ContentHash) -> Result<bool, Error> {
        let outcome = self.make(hash).await;
        let ready = outcome == Outcome::Ready;
        self.record(hash, outcome).await?;
        Ok(ready)
    }

    fn note(&self, err: Option<Error>) {
        if let Some(err) = &err {
            tracing::warn!(%err, "thumbnails ran into a problem");
        }
        self.inner.status.send_if_modified(|status| {
            let last_error = err.as_ref().map(ToString::to_string);
            let changed = status.last_error != last_error;
            status.last_error = last_error;
            changed
        });
    }

    /// The newest stored contents that a file uses and that have no
    /// thumbnail and no decision yet.
    async fn waiting(&self, limit: usize) -> Result<Vec<ContentHash>, Error> {
        self.inner
            .db
            .read(move |conn| {
                conn.prepare(
                    "SELECT b.hash FROM storage_blobs b
                     WHERE b.damage IS NULL
                       AND EXISTS (SELECT 1 FROM storage_versions v WHERE v.blob = b.hash)
                       AND NOT EXISTS (SELECT 1 FROM thumbnails t WHERE t.hash = b.hash)
                     ORDER BY b.created_at DESC
                     LIMIT ?1",
                )?
                .query_map([limit as i64], |row| hash(row, 0))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(Error::from)
            })
            .await
    }

    async fn count(&self) -> Result<(), Error> {
        let (waiting, failed) = self
            .inner
            .db
            .read(|conn| {
                conn.query_row(
                    "SELECT
                       (SELECT count(*) FROM storage_blobs b
                        WHERE b.damage IS NULL
                          AND EXISTS (SELECT 1 FROM storage_versions v WHERE v.blob = b.hash)
                          AND NOT EXISTS (SELECT 1 FROM thumbnails t WHERE t.hash = b.hash)),
                       (SELECT count(*) FROM thumbnails WHERE state = 'failed')",
                    [],
                    |row| Ok((row.get::<_, i64>(0)? as u64, row.get::<_, i64>(1)? as u64)),
                )
                .map_err(Error::from)
            })
            .await?;
        let videos = videos_unavailable(&self.inner.tools);
        let documents = documents_unavailable(&self.inner.tools);
        self.inner.status.send_if_modified(|status| {
            let changed = (
                status.waiting,
                status.failed,
                &status.videos,
                &status.documents,
            ) != (waiting, failed, &videos, &documents);
            status.waiting = waiting;
            status.failed = failed;
            status.videos = videos;
            status.documents = documents;
            changed
        });
        Ok(())
    }

    async fn record(&self, hash: ContentHash, outcome: Outcome) -> Result<(), Error> {
        let (state, reason) = match outcome {
            Outcome::Ready => ("ready", None),
            Outcome::None(reason) => ("none", Some(reason.to_owned())),
            Outcome::Failed(problem) => {
                tracing::info!(contents = %hash, %problem, "couldn't make a thumbnail");
                ("failed", Some(problem))
            }
        };
        self.inner
            .db
            .write(move |tx| {
                tx.execute(
                    concat!(
                        "INSERT INTO thumbnails (hash, state, worker, reason, made_at)
                         VALUES (?1, ?2, ?3, ?4, ",
                        sql_now!(),
                        ") ON CONFLICT (hash) DO UPDATE SET
                             state = excluded.state, worker = excluded.worker,
                             reason = excluded.reason, made_at = excluded.made_at"
                    ),
                    (&hash.0[..], state, WORKER, reason),
                )?;
                if state == "ready" {
                    hint_folders(tx, hash)?;
                }
                Ok::<_, rusqlite::Error>(())
            })
            .await?;
        Ok(())
    }

    /// Forgets contents that are gone from storage, and removes their
    /// thumbnails. Also forgets thumbnails that are missing from the cache,
    /// such as after a restore, since backups leave the cache out, and
    /// videos and documents skipped without ffmpeg or LibreOffice once
    /// they're installed, so they're made again.
    async fn tidy(&self) -> Result<(), Error> {
        let tools = &self.inner.tools;
        // Documents that failed get another try when LibreOffice's packages
        // change, such as one that was still being installed when the
        // server started, and once each time the server starts.
        let stamp = office_stamp(tools);
        let changed = {
            let mut seen = self.inner.office_seen.lock().expect("never poisoned");
            let changed = seen.as_ref() != Some(&stamp);
            *seen = Some(stamp);
            changed
        };
        if changed && tools.has_office() {
            let pattern = format!("{DOCUMENT_FAILED}%");
            self.inner
                .db
                .write(move |tx| {
                    tx.execute(
                        "DELETE FROM thumbnails WHERE state = 'failed' AND reason LIKE ?1",
                        [pattern],
                    )
                })
                .await?;
        }
        for (installed, reason) in [
            (tools.has_ffmpeg(), NO_FFMPEG),
            (tools.has_office(), NO_OFFICE),
        ] {
            if installed {
                self.inner
                    .db
                    .write(move |tx| {
                        tx.execute(
                            "DELETE FROM thumbnails WHERE state = 'none' AND reason = ?1",
                            [reason],
                        )
                    })
                    .await?;
            }
        }

        let ready = self
            .inner
            .db
            .read(|conn| {
                conn.prepare("SELECT hash FROM thumbnails WHERE state = 'ready'")?
                    .query_map([], |row| hash(row, 0))?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(Error::from)
            })
            .await?;
        let missing: Vec<ContentHash> = ready
            .into_iter()
            .filter(|hash| self.find(*hash).is_none())
            .collect();
        if !missing.is_empty() {
            tracing::info!(count = missing.len(), "making missing thumbnails again");
            self.inner
                .db
                .write(move |tx| {
                    let mut delete = tx.prepare("DELETE FROM thumbnails WHERE hash = ?1")?;
                    for hash in &missing {
                        delete.execute([&hash.0[..]])?;
                    }
                    Ok::<_, rusqlite::Error>(())
                })
                .await?;
        }

        let gone = self
            .inner
            .db
            .write(|tx| {
                tx.prepare(
                    "DELETE FROM thumbnails
                     WHERE NOT EXISTS (SELECT 1 FROM storage_blobs b WHERE b.hash = thumbnails.hash)
                     RETURNING hash",
                )?
                .query_map([], |row| hash(row, 0))?
                .collect::<Result<Vec<_>, _>>()
            })
            .await?;
        for hash in gone {
            let paths = [
                self.path(hash, ThumbnailKind::Picture),
                self.path(hash, ThumbnailKind::Text),
                self.preview_path(hash, false),
                self.preview_path(hash, true),
            ];
            for path in paths {
                match std::fs::remove_file(path) {
                    Err(err) if err.kind() != io::ErrorKind::NotFound => return Err(err.into()),
                    _ => {}
                }
            }
        }
        Ok(())
    }

    async fn make(&self, hash: ContentHash) -> Outcome {
        let file = self.inner.storage.contents_file(hash);
        let (kind, size, head) = match sniff(&file) {
            Ok(sniffed) => sniffed,
            Err(err) => return Outcome::Failed(format!("couldn't read the file: {err}")),
        };
        // A document is known by its name as well as its first bytes.
        let kind = if FORMATS
            .iter()
            .any(|format| head.starts_with(format.signature))
        {
            match self.name(hash).await {
                Ok(name) => name
                    .and_then(|name| format(&name, &head))
                    .map(Kind::Document)
                    .or(kind),
                Err(err) => {
                    return Outcome::Failed(format!("couldn't read the file's name: {err}"));
                }
            }
        } else {
            kind
        };
        // Pictures without a signature are known by their name as well.
        let kind = match kind {
            None => match self.name(hash).await {
                Ok(name) => name
                    .and_then(|name| unsigned_still(&name, &head))
                    .map(Kind::Still),
                Err(err) => {
                    return Outcome::Failed(format!("couldn't read the file's name: {err}"));
                }
            },
            kind => kind,
        };
        let kind = match kind {
            // Only the first lines are kept, so text files of any size have one.
            Some(Kind::Text) => return self.done(hash, ThumbnailKind::Text, &text(&head)),
            Some(Kind::Document(_)) if size > MAX_DOCUMENT => {
                return Outcome::None("a document over 100 MB");
            }
            // ffmpeg reads a video's index and a few frames, not all of it.
            Some(kind)
                if size > MAX_FILE && !matches!(kind, Kind::Video(_) | Kind::Document(_)) =>
            {
                return Outcome::None("over 200 MB");
            }
            Some(kind) => kind,
            None => return Outcome::None("not a kind of file with thumbnails"),
        };
        let made = match kind {
            Kind::Image => self.thumbnail(std::fs::File::open(&file)).await,
            Kind::Still(_) if !self.inner.tools.has_ffmpeg() => return Outcome::None(NO_FFMPEG),
            Kind::Still(still) => {
                let (reader, decoders) = still.reader();
                match self.frame(&file, reader, decoders, 0.0, false).await {
                    Ok(png) if !png.is_empty() => {
                        self.thumbnail(self.scratch(&png).map_err(io::Error::other))
                            .await
                    }
                    Ok(_) => Err("ffmpeg found no picture in the file".into()),
                    Err(problem) => Err(problem),
                }
            }
            Kind::Heif => match self.pixels(&file).await {
                Ok(pixels) if pixels > MAX_HEIF_PIXELS => {
                    return Outcome::None("a HEIC or AVIF over 24 megapixels");
                }
                Ok(_) => self.thumbnail(std::fs::File::open(&file)).await,
                Err(problem) => Err(problem),
            },
            Kind::Pdf => self.pdf(&file).await,
            Kind::Video(_) if !self.inner.tools.has_ffmpeg() => return Outcome::None(NO_FFMPEG),
            Kind::Video(container) => match self.video(&file, container).await {
                Ok(Some(frame)) => self.thumbnail(Ok(frame)).await,
                Ok(None) => return Outcome::None("a video file without video, such as a song"),
                Err(problem) => Err(problem),
            },
            Kind::Document(_) if !self.inner.tools.has_office() => return Outcome::None(NO_OFFICE),
            Kind::Document(format) => self
                .document(hash, &file, format)
                .await
                .map_err(|problem| format!("{DOCUMENT_FAILED}{problem}")),
            Kind::Text => unreachable!("text is kept above"),
        };
        match made {
            Ok(webp) => self.done(hash, ThumbnailKind::Picture, &webp),
            Err(problem) => Outcome::Failed(problem),
        }
    }

    /// Keeps a thumbnail that's been made.
    fn done(&self, hash: ContentHash, kind: ThumbnailKind, thumbnail: &[u8]) -> Outcome {
        match self.keep(self.path(hash, kind), thumbnail) {
            Ok(()) => Outcome::Ready,
            Err(err) => Outcome::Failed(format!("couldn't save the thumbnail: {err}")),
        }
    }

    /// The name of the newest file with the contents `hash`.
    async fn name(&self, hash: ContentHash) -> Result<Option<String>, Error> {
        self.inner
            .db
            .read(move |conn| newest_name(conn, hash).map_err(Error::from))
            .await
    }

    /// Runs a program in the sandbox, with libvips's settings.
    async fn work(
        &self,
        program: &Path,
        args: &[OsString],
        input: std::fs::File,
        read: &[PathBuf],
    ) -> Result<Vec<u8>, String> {
        let env = [
            // Only the decoders libvips trusts with hostile files.
            ("VIPS_BLOCK_UNTRUSTED", "1"),
            // Fewer threads use less memory, and the worker isn't in a hurry.
            ("VIPS_CONCURRENCY", "2"),
            ("MALLOC_ARENA_MAX", "2"),
        ];
        self.inner
            .sandbox
            .run(Job {
                program,
                args,
                env: &env,
                input,
                read,
                write: &[],
                sockets: None,
                limits: LIMITS,
            })
            .await
            .map_err(|err| err.to_string())
    }

    /// Shrinks the image on `input` to a thumbnail, as WebP.
    async fn thumbnail(&self, input: io::Result<std::fs::File>) -> Result<Vec<u8>, String> {
        let input = input.map_err(|err| format!("couldn't open the file: {err}"))?;
        let size = SIZE.to_string();
        let args = [
            "thumbnail_source",
            "[descriptor=0]",
            // Keeps no metadata, such as where a photo was taken.
            ".webp[Q=75,strip]",
            &size,
            "--height",
            &size,
            // A picture already smaller stays its size.
            "--size",
            "down",
        ]
        .map(OsString::from);
        let webp = self.work(&self.inner.tools.vips, &args, input, &[]).await?;
        if !webp.starts_with(b"RIFF") {
            return Err("libvips didn't make a WebP".into());
        }
        Ok(webp)
    }

    /// The pixels in a HEIC or AVIF, from its header. libvips reads only a
    /// file it's named, so the worker can read this one file.
    async fn pixels(&self, file: &Path) -> Result<u64, String> {
        let args = [file.as_os_str().to_owned()];
        let input = std::fs::File::open("/dev/null").map_err(|err| err.to_string())?;
        let output = self
            .work(
                &self.inner.tools.vipsheader,
                &args,
                input,
                &[file.to_owned()],
            )
            .await?;
        // Such as "/path: 3088x2320 uchar, 3 bands, srgb, heifload".
        let text = String::from_utf8_lossy(&output);
        let size = text
            .rsplit_once(": ")
            .and_then(|(_, rest)| rest.split_whitespace().next())
            .and_then(|size| size.split_once('x'))
            .and_then(|(width, height)| {
                Some(width.parse::<u64>().ok()? * height.parse::<u64>().ok()?)
            });
        size.ok_or_else(|| format!("libvips didn't say how large the image is: {}", text.trim()))
    }

    /// The first page of a PDF, as a thumbnail. poppler draws the page,
    /// because libvips doesn't trust its own PDF reader, then libvips makes
    /// the WebP.
    async fn pdf(&self, file: &Path) -> Result<Vec<u8>, String> {
        let input =
            std::fs::File::open(file).map_err(|err| format!("couldn't open the file: {err}"))?;
        let size = SIZE.to_string();
        let args = [
            "-f",
            "1",
            "-l",
            "1",
            "-singlefile",
            "-scale-to",
            &size,
            "-png",
            "-",
        ]
        .map(OsString::from);
        let png = self
            .work(&self.inner.tools.pdftoppm, &args, input, &[])
            .await?;
        self.thumbnail(Ok(self.scratch(&png)?)).await
    }

    /// A document's thumbnail. LibreOffice turns its first pages into a PDF,
    /// which is kept as its preview, and the PDF's first page becomes the
    /// thumbnail, as a PDF's does.
    async fn document(
        &self,
        hash: ContentHash,
        file: &Path,
        format: &Format,
    ) -> Result<Vec<u8>, String> {
        let mut profile = self.inner.office.lock().await;
        if profile.is_none() {
            *profile = Some(self.office_profile().await?);
        }
        let profile = profile.as_ref().expect("made above").path().join("profile");
        let scratch = self.scratch_dir()?;
        copy_folder(&profile, &scratch.path().join("profile"))
            .map_err(|err| format!("couldn't copy LibreOffice's settings: {err}"))?;
        let out = scratch.path().join("out");
        std::fs::create_dir(&out).map_err(|err| format!("couldn't make a folder: {err}"))?;

        let pages = format!(
            r#"pdf:{}:{{"PageRange":{{"type":"string","value":"1-{PREVIEW_PAGES}"}}}}"#,
            format.export
        );
        let args = [
            OsString::from(format!(
                "-env:UserInstallation={}",
                file_url(&scratch.path().join("profile"))
            )),
            "--headless".into(),
            "--norestore".into(),
            "--nolockcheck".into(),
            format!("--infilter={}", format.filter).into(),
            "--convert-to".into(),
            pages.into(),
            "--outdir".into(),
            out.clone().into(),
            file.into(),
        ];
        let messages = self
            .office(scratch.path(), &args, &[file.to_owned()])
            .await?;

        // The worker named the PDF after the file. It could only make
        // folders and plain files there.
        let pdf = std::fs::read_dir(&out)
            .and_then(|mut entries| entries.next().transpose())
            .map_err(|err| format!("couldn't read LibreOffice's PDF: {err}"))?
            .map(|entry| entry.path())
            .filter(|path| path.symlink_metadata().is_ok_and(|found| found.is_file()))
            .ok_or_else(|| match messages.as_str() {
                "" => "LibreOffice didn't make a PDF".to_owned(),
                said => format!("LibreOffice didn't make a PDF: {said}"),
            })?;
        let pages = self.pages(&pdf).await?;
        let webp = self.pdf(&pdf).await?;
        // Both are kept, or neither.
        let preview = self.preview_path(hash, pages >= PREVIEW_PAGES);
        std::fs::create_dir_all(preview.parent().expect("a preview is in a folder"))
            .and_then(|()| std::fs::rename(&pdf, &preview))
            .map_err(|err| format!("couldn't keep the preview: {err}"))?;
        Ok(webp)
    }

    /// Makes LibreOffice's settings, once. On new settings, LibreOffice sets
    /// them up and stops, so each document gets a copy of these instead.
    async fn office_profile(&self) -> Result<tempfile::TempDir, String> {
        let made = self.scratch_dir()?;
        let profile = made.path().join("profile");
        let args = [
            OsString::from(format!("-env:UserInstallation={}", file_url(&profile))),
            "--headless".into(),
            "--norestore".into(),
            "--terminate_after_init".into(),
        ];
        self.office(made.path(), &args, &[]).await?;
        let settings = profile.join("user").join("registrymodifications.xcu");
        let read = std::fs::read_to_string(&settings)
            .map_err(|err| format!("LibreOffice didn't make its settings: {err}"))?;
        let Some(end) = read.rfind("</oor:items>") else {
            return Err("LibreOffice's settings aren't what Accordion expects".into());
        };
        let written = format!("{}{DOCUMENT_SETTINGS}{}", &read[..end], &read[end..]);
        std::fs::write(&settings, written)
            .map_err(|err| format!("couldn't change LibreOffice's settings: {err}"))?;
        Ok(made)
    }

    /// Runs LibreOffice in the sandbox, with `scratch` to write in, and
    /// returns what it said on its error output.
    async fn office(
        &self,
        scratch: &Path,
        args: &[OsString],
        files: &[PathBuf],
    ) -> Result<String, String> {
        let scratch_text = scratch
            .to_str()
            .ok_or("the data folder's name isn't UTF-8")?;
        let env = [("HOME", scratch_text), ("TMPDIR", scratch_text)];
        let read: Vec<PathBuf> = DOCUMENT_READS
            .iter()
            .map(PathBuf::from)
            .chain(files.iter().cloned())
            .collect();
        let before = office_sockets();
        let input = std::fs::File::open("/dev/null").map_err(|err| err.to_string())?;
        let result = self
            .inner
            .sandbox
            .run_with_messages(Job {
                program: &self.inner.tools.soffice,
                args,
                env: &env,
                input,
                read: &read,
                write: &[scratch.to_owned()],
                sockets: Some(Path::new(SOCKETS)),
                limits: DOCUMENT_LIMITS,
            })
            .await;
        // LibreOffice can't remove its socket, since the sandbox lets it
        // only make files there, so the server does.
        for socket in office_sockets().difference(&before) {
            let _ = std::fs::remove_file(socket);
        }
        result
            .map(|(_, messages)| messages)
            .map_err(|err| err.to_string())
    }

    /// How many pages a PDF has, from poppler.
    async fn pages(&self, pdf: &Path) -> Result<u32, String> {
        let args = [pdf.as_os_str().to_owned()];
        let input = std::fs::File::open("/dev/null").map_err(|err| err.to_string())?;
        let output = self
            .work(&self.inner.tools.pdfinfo, &args, input, &[pdf.to_owned()])
            .await?;
        let text = String::from_utf8_lossy(&output);
        text.lines()
            .find_map(|line| line.strip_prefix("Pages:"))
            .and_then(|pages| pages.trim().parse().ok())
            .ok_or_else(|| "poppler didn't say how many pages the PDF has".to_owned())
    }

    /// A new, empty folder for a worker to write in, removed when it's
    /// dropped.
    fn scratch_dir(&self) -> Result<tempfile::TempDir, String> {
        tempfile::tempdir_in(self.inner.dir.join("partial"))
            .map_err(|err| format!("couldn't make a scratch folder: {err}"))
    }

    /// A frame from 10% into a video, which skips black frames and fade-ins
    /// at the start, at most [`SIZE`] pixels on its longer side, as a PNG.
    /// `None` if the file has no video, such as a song.
    async fn video(
        &self,
        file: &Path,
        container: Container,
    ) -> Result<Option<std::fs::File>, String> {
        let (start, hdr) = match self.probe(file, container).await {
            Ok(Probe { video: false, .. }) => return Ok(None),
            Ok(probe) => (
                probe.seconds.map_or(0.0, |seconds| seconds / 10.0),
                probe.hdr,
            ),
            // Such as a video with audio in a format ffprobe may not open.
            // ffmpeg skips the audio, so the first frame, as it is, works.
            Err(problem) => {
                tracing::debug!(%problem, "ffprobe couldn't read a video");
                (0.0, false)
            }
        };
        let reader = ["-f", container.format()];
        let mut png = self
            .frame(file, &reader, VIDEO_DECODERS, start, hdr)
            .await?;
        if png.is_empty() && start > 0.0 {
            // The video is shorter than it says.
            png = self.frame(file, &reader, VIDEO_DECODERS, 0.0, hdr).await?;
        }
        if png.is_empty() {
            return Err("ffmpeg found no frame in the video".into());
        }
        self.scratch(&png).map(Some)
    }

    /// How long a video is and whether it's HDR, from ffprobe. Like libvips
    /// for a HEIC's header, it reads the one file it's named, since MOV and
    /// MP4 files often keep their index at the end.
    async fn probe(&self, file: &Path, container: Container) -> Result<Probe, String> {
        let decoders = format!("{VIDEO_DECODERS},{AUDIO_DECODERS}");
        let args = [
            "-v",
            "error",
            // Only the file it's named, never a playlist's other files or
            // web addresses.
            "-protocol_whitelist",
            "file",
            "-f",
            container.format(),
            "-codec_whitelist",
            &decoders,
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=color_transfer:format=duration",
            "-of",
            "json",
        ]
        .map(OsString::from);
        let args: Vec<OsString> = args.into_iter().chain([file.into()]).collect();
        let input = std::fs::File::open("/dev/null").map_err(|err| err.to_string())?;
        let output = self
            .work(&self.inner.tools.ffprobe, &args, input, &[file.to_owned()])
            .await?;
        probe(&output)
    }

    /// One frame from `start` seconds into a video, or a picture's, shrunk,
    /// as a PNG. ffmpeg reads it with only the reader `reader` picks and the
    /// `decoders`. Empty if there's no frame there.
    async fn frame(
        &self,
        file: &Path,
        reader: &[&str],
        decoders: &str,
        start: f64,
        hdr: bool,
    ) -> Result<Vec<u8>, String> {
        // A video already smaller stays its size, as a picture does.
        let shrink = format!(
            "scale=w='min({SIZE},iw)':h='min({SIZE},ih)':force_original_aspect_ratio=decrease"
        );
        let filters = if hdr {
            format!("{shrink},{TONE_MAP}")
        } else {
            shrink
        };
        let start = format!("{start:.3}");
        let before = [
            "-nostdin",
            "-v",
            "error",
            // Every core would double the memory for no gain.
            "-threads",
            "2",
            "-protocol_whitelist",
            "file",
        ]
        .into_iter()
        .chain(reader.iter().copied())
        .chain(["-codec_whitelist", decoders])
        // Seeking to the start does nothing for a video, and makes the
        // icon reader find no picture.
        .chain(if start == "0.000" {
            vec![]
        } else {
            vec!["-ss", &start]
        })
        .chain(["-i"])
        .map(OsString::from);
        let after = [
            "-map",
            "0:v:0",
            "-frames:v",
            "1",
            "-an",
            "-sn",
            "-dn",
            "-vf",
            &filters,
            "-c:v",
            "png",
            "-f",
            "image2pipe",
            "-",
        ]
        .map(OsString::from);
        let args: Vec<OsString> = before
            .into_iter()
            .chain([file.into()])
            .chain(after)
            .collect();
        let input = std::fs::File::open("/dev/null").map_err(|err| err.to_string())?;
        self.work(&self.inner.tools.ffmpeg, &args, input, &[file.to_owned()])
            .await
    }

    /// Keeps a worker's picture in a temporary file, for libvips to read.
    fn scratch(&self, picture: &[u8]) -> Result<std::fs::File, String> {
        tempfile::tempfile_in(self.inner.dir.join("partial"))
            .and_then(|mut file| {
                io::Write::write_all(&mut file, picture)?;
                io::Seek::rewind(&mut file)?;
                Ok(file)
            })
            .map_err(|err| format!("couldn't keep the picture: {err}"))
    }

    /// Saves a thumbnail in the cache, whole or not at all.
    fn keep(&self, path: PathBuf, thumbnail: &[u8]) -> io::Result<()> {
        std::fs::create_dir_all(path.parent().expect("a thumbnail is in a folder"))?;
        let mut partial = tempfile::NamedTempFile::new_in(self.inner.dir.join("partial"))?;
        io::Write::write_all(&mut partial, thumbnail)?;
        partial.persist(&path).map_err(|err| err.error)?;
        Ok(())
    }
}

/// Why no thumbnails can be made here, if that's the case.
fn unavailable(tools: &Tools) -> Option<String> {
    if let Err(err) = accordion_sandbox::check() {
        return Some(format!("{err}."));
    }
    let missing: Vec<&str> = [
        (&tools.vips, "libvips-tools"),
        (&tools.vipsheader, "libvips-tools"),
        (&tools.pdftoppm, "poppler-utils"),
        (&tools.pdfinfo, "poppler-utils"),
    ]
    .into_iter()
    .filter(|(program, _)| !program.is_file())
    .map(|(_, package)| package)
    .collect();
    if missing.is_empty() {
        return None;
    }
    let mut packages = missing;
    packages.dedup();
    Some(format!(
        "the programs that make thumbnails aren't installed. Install {} with apt.",
        packages.join(" and ")
    ))
}

/// Why Office documents have no thumbnails here, if that's the case.
fn documents_unavailable(tools: &Tools) -> Option<String> {
    (!tools.has_office()).then(|| {
        "LibreOffice isn't installed. Install libreoffice-writer-nogui, libreoffice-calc-nogui, \
         and libreoffice-impress-nogui with apt to show the first page of each document."
            .to_owned()
    })
}

/// When LibreOffice's program folder and its settings folder last changed.
/// Installing, updating, or removing its packages adds, replaces, or removes
/// files in them.
type OfficeStamp = [Option<std::time::SystemTime>; 2];

fn office_stamp(tools: &Tools) -> OfficeStamp {
    let program = tools.soffice.parent();
    let changed = |dir: Option<PathBuf>| {
        dir.and_then(|dir| dir.metadata().ok())
            .and_then(|found| found.modified().ok())
    };
    [
        changed(program.map(Path::to_owned)),
        changed(
            program
                .and_then(Path::parent)
                .map(|office| office.join("share").join("registry")),
        ),
    ]
}

/// The server's LibreOffice sockets in [`SOCKETS`].
fn office_sockets() -> std::collections::HashSet<PathBuf> {
    use std::os::unix::fs::{FileTypeExt, MetadataExt};
    let uid = unsafe { libc::getuid() };
    let Ok(entries) = std::fs::read_dir(SOCKETS) else {
        return Default::default();
    };
    entries
        .flatten()
        .filter(|entry| {
            entry
                .file_name()
                .as_encoded_bytes()
                .starts_with(b"OSL_PIPE_")
        })
        .filter(|entry| {
            entry
                .metadata()
                .is_ok_and(|found| found.file_type().is_socket() && found.uid() == uid)
        })
        .map(|entry| entry.path())
        .collect()
}

/// Copies a folder of folders and plain files.
fn copy_folder(from: &Path, to: &Path) -> io::Result<()> {
    std::fs::create_dir(to)?;
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        if kind.is_dir() {
            copy_folder(&entry.path(), &to.join(entry.file_name()))?;
        } else if kind.is_file() {
            std::fs::copy(entry.path(), to.join(entry.file_name()))?;
        }
    }
    Ok(())
}

/// A `file://` address for a folder, as LibreOffice takes them, with
/// anything but letters, digits, and a few marks percent-encoded.
fn file_url(path: &Path) -> String {
    use std::os::unix::ffi::OsStrExt;
    let mut url = String::from("file://");
    for &byte in path.as_os_str().as_bytes() {
        if byte.is_ascii_alphanumeric() || b"/-_.~".contains(&byte) {
            url.push(byte as char);
        } else {
            url.push_str(&format!("%{byte:02X}"));
        }
    }
    url
}

/// Why videos have no thumbnails here, if that's the case.
fn videos_unavailable(tools: &Tools) -> Option<String> {
    (!tools.has_ffmpeg()).then(|| {
        "ffmpeg isn't installed. Install it with apt to show a frame from each video.".to_owned()
    })
}

/// What ffprobe's JSON says about a video.
fn probe(output: &[u8]) -> Result<Probe, String> {
    let json: serde_json::Value = serde_json::from_slice(output)
        .map_err(|err| format!("ffprobe's answer wasn't JSON: {err}"))?;
    let stream = json["streams"].get(0);
    let transfer = stream.and_then(|stream| stream["color_transfer"].as_str());
    Ok(Probe {
        video: stream.is_some(),
        seconds: json["format"]["duration"]
            .as_str()
            .and_then(|seconds| seconds.parse::<f64>().ok())
            .filter(|seconds| seconds.is_finite() && *seconds > 0.0),
        hdr: matches!(transfer, Some("smpte2084" | "arib-std-b67")),
    })
}

/// What kind of file this is, from its first bytes, its size, and those
/// bytes.
pub(crate) fn sniff(file: &Path) -> io::Result<(Option<Kind>, u64, Vec<u8>)> {
    let opened = std::fs::File::open(file)?;
    let size = opened.metadata()?.len();
    let mut head = Vec::with_capacity(HEAD);
    opened.take(HEAD as u64).read_to_end(&mut head)?;
    let whole = head.len() as u64 == size;
    Ok((kind(&head, whole), size, head))
}

/// The kind of file that starts with `head`, which is `whole` if it's the
/// entire file.
fn kind(head: &[u8], whole: bool) -> Option<Kind> {
    const HEIF_BRANDS: [&[u8; 4]; 12] = [
        b"heic", b"heix", b"hevc", b"hevx", b"heim", b"heis", b"hevm", b"hevs", b"mif1", b"msf1",
        b"avif", b"avis",
    ];
    let starts = |magic: &[u8]| head.starts_with(magic);
    if starts(b"\xff\xd8\xff")
        || starts(b"\x89PNG\r\n\x1a\n")
        || starts(b"GIF87a")
        || starts(b"GIF89a")
        || (starts(b"RIFF") && head.get(8..12) == Some(b"WEBP"))
        || starts(b"II*\0")
        || starts(b"MM\0*")
    {
        return Some(Kind::Image);
    }
    if let Some(still) = still(head) {
        return Some(Kind::Still(still));
    }
    if head.get(4..8) == Some(b"ftyp")
        && head
            .get(8..12)
            .is_some_and(|brand| HEIF_BRANDS.iter().any(|known| brand == *known))
    {
        return Some(Kind::Heif);
    }
    if starts(b"%PDF-") {
        return Some(Kind::Pdf);
    }
    if let Some(container) = container(head) {
        return Some(Kind::Video(container));
    }
    is_text(head, whole).then_some(Kind::Text)
}

/// The video format that starts with `head`, if it's one families have.
fn container(head: &[u8]) -> Option<Container> {
    // Songs, audiobooks, and Canon's raw photos are in the same format as
    // MP4, but have no video, or none ffmpeg should read.
    const NOT_VIDEO: [&[u8; 4]; 4] = [b"M4A ", b"M4B ", b"M4P ", b"crx "];
    // Older QuickTime files start with another box. Its size, first, is
    // small, which tells it from text that happens to have these letters.
    const QUICKTIME: [&[u8; 4]; 5] = [b"moov", b"mdat", b"wide", b"free", b"skip"];
    let at = |range: std::ops::Range<usize>| head.get(range);
    if at(4..8) == Some(b"ftyp") {
        let brand = at(8..12)?;
        return (!NOT_VIDEO.iter().any(|known| brand == *known)).then_some(Container::Mov);
    }
    if head.first() == Some(&0)
        && at(4..8).is_some_and(|name| QUICKTIME.iter().any(|known| name == *known))
    {
        return Some(Container::Mov);
    }
    if head.starts_with(b"\x1a\x45\xdf\xa3") {
        return Some(Container::Matroska);
    }
    if head.starts_with(b"RIFF") && at(8..12) == Some(b"AVI ") {
        return Some(Container::Avi);
    }
    if head.starts_with(b"\0\0\x01\xba") {
        return Some(Container::MpegPs);
    }
    if head.starts_with(b"\0\0\x01\xb3") {
        return Some(Container::MpegVideo);
    }
    if head.starts_with(ASF) {
        return Some(Container::Asf);
    }
    if head.starts_with(b"FLV\x01") {
        return Some(Container::Flv);
    }
    // Ogg is mostly songs, so only a file whose first track is Theora,
    // which names itself in the first page, is a video.
    if head.starts_with(b"OggS") && at(0..head.len().min(128)).is_some_and(theora) {
        return Some(Container::Ogg);
    }
    // Transport stream packets start with 0x47 every 188 bytes, or every
    // 192 after a timestamp, as camcorders write them.
    let packets =
        |first: usize, size: usize| (0..3).all(|n| head.get(first + n * size) == Some(&0x47));
    (packets(0, 188) || packets(4, 192)).then_some(Container::MpegTs)
}

/// How every ASF file starts: the GUID of its header.
const ASF: &[u8] = b"\x30\x26\xb2\x75\x8e\x66\xcf\x11\xa6\xd9\x00\xaa\x00\x62\xce\x6c";

/// Whether an Ogg file's first page has a Theora track's header.
fn theora(page: &[u8]) -> bool {
    page.windows(7).any(|window| window == b"\x80theora")
}

/// The picture ffmpeg decodes that starts with `head`, if it's one with a
/// signature. Short signatures are checked with the fields after them, so
/// text that happens to start the same way stays text.
fn still(head: &[u8]) -> Option<Still> {
    let starts = |magic: &[u8]| head.starts_with(magic);
    let at = |range: std::ops::Range<usize>| head.get(range);
    let byte = |index: usize| head.get(index).copied();
    if bitmap(head) {
        return Some(Still::Bmp);
    }
    if starts(b"8BPS") && matches!(at(4..6), Some([0, 1 | 2])) {
        return Some(Still::Psd);
    }
    if starts(b"\0\0\0\x0cjP  \r\n\x87\n") || starts(b"\xff\x4f\xff\x51") {
        return Some(Still::Jpeg2000);
    }
    if starts(b"DDS ") && at(4..8) == Some(&[124, 0, 0, 0]) {
        return Some(Still::Dds);
    }
    if starts(b"\x76\x2f\x31\x01") {
        return Some(Still::Exr);
    }
    if starts(b"\x59\xa6\x6a\x95") {
        return Some(Still::SunRaster);
    }
    if at(2048..2055) == Some(b"PCD_IPI") {
        return Some(Still::PhotoCd);
    }
    if starts(b"\x01\xda")
        && matches!(byte(2), Some(0 | 1))
        && matches!(byte(3), Some(1 | 2))
        && matches!(at(4..6), Some([0, 1..=3]))
    {
        return Some(Still::Sgi);
    }
    let big = |offset: usize| {
        at(offset..offset + 4)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u32::from_be_bytes)
    };
    if big(4) == Some(7) && big(0).is_some_and(|size| (100..4096).contains(&size)) {
        return Some(Still::Xwd);
    }
    if byte(0) == Some(0x0a)
        && matches!(byte(1), Some(0 | 2 | 3 | 4 | 5))
        && byte(2) == Some(1)
        && matches!(byte(3), Some(1 | 2 | 4 | 8))
    {
        return Some(Still::Pcx);
    }
    // "P1" to "P7", then a space and a number or a comment, or for PAM a
    // new line and its first field.
    if byte(0) == Some(b'P') {
        let space = matches!(byte(2), Some(b' ' | b'\t' | b'\r' | b'\n'));
        let then = byte(3).is_some_and(|next| next.is_ascii_digit() || next == b'#');
        match byte(1) {
            Some(b'1'..=b'6') if space && then => return Some(Still::Netpbm),
            Some(b'7') if byte(2) == Some(b'\n') => return Some(Still::Netpbm),
            Some(b'F' | b'f') if space && then => return Some(Still::Pfm),
            _ => {}
        }
    }
    // Icons and cursors: a count, then a directory entry whose reserved
    // byte is zero and whose picture starts after the directory.
    if matches!(at(0..4), Some([0, 0, 1 | 2, 0])) {
        let count = at(4..6).map(|bytes| u16::from_le_bytes([bytes[0], bytes[1]]));
        let offset =
            at(18..22).map(|bytes| u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]));
        if let (Some(count @ 1..=64), Some(offset)) = (count, offset)
            && byte(9) == Some(0)
            && offset >= 6 + 16 * u32::from(count)
        {
            return Some(Still::Icon);
        }
    }
    None
}

/// A picture without a signature, known by its name, if its first bytes
/// fit the format.
fn unsigned_still(name: &str, head: &[u8]) -> Option<Still> {
    let (_, extension) = name.rsplit_once('.')?;
    match extension.to_ascii_lowercase().as_str() {
        // A color map type of 0 or 1, and one of the picture types.
        "tga"
            if matches!(head.get(1), Some(0 | 1))
                && matches!(head.get(2), Some(1 | 2 | 3 | 9 | 10 | 11)) =>
        {
            Some(Still::Targa)
        }
        // Type 0, the only one, and a header byte of 0.
        "wbmp" if head.starts_with(&[0, 0]) => Some(Still::Wbmp),
        "pict" | "pct" => Some(Still::Pict),
        _ => None,
    }
}

/// Whether `head` is a BMP: "BM", then the reserved bytes, which are zero,
/// and a header size one of the BMP versions has. "BM" alone could start
/// a line of text.
fn bitmap(head: &[u8]) -> bool {
    const HEADERS: [u32; 6] = [12, 40, 52, 56, 108, 124];
    head.starts_with(b"BM")
        && head.get(6..10) == Some(&[0, 0, 0, 0])
        && head
            .get(14..18)
            .and_then(|size| size.try_into().ok())
            .is_some_and(|size| HEADERS.contains(&u32::from_le_bytes(size)))
}

/// Whether `head` is UTF-8 text with something to show. When it isn't the
/// whole file, it may end partway through a character.
fn is_text(head: &[u8], whole: bool) -> bool {
    let valid = match std::str::from_utf8(head) {
        Ok(text) => text,
        // Cut off partway through a character, not broken.
        Err(err) if !whole && err.error_len().is_none() => {
            std::str::from_utf8(&head[..err.valid_up_to()]).expect("valid up to here")
        }
        Err(_) => return false,
    };
    let text = valid.strip_prefix('\u{feff}').unwrap_or(valid);
    // Binary files have control characters, and a file of nothing but
    // spaces has nothing to show.
    let controls = |c: char| c.is_control() && !matches!(c, '\t' | '\n' | '\r' | '\u{c}');
    let trimmed = text.trim_start();
    !text.contains(controls)
        && !trimmed.is_empty()
        // An SVG is a picture, and an RTF file is a formatted document, so
        // their markup would look broken.
        && !(trimmed.starts_with("<svg")
            || (trimmed.starts_with("<?xml") && trimmed.contains("<svg"))
            || trimmed.starts_with("{\\rtf"))
}

/// A text thumbnail from the start of a text file: its first lines, with
/// `\n` between them.
fn text(head: &[u8]) -> Vec<u8> {
    let valid = match std::str::from_utf8(head) {
        Ok(text) => text,
        Err(err) => std::str::from_utf8(&head[..err.valid_up_to()]).expect("valid up to here"),
    };
    let text = valid.strip_prefix('\u{feff}').unwrap_or(valid);
    let lines: Vec<&str> = text
        .lines()
        .take(TEXT_LINES)
        .map(|line| line.strip_suffix('\r').unwrap_or(line))
        .collect();
    lines.join("\n").into_bytes()
}

/// The name of the newest file with the contents `hash`.
pub(crate) fn newest_name(
    conn: &rusqlite::Connection,
    hash: ContentHash,
) -> rusqlite::Result<Option<String>> {
    conn.query_row(
        "SELECT i.name FROM storage_versions v
         JOIN storage_items i ON i.id = v.item_id
         WHERE v.blob = ?1
         ORDER BY v.created_at DESC
         LIMIT 1",
        [&hash.0[..]],
        |row| row.get(0),
    )
    .optional()
}

/// Tells whoever can open the folders of files with these contents that
/// their thumbnails are ready, once the transaction commits.
fn hint_folders(conn: &rusqlite::Connection, hash: ContentHash) -> rusqlite::Result<()> {
    let folders = conn
        .prepare(
            "SELECT DISTINCT i.parent_id FROM storage_versions v
             JOIN storage_items i ON i.id = v.item_id
             WHERE v.blob = ?1 AND i.parent_id IS NOT NULL",
        )?
        .query_map([&hash.0[..]], |row| {
            Ok(accordion_types::ItemId::from_uuid(row.get(0)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    for folder in folders {
        db::hint(Hint::item(live::DRIVE_ITEMS, folder));
    }
    Ok(())
}

fn hash(row: &Row<'_>, index: usize) -> rusqlite::Result<ContentHash> {
    let bytes: Vec<u8> = row.get(index)?;
    bytes.try_into().map(ContentHash).map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            rusqlite::types::Type::Blob,
            "a content hash isn't 32 bytes".into(),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn knows_each_kind_by_its_first_bytes() {
        let kind = |head: &[u8]| kind(head, false);
        assert_eq!(kind(b"\xff\xd8\xff\xe0\0\x10JFIF"), Some(Kind::Image));
        assert_eq!(kind(b"\x89PNG\r\n\x1a\n\0\0"), Some(Kind::Image));
        assert_eq!(kind(b"GIF89a\x01\0"), Some(Kind::Image));
        assert_eq!(kind(b"RIFF\0\0\0\0WEBPVP8 "), Some(Kind::Image));
        assert_eq!(kind(b"II*\0\x08\0\0\0"), Some(Kind::Image));
        assert_eq!(kind(b"\0\0\0\x18ftypheic\0\0\0\0"), Some(Kind::Heif));
        assert_eq!(kind(b"\0\0\0\x1cftypavif\0\0\0\0"), Some(Kind::Heif));
        assert_eq!(kind(b"%PDF-1.7\n"), Some(Kind::Pdf));
        // A video in the same box format isn't a picture.
        assert_eq!(
            kind(b"\0\0\0\x18ftypmp42\0\0\0\0"),
            Some(Kind::Video(Container::Mov))
        );
        assert_eq!(kind(b"RIFF\0\0\0\0WAVEfmt "), None);
        assert_eq!(kind(b"PK\x03\x04"), None);
        assert_eq!(kind(b""), None);
        assert_eq!(kind(b"Pick up the bread.\n"), Some(Kind::Text));
        let mut bmp = b"BM\x8a\x7b\x0c\0\0\0\0\0\x8a\0\0\0\x7c\0\0\0".to_vec();
        bmp.resize(64, 0);
        assert_eq!(kind(&bmp), Some(Kind::Still(Still::Bmp)));
        // A note that starts with "BM" is still text.
        assert_eq!(kind(b"BMW service on Friday\n"), Some(Kind::Text));
    }

    #[test]
    fn knows_pictures_ffmpeg_decodes_by_their_first_bytes() {
        let padded = |start: &[u8]| {
            let mut head = start.to_vec();
            head.resize(64, 0);
            head
        };
        let still = |head: &[u8]| still(head);
        assert_eq!(still(&padded(b"8BPS\0\x01")), Some(Still::Psd));
        assert_eq!(
            still(&padded(b"\0\0\0\x0cjP  \r\n\x87\n")),
            Some(Still::Jpeg2000)
        );
        assert_eq!(still(&padded(b"\xff\x4f\xff\x51")), Some(Still::Jpeg2000));
        assert_eq!(still(&padded(b"DDS \x7c\0\0\0")), Some(Still::Dds));
        assert_eq!(still(&padded(b"\x76\x2f\x31\x01")), Some(Still::Exr));
        assert_eq!(still(&padded(b"\x59\xa6\x6a\x95")), Some(Still::SunRaster));
        assert_eq!(still(&padded(b"\x01\xda\x01\x01\0\x03")), Some(Still::Sgi));
        assert_eq!(still(&padded(b"\0\0\0\x64\0\0\0\x07")), Some(Still::Xwd));
        assert_eq!(still(&padded(b"\x0a\x05\x01\x08")), Some(Still::Pcx));
        assert_eq!(still(b"P6\n640 426\n255\n"), Some(Still::Netpbm));
        assert_eq!(still(b"P1\n# made by hand\n"), Some(Still::Netpbm));
        assert_eq!(still(b"P7\nWIDTH 640\n"), Some(Still::Netpbm));
        assert_eq!(still(b"PF\n640 426\n-1.0\n"), Some(Still::Pfm));
        let mut photo_cd = vec![0u8; 2100];
        photo_cd[2048..2055].copy_from_slice(b"PCD_IPI");
        assert_eq!(still(&photo_cd), Some(Still::PhotoCd));
        let mut icon = padded(b"\0\0\x01\0\x01\0\x20\x20\0\0");
        icon[18] = 22;
        assert_eq!(still(&icon), Some(Still::Icon));
        icon[2] = 2;
        assert_eq!(still(&icon), Some(Still::Icon));

        // Text that starts the same way stays text.
        assert_eq!(kind(b"P1 is the first priority\n", false), Some(Kind::Text));
        assert_eq!(kind(b"PF Chang's on Friday\n", false), Some(Kind::Text));
        assert_eq!(still(b"8BPS is not a picture"), None);
        // An icon whose picture would start inside its own directory.
        let mut broken = padded(b"\0\0\x01\0\x02\0");
        broken[18] = 22;
        assert_eq!(still(&broken), None);
    }

    #[test]
    fn knows_pictures_without_a_signature_by_name() {
        assert_eq!(
            unsigned_still("Logo.TGA", b"\0\0\x02\0\0"),
            Some(Still::Targa)
        );
        assert_eq!(unsigned_still("Logo.tga", b"\0\0\x07"), None);
        assert_eq!(unsigned_still("Old.wbmp", b"\0\0\x80"), Some(Still::Wbmp));
        assert_eq!(unsigned_still("Scan.pct", b"\0\0"), Some(Still::Pict));
        assert_eq!(unsigned_still("Notes.txt", b"\0\0"), None);
    }

    #[test]
    fn knows_the_video_formats_families_have() {
        let video = |head: &[u8]| container(head);
        assert_eq!(video(b"\0\0\0\x14ftypqt  \0\0\0\0"), Some(Container::Mov));
        assert_eq!(
            video(b"\0\0\0\x08wide\x01\x23\x45\x67mdat"),
            Some(Container::Mov)
        );
        assert_eq!(
            video(b"\x1a\x45\xdf\xa3\x9f\x42\x86\x81"),
            Some(Container::Matroska)
        );
        assert_eq!(video(b"RIFF\0\0\0\0AVI LIST"), Some(Container::Avi));
        let mut stream = vec![0u8; 600];
        for n in 0..3 {
            stream[n * 188] = 0x47;
        }
        assert_eq!(video(&stream), Some(Container::MpegTs));
        let mut camcorder = vec![0u8; 600];
        for n in 0..3 {
            camcorder[4 + n * 192] = 0x47;
        }
        assert_eq!(video(&camcorder), Some(Container::MpegTs));

        assert_eq!(video(b"\0\0\x01\xba\x44\0\x04\0"), Some(Container::MpegPs));
        assert_eq!(
            video(b"\0\0\x01\xb3\x50\x02\xd0\x31"),
            Some(Container::MpegVideo)
        );
        assert_eq!(video(&[ASF, b"\0\0"].concat()), Some(Container::Asf));
        assert_eq!(video(b"FLV\x01\x05\0\0\0\x09"), Some(Container::Flv));
        let mut ogv = b"OggS\0\x02".to_vec();
        ogv.resize(28, 0);
        ogv.extend_from_slice(b"\x80theora\x03\x02");
        assert_eq!(video(&ogv), Some(Container::Ogg));
        let mut song = b"OggS\0\x02".to_vec();
        song.resize(28, 0);
        song.extend_from_slice(b"\x01vorbis\0\0");
        assert_eq!(video(&song), None);

        assert_eq!(video(b"\0\0\0\x1cftypM4A \0\0\0\0"), None);
        assert_eq!(video(b"\0\0\0\x18ftypcrx \0\0\0\0"), None);
        // Text with "wide" in the right place isn't a QuickTime file.
        assert_eq!(video(b"The wide river"), None);
        assert_eq!(video(b"RIFF\0\0\0\0WAVEfmt "), None);
    }

    #[test]
    fn reads_what_ffprobe_says() {
        let iphone = br#"{"programs": [], "streams": [{"color_transfer": "arib-std-b67"}],
            "format": {"duration": "30.976667"}}"#;
        assert_eq!(
            probe(iphone).unwrap(),
            Probe {
                video: true,
                seconds: Some(30.976667),
                hdr: true
            }
        );
        let ordinary =
            br#"{"streams": [{"color_transfer": "bt709"}], "format": {"duration": "N/A"}}"#;
        assert_eq!(
            probe(ordinary).unwrap(),
            Probe {
                video: true,
                seconds: None,
                hdr: false
            }
        );
        let song = br#"{"streams": [], "format": {"duration": "2.000000"}}"#;
        assert!(!probe(song).unwrap().video);
        assert!(probe(b"not json").is_err());
    }

    #[test]
    fn knows_text_from_binary_files() {
        assert!(is_text(b"name,age\r\nAlex,9\r\n", true));
        assert!(is_text(
            "\u{feff}# Caf\u{e9} \u{2615}\n\tTabs too".as_bytes(),
            true
        ));
        // Cut off partway through "é", because only the start was read.
        assert!(is_text(b"Caf\xc3", false));
        assert!(!is_text(b"Caf\xc3", true));
        assert!(!is_text(b"Latin-1 caf\xe9 isn't UTF-8", true));
        assert!(!is_text(b"\x7fELF\x02\x01\x01\0", false));
        assert!(!is_text(b"   \n\n", true));
        assert!(!is_text(
            b"<svg xmlns=\"http://www.w3.org/2000/svg\"/>",
            true
        ));
        assert!(!is_text(b"<?xml version=\"1.0\"?>\n<svg/>", true));
        assert!(is_text(b"<?xml version=\"1.0\"?>\n<notes/>", true));
        assert!(!is_text(b"{\\rtf1\\ansi Hello}", true));
    }

    #[test]
    fn knows_office_documents_by_name_and_first_bytes() {
        let found = |name: &str, head: &[u8]| format(name, head).map(|format| format.filter);
        assert_eq!(
            found("Report.DOCX", b"PK\x03\x04\x14\0"),
            Some("MS Word 2007 XML")
        );
        assert_eq!(found("budget.xls", COMPOUND), Some("MS Excel 97"));
        assert_eq!(
            found("letter.rtf", b"{\\rtf1\\ansi"),
            Some("Rich Text Format")
        );
        assert_eq!(found("slides.odp", b"PK\x03\x04"), Some("impress8"));
        // A web page named as a document isn't one.
        assert_eq!(found("invoice.docx", b"<html><body>"), None);
        assert_eq!(found("archive.zip", b"PK\x03\x04"), None);
        assert_eq!(found("Keynote.key", b"PK\x03\x04"), None);
        assert_eq!(found("docx", b"PK\x03\x04"), None);
    }

    #[test]
    fn writes_file_addresses_for_libreoffice() {
        assert_eq!(
            file_url(Path::new("/var/lib/accordion/cache/a b%c")),
            "file:///var/lib/accordion/cache/a%20b%25c"
        );
    }

    #[test]
    fn keeps_a_text_files_first_lines() {
        let long: String = (1..=100).map(|n| format!("Line {n}\r\n")).collect();
        let kept = String::from_utf8(text(long.as_bytes())).unwrap();
        assert_eq!(kept.lines().count(), TEXT_LINES);
        assert!(kept.starts_with("Line 1\nLine 2\n"));
        assert!(!kept.contains('\r'));
        // The byte order mark goes, and so does a character cut off at the end.
        assert_eq!(text(b"\xef\xbb\xbfCaf\xc3"), b"Caf");
    }
}

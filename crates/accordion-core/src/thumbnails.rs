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

static MIGRATIONS: &[Migration] = &[
    Migration {
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
    },
    Migration {
        version: 2,
        name: "Add picture previews",
        sql: "
    -- A large picture for the viewer, made after the thumbnail, of
    -- pictures the browser can't show (decision 26). NULL: not decided
    -- yet. picture: in cache/previews. none: not needed, or can't be
    -- made. failed: the worker couldn't make one.
    ALTER TABLE thumbnails ADD COLUMN preview TEXT
        CHECK (preview IN ('picture', 'none', 'failed'));
    -- Why there's none, or what went wrong.
    ALTER TABLE thumbnails ADD COLUMN preview_reason TEXT;
",
    },
];

/// The version of what makes thumbnails. Raise it when it can make
/// thumbnails of files it couldn't before, so they get another try.
pub const WORKER: u32 = 8;

/// The longer side of a thumbnail, in pixels.
const SIZE: u32 = 480;

/// The longer side of a picture's preview, in pixels.
const PREVIEW_SIZE: u32 = 2560;

/// What a worker may use for each picture preview. A 2560-pixel PNG on
/// its way to libvips is larger than a thumbnail's output limit.
const PREVIEW_LIMITS: Limits = Limits {
    memory: 1024 * 1024 * 1024,
    cpu: Duration::from_secs(30),
    time: Duration::from_secs(45),
    output: 64 * 1024 * 1024,
};

/// How large a worker's picture is, and what the worker may use.
#[derive(Clone, Copy)]
struct Output {
    /// The longer side, in pixels.
    longest: u32,
    limits: Limits,
    /// Whether libvips turns the picture as its orientation says.
    turn: bool,
}

const THUMBNAIL: Output = Output {
    longest: SIZE,
    limits: LIMITS,
    turn: true,
};

const PREVIEW: Output = Output {
    longest: PREVIEW_SIZE,
    limits: PREVIEW_LIMITS,
    turn: true,
};

/// Larger files get no thumbnail.
const MAX_FILE: u64 = 200 * 1024 * 1024;

/// HEIC and AVIF files are decoded whole, so larger ones get no thumbnail.
/// A 51-megapixel HEIC takes about 540 MB, within a worker's 1 GB, so
/// HEIC goes up to 64. AVIF's decoder hasn't been measured, so it stays at
/// 24.
const MAX_HEIC_PIXELS: u64 = 64_000_000;
const MAX_AVIF_PIXELS: u64 = 24_000_000;

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

/// Why a camera raw photo, an SVG, or a DjVu book has no thumbnail when its
/// program isn't installed. Files with these reasons get another try once
/// it is.
const NO_LIBRAW: &str = "LibRaw isn't installed";
const NO_RSVG: &str = "librsvg isn't installed";
const NO_DJVU: &str = "djvulibre isn't installed";

/// Why a camera raw photo has no thumbnail when the camera kept no JPEG in
/// it, or only a bitmap libvips doesn't trust.
const NO_EMBEDDED: &str = "a camera raw photo without a JPEG preview in it";

/// Why an MNG has no thumbnail when it doesn't start with a whole PNG, such
/// as one that starts with a JPEG or a change to another picture.
const NO_MNG_PICTURE: &str = "an MNG that doesn't start with a PNG picture";

/// How every MNG file starts.
const MNG: &[u8] = b"\x8aMNG\r\n\x1a\n";

/// The fontconfig settings and cache, for an SVG's text. The fonts
/// themselves are under `/usr`, which every worker reads.
const FONT_READS: [&str; 2] = ["/etc/fonts", "/var/cache/fontconfig"];

/// Camera raw formats that are TIFF files inside, known by their name since
/// their first bytes are a TIFF's. The others have their own signatures.
const TIFF_RAWS: [&str; 17] = [
    "3fr", "arw", "cr2", "dcr", "dng", "erf", "fff", "iiq", "kdc", "mef", "mos", "nef", "nrw",
    "pef", "sr2", "srf", "srw",
];

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
    /// LibRaw's sample program that finds the JPEG a camera keeps in a raw
    /// photo. Optional, like the rest below.
    pub simple_dcraw: PathBuf,
    /// librsvg's program, for SVG drawings.
    pub rsvg_convert: PathBuf,
    /// djvulibre's program, for DjVu books.
    pub ddjvu: PathBuf,
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
            simple_dcraw: "/usr/bin/simple_dcraw".into(),
            rsvg_convert: "/usr/bin/rsvg-convert".into(),
            ddjvu: "/usr/bin/ddjvu".into(),
        }
    }

    fn has_ffmpeg(&self) -> bool {
        self.ffmpeg.is_file() && self.ffprobe.is_file()
    }

    fn has_office(&self) -> bool {
        self.soffice.is_file()
    }

    fn has_libraw(&self) -> bool {
        self.simple_dcraw.is_file()
    }

    fn has_rsvg(&self) -> bool {
        self.rsvg_convert.is_file()
    }

    fn has_djvu(&self) -> bool {
        self.ddjvu.is_file()
    }
}

/// A thumbnail in the cache.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Thumbnail {
    pub kind: ThumbnailKind,
    pub path: PathBuf,
    /// When it was made. A thumbnail made again, such as by a newer worker,
    /// has a new time, so addresses that browsers keep can include it.
    pub made: std::time::SystemTime,
}

impl Thumbnail {
    /// A short text that changes each time the thumbnail is made, for the
    /// addresses browsers keep it under.
    pub fn version(&self) -> String {
        let made = self
            .made
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        format!("{:x}", made.as_millis())
    }
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
    /// A sentence saying which pictures show an icon, such as camera raw
    /// photos, and which packages would give them thumbnails.
    pub pictures: Option<String>,
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
    /// `cache/previews/`, for pictures' previews.
    previews: PathBuf,
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
    /// A camera's raw photo, shown by the JPEG the camera keeps in it.
    Raw,
    /// An SVG drawing, or a compressed one.
    Svg,
    /// A DjVu book or scan, which gets a PDF preview, as a document does.
    Djvu,
    /// An MNG animation, shown by the PNG picture it starts with.
    Mng,
    /// A picture only one of libvips's untrusted loaders reads, which the
    /// worker runs on its own.
    Loaded(Loader),
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
    /// X pixmap and X bitmap, pictures written as C source.
    Xpm,
    Xbm,
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
            Self::Xpm => (&["-f", "xpm_pipe"], "xpm"),
            Self::Xbm => (&["-f", "xbm_pipe"], "xbm"),
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

/// Pictures that libvips reads only with a loader it doesn't trust with
/// hostile files, and nothing else reads. The worker names the one loader,
/// so the file can't reach libvips's others, and runs it in the sandbox,
/// as it does ffmpeg's older decoders (decision 26).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Loader {
    /// Radiance HDR pictures, `.hdr`.
    Radiance,
    /// FITS, the pictures telescopes and astronomers keep.
    Fits,
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
        let previews = data_dir.join("cache").join("previews");
        std::fs::create_dir_all(&previews)?;
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
        let pictures = pictures_unavailable(&tools);
        if let Some(reason) = &unavailable {
            tracing::warn!("no thumbnails: {reason}");
        }
        Ok(Self {
            inner: Arc::new(Inner {
                db,
                storage,
                dir,
                previews,
                sandbox: Sandbox::new(&tools.launcher),
                tools,
                status: watch::Sender::new(ThumbnailStatus {
                    unavailable,
                    videos,
                    documents,
                    pictures,
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
            .find_map(|kind| {
                let path = self.path(hash, kind);
                let found = std::fs::metadata(&path)
                    .ok()
                    .filter(|found| found.is_file())?;
                Some(Thumbnail {
                    kind,
                    made: found.modified().ok()?,
                    path,
                })
            })
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

    /// The large picture the viewer shows of the contents with `hash`, for a
    /// picture the browser can't show itself, if there is one yet.
    pub fn picture(&self, hash: ContentHash) -> Option<PathBuf> {
        Some(self.picture_path(hash)).filter(|path| path.is_file())
    }

    fn picture_path(&self, hash: ContentHash) -> PathBuf {
        self.inner
            .previews
            .join(blobs::shard(hash))
            .with_extension("webp")
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
            // Previews come after every waiting thumbnail, since a grid
            // needs those first.
            let previews = if waiting.is_empty() {
                match self.previews_waiting(BATCH).await {
                    Ok(previews) => previews,
                    Err(err) => {
                        self.note(Some(err));
                        Vec::new()
                    }
                }
            } else {
                Vec::new()
            };
            for hash in &previews {
                let outcome = tokio::select! {
                    () = &mut stopped => return,
                    outcome = self.make_preview(*hash) => outcome,
                };
                let result = self.record_preview(*hash, outcome).await;
                self.note(result.err());
            }
            let result = self.count().await;
            self.note(result.err());
            if waiting.is_empty() && previews.is_empty() {
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

    /// Records whether the contents with `hash` have a picture preview.
    async fn record_preview(&self, hash: ContentHash, outcome: Outcome) -> Result<(), Error> {
        let (state, reason) = match outcome {
            Outcome::Ready => ("picture", None),
            Outcome::None(reason) => ("none", Some(reason.to_owned())),
            Outcome::Failed(problem) => {
                tracing::info!(contents = %hash, %problem, "couldn't make a preview");
                ("failed", Some(problem))
            }
        };
        self.inner
            .db
            .write(move |tx| {
                tx.execute(
                    "UPDATE thumbnails SET preview = ?2, preview_reason = ?3 WHERE hash = ?1",
                    (&hash.0[..], state, reason),
                )?;
                // Open lists load the items again, which now have a preview.
                if state == "picture" {
                    hint_folders(tx, hash)?;
                }
                Ok::<_, rusqlite::Error>(())
            })
            .await?;
        Ok(())
    }

    /// Contents with a thumbnail whose preview isn't decided yet, newest
    /// first.
    async fn previews_waiting(&self, limit: usize) -> Result<Vec<ContentHash>, Error> {
        self.inner
            .db
            .read(move |conn| {
                conn.prepare(
                    "SELECT t.hash FROM thumbnails t
                     WHERE t.state = 'ready' AND t.preview IS NULL
                     ORDER BY t.made_at DESC
                     LIMIT ?1",
                )?
                .query_map([limit as i64], |row| hash(row, 0))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(Error::from)
            })
            .await
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
        let pictures = pictures_unavailable(&self.inner.tools);
        self.inner.status.send_if_modified(|status| {
            let changed = (
                status.waiting,
                status.failed,
                &status.videos,
                &status.documents,
                &status.pictures,
            ) != (waiting, failed, &videos, &documents, &pictures);
            status.waiting = waiting;
            status.failed = failed;
            status.videos = videos;
            status.documents = documents;
            status.pictures = pictures;
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
                             reason = excluded.reason, made_at = excluded.made_at,
                             preview = NULL, preview_reason = NULL"
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
            (tools.has_libraw(), NO_LIBRAW),
            (tools.has_rsvg(), NO_RSVG),
            (tools.has_djvu(), NO_DJVU),
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

        // Previews the cache lost are made again.
        let pictures = self
            .inner
            .db
            .read(|conn| {
                conn.prepare("SELECT hash FROM thumbnails WHERE preview = 'picture'")?
                    .query_map([], |row| hash(row, 0))?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(Error::from)
            })
            .await?;
        let lost: Vec<ContentHash> = pictures
            .into_iter()
            .filter(|hash| self.picture(*hash).is_none())
            .collect();
        if !lost.is_empty() {
            self.inner
                .db
                .write(move |tx| {
                    let mut reset =
                        tx.prepare("UPDATE thumbnails SET preview = NULL WHERE hash = ?1")?;
                    for hash in &lost {
                        reset.execute([&hash.0[..]])?;
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
                self.picture_path(hash),
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

    /// What kind of file the contents with `hash` are, by their first
    /// bytes and, for documents and pictures without a signature, their
    /// name. Also their size and first bytes.
    async fn identify(
        &self,
        file: &Path,
        hash: ContentHash,
    ) -> Result<(Option<Kind>, u64, Vec<u8>), String> {
        let (kind, size, head) =
            sniff(file).map_err(|err| format!("couldn't read the file: {err}"))?;
        let by_name = kind.is_none()
            || (kind == Some(Kind::Image) && tiff(&head))
            || FORMATS
                .iter()
                .any(|format| head.starts_with(format.signature));
        if !by_name {
            return Ok((kind, size, head));
        }
        let kind = match self
            .name(hash)
            .await
            .map_err(|err| format!("couldn't read the file's name: {err}"))?
        {
            Some(name) => named(&name, &head, kind),
            None => kind,
        };
        Ok((kind, size, head))
    }

    async fn make(&self, hash: ContentHash) -> Outcome {
        let file = self.inner.storage.contents_file(hash);
        let (kind, size, head) = match self.identify(&file, hash).await {
            Ok(identified) => identified,
            Err(problem) => return Outcome::Failed(problem),
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
                match self
                    .frame(&file, reader, decoders, 0.0, false, THUMBNAIL)
                    .await
                {
                    Ok(png) if !png.is_empty() => {
                        self.thumbnail(self.scratch(&png).map_err(io::Error::other))
                            .await
                    }
                    Ok(_) => Err("ffmpeg found no picture in the file".into()),
                    Err(problem) => Err(problem),
                }
            }
            Kind::Heif => match self.pixels(&file).await {
                Ok(pixels) if pixels > heif_limit(&head).0 => {
                    return Outcome::None(heif_limit(&head).1);
                }
                Ok(_) => self.thumbnail(std::fs::File::open(&file)).await,
                Err(problem) => Err(problem),
            },
            Kind::Raw if !self.inner.tools.has_libraw() => return Outcome::None(NO_LIBRAW),
            Kind::Raw => match self.embedded(&file).await {
                Ok(Some((jpeg, turn))) => self.shrink(Ok(jpeg), Output { turn, ..THUMBNAIL }).await,
                Ok(None) => return Outcome::None(NO_EMBEDDED),
                Err(problem) => Err(problem),
            },
            Kind::Svg if !self.inner.tools.has_rsvg() => return Outcome::None(NO_RSVG),
            Kind::Svg => self.drawing(&file, THUMBNAIL).await,
            Kind::Djvu if !self.inner.tools.has_djvu() => return Outcome::None(NO_DJVU),
            Kind::Djvu => self.djvu(hash, &file).await,
            Kind::Mng => match mng_picture(&file) {
                Ok(Some(png)) => {
                    self.thumbnail(self.scratch(&png).map_err(io::Error::other))
                        .await
                }
                Ok(None) => return Outcome::None(NO_MNG_PICTURE),
                Err(problem) => Err(problem),
            },
            Kind::Loaded(loader) => self.loaded(&file, loader, THUMBNAIL).await,
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

    /// Makes the large picture the viewer shows of a picture the browser
    /// can't show itself, such as a TIFF, a HEIC, or a Photoshop file.
    async fn make_preview(&self, hash: ContentHash) -> Outcome {
        let file = self.inner.storage.contents_file(hash);
        let (kind, _, head) = match self.identify(&file, hash).await {
            Ok(identified) => identified,
            Err(problem) => return Outcome::Failed(problem),
        };
        let tiff = tiff(&head);
        let made = match kind {
            Some(Kind::Image) if tiff => self.shrink(std::fs::File::open(&file), PREVIEW).await,
            Some(Kind::Heif) if !avif(&head) => match self.pixels(&file).await {
                Ok(pixels) if pixels > MAX_HEIC_PIXELS => {
                    return Outcome::None(heif_limit(&head).1);
                }
                Ok(_) => self.shrink(std::fs::File::open(&file), PREVIEW).await,
                Err(problem) => Err(problem),
            },
            Some(Kind::Still(Still::Bmp | Still::Icon)) => {
                return Outcome::None("the browser shows it");
            }
            Some(Kind::Mng) => match mng_picture(&file) {
                Ok(Some(png)) => {
                    self.shrink(self.scratch(&png).map_err(io::Error::other), PREVIEW)
                        .await
                }
                Ok(None) => return Outcome::None(NO_MNG_PICTURE),
                Err(problem) => Err(problem),
            },
            Some(Kind::Loaded(loader)) => self.loaded(&file, loader, PREVIEW).await,
            Some(Kind::Raw) if !self.inner.tools.has_libraw() => return Outcome::None(NO_LIBRAW),
            Some(Kind::Raw) => match self.embedded(&file).await {
                Ok(Some((jpeg, turn))) => self.shrink(Ok(jpeg), Output { turn, ..PREVIEW }).await,
                Ok(None) => return Outcome::None(NO_EMBEDDED),
                Err(problem) => Err(problem),
            },
            Some(Kind::Svg) if !self.inner.tools.has_rsvg() => return Outcome::None(NO_RSVG),
            // Drawn as a picture, so an SVG's scripts never reach the
            // browser.
            Some(Kind::Svg) => self.drawing(&file, PREVIEW).await,
            Some(Kind::Still(_)) if !self.inner.tools.has_ffmpeg() => {
                return Outcome::None(NO_FFMPEG);
            }
            Some(Kind::Still(still)) => {
                let (reader, decoders) = still.reader();
                match self
                    .frame(&file, reader, decoders, 0.0, false, PREVIEW)
                    .await
                {
                    Ok(png) if !png.is_empty() => {
                        self.shrink(self.scratch(&png).map_err(io::Error::other), PREVIEW)
                            .await
                    }
                    Ok(_) => Err("ffmpeg found no picture in the file".into()),
                    Err(problem) => Err(problem),
                }
            }
            _ => return Outcome::None("the browser shows it, or it isn't a picture"),
        };
        match made {
            Ok(webp) => match self.keep(self.picture_path(hash), &webp) {
                Ok(()) => Outcome::Ready,
                Err(err) => Outcome::Failed(format!("couldn't save the preview: {err}")),
            },
            Err(problem) => Outcome::Failed(problem),
        }
    }

    /// Keeps a thumbnail that's been made.
    fn done(&self, hash: ContentHash, kind: ThumbnailKind, thumbnail: &[u8]) -> Outcome {
        // A thumbnail of the other kind, from before a newer worker knew
        // better, such as text for an XPM picture, goes.
        let other = match kind {
            ThumbnailKind::Picture => ThumbnailKind::Text,
            ThumbnailKind::Text => ThumbnailKind::Picture,
        };
        let kept = self.keep(self.path(hash, kind), thumbnail).and_then(|()| {
            match std::fs::remove_file(self.path(hash, other)) {
                Err(err) if err.kind() != io::ErrorKind::NotFound => Err(err),
                _ => Ok(()),
            }
        });
        match kept {
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
        self.work_within(program, args, input, read, LIMITS).await
    }

    /// The same, with `limits` instead of a thumbnail's.
    async fn work_within(
        &self,
        program: &Path,
        args: &[OsString],
        input: std::fs::File,
        read: &[PathBuf],
        limits: Limits,
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
                limits,
            })
            .await
            .map_err(|err| err.to_string())
    }

    /// Shrinks the image on `input` to a thumbnail, as WebP.
    async fn thumbnail(&self, input: io::Result<std::fs::File>) -> Result<Vec<u8>, String> {
        self.shrink(input, THUMBNAIL).await
    }

    /// Shrinks the image on `input` to at most `longest` pixels on its
    /// longer side, as WebP.
    async fn shrink(
        &self,
        input: io::Result<std::fs::File>,
        output: Output,
    ) -> Result<Vec<u8>, String> {
        let input = input.map_err(|err| format!("couldn't open the file: {err}"))?;
        let size = output.longest.to_string();
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
        .into_iter()
        .chain((!output.turn).then_some("--no-rotate"))
        .map(OsString::from)
        .collect::<Vec<_>>();
        let webp = self
            .work_within(&self.inner.tools.vips, &args, input, &[], output.limits)
            .await?;
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
        self.keep_pdf(hash, &pdf).await
    }

    /// Keeps a worker's PDF of a file's first pages as its preview, and
    /// returns a thumbnail of its first page.
    async fn keep_pdf(&self, hash: ContentHash, pdf: &Path) -> Result<Vec<u8>, String> {
        let pages = self.pages(pdf).await?;
        let webp = self.pdf(pdf).await?;
        // Both are kept, or neither.
        let preview = self.preview_path(hash, pages >= PREVIEW_PAGES);
        std::fs::create_dir_all(preview.parent().expect("a preview is in a folder"))
            .and_then(|()| std::fs::rename(pdf, &preview))
            .map_err(|err| format!("couldn't keep the preview: {err}"))?;
        Ok(webp)
    }

    /// The JPEG a camera keeps in its raw photo, full size on most cameras,
    /// as LibRaw finds it, and whether to turn it as its orientation says.
    /// `None` if there's no JPEG.
    ///
    /// LibRaw writes it next to the file it reads, so it reads the file
    /// through a link in a scratch folder, the only place it can write. The
    /// sandbox still lets it read only the file itself.
    async fn embedded(&self, file: &Path) -> Result<Option<(std::fs::File, bool)>, String> {
        let scratch = self.scratch_dir()?;
        let link = scratch.path().join("photo");
        std::os::unix::fs::symlink(file, &link)
            .map_err(|err| format!("couldn't link to the file: {err}"))?;
        let args = [OsString::from("-e"), link.into()];
        let input = std::fs::File::open("/dev/null").map_err(|err| err.to_string())?;
        self.inner
            .sandbox
            .run(Job {
                program: &self.inner.tools.simple_dcraw,
                args: &args,
                env: &[],
                input,
                read: &[file.to_owned()],
                write: &[scratch.path().to_owned()],
                sockets: None,
                limits: LIMITS,
            })
            .await
            .map_err(|err| err.to_string())?;
        // A bitmap preview would be a .ppm, which libvips doesn't trust.
        // The worker could only make folders and plain files there.
        let jpeg = scratch.path().join("photo.thumb.jpg");
        if !jpeg.symlink_metadata().is_ok_and(|found| found.is_file()) {
            return Ok(None);
        }
        let args = [OsString::from("-a"), jpeg.clone().into()];
        let input = std::fs::File::open("/dev/null").map_err(|err| err.to_string())?;
        let header = self
            .work(
                &self.inner.tools.vipsheader,
                &args,
                input,
                std::slice::from_ref(&jpeg),
            )
            .await?;
        // The open file outlasts the scratch folder.
        let opened = std::fs::File::open(&jpeg)
            .map_err(|err| format!("couldn't open LibRaw's JPEG: {err}"))?;
        Ok(Some((opened, turns(&String::from_utf8_lossy(&header)))))
    }

    /// An SVG drawn as a picture that fits `output`, as WebP. librsvg reads
    /// the drawing on its input, so it has no folder to load the files an
    /// SVG names from, and it never runs an SVG's scripts.
    async fn drawing(&self, file: &Path, output: Output) -> Result<Vec<u8>, String> {
        let input =
            std::fs::File::open(file).map_err(|err| format!("couldn't open the file: {err}"))?;
        let size = output.longest.to_string();
        let args = [
            "--width",
            &size,
            "--height",
            &size,
            "--keep-aspect-ratio",
            "--format",
            "png",
            "-",
        ]
        .map(OsString::from);
        let read: Vec<PathBuf> = FONT_READS.iter().map(PathBuf::from).collect();
        let png = self
            .inner
            .sandbox
            .run(Job {
                program: &self.inner.tools.rsvg_convert,
                args: &args,
                env: &[],
                input,
                read: &read,
                write: &[],
                sockets: None,
                limits: output.limits,
            })
            .await
            .map_err(|err| err.to_string())?;
        if !png.starts_with(b"\x89PNG") {
            return Err("librsvg didn't draw a picture".into());
        }
        self.shrink(self.scratch(&png).map_err(io::Error::other), output)
            .await
    }

    /// A picture read with only libvips's `loader`, as WebP that fits
    /// `output`. That loader turns the picture into a PNG, which libvips
    /// then shrinks with its trusted PNG reader.
    async fn loaded(&self, file: &Path, loader: Loader, output: Output) -> Result<Vec<u8>, String> {
        let (args, input, read) = match loader {
            // The Radiance loader reads its input.
            Loader::Radiance => (
                vec!["radload_source".into(), "[descriptor=0]".into()],
                std::fs::File::open(file),
                vec![],
            ),
            // The FITS loader reads only a file it's named.
            Loader::Fits => (
                vec!["fitsload".into(), file.into()],
                std::fs::File::open("/dev/null"),
                vec![file.to_owned()],
            ),
        };
        let args: Vec<OsString> = args.into_iter().chain([".png".into()]).collect();
        let input = input.map_err(|err| format!("couldn't open the file: {err}"))?;
        // No VIPS_BLOCK_UNTRUSTED, since the loader is one it blocks. The
        // worker names it, so the file reaches no other.
        let env = [("VIPS_CONCURRENCY", "2"), ("MALLOC_ARENA_MAX", "2")];
        let png = self
            .inner
            .sandbox
            .run(Job {
                program: &self.inner.tools.vips,
                args: &args,
                env: &env,
                input,
                read: &read,
                write: &[],
                sockets: None,
                // A whole picture as a PNG is larger than a thumbnail's
                // output limit.
                limits: PREVIEW_LIMITS,
            })
            .await
            .map_err(|err| err.to_string())?;
        if !png.starts_with(b"\x89PNG") {
            return Err("libvips didn't make a PNG".into());
        }
        self.shrink(self.scratch(&png).map_err(io::Error::other), output)
            .await
    }

    /// A DjVu book's thumbnail. djvulibre turns its first pages into a PDF,
    /// at their own resolution, which is kept as its preview, as a
    /// document's is.
    async fn djvu(&self, hash: ContentHash, file: &Path) -> Result<Vec<u8>, String> {
        let scratch = self.scratch_dir()?;
        let scratch_text = scratch
            .path()
            .to_str()
            .ok_or("the data folder's name isn't UTF-8")?;
        let pdf = scratch.path().join("book.pdf");
        let args = [
            OsString::from("-format=pdf"),
            format!("-page=1-{PREVIEW_PAGES}").into(),
            file.into(),
            pdf.clone().into(),
        ];
        // djvulibre writes each page as a TIFF first, in its temporary
        // folder.
        let env = [("TMPDIR", scratch_text)];
        let input = std::fs::File::open("/dev/null").map_err(|err| err.to_string())?;
        self.inner
            .sandbox
            .run(Job {
                program: &self.inner.tools.ddjvu,
                args: &args,
                env: &env,
                input,
                read: &[file.to_owned()],
                write: &[scratch.path().to_owned()],
                sockets: None,
                limits: DOCUMENT_LIMITS,
            })
            .await
            .map_err(|err| err.to_string())?;
        if !pdf.symlink_metadata().is_ok_and(|found| found.is_file()) {
            return Err("djvulibre didn't make a PDF".into());
        }
        self.keep_pdf(hash, &pdf).await
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
            .frame(file, &reader, VIDEO_DECODERS, start, hdr, THUMBNAIL)
            .await?;
        if png.is_empty() && start > 0.0 {
            // The video is shorter than it says.
            png = self
                .frame(file, &reader, VIDEO_DECODERS, 0.0, hdr, THUMBNAIL)
                .await?;
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
        output: Output,
    ) -> Result<Vec<u8>, String> {
        let longest = output.longest;
        // A video already smaller stays its size, as a picture does.
        let shrink = format!(
            "scale=w='min({longest},iw)':h='min({longest},ih)':force_original_aspect_ratio=decrease"
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
        self.work_within(
            &self.inner.tools.ffmpeg,
            &args,
            input,
            &[file.to_owned()],
            output.limits,
        )
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

/// Which pictures have no thumbnails here because a program isn't
/// installed, and the packages that would give them one, as a sentence.
fn pictures_unavailable(tools: &Tools) -> Option<String> {
    let missing: Vec<(&str, &str)> = [
        (tools.has_libraw(), "camera raw photos", "libraw-bin"),
        (tools.has_rsvg(), "SVG drawings", "librsvg2-bin"),
        (tools.has_djvu(), "DjVu books", "djvulibre-bin"),
    ]
    .into_iter()
    .filter(|(installed, _, _)| !installed)
    .map(|(_, files, package)| (files, package))
    .collect();
    if missing.is_empty() {
        return None;
    }
    let (files, packages): (Vec<&str>, Vec<&str>) = missing.into_iter().unzip();
    let files = list(&files);
    Some(format!(
        "{}{} show an icon instead. Install {} with apt to give them thumbnails and previews.",
        files[..1].to_uppercase(),
        &files[1..],
        list(&packages),
    ))
}

/// "a", "a and b", or "a, b, and c".
fn list(items: &[&str]) -> String {
    match items {
        [] => String::new(),
        [one] => (*one).to_owned(),
        [first, second] => format!("{first} and {second}"),
        [rest @ .., last] => format!("{}, and {last}", rest.join(", ")),
    }
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
    // Before pictures, since some raw photos are TIFF files inside.
    if raw(head) {
        return Some(Kind::Raw);
    }
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
    if djvu(head) {
        return Some(Kind::Djvu);
    }
    if starts(MNG) {
        return Some(Kind::Mng);
    }
    if starts(b"#?RADIANCE\n") || starts(b"#?RGBE\n") {
        return Some(Kind::Loaded(Loader::Radiance));
    }
    // The first of the header's 80-character lines says it's a standard
    // FITS file.
    if starts(b"SIMPLE  =") && head.get(29) == Some(&b'T') {
        return Some(Kind::Loaded(Loader::Fits));
    }
    if let Some(container) = container(head) {
        return Some(Kind::Video(container));
    }
    // SVG is text, so it's checked before text is.
    if svg(head) {
        return Some(Kind::Svg);
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
    // These are text, so they're checked before text is.
    if starts(b"/* XPM */") {
        return Some(Still::Xpm);
    }
    if starts(b"#define ") && head.windows(7).any(|window| window == b"_width ") {
        return Some(Still::Xbm);
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

/// What a file is by its name as well as its first bytes, for the formats
/// whose first bytes don't say on their own: Office documents, which are
/// ZIP or compound files inside, camera raw photos that are TIFF files
/// inside, compressed SVG, and pictures without a signature. `kind` is what
/// the first bytes say.
fn named(name: &str, head: &[u8], kind: Option<Kind>) -> Option<Kind> {
    if let Some(format) = format(name, head) {
        return Some(Kind::Document(format));
    }
    let extension = name
        .rsplit_once('.')
        .map(|(_, extension)| extension.to_ascii_lowercase())
        .unwrap_or_default();
    match kind {
        Some(Kind::Image) if tiff(head) && TIFF_RAWS.contains(&extension.as_str()) => {
            Some(Kind::Raw)
        }
        None if extension == "svgz" && head.starts_with(b"\x1f\x8b") => Some(Kind::Svg),
        None => unsigned_still(name, head).map(Kind::Still),
        kind => kind,
    }
}

/// Whether to turn a raw photo's JPEG as its orientation says, from what
/// `vipsheader -a` says about it. LibRaw gives each JPEG the raw photo's
/// orientation, but some cameras keep a JPEG that's already turned. A
/// camera's sensor is wider than it's tall, so a JPEG that's taller than
/// it's wide, with an orientation that turns it on its side, already has
/// been.
fn turns(header: &str) -> bool {
    let field = |name: &str| {
        header.lines().find_map(|line| {
            line.strip_prefix(name)?
                .strip_prefix(':')?
                .trim()
                .parse::<u32>()
                .ok()
        })
    };
    let sideways = matches!(field("orientation"), Some(5..=8));
    let tall =
        matches!((field("width"), field("height")), (Some(width), Some(height)) if height > width);
    !(sideways && tall)
}

/// The PNG picture an MNG animation starts with, as a PNG file, or `None`
/// if it starts some other way. MNG keeps PNG's chunks as they are, from
/// IHDR to IEND, so this only finds where they are. libvips decodes them,
/// in the sandbox.
fn mng_picture(file: &Path) -> Result<Option<Vec<u8>>, String> {
    let read = std::fs::File::open(file)
        .and_then(|opened| {
            let mut read = Vec::new();
            opened.take(MAX_FILE).read_to_end(&mut read)?;
            Ok(read)
        })
        .map_err(|err| format!("couldn't read the file: {err}"))?;
    Ok(first_png(&read))
}

/// The first PNG picture in the MNG `mng`, as a PNG file.
fn first_png(mng: &[u8]) -> Option<Vec<u8>> {
    let mut at = mng.strip_prefix(MNG).map(|_| MNG.len())?;
    let mut start = None;
    while let Some(header) = mng.get(at..at + 8) {
        let length = u32::from_be_bytes(header[..4].try_into().ok()?) as usize;
        let name = &header[4..8];
        // The length, the name, the data, and a checksum.
        let end = at.checked_add(12)?.checked_add(length)?;
        if end > mng.len() {
            return None;
        }
        match name {
            b"IHDR" => start = Some(at),
            b"IEND" => {
                let mut png = b"\x89PNG\r\n\x1a\n".to_vec();
                png.extend_from_slice(&mng[start?..end]);
                return Some(png);
            }
            // A JPEG or a change to a picture, which aren't PNG.
            b"JHDR" | b"DHDR" | b"BASI" | b"MEND" if start.is_none() => return None,
            _ => {}
        }
        at = end;
    }
    None
}

/// Whether a HEIF file is an AVIF rather than a HEIC, by its brand.
fn avif(head: &[u8]) -> bool {
    head.get(8..12)
        .is_some_and(|brand| brand == b"avif" || brand == b"avis")
}

/// The most pixels a HEIC or AVIF may have, and why a larger one has no
/// thumbnail.
fn heif_limit(head: &[u8]) -> (u64, &'static str) {
    if avif(head) {
        (MAX_AVIF_PIXELS, "an AVIF over 24 megapixels")
    } else {
        (MAX_HEIC_PIXELS, "a HEIC over 64 megapixels")
    }
}

/// Whether `head` starts as a TIFF does, in either byte order.
fn tiff(head: &[u8]) -> bool {
    head.starts_with(b"II*\0") || head.starts_with(b"MM\0*")
}

/// Whether `head` is a camera raw photo with a signature of its own.
/// Canon's CR2 files are TIFF files that say so after the TIFF header.
fn raw(head: &[u8]) -> bool {
    const SIGNATURES: [&[u8]; 9] = [
        // Olympus ORF, in either byte order and an older kind.
        b"IIRO",
        b"IIRS",
        b"MMOR",
        // Panasonic RW2 and Leica RWL.
        b"IIU\0",
        b"FUJIFILMCCD-RAW",
        // Minolta MRW.
        b"\0MRM",
        // Sigma X3F.
        b"FOVb",
        // Canon's older CRW.
        b"II\x1a\0\0\0HEAPCCDR",
        b"II*\0\x10\0\0\0CR",
    ];
    SIGNATURES.iter().any(|signature| head.starts_with(signature))
        // Canon's CR3 is in the same format as MP4.
        || (head.get(4..8) == Some(b"ftyp") && head.get(8..12) == Some(b"crx "))
}

/// Whether `head` is a DjVu document: one page, or several in one file.
fn djvu(head: &[u8]) -> bool {
    head.starts_with(b"AT&TFORM") && matches!(head.get(12..16), Some(b"DJVU" | b"DJVM"))
}

/// Whether `head` is an SVG drawing: XML whose first element is `<svg`,
/// after any XML declaration, comments, and document type. Other XML, and
/// web pages with drawings in them, stay text.
fn svg(head: &[u8]) -> bool {
    let mut rest = head.strip_prefix(b"\xef\xbb\xbf").unwrap_or(head);
    loop {
        rest = rest.trim_ascii_start();
        let skip_past = |rest: &[u8], end: &[u8]| {
            rest.windows(end.len())
                .position(|window| window == end)
                .map(|at| at + end.len())
        };
        let skipped = if rest.starts_with(b"<?") {
            skip_past(rest, b"?>")
        } else if rest.starts_with(b"<!--") {
            skip_past(rest, b"-->")
        } else if rest.len() >= 9 && rest[..9].eq_ignore_ascii_case(b"<!DOCTYPE") {
            // A document type may have its own declarations, in brackets.
            match (skip_past(rest, b"["), skip_past(rest, b">")) {
                (Some(open), Some(close)) if open < close => skip_past(rest, b"]")
                    .and_then(|end| skip_past(&rest[end..], b">").map(|after| end + after)),
                (_, close) => close,
            }
        } else {
            break;
        };
        match skipped {
            Some(at) => rest = &rest[at..],
            None => return false,
        }
    }
    rest.strip_prefix(b"<svg").is_some_and(|after| {
        after
            .first()
            .is_some_and(|next| next.is_ascii_whitespace() || matches!(next, b'>' | b'/'))
    })
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
        // An RTF file is a formatted document, so its markup would look
        // broken. SVG drawings are found before text is.
        && !trimmed.starts_with("{\\rtf")
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
        assert_eq!(
            still(b"/* XPM */\nstatic char *logo[] = {"),
            Some(Still::Xpm)
        );
        assert_eq!(
            still(b"#define logo_width 16\n#define logo_height 16\n"),
            Some(Still::Xbm)
        );
        // C source that defines other things is still text.
        assert_eq!(
            kind(b"#define MAX 10\nint main() {}\n", false),
            Some(Kind::Text)
        );
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
    fn knows_camera_raw_photos() {
        let raw = |head: &[u8]| kind(head, false);
        assert_eq!(raw(b"II*\0\x10\0\0\0CR\x02\0"), Some(Kind::Raw));
        assert_eq!(raw(b"IIRO\x08\0\0\0"), Some(Kind::Raw));
        assert_eq!(raw(b"IIU\0\x18\0\0\0"), Some(Kind::Raw));
        assert_eq!(raw(b"FUJIFILMCCD-RAW 0201"), Some(Kind::Raw));
        assert_eq!(raw(b"\0\0\0\x18ftypcrx \0\0\0\x01"), Some(Kind::Raw));
        // Nikon's, Sony's, and Adobe's are TIFF files, known by their name.
        let tiff = b"II*\0\x08\0\0\0";
        assert_eq!(raw(tiff), Some(Kind::Image));
        assert_eq!(named("DSC_0001.NEF", tiff, raw(tiff)), Some(Kind::Raw));
        assert_eq!(named("photo.dng", tiff, raw(tiff)), Some(Kind::Raw));
        assert_eq!(named("scan.tif", tiff, raw(tiff)), Some(Kind::Image));
        // A JPEG named like a raw photo is still a JPEG.
        let jpeg = b"\xff\xd8\xff\xe0";
        assert_eq!(named("photo.nef", jpeg, raw(jpeg)), Some(Kind::Image));
    }

    #[test]
    fn turns_a_raw_photos_jpeg_only_once() {
        // Kept as the sensor saw it: turned as the photo says.
        assert!(turns("width: 4256\nheight: 2832\norientation: 8\n"));
        // Already turned by the camera, which LibRaw doesn't know.
        assert!(!turns("width: 2832\nheight: 4256\norientation: 8\n"));
        // Upside down is turned either way.
        assert!(turns("width: 4256\nheight: 2832\norientation: 3\n"));
        assert!(turns("width: 4256\nheight: 2832\n"));
    }

    #[test]
    fn knows_svg_drawings_from_other_xml() {
        let drawing = |head: &[u8]| kind(head, true);
        assert_eq!(
            drawing(b"<svg xmlns=\"http://www.w3.org/2000/svg\"/>"),
            Some(Kind::Svg)
        );
        assert_eq!(
            drawing(
                b"\xef\xbb\xbf<?xml version=\"1.0\"?>\n<!-- Made in Inkscape -->\n\
                  <!DOCTYPE svg PUBLIC \"-//W3C//DTD SVG 1.1//EN\" \"svg11.dtd\" [\n\
                  <!ENTITY ns \"x\">\n] >\n<svg\n width=\"10\">"
            ),
            Some(Kind::Svg)
        );
        assert_eq!(drawing(b"<svg>"), Some(Kind::Svg));
        // Other XML, and web pages with drawings in them, stay text.
        assert_eq!(drawing(b"<?xml version=\"1.0\"?><svgx/>"), Some(Kind::Text));
        assert_eq!(
            drawing(b"<?xml version=\"1.0\"?><kml><svg/></kml>"),
            Some(Kind::Text)
        );
        assert_eq!(
            drawing(b"<!DOCTYPE html><html><svg></svg></html>"),
            Some(Kind::Text)
        );
        assert_eq!(drawing(b"<!-- never ends <svg>"), Some(Kind::Text));
        // Compressed SVG is known by its name.
        assert_eq!(named("map.svgz", b"\x1f\x8b\x08\0", None), Some(Kind::Svg));
        assert_eq!(named("map.gz", b"\x1f\x8b\x08\0", None), None);
    }

    #[test]
    fn knows_pictures_only_one_libvips_loader_reads() {
        assert_eq!(
            kind(b"#?RADIANCE\nFORMAT=32-bit_rle_rgbe\n", false),
            Some(Kind::Loaded(Loader::Radiance))
        );
        assert_eq!(
            kind(b"#?RGBE\nGAMMA=0.45\n", false),
            Some(Kind::Loaded(Loader::Radiance))
        );
        let fits = b"SIMPLE  =                    T / conforms to FITS standard";
        assert_eq!(kind(fits, false), Some(Kind::Loaded(Loader::Fits)));
        // A line of text that starts the same way stays text.
        assert_eq!(kind(b"SIMPLE  = not a picture", true), Some(Kind::Text));
    }

    #[test]
    fn finds_the_png_an_mng_starts_with() {
        let chunk = |name: &[u8; 4], data: &[u8]| {
            let mut chunk = (data.len() as u32).to_be_bytes().to_vec();
            chunk.extend_from_slice(name);
            chunk.extend_from_slice(data);
            chunk.extend_from_slice(&[0; 4]);
            chunk
        };
        let picture = [
            chunk(b"IHDR", &[0; 13]),
            chunk(b"IDAT", b"pixels"),
            chunk(b"IEND", b""),
        ]
        .concat();
        let mng = [
            MNG,
            &chunk(b"MHDR", &[0; 28]),
            &picture,
            &chunk(b"MEND", b""),
        ]
        .concat();
        assert_eq!(kind(&mng, false), Some(Kind::Mng));
        assert_eq!(
            first_png(&mng),
            Some([b"\x89PNG\r\n\x1a\n".as_slice(), &picture].concat())
        );
        // One that starts with a JPEG, or is cut off, has none.
        let jpeg = [MNG, &chunk(b"MHDR", &[0; 28]), &chunk(b"JHDR", &[0; 16])].concat();
        assert_eq!(first_png(&jpeg), None);
        assert_eq!(first_png(&mng[..mng.len() - 30]), None);
        // A length that runs past the end.
        let mut long = mng.clone();
        long[8..12].copy_from_slice(&u32::MAX.to_be_bytes());
        assert_eq!(first_png(&long), None);
    }

    #[test]
    fn decodes_larger_heic_than_avif() {
        let heic = b"\0\0\0\x18ftypheic\0\0\0\0";
        let avif = b"\0\0\0\x18ftypavif\0\0\0\0";
        assert_eq!(heif_limit(heic).0, 64_000_000);
        assert_eq!(heif_limit(avif).0, 24_000_000);
    }

    #[test]
    fn knows_djvu_books() {
        assert_eq!(
            kind(b"AT&TFORM\0\x02\x57\x9cDJVMDIRM", false),
            Some(Kind::Djvu)
        );
        assert_eq!(kind(b"AT&TFORM\0\0\x10\0DJVUINFO", false), Some(Kind::Djvu));
        // A shared dictionary that other files include has no pages.
        assert_eq!(kind(b"AT&TFORM\0\0\x10\0DJVIDjbz", false), None);
    }

    #[test]
    fn says_which_pictures_need_which_packages() {
        let missing = Tools {
            simple_dcraw: "/nowhere/simple_dcraw".into(),
            rsvg_convert: "/nowhere/rsvg-convert".into(),
            ddjvu: "/nowhere/ddjvu".into(),
            ..Tools::installed("/nowhere/accordion")
        };
        assert_eq!(
            pictures_unavailable(&missing).unwrap(),
            "Camera raw photos, SVG drawings, and DjVu books show an icon instead. Install \
             libraw-bin, librsvg2-bin, and djvulibre-bin with apt to give them thumbnails and \
             previews."
        );
        let one = Tools {
            simple_dcraw: "/usr/bin/true".into(),
            rsvg_convert: "/usr/bin/true".into(),
            ..missing
        };
        assert_eq!(
            pictures_unavailable(&one).unwrap(),
            "DjVu books show an icon instead. Install djvulibre-bin with apt to give them \
             thumbnails and previews."
        );
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
        // SVG drawings are pictures, found before text is.
        assert_eq!(
            kind(b"<?xml version=\"1.0\"?>\n<svg/>", true),
            Some(Kind::Svg)
        );
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

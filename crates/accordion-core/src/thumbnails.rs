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
//! An e-book's thumbnail is its cover, and its preview is its details as
//! JSON, both from Accordion's own reader in [`book`], which runs in the
//! sandbox too.
//!
//! A file that fails isn't tried again until [`WORKER`] changes, such as
//! when an update makes thumbnails of more kinds of file.
//!
//! After the thumbnails and previews, the worker reads each file's
//! [`Facts`] for Drive's details panel: a picture's or a video's size, how
//! long a video or a sound lasts, and how many pages a document has. The
//! same sandboxed programs read them, and only the numbers are kept.

use std::collections::HashMap;
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

pub mod book;

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
    Migration {
        version: 3,
        name: "Add sound previews",
        sql: "
    -- audio: a sound the browser can't play, converted to AAC in
    -- cache/previews (decision 26). SQLite can't change a CHECK, so the
    -- table is made again.
    CREATE TABLE thumbnails_new (
        hash BLOB PRIMARY KEY CHECK (length(hash) = 32),
        state TEXT NOT NULL CHECK (state IN ('ready', 'none', 'failed')),
        worker INTEGER NOT NULL,
        reason TEXT,
        made_at TEXT NOT NULL,
        preview TEXT CHECK (preview IN ('picture', 'audio', 'none', 'failed')),
        preview_reason TEXT
    ) STRICT, WITHOUT ROWID;
    INSERT INTO thumbnails_new
        SELECT hash, state, worker, reason, made_at, preview, preview_reason FROM thumbnails;
    DROP TABLE thumbnails;
    ALTER TABLE thumbnails_new RENAME TO thumbnails;
    CREATE INDEX thumbnails_failed ON thumbnails (state) WHERE state = 'failed';
",
    },
    Migration {
        version: 4,
        name: "Add video previews and their settings",
        sql: "
    -- video: a video the browser can't play, repackaged or converted in
    -- cache/previews. waiting: one that needs that, which the worker makes
    -- in turn, or when someone opens it (decision 26).
    CREATE TABLE thumbnails_new (
        hash BLOB PRIMARY KEY CHECK (length(hash) = 32),
        state TEXT NOT NULL CHECK (state IN ('ready', 'none', 'failed')),
        worker INTEGER NOT NULL,
        reason TEXT,
        made_at TEXT NOT NULL,
        preview TEXT CHECK (preview IN ('picture', 'audio', 'video', 'waiting', 'none', 'failed')),
        preview_reason TEXT,
        -- For a video, copy: its picture as it is, in a container the
        -- browser opens, with its sound converted if need be. convert: its
        -- picture converted too, which takes much longer.
        plan TEXT CHECK (plan IN ('copy', 'convert')),
        -- When someone opened a waiting video, so it goes first.
        wanted_at TEXT,
        -- When someone last watched the video's preview, so the cache limit
        -- removes the least recently watched first.
        opened_at TEXT,
        -- Whether the cache limit removed the video's preview, so it's
        -- made again only when someone opens it.
        removed INTEGER NOT NULL DEFAULT 0 CHECK (removed IN (0, 1))
    ) STRICT, WITHOUT ROWID;
    INSERT INTO thumbnails_new
        (hash, state, worker, reason, made_at, preview, preview_reason)
        SELECT hash, state, worker, reason, made_at, preview, preview_reason FROM thumbnails;
    DROP TABLE thumbnails;
    ALTER TABLE thumbnails_new RENAME TO thumbnails;
    CREATE INDEX thumbnails_failed ON thumbnails (state) WHERE state = 'failed';
    CREATE INDEX thumbnails_videos ON thumbnails (preview) WHERE preview IN ('video', 'waiting');

    -- The admin's choices for previews. convert_videos is upload, opened,
    -- or never, or NULL to follow what the server's hardware suits.
    CREATE TABLE thumbnail_settings (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        convert_videos TEXT CHECK (convert_videos IN ('upload', 'opened', 'never')),
        -- The share of the data disk previews may use, in percent.
        space_percent INTEGER NOT NULL CHECK (space_percent BETWEEN 1 AND 50)
    ) STRICT;
    INSERT INTO thumbnail_settings VALUES (1, NULL, 10);
",
    },
    Migration {
        version: 5,
        name: "Add e-book previews",
        sql: "
    -- book: an e-book's details, its title, authors, description, and
    -- table of contents, as JSON in cache/previews (decision 26).
    CREATE TABLE thumbnails_new (
        hash BLOB PRIMARY KEY CHECK (length(hash) = 32),
        state TEXT NOT NULL CHECK (state IN ('ready', 'none', 'failed')),
        worker INTEGER NOT NULL,
        reason TEXT,
        made_at TEXT NOT NULL,
        preview TEXT CHECK (preview IN ('picture', 'audio', 'video', 'book', 'waiting', 'none',
                                        'failed')),
        preview_reason TEXT,
        plan TEXT CHECK (plan IN ('copy', 'convert')),
        wanted_at TEXT,
        opened_at TEXT,
        removed INTEGER NOT NULL DEFAULT 0 CHECK (removed IN (0, 1))
    ) STRICT, WITHOUT ROWID;
    INSERT INTO thumbnails_new SELECT * FROM thumbnails;
    DROP TABLE thumbnails;
    ALTER TABLE thumbnails_new RENAME TO thumbnails;
    CREATE INDEX thumbnails_failed ON thumbnails (state) WHERE state = 'failed';
    CREATE INDEX thumbnails_videos ON thumbnails (preview) WHERE preview IN ('video', 'waiting');
",
    },
    Migration {
        version: 6,
        name: "Add file facts",
        sql: "
    -- What the worker read about each file, for Drive's details panel.
    -- facts: the FACTS version that read them, or NULL for not read yet.
    -- width and height: a picture's or a video's size as it's shown, in
    -- pixels. seconds: how long a video or a sound lasts. pages: how many
    -- pages a document has, and more_pages: whether it goes on past them,
    -- when its preview stops at the most a preview has.
    ALTER TABLE thumbnails ADD COLUMN facts INTEGER;
    ALTER TABLE thumbnails ADD COLUMN width INTEGER;
    ALTER TABLE thumbnails ADD COLUMN height INTEGER;
    ALTER TABLE thumbnails ADD COLUMN seconds REAL;
    ALTER TABLE thumbnails ADD COLUMN pages INTEGER;
    ALTER TABLE thumbnails ADD COLUMN more_pages INTEGER NOT NULL DEFAULT 0
        CHECK (more_pages IN (0, 1));
    CREATE INDEX thumbnails_facts ON thumbnails (facts);
",
    },
];

/// The version of what makes thumbnails. Raise it when it can make
/// thumbnails of files it couldn't before, so they get another try.
pub const WORKER: u32 = 15;

/// The version of what reads files' [`Facts`]. Raise it when it reads
/// more, so every file is read again.
const FACTS: u32 = 4;

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

/// What ffmpeg may use to convert a sound the browser can't play. Three
/// hours of sound takes about 3 minutes on a fast computer, and longer on
/// a Raspberry Pi.
const SOUND_LIMITS: Limits = Limits {
    memory: 1024 * 1024 * 1024,
    cpu: Duration::from_secs(15 * 60),
    time: Duration::from_secs(20 * 60),
    output: 1024 * 1024,
};

/// Longer sounds get no converted preview, so one can't keep the worker
/// busy for long. Three hours at 128 kbit/s is about 170 MB.
const MAX_SOUND_SECONDS: f64 = 3.0 * 60.0 * 60.0;

/// What ffmpeg may use to repackage a video, which reads and writes it
/// all, and may convert its sound.
const COPY_LIMITS: Limits = Limits {
    memory: 1024 * 1024 * 1024,
    cpu: Duration::from_secs(30 * 60),
    time: Duration::from_secs(60 * 60),
    output: 1024 * 1024,
};

/// What ffmpeg may use to convert a video. Converting takes several times
/// a video's length in processor time, and more on small servers, so a
/// feature film takes hours. Step 6 of decision 26 measures it.
const CONVERT_LIMITS: Limits = Limits {
    memory: 1536 * 1024 * 1024,
    cpu: Duration::from_secs(6 * 60 * 60),
    time: Duration::from_secs(4 * 60 * 60),
    output: 1024 * 1024,
};

/// Larger videos get no converted preview. They keep their thumbnail and
/// the download.
const MAX_VIDEO: u64 = 4 * 1024 * 1024 * 1024;

/// The longer side of a converted video, in pixels: 1080p.
const VIDEO_SIZE: u32 = 1920;

/// Servers with this many cores or more convert videos after upload.
/// Smaller ones convert a video when someone first opens it.
const UPLOAD_CORES: usize = 6;

/// How long after someone last watched a video's preview the worker notes
/// it again, so watching doesn't write to the database at every seek.
const OPENED_EVERY: Duration = Duration::from_secs(60 * 60);

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

/// What ffprobe may use to count a bare video's frames, which reads it all
/// without decoding it: about a second for 100 MB on a fast computer.
const COUNT_LIMITS: Limits = Limits {
    memory: 256 * 1024 * 1024,
    cpu: Duration::from_secs(5 * 60),
    time: Duration::from_secs(10 * 60),
    output: 1024 * 1024,
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
                              vp6f,theora,rv10,rv20,rv30,rv40,dnxhd,dvvideo";

/// ffprobe opens a decoder for every track, and stops at one it may not use,
/// so it may also use the decoders for the audio in those videos. ffmpeg
/// skips the audio.
const AUDIO_DECODERS: &str = "aac,mp3float,mp3,opus,libopus,vorbis,libvorbis,ac3,eac3,\
                              flac,alac,pcm_s16le,pcm_s16be,pcm_s24le,pcm_s24be,pcm_s32le,\
                              pcm_f32le,pcm_bluray,mp2,mp2float,wmav1,wmav2,cook,sipr,\
                              real_144,real_288,atrac3";

/// The decoders ffmpeg may use for songs and other sound files: the codecs
/// in the formats families have, from MP3 to Amiga, telephone, and Sound
/// Blaster audio.
const SOUND_DECODERS: &str = "mp3float,mp3,mp2float,mp2,mp1float,mp1,aac,aac_fixed,alac,flac,\
                              vorbis,libvorbis,opus,libopus,speex,libspeex,pcm_u8,pcm_s8,\
                              pcm_s8_planar,pcm_s16le,pcm_s16be,pcm_s16le_planar,\
                              pcm_s16be_planar,pcm_s24le,pcm_s24be,pcm_s24le_planar,pcm_s32le,\
                              pcm_s32be,pcm_s32le_planar,pcm_u16le,pcm_u16be,pcm_u24le,\
                              pcm_u24be,pcm_u32le,pcm_u32be,pcm_f32le,pcm_f32be,pcm_f64le,\
                              pcm_f64be,pcm_alaw,pcm_mulaw,pcm_bluray,pcm_dvd,adpcm_ms,\
                              adpcm_ima_wav,adpcm_ima_qt,adpcm_yamaha,g722,g726,g726le,gsm,\
                              libgsm,gsm_ms,libgsm_ms,mace3,mace6,qdm2,qdmc,8svx_exp,8svx_fib,\
                              wmav1,wmav2,wmapro,wmalossless,wmavoice,ape,wavpack,tta,amrnb,\
                              amrwb,ac3,ac3_fixed,eac3,dca,truehd,mlp,cook,ralf,sipr,real_144,\
                              real_288,atrac3,atrac3plus,dsd_lsbf,dsd_msbf,dsd_lsbf_planar,\
                              dsd_msbf_planar,hcom";

/// The decoders for a song's cover art, which ffprobe opens with the
/// sound.
const COVER_DECODERS: &str = "mjpeg,png,bmp,gif,webp";

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
const NO_SOX: &str = "SoX isn't installed";

/// Why a camera raw photo has no thumbnail when the camera kept no JPEG in
/// it, or only a bitmap libvips doesn't trust.
const NO_EMBEDDED: &str = "a camera raw photo without a JPEG preview in it";

/// Why an MNG has no thumbnail when it doesn't start with a whole PNG, such
/// as one that starts with a JPEG or a change to another picture.
const NO_MNG_PICTURE: &str = "an MNG that doesn't start with a PNG picture";

/// Why a song has no thumbnail.
const NO_COVER: &str = "a sound without cover art";

/// The sample text in a font's thumbnail and preview.
const PANGRAM_START: &str = "The quick brown fox";
const PANGRAM: &str = "The quick brown fox jumps over the lazy dog";

/// Fontconfig's settings for drawing a font's sample: no fonts of its own.
const NO_FONTS: &str = "<?xml version=\"1.0\"?>\n<fontconfig></fontconfig>\n";

/// Which picture of a font's sample text to draw.
#[derive(Clone, Copy)]
enum FontPage {
    Thumbnail,
    /// A page of samples, for fonts the browser doesn't draw.
    Preview,
}

/// Why an e-book has no thumbnail.
const NO_BOOK_COVER: &str = "an e-book without a cover";

/// What the e-book reader may use. It reads a few small parts of a book,
/// or the whole of a FictionBook, which is at most 64 MB.
const BOOK_LIMITS: Limits = Limits {
    memory: 1024 * 1024 * 1024,
    cpu: Duration::from_secs(20),
    time: Duration::from_secs(30),
    output: 40 * 1024 * 1024,
};

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
    /// SoX, for older sound formats ffmpeg can't read.
    pub sox: PathBuf,
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
            sox: "/usr/bin/sox".into(),
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

    fn has_sox(&self) -> bool {
        self.sox.is_file()
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

/// What the worker read about a file, for Drive's details panel. Each is
/// `None` for a kind of file without it, or when the worker couldn't read
/// it.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Facts {
    /// A picture's or a video's width in pixels, as it's shown, so a photo
    /// or a video that's turned upright has its width and height swapped.
    pub width: Option<u32>,
    pub height: Option<u32>,
    /// How long a video or a sound lasts.
    pub seconds: Option<f64>,
    /// How many pages a PDF or a document has, or a presentation's slides.
    pub pages: Option<u32>,
    /// Whether a document goes on past `pages`, because its preview stops
    /// at [`PREVIEW_PAGES`] and only the preview is counted.
    pub more_pages: bool,
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
    /// Sentences saying which pictures show an icon, such as camera raw
    /// photos, and which older sound files can't play, and which packages
    /// would fix that.
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
    /// Videos waiting for their preview, and how each is made, so item
    /// lists can say so without asking the database.
    videos_waiting: std::sync::Mutex<HashMap<ContentHash, Plan>>,
    /// When the server converts videos: the admin's choice, or what the
    /// server's hardware suits.
    conversion: std::sync::Mutex<VideoConversion>,
    /// Wakes the video worker when a video starts waiting, someone opens
    /// one, or the setting changes.
    videos_wake: tokio::sync::Notify,
    /// When each video's preview was last noted as watched.
    watched: std::sync::Mutex<HashMap<ContentHash, std::time::Instant>>,
}

/// When the server converts videos the browser can't play. Repackaging,
/// which is quick, happens after upload whatever this says.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VideoConversion {
    /// In the background, after upload, so a video is ready when someone
    /// opens it.
    Upload,
    /// When someone first opens the video, so only watched videos take
    /// work and space.
    Opened,
    /// Never: people download the video to watch it.
    Never,
}

impl VideoConversion {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Upload => "upload",
            Self::Opened => "opened",
            Self::Never => "never",
        }
    }

    pub fn parse(text: &str) -> Option<Self> {
        [Self::Upload, Self::Opened, Self::Never]
            .into_iter()
            .find(|choice| choice.as_str() == text)
    }

    /// What this server's hardware suits: after upload with 6 or more
    /// cores, and when opened with fewer.
    pub fn automatic() -> Self {
        if cores() >= UPLOAD_CORES {
            Self::Upload
        } else {
            Self::Opened
        }
    }
}

/// How many cores the server has.
pub fn cores() -> usize {
    std::thread::available_parallelism().map_or(1, usize::from)
}

/// The admin's choices for previews.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PreviewSettings {
    /// `None` follows [`VideoConversion::automatic`].
    pub convert_videos: Option<VideoConversion>,
    /// The share of the data disk previews may use, in percent: 1 to 50.
    pub space_percent: u32,
}

/// How much room previews take, in bytes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PreviewSpace {
    pub used: u64,
    /// The share of the data disk they may use.
    pub limit: u64,
    /// The size of the data disk.
    pub disk: u64,
}

/// How a video's preview is made.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Plan {
    /// Its picture as it is, in a container the browser opens, with its
    /// sound converted if the browser can't play it.
    Copy,
    /// Its picture converted too.
    Convert,
}

impl Plan {
    fn as_str(self) -> &'static str {
        match self {
            Self::Copy => "copy",
            Self::Convert => "convert",
        }
    }
}

/// What became of one file.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Ready,
    /// A video that needs a preview made, which the video worker makes in
    /// turn.
    Waiting(Plan),
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
    /// A song or another sound file, shown by its cover art.
    Audio(Sound),
    /// An older sound format only SoX reads, which has no cover art.
    Sox(Sox),
    /// UTF-8 text, such as notes, Markdown, CSV, or code.
    Text,
    Document(&'static Format),
    /// An e-book, shown by its cover, with its details as its preview.
    Book(book::Format),
    /// A font, shown by sample text that ffmpeg draws in it.
    Font(Font),
}

/// The font formats FreeType reads that have outlines, so they draw at any
/// size.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Font {
    /// TrueType, `.ttf`.
    TrueType,
    /// OpenType with PostScript outlines, `.otf`.
    OpenType,
    /// Several TrueType or OpenType fonts in one file, `.ttc` and `.otc`.
    Collection,
    /// Web fonts, `.woff` and `.woff2`.
    Woff,
    Woff2,
    /// PostScript Type 1, `.pfb` and `.pfa`.
    Type1,
}

impl Font {
    /// Whether the browser draws fonts in this format itself, so the viewer
    /// shows its sample from the font file. Others get a picture of one.
    fn browser_draws(self) -> bool {
        !matches!(self, Self::Collection | Self::Type1)
    }
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
    /// RealMedia: `.rm`, `.rmvb`, and some `.ra` files.
    RealMedia,
    /// MXF, from professional cameras and editing.
    Mxf,
    /// Windows Media Center's TV recordings, `.wtv`.
    Wtv,
    /// DV, from older camcorders, `.dv`.
    Dv,
    /// Motion JPEG on its own, `.mjpeg`, which starts as a JPEG does.
    Mjpeg,
    /// H.264 and HEVC on their own, without a container: `.h264`, `.hevc`,
    /// and `.h265`.
    H264,
    Hevc,
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
            Self::RealMedia => "rm",
            Self::Mxf => "mxf",
            Self::Wtv => "wtv",
            Self::Dv => "dv",
            Self::Mjpeg => "mjpeg",
            Self::H264 => "h264",
            Self::Hevc => "hevc",
        }
    }

    /// For video on its own, which doesn't say how long it lasts, so its
    /// frames are counted instead: how many of the frame rate ffprobe gives
    /// make one frame. H.264 counts its time in fields, two to a frame.
    /// Motion JPEG on its own has no frame rate, so ffmpeg plays it at 25
    /// frames a second, and its length is how long it plays for.
    fn ticks_per_frame(self) -> Option<f64> {
        match self {
            Self::H264 => Some(2.0),
            Self::MpegVideo | Self::Mjpeg | Self::Hevc => Some(1.0),
            _ => None,
        }
    }
}

/// The sound formats families have. ffmpeg reads each with only the one
/// demuxer, as it does videos (decision 26).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Sound {
    /// MP3, and the older MP2 and MP1.
    Mpeg,
    /// AAC on its own, as `.aac` files are.
    Adts,
    /// Songs, audiobooks, and ringtones in MP4: `.m4a`, `.m4b`, and `.m4r`.
    Mp4,
    Flac,
    /// Vorbis, Opus, Speex, or FLAC in Ogg.
    Ogg,
    /// WAV, and its larger forms, RF64 and BW64.
    Wav,
    /// Sony's Wave64.
    Wave64,
    /// AIFF and AIFF-C.
    Aiff,
    /// Amiga's 8SVX and MAUD, and DSD in DSDIFF (`.dff`).
    Iff,
    /// DSD in Sony's DSF.
    Dsf,
    /// Monkey's Audio, `.ape`.
    Ape,
    WavPack,
    /// True Audio, `.tta`.
    Tta,
    /// Sun and NeXT audio: `.au` and `.snd`.
    Au,
    /// Apple's Core Audio Format, `.caf`.
    Caf,
    /// Phones' voice recordings, `.amr`.
    Amr,
    /// RealAudio before RealMedia, `.ra`.
    RealAudio,
    /// Sound Blaster's `.voc`.
    Voc,
    /// Berkeley, IRCAM, and CARL sound, `.ircam`.
    Ircam,
    /// NIST SPHERE speech recordings: `.nist` and `.sph`.
    Nist,
    /// Atari's Audio Visual Research, `.avr`.
    Avr,
    /// Macintosh HCOM.
    Hcom,
    /// Portable Voice Format, `.pvf`.
    Pvf,
    /// Dolby Digital on its own, as `.ac3` files are.
    Ac3,
    /// Dolby Digital Plus on its own.
    Eac3,
    /// DTS on its own.
    Dts,
    /// Tracker music: `.mod`, `.xm`, `.s3m`, and `.it`, which ffmpeg plays
    /// through libopenmpt.
    Module,
}

impl Sound {
    fn format(self) -> &'static str {
        match self {
            Self::Mpeg => "mp3",
            Self::Adts => "aac",
            Self::Mp4 => "mov",
            Self::Flac => "flac",
            Self::Ogg => "ogg",
            Self::Wav => "wav",
            Self::Wave64 => "w64",
            Self::Aiff => "aiff",
            Self::Iff => "iff",
            Self::Dsf => "dsf",
            Self::Ape => "ape",
            Self::WavPack => "wv",
            Self::Tta => "tta",
            Self::Au => "au",
            Self::Caf => "caf",
            Self::Amr => "amr",
            Self::RealAudio => "rm",
            Self::Voc => "voc",
            Self::Ircam => "ircam",
            Self::Nist => "nistsphere",
            Self::Avr => "avr",
            Self::Hcom => "hcom",
            Self::Pvf => "pvf",
            Self::Ac3 => "ac3",
            Self::Eac3 => "eac3",
            Self::Dts => "dts",
            Self::Module => "libopenmpt",
        }
    }
}

/// Older sound formats ffmpeg can't read. SoX reads each with only its own
/// reader and turns it into WAV, which ffmpeg converts as it does other
/// sound (decision 26). Formats without a header don't say their sample
/// rate, so SoX plays them at the rate it assumes for the format.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Sox {
    /// An audio CD's tracks as they are on the disc.
    Cdda,
    /// CVSD, the voice codec of military radios and some phones, without
    /// a header: `.cvsd` or `.cvs`, and `.cvu` for its unfiltered form.
    Cvsd,
    Cvu,
    /// CVSD with a header, from voice mail: `.dvms` or `.vms`.
    Dvms,
    /// 8-bit sound without a header: `.fssd` or `.sou`.
    Fssd,
    /// Grandstream phones' ringtones.
    Gsrt,
    /// 8-bit sound from Sounder and SoundTool, two DOS programs.
    Sndr,
    Sndt,
    /// Yamaha TX16W sampler sounds.
    Txw,
    /// Ensoniq PARIS recordings, `.paf` or `.fap`.
    Paris,
    /// Turtle Beach SampleVision samples, `.smp`.
    SampleVision,
    /// Psion organizers' voice notes, `.wve`.
    Psion,
    /// Speech recordings for the HTK speech recognition toolkit, `.htk`.
    Htk,
    /// Dialogic's telephone sound, as phone systems record calls, `.vox`.
    Vox,
    /// Sound Designer II, `.sd2`. A Mac keeps its sample rate in the
    /// file's resource fork, which an upload leaves behind, so it's read
    /// as what Pro Tools recorded most: 16-bit stereo at 44.1 kHz.
    Sd2,
}

impl Sox {
    /// What tells SoX the format, before the file.
    fn reader(self) -> &'static [&'static str] {
        match self {
            Self::Cdda => &["-t", "cdda"],
            Self::Cvsd => &["-t", "cvsd"],
            Self::Cvu => &["-t", "cvu"],
            Self::Dvms => &["-t", "dvms"],
            Self::Fssd => &["-t", "u8"],
            Self::Gsrt => &["-t", "gsrt"],
            Self::Sndr => &["-t", "sndr"],
            Self::Sndt => &["-t", "sndt"],
            Self::Txw => &["-t", "txw"],
            Self::Paris => &["-t", "paf"],
            Self::SampleVision => &["-t", "smp"],
            Self::Psion => &["-t", "wve"],
            Self::Htk => &["-t", "htk"],
            Self::Vox => &["-t", "vox"],
            Self::Sd2 => &[
                "-t",
                "raw",
                "-e",
                "signed-integer",
                "-b",
                "16",
                "-B",
                "-c",
                "2",
                "-r",
                "44100",
            ],
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

/// What ffprobe says about a sound file's tracks, or a video's.
#[derive(Debug, Default, PartialEq)]
struct Tracks {
    /// The first sound track.
    sound: Option<Track>,
    /// The first video track, not counting cover art.
    picture: Option<Picture>,
    /// Whether there's cover art: a picture kept with the sound.
    cover: bool,
    /// Whether there's video, not counting cover art.
    video: bool,
    seconds: Option<f64>,
}

/// A sound track.
#[derive(Debug, PartialEq)]
struct Track {
    codec: String,
    /// Samples a second.
    rate: u32,
    channels: u32,
}

/// A video track.
#[derive(Debug, PartialEq)]
struct Picture {
    codec: String,
    /// How its pixels are kept, such as `yuv420p`, which says how many bits
    /// each has.
    pixels: String,
    /// Whether it's HDR, as HLG or PQ.
    hdr: bool,
}

/// How a video's preview is made from it.
#[derive(Debug, PartialEq)]
struct Recipe {
    /// WebM, for VP8, VP9, and AV1, rather than MP4.
    webm: bool,
    /// Whether the picture is copied as it is, rather than converted.
    copy_picture: bool,
    /// Whether the sound is copied as it is, rather than converted.
    copy_sound: bool,
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
        // A newer worker may manage what an older one couldn't, including
        // previews of files whose thumbnail stays as it is.
        db.write(|tx| {
            tx.execute(
                "DELETE FROM thumbnails WHERE state != 'ready' AND worker < ?1",
                [WORKER],
            )?;
            tx.execute(
                "UPDATE thumbnails SET preview = NULL, preview_reason = NULL, worker = ?1
                 WHERE worker < ?1 AND preview IN ('none', 'failed')",
                [WORKER],
            )
        })
        .await?;
        let settings = db.read(read_settings).await?;
        let videos_waiting = db
            .read(|conn| {
                conn.prepare("SELECT hash, plan FROM thumbnails WHERE preview = 'waiting'")?
                    .query_map([], |row| {
                        let plan = match row.get_ref(1)?.as_str_or_null()? {
                            Some("convert") => Plan::Convert,
                            _ => Plan::Copy,
                        };
                        Ok((hash(row, 0)?, plan))
                    })?
                    .collect::<Result<HashMap<_, _>, _>>()
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
                videos_waiting: std::sync::Mutex::new(videos_waiting),
                conversion: std::sync::Mutex::new(
                    settings
                        .convert_videos
                        .unwrap_or_else(VideoConversion::automatic),
                ),
                videos_wake: tokio::sync::Notify::new(),
                watched: std::sync::Mutex::new(HashMap::new()),
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

    /// The sound the viewer plays of the contents with `hash`, as AAC in
    /// MP4, for a sound the browser can't play itself, if there is one yet.
    pub fn sound(&self, hash: ContentHash) -> Option<PathBuf> {
        Some(self.sound_path(hash)).filter(|path| path.is_file())
    }

    /// An e-book's details for the viewer, as JSON: its `title`, `authors`,
    /// `description`, and `contents`, a list of entries with a `title` and
    /// a `depth`, if there are any yet.
    pub fn book(&self, hash: ContentHash) -> Option<PathBuf> {
        Some(self.book_path(hash)).filter(|path| path.is_file())
    }

    /// What the worker read about the contents with `hash`, or `None` if
    /// it hasn't read them yet.
    pub async fn facts(&self, hash: ContentHash) -> Result<Option<Facts>, Error> {
        self.inner
            .db
            .read(move |conn| {
                conn.query_row(
                    "SELECT width, height, seconds, pages, more_pages FROM thumbnails
                     WHERE hash = ?1 AND facts IS NOT NULL",
                    [&hash.0[..]],
                    |row| {
                        Ok(Facts {
                            width: row.get(0)?,
                            height: row.get(1)?,
                            seconds: row.get(2)?,
                            pages: row.get(3)?,
                            more_pages: row.get(4)?,
                        })
                    },
                )
                .optional()
                .map_err(Error::from)
            })
            .await
    }

    fn book_path(&self, hash: ContentHash) -> PathBuf {
        self.inner
            .previews
            .join(blobs::shard(hash))
            .with_extension("json")
    }

    fn sound_path(&self, hash: ContentHash) -> PathBuf {
        self.inner
            .previews
            .join(blobs::shard(hash))
            .with_extension("m4a")
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
        // Videos take minutes, so they have a worker of their own, and
        // thumbnails don't wait behind them.
        tokio::select! {
            () = &mut stopped => {}
            () = self.pictures() => {}
            () = self.videos() => {}
        }
    }

    /// Makes thumbnails and the quick previews, forever.
    async fn pictures(&self) {
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
                let outcome = self.make(*hash).await;
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
                let outcome = self.make_preview(*hash).await;
                let result = self.record_preview(*hash, outcome).await;
                self.note(result.err());
            }
            // Facts come last. A document's pages are counted in its
            // preview, which its thumbnail makes.
            let facts = if waiting.is_empty() && previews.is_empty() {
                match self.facts_waiting(BATCH).await {
                    Ok(facts) => facts,
                    Err(err) => {
                        self.note(Some(err));
                        Vec::new()
                    }
                }
            } else {
                Vec::new()
            };
            for hash in &facts {
                let read = self.read_facts(*hash).await;
                let result = self.record_facts(*hash, read).await;
                self.note(result.err());
            }
            let result = self.count().await;
            self.note(result.err());
            if waiting.is_empty() && previews.is_empty() && facts.is_empty() {
                tokio::select! {
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

    /// Reads the facts of the contents with `hash` now, whether or not
    /// they're waiting, and records them. A document's pages need its
    /// thumbnail made first.
    pub async fn read_facts_now(&self, hash: ContentHash) -> Result<Facts, Error> {
        let facts = self.read_facts(hash).await;
        self.record_facts(hash, facts.clone()).await?;
        Ok(facts)
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

    /// Records whether the contents with `hash` have a preview.
    async fn record_preview(&self, hash: ContentHash, outcome: Outcome) -> Result<(), Error> {
        let (state, reason, plan) = match outcome {
            // The worker has made it, so its file says which it is.
            Outcome::Ready if self.sound(hash).is_some() => ("audio", None, None),
            Outcome::Ready if self.book(hash).is_some() => ("book", None, None),
            Outcome::Ready => ("picture", None, None),
            Outcome::Waiting(plan) => ("waiting", None, Some(plan)),
            Outcome::None(reason) => ("none", Some(reason.to_owned()), None),
            Outcome::Failed(problem) => {
                tracing::info!(contents = %hash, %problem, "couldn't make a preview");
                ("failed", Some(problem), None)
            }
        };
        self.inner
            .db
            .write(move |tx| {
                tx.execute(
                    "UPDATE thumbnails SET preview = ?2, preview_reason = ?3, plan = ?4,
                         wanted_at = NULL, removed = 0
                     WHERE hash = ?1",
                    (&hash.0[..], state, reason, plan.map(Plan::as_str)),
                )?;
                // Open lists load the items again, which now have a preview,
                // or say a video's is coming.
                if state != "none" && state != "failed" {
                    hint_folders(tx, hash)?;
                }
                Ok::<_, rusqlite::Error>(())
            })
            .await?;
        if let Some(plan) = plan {
            self.lock_waiting().insert(hash, plan);
            self.inner.videos_wake.notify_one();
        }
        Ok(())
    }

    fn lock_waiting(&self) -> std::sync::MutexGuard<'_, HashMap<ContentHash, Plan>> {
        self.inner.videos_waiting.lock().expect("never poisoned")
    }

    fn conversion(&self) -> VideoConversion {
        *self.inner.conversion.lock().expect("never poisoned")
    }

    /// Whether the video with `hash` is waiting for a preview the server
    /// will make: in turn, or once someone opens it. A video waiting to be
    /// converted on a server that never converts isn't.
    pub fn video_waiting(&self, hash: ContentHash) -> bool {
        match self.lock_waiting().get(&hash) {
            Some(Plan::Copy) => true,
            Some(Plan::Convert) => self.conversion() != VideoConversion::Never,
            None => false,
        }
    }

    /// Notes that someone opened the video with `hash`, which is waiting for
    /// its preview, so the worker makes it next. Returns whether the server
    /// will make it.
    pub async fn want(&self, hash: ContentHash) -> Result<bool, Error> {
        if !self.video_waiting(hash) {
            return Ok(false);
        }
        self.inner
            .db
            .write(move |tx| {
                tx.execute(
                    concat!(
                        "UPDATE thumbnails SET wanted_at = ",
                        sql_now!(),
                        " WHERE hash = ?1 AND preview = 'waiting' AND wanted_at IS NULL"
                    ),
                    [&hash.0[..]],
                )
            })
            .await?;
        self.inner.videos_wake.notify_one();
        Ok(true)
    }

    /// Notes that someone is watching the preview of the video with `hash`,
    /// so the cache limit removes it after ones watched less recently.
    pub async fn watched(&self, hash: ContentHash) -> Result<(), Error> {
        {
            let mut watched = self.inner.watched.lock().expect("never poisoned");
            let now = std::time::Instant::now();
            if watched
                .get(&hash)
                .is_some_and(|at| now.duration_since(*at) < OPENED_EVERY)
            {
                return Ok(());
            }
            watched.insert(hash, now);
        }
        self.inner
            .db
            .write(move |tx| {
                tx.execute(
                    concat!(
                        "UPDATE thumbnails SET opened_at = ",
                        sql_now!(),
                        " WHERE hash = ?1"
                    ),
                    [&hash.0[..]],
                )
            })
            .await?;
        Ok(())
    }

    /// The admin's choices for previews.
    pub async fn settings(&self) -> Result<PreviewSettings, Error> {
        Ok(self.inner.db.read(read_settings).await?)
    }

    /// Changes the admin's choices for previews. Returns whether they
    /// changed. Previews over a smaller limit go now.
    pub async fn set_settings(&self, settings: PreviewSettings) -> Result<bool, Error> {
        let changed = self
            .inner
            .db
            .write(move |tx| {
                tx.execute(
                    "UPDATE thumbnail_settings SET convert_videos = ?1, space_percent = ?2
                     WHERE convert_videos IS NOT ?1 OR space_percent != ?2",
                    (
                        settings.convert_videos.map(VideoConversion::as_str),
                        settings.space_percent.clamp(1, 50),
                    ),
                )
            })
            .await?
            > 0;
        *self.inner.conversion.lock().expect("never poisoned") = settings
            .convert_videos
            .unwrap_or_else(VideoConversion::automatic);
        self.inner.videos_wake.notify_one();
        if changed {
            self.limit_space(None).await?;
        }
        Ok(changed)
    }

    /// How much room previews take, and may take.
    pub async fn space(&self) -> Result<PreviewSpace, Error> {
        let settings = self.settings().await?;
        let previews = self.inner.previews.clone();
        let (used, disk) = tokio::task::spawn_blocking(move || {
            Ok::<_, io::Error>((folder_size(&previews)?, disk_size(&previews)?))
        })
        .await
        .map_err(io::Error::other)??;
        Ok(PreviewSpace {
            used,
            limit: disk / 100 * u64::from(settings.space_percent),
            disk,
        })
    }

    /// Removes the least recently watched videos' previews while previews
    /// take more than their limit, except `keep`'s, which was just made.
    /// They're made again when someone opens them. Pictures' and sounds'
    /// previews count, but stay, since they're small.
    async fn limit_space(&self, keep: Option<ContentHash>) -> Result<(), Error> {
        let space = self.space().await?;
        if space.used <= space.limit {
            return Ok(());
        }
        let videos = self
            .inner
            .db
            .read(|conn| {
                conn.prepare(
                    "SELECT hash, plan FROM thumbnails WHERE preview = 'video'
                     ORDER BY coalesce(opened_at, made_at)",
                )?
                .query_map([], |row| {
                    let plan = match row.get_ref(1)?.as_str_or_null()? {
                        Some("convert") => Plan::Convert,
                        _ => Plan::Copy,
                    };
                    Ok((hash(row, 0)?, plan))
                })?
                .collect::<Result<Vec<_>, _>>()
                .map_err(Error::from)
            })
            .await?;
        let plans: HashMap<ContentHash, Plan> = videos.iter().copied().collect();
        let sizes: Vec<(ContentHash, u64)> = videos
            .iter()
            .filter_map(|&(hash, _)| {
                let (path, _) = self.video(hash)?;
                Some((hash, std::fs::metadata(path).ok()?.len()))
            })
            .collect();
        let remove = over_limit(&sizes, space.used, space.limit, keep);
        if remove.is_empty() {
            return Ok(());
        }
        tracing::info!(
            count = remove.len(),
            "removing videos' previews over the limit"
        );
        for hash in &remove {
            if let Some((path, _)) = self.video(*hash) {
                std::fs::remove_file(path)?;
            }
        }
        let removed = remove.clone();
        self.inner
            .db
            .write(move |tx| {
                let mut update = tx.prepare(
                    "UPDATE thumbnails SET preview = 'waiting', removed = 1, wanted_at = NULL
                     WHERE hash = ?1",
                )?;
                for hash in &removed {
                    update.execute([&hash.0[..]])?;
                    hint_folders(tx, *hash)?;
                }
                Ok::<_, rusqlite::Error>(())
            })
            .await?;
        let mut waiting = self.lock_waiting();
        for hash in remove {
            waiting.insert(hash, plans[&hash]);
        }
        Ok(())
    }

    /// Makes videos' previews, one at a time, forever: those someone
    /// opened first, then, as the setting says, the rest.
    async fn videos(&self) {
        loop {
            // Before looking, so a wake after the look isn't missed.
            let woken = self.inner.videos_wake.notified();
            let mut woken = std::pin::pin!(woken);
            woken.as_mut().enable();
            match self.next_video().await {
                Ok(Some((hash, plan))) => {
                    let outcome = self.make_video(hash, plan).await;
                    let made = outcome == Outcome::Ready;
                    let result = self.record_video(hash, outcome).await;
                    self.note(result.err());
                    if made {
                        let result = self.limit_space(Some(hash)).await;
                        self.note(result.err());
                    }
                }
                Ok(None) => {
                    tokio::select! {
                        () = woken => {}
                        () = tokio::time::sleep(TIDY_EVERY) => {}
                    }
                }
                Err(err) => {
                    self.note(Some(err));
                    tokio::time::sleep(Duration::from_secs(60)).await;
                }
            }
        }
    }

    /// The next video to make a preview of: one someone opened, newest
    /// first, then, after upload, one to repackage, then one to convert.
    /// Previews the cache limit removed are made again only when someone
    /// opens them, and so are new ones while previews are at their limit.
    async fn next_video(&self) -> Result<Option<(ContentHash, Plan)>, Error> {
        let conversion = self.conversion();
        let upload = conversion == VideoConversion::Upload;
        let converts = conversion != VideoConversion::Never;
        // Once previews fill their space, a new one would only push out
        // another, so only videos someone opens are made.
        let room = match self.space().await {
            Ok(space) => space.used < space.limit,
            Err(err) => {
                tracing::warn!(%err, "couldn't measure previews' space");
                true
            }
        };
        self.inner
            .db
            .read(move |conn| {
                conn.query_row(
                    "SELECT hash, plan FROM thumbnails
                     WHERE preview = 'waiting'
                       AND (plan = 'copy' OR ?2)
                       AND (wanted_at IS NOT NULL
                            OR (?3 AND removed = 0 AND (plan = 'copy' OR ?1)))
                     ORDER BY wanted_at IS NULL, wanted_at DESC, plan = 'convert', made_at DESC
                     LIMIT 1",
                    (upload, converts, room),
                    |row| {
                        let plan = match row.get_ref(1)?.as_str_or_null()? {
                            Some("convert") => Plan::Convert,
                            _ => Plan::Copy,
                        };
                        Ok((hash(row, 0)?, plan))
                    },
                )
                .optional()
                .map_err(Error::from)
            })
            .await
    }

    /// Records how a video's preview went.
    async fn record_video(&self, hash: ContentHash, outcome: Outcome) -> Result<(), Error> {
        let (state, reason, plan) = match outcome {
            Outcome::Ready => ("video", None, None),
            // Repackaging failed, so it's converted instead, as the setting
            // says.
            Outcome::Waiting(plan) => ("waiting", None, Some(plan)),
            Outcome::None(reason) => ("none", Some(reason.to_owned()), None),
            Outcome::Failed(problem) => {
                tracing::info!(contents = %hash, %problem, "couldn't make a video's preview");
                ("failed", Some(problem), None)
            }
        };
        self.inner
            .db
            .write(move |tx| {
                tx.execute(
                    concat!(
                        "UPDATE thumbnails SET preview = ?2, preview_reason = ?3,
                             plan = coalesce(?4, plan), removed = 0,
                             wanted_at = CASE WHEN ?2 = 'waiting' THEN wanted_at END,
                             opened_at = ",
                        sql_now!(),
                        " WHERE hash = ?1 AND preview = 'waiting'"
                    ),
                    (&hash.0[..], state, reason, plan.map(Plan::as_str)),
                )?;
                hint_folders(tx, hash)
            })
            .await?;
        let mut waiting = self.lock_waiting();
        match plan {
            Some(plan) => {
                waiting.insert(hash, plan);
            }
            None => {
                waiting.remove(&hash);
            }
        }
        Ok(())
    }

    /// Makes the preview of the video with `hash` as `plan` says: its
    /// picture copied if browsers play it, or converted.
    async fn make_video(&self, hash: ContentHash, plan: Plan) -> Outcome {
        let file = self.inner.storage.contents_file(hash);
        let (container, size) = match self.identify(&file, hash).await {
            Ok((Some(Kind::Video(container)), size, _)) => (container, size),
            Ok(_) => return Outcome::None("not a video"),
            Err(problem) => return Outcome::Failed(problem),
        };
        if size > MAX_VIDEO {
            return Outcome::None("a video over 4 GB");
        }
        let decoders = format!("{VIDEO_DECODERS},{SOUND_DECODERS},{COVER_DECODERS}");
        let tracks = match self.tracks(&file, container.format(), &decoders).await {
            Ok(tracks) => tracks,
            Err(problem) => return Outcome::Failed(problem),
        };
        let name = match self.name(hash).await {
            Ok(name) => name.unwrap_or_default(),
            Err(err) => return Outcome::Failed(format!("couldn't read the file's name: {err}")),
        };
        let Some(mut recipe) = recipe(&name, container, &tracks) else {
            return Outcome::None("the browser plays it");
        };
        if plan == Plan::Convert {
            recipe = Recipe {
                webm: false,
                copy_picture: false,
                copy_sound: recipe.copy_sound && !recipe.webm,
            };
        }
        let scratch = match self.scratch_dir() {
            Ok(scratch) => scratch,
            Err(problem) => return Outcome::Failed(problem),
        };
        let made = scratch.path().join("video");
        let args = video_args(
            &file,
            container.format(),
            &decoders,
            &tracks,
            &recipe,
            &made,
        );
        let limits = if recipe.copy_picture {
            COPY_LIMITS
        } else {
            CONVERT_LIMITS
        };
        let input = match std::fs::File::open("/dev/null") {
            Ok(input) => input,
            Err(err) => return Outcome::Failed(err.to_string()),
        };
        let converted = self
            .inner
            .sandbox
            .run(Job {
                program: &self.inner.tools.ffmpeg,
                args: &args,
                // glibc reserves memory for each thread's own heap, which
                // counts toward the limit, unless it shares a few.
                env: &[("MALLOC_ARENA_MAX", "2")],
                input,
                read: std::slice::from_ref(&file),
                write: &[scratch.path().to_owned()],
                sockets: None,
                limits,
            })
            .await
            .map_err(|err| err.to_string())
            .and_then(|_| {
                // The worker could only make folders and plain files there.
                made.symlink_metadata()
                    .is_ok_and(|found| found.is_file() && found.len() > 0)
                    .then_some(())
                    .ok_or_else(|| "ffmpeg didn't make the video".to_owned())
            });
        match converted {
            Ok(()) => {
                let path = self.video_path(hash, recipe.webm);
                let kept =
                    std::fs::create_dir_all(path.parent().expect("a preview is in a folder"))
                        .and_then(|()| std::fs::rename(&made, &path));
                match kept {
                    Ok(()) => Outcome::Ready,
                    Err(err) => Outcome::Failed(format!("couldn't keep the preview: {err}")),
                }
            }
            // Some files' pictures can't be copied as they are, such as
            // H.264 in an older AVI, so they're converted instead.
            Err(problem) if recipe.copy_picture => {
                tracing::info!(contents = %hash, %problem, "couldn't repackage a video");
                Outcome::Waiting(Plan::Convert)
            }
            Err(problem) => Outcome::Failed(problem),
        }
    }

    /// The preview the viewer plays of the video with `hash`, if there is
    /// one yet, and its content type.
    pub fn video(&self, hash: ContentHash) -> Option<(PathBuf, &'static str)> {
        [(false, "video/mp4"), (true, "video/webm")]
            .into_iter()
            .map(|(webm, kind)| (self.video_path(hash, webm), kind))
            .find(|(path, _)| path.is_file())
    }

    fn video_path(&self, hash: ContentHash, webm: bool) -> PathBuf {
        self.inner
            .previews
            .join(blobs::shard(hash))
            .with_extension(if webm { "webm" } else { "mp4" })
    }

    /// Contents whose preview isn't decided yet, newest first: those with a
    /// thumbnail, and those without one, since a song without cover art
    /// may still need its sound converted.
    async fn previews_waiting(&self, limit: usize) -> Result<Vec<ContentHash>, Error> {
        self.inner
            .db
            .read(move |conn| {
                conn.prepare(
                    "SELECT t.hash FROM thumbnails t
                     WHERE t.state IN ('ready', 'none') AND t.preview IS NULL
                     ORDER BY t.made_at DESC
                     LIMIT ?1",
                )?
                .query_map([limit as i64], |row| hash(row, 0))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(Error::from)
            })
            .await
    }

    /// The newest contents with a thumbnail decided whose facts haven't
    /// been read, or were read by an older [`FACTS`].
    async fn facts_waiting(&self, limit: usize) -> Result<Vec<ContentHash>, Error> {
        self.inner
            .db
            .read(move |conn| {
                conn.prepare(
                    "SELECT hash FROM thumbnails
                     WHERE facts IS NULL OR facts < ?2
                     ORDER BY made_at DESC
                     LIMIT ?1",
                )?
                .query_map((limit as i64, FACTS), |row| hash(row, 0))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(Error::from)
            })
            .await
    }

    /// Keeps what the worker read about the contents with `hash`.
    async fn record_facts(&self, hash: ContentHash, facts: Facts) -> Result<(), Error> {
        let found = facts != Facts::default();
        self.inner
            .db
            .write(move |tx| {
                // Open lists load the items again, so a details panel that's
                // open shows them.
                if found {
                    hint_folders(tx, hash)?;
                }
                tx.execute(
                    "UPDATE thumbnails SET facts = ?2, width = ?3, height = ?4, seconds = ?5,
                         pages = ?6, more_pages = ?7
                     WHERE hash = ?1",
                    (
                        &hash.0[..],
                        FACTS,
                        facts.width,
                        facts.height,
                        facts.seconds,
                        facts.pages,
                        facts.more_pages,
                    ),
                )
            })
            .await?;
        Ok(())
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
            Outcome::Waiting(_) => unreachable!("only previews wait"),
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
                             preview = NULL, preview_reason = NULL, plan = NULL,
                             wanted_at = NULL, removed = 0, facts = NULL"
                    ),
                    (&hash.0[..], state, WORKER, reason),
                )?;
                if state == "ready" {
                    hint_folders(tx, hash)?;
                }
                Ok::<_, rusqlite::Error>(())
            })
            .await?;
        // Its preview is decided again.
        self.lock_waiting().remove(&hash);
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
            (tools.has_sox(), NO_SOX),
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
        let previews = self
            .inner
            .db
            .read(|conn| {
                conn.prepare(
                    "SELECT hash, preview FROM thumbnails
                     WHERE preview IN ('picture', 'audio', 'video', 'book')",
                )?
                .query_map([], |row| Ok((hash(row, 0)?, row.get::<_, String>(1)?)))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(Error::from)
            })
            .await?;
        let lost: Vec<(ContentHash, bool)> = previews
            .into_iter()
            .filter(|(hash, preview)| match preview.as_str() {
                "audio" => self.sound(*hash).is_none(),
                "video" => self.video(*hash).is_none(),
                "book" => self.book(*hash).is_none(),
                _ => self.picture(*hash).is_none(),
            })
            .map(|(hash, preview)| (hash, preview == "video"))
            .collect();
        if !lost.is_empty() {
            let videos: Vec<ContentHash> = lost
                .iter()
                .filter(|(_, video)| *video)
                .map(|(hash, _)| *hash)
                .collect();
            self.inner
                .db
                .write(move |tx| {
                    // A video's is made again when someone opens it, as one
                    // the cache limit removed is.
                    let mut reset = tx.prepare(
                        "UPDATE thumbnails
                         SET preview = CASE WHEN ?2 THEN 'waiting' END,
                             removed = ?2, wanted_at = NULL
                         WHERE hash = ?1",
                    )?;
                    for (hash, video) in &lost {
                        reset.execute((&hash.0[..], video))?;
                    }
                    Ok::<_, rusqlite::Error>(())
                })
                .await?;
            let plans = self
                .inner
                .db
                .read(move |conn| {
                    let mut plan = conn.prepare("SELECT plan FROM thumbnails WHERE hash = ?1")?;
                    videos
                        .iter()
                        .map(|hash| {
                            let convert = plan
                                .query_row([&hash.0[..]], |row| {
                                    Ok(row.get_ref(0)?.as_str_or_null()? == Some("convert"))
                                })
                                .optional()?
                                .unwrap_or(false);
                            Ok((*hash, if convert { Plan::Convert } else { Plan::Copy }))
                        })
                        .collect::<Result<Vec<_>, rusqlite::Error>>()
                })
                .await?;
            self.lock_waiting().extend(plans);
        }
        self.limit_space(None).await?;

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
            self.lock_waiting().remove(&hash);
            let paths = [
                self.path(hash, ThumbnailKind::Picture),
                self.path(hash, ThumbnailKind::Text),
                self.preview_path(hash, false),
                self.preview_path(hash, true),
                self.picture_path(hash),
                self.sound_path(hash),
                self.book_path(hash),
                self.video_path(hash, false),
                self.video_path(hash, true),
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
        // A JPEG may be the first frame of a Motion JPEG stream.
        let by_name = kind.is_none()
            || (kind == Some(Kind::Image) && (tiff(&head) || head.starts_with(b"\xff\xd8")))
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
            // ffmpeg reads a video's index and a few frames, not all of it,
            // and only a song's tags.
            Some(kind)
                if size > MAX_FILE
                    && !matches!(
                        kind,
                        Kind::Video(_) | Kind::Audio(_) | Kind::Sox(_) | Kind::Document(_)
                    ) =>
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
            Kind::Video(container) => match self.video_frame(&file, container).await {
                Ok(Some(frame)) => self.thumbnail(Ok(frame)).await,
                Ok(None) => return Outcome::None(NO_COVER),
                Err(problem) => Err(problem),
            },
            Kind::Audio(_) if !self.inner.tools.has_ffmpeg() => return Outcome::None(NO_FFMPEG),
            Kind::Audio(sound) => match self.cover(&file, sound.format()).await {
                Ok(Some(png)) => {
                    self.thumbnail(self.scratch(&png).map_err(io::Error::other))
                        .await
                }
                Ok(None) => return Outcome::None(NO_COVER),
                Err(problem) => Err(problem),
            },
            // The reasons say what's missing, so these get another try once
            // it's installed, and the preview with them.
            Kind::Sox(_) if !self.inner.tools.has_sox() => return Outcome::None(NO_SOX),
            Kind::Sox(_) if !self.inner.tools.has_ffmpeg() => return Outcome::None(NO_FFMPEG),
            Kind::Sox(_) => return Outcome::None(NO_COVER),
            Kind::Document(_) if !self.inner.tools.has_office() => return Outcome::None(NO_OFFICE),
            Kind::Document(format) => self
                .document(hash, &file, format)
                .await
                .map_err(|problem| format!("{DOCUMENT_FAILED}{problem}")),
            Kind::Font(_) if !self.inner.tools.has_ffmpeg() => return Outcome::None(NO_FFMPEG),
            Kind::Font(_) => match self.font_sample(&file, FontPage::Thumbnail).await {
                Ok(png) => {
                    self.thumbnail(self.scratch(&png).map_err(io::Error::other))
                        .await
                }
                Err(problem) => Err(problem),
            },
            Kind::Book(format) => match self.read_book(&file, format, "cover").await {
                Ok(cover) if cover.is_empty() => return Outcome::None(NO_BOOK_COVER),
                Ok(cover) => {
                    self.thumbnail(self.scratch(&cover).map_err(io::Error::other))
                        .await
                }
                Err(problem) => Err(problem),
            },
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
        let (kind, size, head) = match self.identify(&file, hash).await {
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
            Some(Kind::Audio(_) | Kind::Sox(_) | Kind::Video(_))
                if !self.inner.tools.has_ffmpeg() =>
            {
                return Outcome::None(NO_FFMPEG);
            }
            Some(Kind::Audio(sound)) => {
                let decoders = format!("{SOUND_DECODERS},{COVER_DECODERS}");
                return self.listen(hash, &file, sound.format(), &decoders).await;
            }
            Some(Kind::Sox(_)) if !self.inner.tools.has_sox() => return Outcome::None(NO_SOX),
            Some(Kind::Sox(format)) => return self.old_sound(hash, &file, format).await,
            Some(Kind::Video(container)) => {
                return self.plan_video(hash, &file, container, size).await;
            }
            Some(Kind::Book(format)) => return self.book_details(hash, &file, format).await,
            Some(Kind::Font(font)) if font.browser_draws() => {
                return Outcome::None("the browser draws it");
            }
            Some(Kind::Font(_)) if !self.inner.tools.has_ffmpeg() => {
                return Outcome::None(NO_FFMPEG);
            }
            Some(Kind::Font(_)) => match self.font_sample(&file, FontPage::Preview).await {
                Ok(png) => {
                    self.shrink(self.scratch(&png).map_err(io::Error::other), PREVIEW)
                        .await
                }
                Err(problem) => Err(problem),
            },
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

    /// Reads what the details panel shows about the contents with `hash`.
    /// What the worker can't read stays `None`, and isn't tried again
    /// until [`FACTS`] changes or the thumbnail is made again.
    async fn read_facts(&self, hash: ContentHash) -> Facts {
        let file = self.inner.storage.contents_file(hash);
        let kind = match self.identify(&file, hash).await {
            Ok((kind, _, _)) => kind,
            Err(problem) => {
                tracing::debug!(contents = %hash, %problem, "couldn't read a file's facts");
                return Facts::default();
            }
        };
        let ffmpeg = self.inner.tools.has_ffmpeg();
        let read = match kind {
            Some(Kind::Image | Kind::Heif) => self.picture_size(&file).await,
            Some(Kind::Still(still)) if ffmpeg => {
                let (reader, decoders) = still.reader();
                self.stream_facts(&file, reader, decoders, None).await
            }
            Some(Kind::Video(container)) if ffmpeg => {
                let decoders = format!("{VIDEO_DECODERS},{AUDIO_DECODERS}");
                let reader = ["-f", container.format()];
                self.stream_facts(&file, &reader, &decoders, container.ticks_per_frame())
                    .await
            }
            Some(Kind::Audio(sound)) if ffmpeg => {
                let decoders = format!("{SOUND_DECODERS},{COVER_DECODERS}");
                self.tracks(&file, sound.format(), &decoders)
                    .await
                    .map(|tracks| Facts {
                        seconds: tracks.seconds,
                        ..Facts::default()
                    })
            }
            Some(Kind::Pdf) => self.pages(&file).await.map(|pages| Facts {
                pages: Some(pages),
                ..Facts::default()
            }),
            // Only the preview is counted, since LibreOffice reads the
            // whole document to count its pages.
            Some(Kind::Document(_) | Kind::Djvu) => match self.preview(hash) {
                Some(preview) => self.pages(&preview.path).await.map(|pages| Facts {
                    pages: Some(pages),
                    more_pages: preview.partial,
                    ..Facts::default()
                }),
                None => Ok(Facts::default()),
            },
            _ => Ok(Facts::default()),
        };
        read.unwrap_or_else(|problem| {
            tracing::debug!(contents = %hash, %problem, "couldn't read a file's facts");
            Facts::default()
        })
    }

    /// A picture's width and height as it's shown, from its header. A photo
    /// whose orientation says to turn it a quarter has them swapped.
    async fn picture_size(&self, file: &Path) -> Result<Facts, String> {
        let args = ["-f", "width", "-f", "height", "-f", "orientation"]
            .map(OsString::from)
            .into_iter()
            .chain([file.as_os_str().to_owned()])
            .collect::<Vec<_>>();
        let input = std::fs::File::open("/dev/null").map_err(|err| err.to_string())?;
        let output = self
            .work(
                &self.inner.tools.vipsheader,
                &args,
                input,
                &[file.to_owned()],
            )
            .await?;
        picture_size(&output)
            .ok_or_else(|| "libvips didn't say how large the picture is".to_owned())
    }

    /// A video's or a picture's size as it's shown, and how long it lasts,
    /// from ffprobe reading it with only the `reader` and the `decoders`.
    /// With `ticks_per_frame`, ffprobe counts its frames too, for a video
    /// that doesn't say how long it lasts.
    async fn stream_facts(
        &self,
        file: &Path,
        reader: &[&str],
        decoders: &str,
        ticks_per_frame: Option<f64>,
    ) -> Result<Facts, String> {
        let entries = "stream=width,height,r_frame_rate,nb_read_packets\
            :stream_side_data=rotation:format=duration";
        let args = ["-v", "error", "-protocol_whitelist", "file"]
            .into_iter()
            .chain(reader.iter().copied())
            .chain(ticks_per_frame.map(|_| "-count_packets"))
            .chain([
                "-codec_whitelist",
                decoders,
                "-select_streams",
                "v:0",
                "-show_entries",
                entries,
                "-of",
                "json",
            ])
            .map(OsString::from)
            .chain([file.as_os_str().to_owned()])
            .collect::<Vec<_>>();
        let input = std::fs::File::open("/dev/null").map_err(|err| err.to_string())?;
        let limits = match ticks_per_frame {
            Some(_) => COUNT_LIMITS,
            None => LIMITS,
        };
        let output = self
            .work_within(
                &self.inner.tools.ffprobe,
                &args,
                input,
                &[file.to_owned()],
                limits,
            )
            .await?;
        stream_facts(&output, ticks_per_frame.unwrap_or(1.0))
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
    async fn video_frame(
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

    /// A song's cover art, shrunk, as a PNG, from ffmpeg reading it with only
    /// the `demuxer`. `None` if it has none.
    async fn cover(&self, file: &Path, demuxer: &str) -> Result<Option<Vec<u8>>, String> {
        let decoders = format!("{SOUND_DECODERS},{COVER_DECODERS}");
        if !self.tracks(file, demuxer, &decoders).await?.cover {
            return Ok(None);
        }
        let png = self
            .frame(file, &["-f", demuxer], &decoders, 0.0, false, THUMBNAIL)
            .await?;
        if png.is_empty() {
            return Err("ffmpeg found no picture in the cover art".into());
        }
        Ok(Some(png))
    }

    /// A file's tracks, from ffprobe reading it with only the `demuxer` and
    /// the `decoders`.
    async fn tracks(&self, file: &Path, demuxer: &str, decoders: &str) -> Result<Tracks, String> {
        let args = [
            "-v",
            "error",
            "-protocol_whitelist",
            "file",
            "-f",
            demuxer,
            "-codec_whitelist",
            decoders,
            "-show_entries",
            "stream=codec_type,codec_name,sample_rate,channels,pix_fmt,color_transfer:\
             stream_disposition=attached_pic:format=duration",
            "-of",
            "json",
        ]
        .map(OsString::from);
        let args: Vec<OsString> = args.into_iter().chain([file.into()]).collect();
        let input = std::fs::File::open("/dev/null").map_err(|err| err.to_string())?;
        let output = self
            .work(&self.inner.tools.ffprobe, &args, input, &[file.to_owned()])
            .await?;
        tracks(&output)
    }

    /// Converts a sound the browser can't play to AAC in MP4, which every
    /// browser plays, and keeps it as its preview. ffmpeg reads the file
    /// with only the `demuxer` and the `decoders`.
    async fn listen(
        &self,
        hash: ContentHash,
        file: &Path,
        demuxer: &str,
        decoders: &str,
    ) -> Outcome {
        let tracks = match self.tracks(file, demuxer, decoders).await {
            Ok(tracks) => tracks,
            Err(problem) => return Outcome::Failed(problem),
        };
        if tracks.video {
            return Outcome::None("a video");
        }
        let Some(track) = tracks.sound else {
            return Outcome::None("a file without sound");
        };
        let name = match self.name(hash).await {
            Ok(name) => name.unwrap_or_default(),
            Err(err) => return Outcome::Failed(format!("couldn't read the file's name: {err}")),
        };
        if plays(&name, &track.codec) {
            return Outcome::None("the browser plays it");
        }
        self.keep_sound(hash, file, demuxer, decoders, &track, tracks.seconds)
            .await
    }

    /// Decides how a video's preview is made, which the video worker then
    /// makes in turn. Videos with only sound, such as Windows Media songs,
    /// have it converted now, as other sound is.
    async fn plan_video(
        &self,
        hash: ContentHash,
        file: &Path,
        container: Container,
        size: u64,
    ) -> Outcome {
        let decoders = format!("{VIDEO_DECODERS},{SOUND_DECODERS},{COVER_DECODERS}");
        let tracks = match self.tracks(file, container.format(), &decoders).await {
            Ok(tracks) => tracks,
            Err(problem) => return Outcome::Failed(problem),
        };
        if tracks.picture.is_none() {
            return self.listen(hash, file, container.format(), &decoders).await;
        }
        if size > MAX_VIDEO {
            return Outcome::None("a video over 4 GB");
        }
        let name = match self.name(hash).await {
            Ok(name) => name.unwrap_or_default(),
            Err(err) => return Outcome::Failed(format!("couldn't read the file's name: {err}")),
        };
        match recipe(&name, container, &tracks) {
            None => Outcome::None("the browser plays it"),
            Some(recipe) if recipe.copy_picture => Outcome::Waiting(Plan::Copy),
            Some(_) => Outcome::Waiting(Plan::Convert),
        }
    }

    /// Converts a sound only SoX reads. SoX turns it into WAV in a scratch
    /// folder, which ffmpeg converts as it does other sound. SoX stops a
    /// second past the longest sound that gets a preview, so a longer one
    /// takes no more room than that, and ffmpeg then finds it too long.
    async fn old_sound(&self, hash: ContentHash, file: &Path, format: Sox) -> Outcome {
        let scratch = match self.scratch_dir() {
            Ok(scratch) => scratch,
            Err(problem) => return Outcome::Failed(problem),
        };
        let wav = scratch.path().join("sound.wav");
        let length = (MAX_SOUND_SECONDS + 1.0).to_string();
        let args: Vec<OsString> = ["-V1", "--no-glob", "--single-threaded"]
            .into_iter()
            .chain(format.reader().iter().copied())
            .map(OsString::from)
            .chain([file.into()])
            .chain(["-t", "wav", "-e", "signed-integer", "-b", "16"].map(OsString::from))
            .chain([wav.clone().into()])
            .chain(["trim", "0", &length].map(OsString::from))
            .collect();
        let input = match std::fs::File::open("/dev/null") {
            Ok(input) => input,
            Err(err) => return Outcome::Failed(err.to_string()),
        };
        let read = self
            .inner
            .sandbox
            .run(Job {
                program: &self.inner.tools.sox,
                args: &args,
                env: &[],
                input,
                read: &[file.to_owned()],
                write: &[scratch.path().to_owned()],
                sockets: None,
                limits: SOUND_LIMITS,
            })
            .await;
        if let Err(err) = read {
            return Outcome::Failed(err.to_string());
        }
        // The worker could only make folders and plain files there.
        if !wav
            .symlink_metadata()
            .is_ok_and(|found| found.is_file() && found.len() > 0)
        {
            return Outcome::Failed("SoX didn't read the sound".into());
        }
        let tracks = match self.tracks(&wav, "wav", "pcm_s16le").await {
            Ok(tracks) => tracks,
            Err(problem) => return Outcome::Failed(problem),
        };
        let Some(track) = tracks.sound else {
            return Outcome::None("a file without sound");
        };
        self.keep_sound(hash, &wav, "wav", "pcm_s16le", &track, tracks.seconds)
            .await
    }

    /// Converts the sound to AAC and keeps it as the preview, unless it's
    /// longer than [`MAX_SOUND_SECONDS`].
    async fn keep_sound(
        &self,
        hash: ContentHash,
        file: &Path,
        demuxer: &str,
        decoders: &str,
        track: &Track,
        seconds: Option<f64>,
    ) -> Outcome {
        if seconds.is_some_and(|seconds| seconds > MAX_SOUND_SECONDS) {
            return Outcome::None("a sound over 3 hours");
        }
        match self.convert(file, demuxer, decoders, track).await {
            Ok(converted) => {
                let path = self.sound_path(hash);
                let kept =
                    std::fs::create_dir_all(path.parent().expect("a preview is in a folder"))
                        .and_then(|()| std::fs::rename(converted.path().join("sound.m4a"), &path));
                match kept {
                    Ok(()) => Outcome::Ready,
                    Err(err) => Outcome::Failed(format!("couldn't keep the preview: {err}")),
                }
            }
            Err(problem) => Outcome::Failed(problem),
        }
    }

    /// Runs ffmpeg to convert the sound, and returns the scratch folder
    /// with `sound.m4a` in it. MP4 keeps its index at the end unless it's
    /// moved to the start, which needs a file, not the worker's output.
    async fn convert(
        &self,
        file: &Path,
        demuxer: &str,
        decoders: &str,
        track: &Track,
    ) -> Result<tempfile::TempDir, String> {
        let scratch = self.scratch_dir()?;
        let sound = scratch.path().join("sound.m4a");
        let rate = rate(track.rate).to_string();
        let mut args: Vec<OsString> = [
            "-nostdin",
            "-v",
            "error",
            "-threads",
            "2",
            "-protocol_whitelist",
            "file",
            "-f",
            demuxer,
            "-codec_whitelist",
            decoders,
            "-i",
        ]
        .map(OsString::from)
        .into_iter()
        .chain([file.into()])
        .chain(["-map", "0:a:0", "-vn", "-sn", "-dn", "-map_metadata", "-1"].map(OsString::from))
        .collect();
        // Surround sound is mixed down to stereo, which 128 kbit/s suits.
        // The count is always given, since AAC can't take channels in an
        // order the file doesn't say, as some Speex files have.
        let channels = track.channels.clamp(1, 2).to_string();
        args.extend(
            [
                "-ac",
                &channels,
                "-ar",
                &rate,
                "-c:a",
                "aac",
                "-b:a",
                "128k",
                "-movflags",
                "+faststart",
                "-f",
                "mp4",
            ]
            .map(OsString::from),
        );
        args.push(sound.clone().into());
        let input = std::fs::File::open("/dev/null").map_err(|err| err.to_string())?;
        self.inner
            .sandbox
            .run(Job {
                program: &self.inner.tools.ffmpeg,
                args: &args,
                env: &[],
                input,
                read: &[file.to_owned()],
                write: &[scratch.path().to_owned()],
                sockets: None,
                limits: SOUND_LIMITS,
            })
            .await
            .map_err(|err| err.to_string())?;
        // The worker could only make folders and plain files there.
        if !sound
            .symlink_metadata()
            .is_ok_and(|found| found.is_file() && found.len() > 0)
        {
            return Err("ffmpeg didn't convert the sound".into());
        }
        Ok(scratch)
    }

    /// Keeps a worker's picture in a temporary file, for libvips to read.
    /// Sample text drawn in the font in `file`, as a PNG, from ffmpeg and
    /// FreeType in the sandbox.
    async fn font_sample(&self, file: &Path, page: FontPage) -> Result<Vec<u8>, String> {
        let font = filter_value(&file.to_string_lossy());
        let (size, lines): (&str, &[(&str, u32, &str, u32)]) = match page {
            FontPage::Thumbnail => (
                "480x360",
                &[
                    ("Aa", 160, "(w-text_w)/2", 50),
                    (PANGRAM_START, 32, "(w-text_w)/2", 260),
                ],
            ),
            FontPage::Preview => (
                "1920x1440",
                &[
                    ("Aa", 240, "80", 60),
                    ("ABCDEFGHIJKLMNOPQRSTUVWXYZ", 64, "80", 380),
                    ("abcdefghijklmnopqrstuvwxyz", 64, "80", 480),
                    ("0123456789 .!?&@()", 64, "80", 580),
                    (PANGRAM, 24, "80", 740),
                    (PANGRAM, 36, "80", 810),
                    (PANGRAM, 48, "80", 900),
                    (PANGRAM, 72, "80", 1010),
                    (PANGRAM, 96, "80", 1150),
                ],
            ),
        };
        let filters: Vec<String> = lines
            .iter()
            .enumerate()
            .map(|(index, (text, points, x, y))| {
                // The first line is darker, as the web app's text is.
                let color = if index == 0 { "0x1c2430" } else { "0x3c4552" };
                format!(
                    "drawtext=fontfile={font}:expansion=none:text={}:fontsize={points}:\
                     fontcolor={color}:x={x}:y={y}",
                    filter_value(text)
                )
            })
            .collect();
        let source = format!("color=c=white:s={size}:d=1");
        let filters = filters.join(",");
        let args = [
            "-nostdin",
            "-v",
            "error",
            "-threads",
            "2",
            "-f",
            "lavfi",
            "-i",
            &source,
            "-vf",
            &filters,
            "-frames:v",
            "1",
            "-c:v",
            "png",
            "-f",
            "image2pipe",
            "-",
        ]
        .map(OsString::from);
        let input = std::fs::File::open("/dev/null").map_err(|err| err.to_string())?;
        let limits = match page {
            FontPage::Thumbnail => LIMITS,
            FontPage::Preview => PREVIEW_LIMITS,
        };
        // ffmpeg draws in one of the system's fonts when it can't read the
        // file, so a broken font would show another's letters. Fontconfig,
        // which finds those, gets settings with none.
        let config = self.inner.dir.join("fonts.conf");
        if !config.is_file() {
            self.keep(config.clone(), NO_FONTS.as_bytes())
                .map_err(|err| format!("couldn't keep the font settings: {err}"))?;
        }
        let config_path = config
            .to_str()
            .ok_or("the data folder's path isn't UTF-8")?;
        let png = self
            .inner
            .sandbox
            .run(Job {
                program: &self.inner.tools.ffmpeg,
                args: &args,
                env: &[("FONTCONFIG_FILE", config_path), ("MALLOC_ARENA_MAX", "2")],
                input,
                read: &[file.to_owned(), config.clone()],
                write: &[],
                sockets: None,
                limits,
            })
            .await
            .map_err(|err| err.to_string())?;
        if png.is_empty() {
            return Err("ffmpeg drew nothing in the font".into());
        }
        Ok(png)
    }

    /// Runs the e-book reader in the sandbox, for the book's `cover` or its
    /// `details`.
    async fn read_book(
        &self,
        file: &Path,
        format: book::Format,
        part: &str,
    ) -> Result<Vec<u8>, String> {
        let input =
            std::fs::File::open(file).map_err(|err| format!("couldn't open the file: {err}"))?;
        let args = [book::COMMAND, format.as_str(), part].map(OsString::from);
        self.work_within(&self.inner.tools.launcher, &args, input, &[], BOOK_LIMITS)
            .await
    }

    /// Keeps an e-book's details, for the viewer.
    async fn book_details(&self, hash: ContentHash, file: &Path, format: book::Format) -> Outcome {
        let details = match self.read_book(file, format, "details").await {
            Ok(details) => details,
            Err(problem) => return Outcome::Failed(problem),
        };
        let empty = serde_json::from_slice::<serde_json::Value>(&details)
            .ok()
            .and_then(|details| {
                let object = details.as_object()?;
                Some(object.values().all(|value| match value {
                    serde_json::Value::Null => true,
                    serde_json::Value::Array(values) => values.is_empty(),
                    _ => false,
                }))
            });
        match empty {
            None => Outcome::Failed("the e-book reader wrote something other than details".into()),
            // A comic book's pages are pictures, so one without details
            // shows its cover large instead.
            Some(true) if format == book::Format::Comic => {
                match self.read_book(file, format, "cover").await {
                    Ok(cover) if cover.is_empty() => Outcome::None(NO_BOOK_COVER),
                    Ok(cover) => match self
                        .shrink(self.scratch(&cover).map_err(io::Error::other), PREVIEW)
                        .await
                    {
                        Ok(webp) => match self.keep(self.picture_path(hash), &webp) {
                            Ok(()) => Outcome::Ready,
                            Err(err) => {
                                Outcome::Failed(format!("couldn't save the preview: {err}"))
                            }
                        },
                        Err(problem) => Outcome::Failed(problem),
                    },
                    Err(problem) => Outcome::Failed(problem),
                }
            }
            Some(true) => Outcome::None("an e-book without a title or contents"),
            Some(false) => match self.keep(self.book_path(hash), &details) {
                Ok(()) => Outcome::Ready,
                Err(err) => Outcome::Failed(format!("couldn't save the preview: {err}")),
            },
        }
    }

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
/// installed, and which older sound files can't play, and the packages
/// that would fix that, as sentences.
fn pictures_unavailable(tools: &Tools) -> Option<String> {
    let sounds = (!tools.has_sox()).then_some(
        "Some older sound files, such as Yamaha TX16W samples, can't play. Install sox with apt \
         to play them.",
    );
    let pictures = missing_pictures(tools);
    match (pictures, sounds) {
        (Some(pictures), Some(sounds)) => Some(format!("{pictures} {sounds}")),
        (pictures, sounds) => pictures.or(sounds.map(str::to_owned)),
    }
}

fn missing_pictures(tools: &Tools) -> Option<String> {
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

/// Why videos and songs have no thumbnails here, if that's the case.
fn videos_unavailable(tools: &Tools) -> Option<String> {
    (!tools.has_ffmpeg()).then(|| {
        "ffmpeg isn't installed. Install it with apt to show a frame from each video and \
         songs' cover art, and to play audio the browser can't."
            .to_owned()
    })
}

/// A picture's size as it's shown, from libvips's width, height, and
/// orientation, one to a line. Orientations 5 to 8 turn it a quarter.
fn picture_size(output: &[u8]) -> Option<Facts> {
    let text = std::str::from_utf8(output).ok()?;
    let mut lines = text.lines().map(|line| line.trim().parse::<u32>().ok());
    let (width, height) = (lines.next()??, lines.next()??);
    let orientation = lines.next().flatten().unwrap_or(1);
    let (width, height) = if (5..=8).contains(&orientation) {
        (height, width)
    } else {
        (width, height)
    };
    Some(Facts {
        width: Some(width),
        height: Some(height),
        ..Facts::default()
    })
}

/// What ffprobe's JSON says about a video's or a picture's size as it's
/// shown, and how long it lasts. A video turned a quarter, as phones store
/// portrait video, has its width and height swapped. A video that doesn't
/// say how long it lasts, but whose frames were counted, lasts as long as
/// its frames take at its frame rate, with `ticks_per_frame` of ffprobe's
/// frame rate to each frame.
fn stream_facts(output: &[u8], ticks_per_frame: f64) -> Result<Facts, String> {
    let json: serde_json::Value = serde_json::from_slice(output)
        .map_err(|err| format!("ffprobe's answer wasn't JSON: {err}"))?;
    let stream = &json["streams"][0];
    let side = |name: &str| {
        stream[name]
            .as_u64()
            .and_then(|pixels| u32::try_from(pixels).ok())
            .filter(|pixels| *pixels > 0)
    };
    let turned = stream["side_data_list"]
        .as_array()
        .and_then(|list| list.iter().find_map(|data| data["rotation"].as_f64()))
        .is_some_and(|rotation| (rotation.round() as i64).rem_euclid(180) == 90);
    let (width, height) = match (side("width"), side("height")) {
        (Some(width), Some(height)) if turned => (Some(height), Some(width)),
        (Some(width), Some(height)) => (Some(width), Some(height)),
        _ => (None, None),
    };
    let counted = || {
        let frames = stream["nb_read_packets"].as_str()?.parse::<f64>().ok()?;
        let (per, every) = stream["r_frame_rate"].as_str()?.split_once('/')?;
        let ticks = per.parse::<f64>().ok()? / every.parse::<f64>().ok()?;
        let rate = ticks / ticks_per_frame;
        Some(frames / rate)
    };
    Ok(Facts {
        width,
        height,
        seconds: json["format"]["duration"]
            .as_str()
            .and_then(|seconds| seconds.parse::<f64>().ok())
            .or_else(counted)
            .filter(|seconds| seconds.is_finite() && *seconds > 0.0),
        ..Facts::default()
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

/// What ffprobe's JSON says about a file's tracks.
fn tracks(output: &[u8]) -> Result<Tracks, String> {
    let json: serde_json::Value = serde_json::from_slice(output)
        .map_err(|err| format!("ffprobe's answer wasn't JSON: {err}"))?;
    let streams = json["streams"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default();
    let video = |cover: bool| {
        streams.iter().any(|stream| {
            stream["codec_type"] == "video" && (stream["disposition"]["attached_pic"] == 1) == cover
        })
    };
    let sound = streams
        .iter()
        .find(|stream| stream["codec_type"] == "audio")
        .map(|stream| Track {
            codec: stream["codec_name"].as_str().unwrap_or_default().to_owned(),
            rate: stream["sample_rate"]
                .as_str()
                .and_then(|rate| rate.parse().ok())
                .unwrap_or_default(),
            channels: stream["channels"]
                .as_u64()
                .and_then(|channels| u32::try_from(channels).ok())
                .unwrap_or_default(),
        });
    let picture = streams
        .iter()
        .find(|stream| {
            stream["codec_type"] == "video" && stream["disposition"]["attached_pic"] != 1
        })
        .map(|stream| Picture {
            codec: stream["codec_name"].as_str().unwrap_or_default().to_owned(),
            pixels: stream["pix_fmt"].as_str().unwrap_or_default().to_owned(),
            hdr: matches!(
                stream["color_transfer"].as_str(),
                Some("smpte2084" | "arib-std-b67")
            ),
        });
    Ok(Tracks {
        sound,
        picture,
        cover: video(true),
        video: video(false),
        seconds: json["format"]["duration"]
            .as_str()
            .and_then(|seconds| seconds.parse::<f64>().ok())
            .filter(|seconds| seconds.is_finite() && *seconds > 0.0),
    })
}

/// How to make the preview of a video named `name`, in `container`, with
/// `tracks`, which has a picture. `None` if browsers play it as it is, as
/// Drive sends it: an MP4 or QuickTime file with H.264 and AAC or MP3, or a
/// WebM with VP8, VP9, or AV1 and Opus or Vorbis. Other codecs, such as
/// HEVC, which only some browsers play, and H.264 with more than 8 bits,
/// which few do, are converted.
fn recipe(name: &str, container: Container, tracks: &Tracks) -> Option<Recipe> {
    let picture = tracks.picture.as_ref()?;
    let sound = tracks.sound.as_ref().map(|track| track.codec.as_str());
    let extension = name
        .rsplit_once('.')
        .map(|(_, extension)| extension.to_ascii_lowercase())
        .unwrap_or_default();
    let mp4_picture =
        picture.codec == "h264" && matches!(picture.pixels.as_str(), "yuv420p" | "yuvj420p");
    let webm_picture = matches!(picture.codec.as_str(), "vp8" | "vp9" | "av1")
        && matches!(
            picture.pixels.as_str(),
            "yuv420p" | "yuvj420p" | "yuv420p10le"
        );
    let mp4_sound = sound.is_none_or(|codec| matches!(codec, "aac" | "mp3"));
    let webm_sound = sound.is_none_or(|codec| matches!(codec, "opus" | "vorbis"));
    let named_mp4 = matches!(extension.as_str(), "mp4" | "m4v" | "mov");
    if container == Container::Mov && named_mp4 && mp4_picture && mp4_sound {
        return None;
    }
    if container == Container::Matroska && extension == "webm" && webm_picture && webm_sound {
        return None;
    }
    Some(if webm_picture {
        Recipe {
            webm: true,
            copy_picture: true,
            copy_sound: webm_sound,
        }
    } else {
        Recipe {
            webm: false,
            copy_picture: mp4_picture,
            copy_sound: mp4_sound,
        }
    })
}

/// ffmpeg's arguments to make a video's preview at `output`, reading `file`
/// with only the `demuxer` and the `decoders`. A converted picture is at
/// most [`VIDEO_SIZE`] on its longer side, at most 60 frames a second, and
/// HDR is turned into ordinary colors. Sound is at most stereo.
fn video_args(
    file: &Path,
    demuxer: &str,
    decoders: &str,
    tracks: &Tracks,
    recipe: &Recipe,
    output: &Path,
) -> Vec<OsString> {
    let mut args: Vec<OsString> = [
        "-nostdin",
        "-v",
        "error",
        // Each of the decoder's threads holds frames too, which for 4K
        // HDR would pass the memory limit on a server with many cores.
        "-threads",
        "4",
        "-protocol_whitelist",
        "file",
        "-f",
        demuxer,
        "-codec_whitelist",
        decoders,
        "-i",
    ]
    .map(OsString::from)
    .into_iter()
    .chain([file.into()])
    .collect();
    let mut add = |more: &[&str]| args.extend(more.iter().map(OsString::from));
    add(&[
        "-map",
        "0:v:0",
        "-map",
        "0:a:0?",
        "-sn",
        "-dn",
        "-map_metadata",
        "-1",
    ]);
    if recipe.copy_picture {
        add(&["-c:v", "copy"]);
    } else {
        // Interlaced video, as from DVDs and camcorders, is deinterlaced.
        // The longer side is shrunk, so a phone's upright video stays 1080p.
        let shrink = format!(
            "bwdif=deint=interlaced,scale=w='if(gte(iw,ih),min({VIDEO_SIZE},trunc(iw/2)*2),-2)':\
             h='if(gte(iw,ih),-2,min({VIDEO_SIZE},trunc(ih/2)*2))'"
        );
        let hdr = tracks.picture.as_ref().is_some_and(|picture| picture.hdr);
        let filters = if hdr {
            format!("{shrink},{TONE_MAP}")
        } else {
            format!("{shrink},format=yuv420p")
        };
        add(&["-vf", &filters, "-fpsmax", "60"]);
        add(&[
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "23",
            "-profile:v",
            "high",
            // Each of x264's threads holds frames of its own.
            "-threads",
            "4",
        ]);
    }
    if let Some(track) = &tracks.sound {
        let channels = track.channels.clamp(1, 2).to_string();
        if recipe.copy_sound {
            add(&["-c:a", "copy"]);
        } else if recipe.webm {
            // Opus takes only 48 kHz.
            add(&[
                "-c:a", "libopus", "-b:a", "128k", "-ac", &channels, "-ar", "48000",
            ]);
        } else {
            let rate = rate(track.rate).to_string();
            add(&[
                "-c:a", "aac", "-b:a", "128k", "-ac", &channels, "-ar", &rate,
            ]);
        }
    }
    if recipe.webm {
        add(&["-f", "webm"]);
    } else {
        // The index goes at the start, so the browser can play it before
        // it has it all.
        add(&["-movflags", "+faststart", "-f", "mp4"]);
    }
    args.push(output.into());
    args
}

/// Which videos to remove, in order, until `used` is within `limit`:
/// the least recently watched first, never `keep`.
fn over_limit(
    videos: &[(ContentHash, u64)],
    mut used: u64,
    limit: u64,
    keep: Option<ContentHash>,
) -> Vec<ContentHash> {
    let mut remove = Vec::new();
    for &(hash, size) in videos {
        if used <= limit {
            break;
        }
        if Some(hash) == keep {
            continue;
        }
        used = used.saturating_sub(size);
        remove.push(hash);
    }
    remove
}

/// The admin's choices for previews.
fn read_settings(conn: &rusqlite::Connection) -> rusqlite::Result<PreviewSettings> {
    conn.query_row(
        "SELECT convert_videos, space_percent FROM thumbnail_settings",
        [],
        |row| {
            Ok(PreviewSettings {
                convert_videos: row
                    .get::<_, Option<String>>(0)?
                    .and_then(|choice| VideoConversion::parse(&choice)),
                space_percent: row.get(1)?,
            })
        },
    )
}

/// The size of every file in `dir` and the folders in it.
fn folder_size(dir: &Path) -> io::Result<u64> {
    let mut size = 0;
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        if kind.is_dir() {
            size += folder_size(&entry.path())?;
        } else if kind.is_file() {
            size += entry.metadata()?.len();
        }
    }
    Ok(size)
}

/// The size of the disk `dir` is on, in bytes.
fn disk_size(dir: &Path) -> io::Result<u64> {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(dir.as_os_str().as_bytes()).map_err(io::Error::other)?;
    let mut stats: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(path.as_ptr(), &mut stats) } != 0 {
        return Err(io::Error::last_os_error());
    }
    #[allow(clippy::unnecessary_cast)]
    Ok(stats.f_blocks as u64 * stats.f_frsize as u64)
}

/// Whether the browser plays a sound file named `name`, whose sound is in
/// `codec`, as Drive sends it: as the type its name says. These are the
/// names Drive plays, and the codecs browsers play in each.
fn plays(name: &str, codec: &str) -> bool {
    let extension = name
        .rsplit_once('.')
        .map(|(_, extension)| extension.to_ascii_lowercase())
        .unwrap_or_default();
    match extension.as_str() {
        "mp3" => codec == "mp3",
        "m4a" | "m4b" | "m4r" | "aac" => codec == "aac",
        // Drive sends these as video, which plays sound on its own too.
        "mp4" | "m4v" | "mov" => matches!(codec, "aac" | "mp3"),
        "webm" => matches!(codec, "opus" | "vorbis"),
        "ogg" | "oga" | "opus" => matches!(codec, "vorbis" | "opus" | "flac"),
        "flac" => codec == "flac",
        "wav" => matches!(
            codec,
            "pcm_u8" | "pcm_s16le" | "pcm_s24le" | "pcm_s32le" | "pcm_f32le"
        ),
        _ => false,
    }
}

/// The sample rate for a converted sound: the sound's own, if AAC has it
/// and it isn't more than people hear, or else the nearest common one.
fn rate(rate: u32) -> u32 {
    const AAC: [u32; 12] = [
        8000, 11025, 12000, 16000, 22050, 24000, 32000, 44100, 48000, 64000, 88200, 96000,
    ];
    match rate {
        rate if rate <= 48000 && AAC.contains(&rate) => rate,
        // Such as DSD, at 64 times a CD's 44,100.
        rate if rate > 0 && rate % 11025 == 0 => 44100,
        _ => 48000,
    }
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
    if let Some(format) = ebook(head) {
        return Some(Kind::Book(format));
    }
    // Before text, since a Type 1 font can be text.
    if let Some(font) = font(head) {
        return Some(Kind::Font(font));
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
    if let Some(sound) = sound(head, whole) {
        return Some(Kind::Audio(sound));
    }
    // Known by their first bytes, since SoundTool files are often named
    // `.snd` and TX16W sounds `.w01`, and other programs use `.paf`,
    // `.smp`, and `.wve` for their own files.
    let old = if starts(b"SOUND\x1a") {
        Some(Sox::Sndt)
    } else if starts(b"LM8953\0") {
        Some(Sox::Txw)
    } else if starts(b" paf\0") || starts(b"fap \0") {
        Some(Sox::Paris)
    } else if starts(b"SOUND SAMPLE DATA ") {
        Some(Sox::SampleVision)
    } else if starts(b"ALawSoundFile**") {
        Some(Sox::Psion)
    } else {
        None
    };
    if let Some(old) = old
        && !is_text(head, whole)
    {
        return Some(Kind::Sox(old));
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
    if head.starts_with(b".RMF\0\0\0") {
        return Some(Container::RealMedia);
    }
    // A SMPTE key, then a partition pack's.
    if head.starts_with(b"\x06\x0e\x2b\x34\x02\x05\x01\x01\x0d\x01\x02\x01\x01") {
        return Some(Container::Mxf);
    }
    if head.starts_with(WTV) {
        return Some(Container::Wtv);
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

/// The sound format that starts with `head`, which is `whole` if it's the
/// entire file, if it's one with a signature. Most signatures are letters,
/// such as "MAC ", so they only count when the file isn't text, as a sound
/// file's header has numbers in it and is followed by its sound.
fn sound(head: &[u8], whole: bool) -> Option<Sound> {
    const W64: &[u8] = b"riff\x2e\x91\xcf\x11\xa5\xd6\x28\xdb\x04\xc1\x00\x00";
    const MP4: [&[u8; 4]; 3] = [b"M4A ", b"M4B ", b"M4P "];
    const IRCAM: [[u8; 4]; 8] = [
        [0x64, 0xa3, 0x01, 0x00],
        [0x64, 0xa3, 0x02, 0x00],
        [0x64, 0xa3, 0x03, 0x00],
        [0x64, 0xa3, 0x04, 0x00],
        [0x00, 0x01, 0xa3, 0x64],
        [0x00, 0x02, 0xa3, 0x64],
        [0x00, 0x03, 0xa3, 0x64],
        [0x00, 0x04, 0xa3, 0x64],
    ];
    const DTS: [[u8; 4]; 4] = [
        [0x7f, 0xfe, 0x80, 0x01],
        [0xfe, 0x7f, 0x01, 0x80],
        [0x1f, 0xff, 0xe8, 0x00],
        [0xff, 0x1f, 0x00, 0xe8],
    ];
    let starts = |magic: &[u8]| head.starts_with(magic);
    let at = |range: std::ops::Range<usize>| head.get(range);
    // ID3 tags, then a version from 2 to 4, and a size of 7-bit bytes.
    let id3 = starts(b"ID3")
        && matches!(head.get(3), Some(2..=4))
        && at(6..10).is_some_and(|size| size.iter().all(|byte| *byte < 0x80));
    let found = if id3 {
        Sound::Mpeg
    } else if starts(b"fLaC") {
        Sound::Flac
    } else if starts(b"OggS") {
        Sound::Ogg
    } else if (starts(b"RIFF") || starts(b"RF64") || starts(b"BW64")) && at(8..12) == Some(b"WAVE")
    {
        Sound::Wav
    } else if starts(W64) {
        Sound::Wave64
    } else if starts(b"FORM") && matches!(at(8..12), Some(b"AIFF" | b"AIFC")) {
        Sound::Aiff
    } else if (starts(b"FORM") && matches!(at(8..12), Some(b"8SVX" | b"16SV" | b"MAUD")))
        || (starts(b"FRM8") && at(12..16) == Some(b"DSD "))
    {
        Sound::Iff
    } else if starts(b"DSD ") && at(4..12) == Some(&28u64.to_le_bytes()) {
        Sound::Dsf
    } else if starts(b"MAC ") {
        Sound::Ape
    } else if starts(b"wvpk") {
        Sound::WavPack
    } else if starts(b"TTA1") {
        Sound::Tta
    } else if starts(b".snd") {
        Sound::Au
    } else if starts(b"caff") {
        Sound::Caf
    } else if starts(b"#!AMR\n") || starts(b"#!AMR-WB\n") {
        Sound::Amr
    } else if starts(b".ra\xfd") {
        Sound::RealAudio
    } else if starts(b"Creative Voice File\x1a") {
        Sound::Voc
    } else if IRCAM.iter().any(|magic| starts(magic)) {
        Sound::Ircam
    } else if starts(b"NIST_1A\n") {
        Sound::Nist
    } else if starts(b"2BIT")
        && matches!(at(12..14), Some([0, 0] | [0xff, 0xff]))
        && matches!(at(14..16), Some([0, 8] | [0, 16]))
    {
        Sound::Avr
    } else if at(65..69) == Some(b"FSSD") && at(128..132) == Some(b"HCOM") {
        Sound::Hcom
    } else if starts(b"PVF1\n") || starts(b"PVF2\n") {
        Sound::Pvf
    } else if DTS.iter().any(|magic| starts(magic)) {
        Sound::Dts
    } else if starts(b"Extended Module: ") || starts(b"IMPM") || at(44..48) == Some(b"SCRM") {
        Sound::Module
    } else if at(4..8) == Some(b"ftyp")
        && at(8..12).is_some_and(|brand| MP4.iter().any(|known| brand == *known))
    {
        Sound::Mp4
    } else {
        return None;
    };
    (!is_text(head, whole)).then_some(found)
}

/// A sound without a signature, known by its name, if its first bytes
/// fit the format.
fn unsigned_sound(extension: &str, head: &[u8]) -> Option<Sound> {
    let byte = |index: usize| head.get(index).copied().unwrap_or_default();
    // MPEG audio frames start with 11 bits set, then a version, a layer,
    // a bit rate, and a sample rate, each of which has a value it can't be.
    let mpeg = byte(0) == 0xff
        && byte(1) & 0xe0 == 0xe0
        && (byte(1) >> 3) & 3 != 1
        && (byte(1) >> 1) & 3 != 0
        && byte(2) >> 4 != 0xf
        && (byte(2) >> 2) & 3 != 3;
    // AAC frames start the same way, with a layer of 0.
    let adts = byte(0) == 0xff && byte(1) & 0xf6 == 0xf0;
    let dolby = head.starts_with(&[0x0b, 0x77]);
    // A module's four letters after its 31 instruments, such as "M.K.".
    let module = head.get(1080..1084).is_some_and(|tag| {
        matches!(tag, b"M.K." | b"M!K!" | b"M&K!" | b"FLT4" | b"FLT8")
            || (tag[0].is_ascii_digit() && &tag[1..] == b"CHN")
            || (tag[..2].iter().all(u8::is_ascii_digit) && &tag[2..] == b"CH")
    });
    match extension {
        "mp3" | "mp2" | "mp1" | "mpa" | "mpga" if mpeg => Some(Sound::Mpeg),
        "aac" if adts => Some(Sound::Adts),
        // Dolby Digital Plus says so with a stream version over 10.
        "ac3" | "eac3" | "ec3" if dolby && byte(5) >> 3 > 10 => Some(Sound::Eac3),
        "ac3" if dolby => Some(Sound::Ac3),
        "mod" if module => Some(Sound::Module),
        _ => None,
    }
}

/// An older sound only SoX reads that has no signature, known by its name,
/// if its first bytes fit the format. Formats without a header can be any
/// bytes, but aren't text, which [`kind`] has already ruled out.
fn unsigned_sox(extension: &str, head: &[u8]) -> Option<Sox> {
    let at = |range: std::ops::Range<usize>| head.get(range);
    let little = |offset: usize| {
        at(offset..offset + 2).map(|bytes| u16::from_le_bytes([bytes[0], bytes[1]]))
    };
    // A DVMS header's last field is the sum of its other bytes, which SoX
    // checks too.
    let dvms = at(0..120).is_some_and(|header| {
        let sum = header[..117]
            .iter()
            .fold(0u16, |sum, byte| sum.wrapping_add(u16::from(*byte)));
        sum != 0 && little(118) == Some(sum)
    });
    // A ringtone's size in 16-bit words comes first, and its header alone
    // is 512 bytes.
    let gsrt = at(0..4)
        .is_some_and(|size| u32::from_be_bytes([size[0], size[1], size[2], size[3]]) >= 256);
    // Sounder's header is a zero, then the sample rate.
    let sndr = little(0) == Some(0) && little(2).is_some_and(|rate| rate >= 1000);
    // HTK's header ends with 2-byte samples and the kind of data, where
    // sound is 0, after the time between samples in 100 ns, which is at
    // most 10,000 for 1 kHz.
    let big = |offset: usize| {
        at(offset..offset + 4)
            .map(|bytes| u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    };
    let htk = big(4).is_some_and(|period| (1..=10_000).contains(&period))
        && at(8..12) == Some(&[0, 2, 0, 0]);
    match extension {
        "cdda" => Some(Sox::Cdda),
        "cvsd" | "cvs" => Some(Sox::Cvsd),
        "cvu" => Some(Sox::Cvu),
        "dvms" | "vms" if dvms => Some(Sox::Dvms),
        "fssd" | "sou" => Some(Sox::Fssd),
        "gsrt" if gsrt => Some(Sox::Gsrt),
        "sndr" if sndr => Some(Sox::Sndr),
        "htk" if htk => Some(Sox::Htk),
        // MagicaVoxel's 3D models are `.vox` files too.
        "vox" if !head.starts_with(b"VOX ") => Some(Sox::Vox),
        "sd2" => Some(Sox::Sd2),
        _ => None,
    }
}

/// How every WTV file starts: the GUID of its header.
const WTV: &[u8] = b"\xb7\xd8\x00\x20\x37\x49\xda\x11\xa6\x4e\x00\x07\xe9\x5e\xad\x8d";

/// A video without a container, known by its name, if its first bytes fit
/// the format.
fn unsigned_video(extension: &str, head: &[u8]) -> Option<Container> {
    // H.264 and HEVC streams start with a start code, then the first unit's
    // type: H.264's settings or a delimiter, or HEVC's video settings.
    let unit = if head.starts_with(&[0, 0, 0, 1]) {
        head.get(4)
    } else if head.starts_with(&[0, 0, 1]) {
        head.get(3)
    } else {
        None
    };
    let h264 = unit.is_some_and(|unit| matches!(unit & 0x1f, 7 | 9) && unit & 0x80 == 0);
    let hevc = unit.is_some_and(|unit| matches!((unit >> 1) & 0x3f, 32 | 35) && unit & 0x80 == 0);
    // A DV frame starts with its header block.
    let dv = head.starts_with(&[0x1f, 0x07, 0x00]);
    match extension {
        "h264" | "264" if h264 => Some(Container::H264),
        "hevc" | "h265" | "265" if hevc => Some(Container::Hevc),
        "dv" if dv => Some(Container::Dv),
        _ => None,
    }
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
        // A Motion JPEG stream starts with its first frame.
        Some(Kind::Image) if matches!(extension.as_str(), "mjpeg" | "mjpg") => {
            Some(Kind::Video(Container::Mjpeg))
        }
        None if extension == "svgz" && head.starts_with(b"\x1f\x8b") => Some(Kind::Svg),
        // An EPUB is meant to start by saying so, but some don't.
        None if matches!(extension.as_str(), "epub" | "kepub" | "ibooks")
            && head.starts_with(ZIP) =>
        {
            Some(Kind::Book(book::Format::Epub))
        }
        // A comic book is a zip file of its pages.
        None if extension == "cbz" && head.starts_with(ZIP) => {
            Some(Kind::Book(book::Format::Comic))
        }
        None => unsigned_still(name, head)
            .map(Kind::Still)
            .or_else(|| unsigned_sound(&extension, head).map(Kind::Audio))
            .or_else(|| unsigned_sox(&extension, head).map(Kind::Sox))
            .or_else(|| unsigned_video(&extension, head).map(Kind::Video)),
        kind => kind,
    }
}

/// The font format that starts with `head`, if it's one with outlines.
fn font(head: &[u8]) -> Option<Font> {
    let starts = |magic: &[u8]| head.starts_with(magic);
    // TrueType and OpenType files start with a list of tables, each named
    // with four letters.
    let tables = || {
        let count = u16::from_be_bytes([*head.get(4)?, *head.get(5)?]);
        let first = head.get(12..16)?;
        ((1..=100).contains(&count) && first.iter().all(|c| c.is_ascii_graphic() || *c == b' '))
            .then_some(())
    };
    if starts(b"wOFF") {
        Some(Font::Woff)
    } else if starts(b"wOF2") {
        Some(Font::Woff2)
    } else if starts(b"ttcf") {
        Some(Font::Collection)
    } else if starts(b"OTTO") && tables().is_some() {
        Some(Font::OpenType)
    } else if (starts(b"\0\x01\0\0") || starts(b"true")) && tables().is_some() {
        Some(Font::TrueType)
    } else if starts(b"%!PS-AdobeFont-1")
        || starts(b"%!FontType1")
        // A binary Type 1 font's first segment is its text.
        || (starts(b"\x80\x01")
            && head.get(6..).is_some_and(|text| {
                text.starts_with(b"%!PS-AdobeFont-1") || text.starts_with(b"%!FontType1")
            }))
    {
        Some(Font::Type1)
    } else {
        None
    }
}

/// `text` as a value in an ffmpeg filter graph, escaped for the filter's
/// options and then for the graph.
fn filter_value(text: &str) -> String {
    let mut option = String::new();
    for c in text.chars() {
        if matches!(c, '\\' | '\'' | ':') {
            option.push('\\');
        }
        option.push(c);
    }
    let mut graph = String::new();
    for c in option.chars() {
        if matches!(c, '\\' | '\'' | '[' | ']' | ',' | ';') {
            graph.push('\\');
        }
        graph.push(c);
    }
    graph
}

/// The e-book format that starts with `head`, if it's one this reads.
fn ebook(head: &[u8]) -> Option<book::Format> {
    if head.starts_with(ZIP) && head.get(30..58) == Some(b"mimetypeapplication/epub+zip") {
        return Some(book::Format::Epub);
    }
    // A Palm database, named at its start, says what it is at 60.
    if head.get(60..68) == Some(b"BOOKMOBI") {
        return Some(book::Format::Mobi);
    }
    // FictionBook is XML, after a declaration and perhaps a comment.
    let xml = head.strip_prefix(b"\xef\xbb\xbf").unwrap_or(head);
    if (xml.starts_with(b"<?xml") || xml.starts_with(b"<FictionBook"))
        && xml.windows(12).any(|window| window == b"<FictionBook")
    {
        return Some(book::Format::Fb2);
    }
    None
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
        assert_eq!(kind(b"RIFF\0\0\0\0WAVEfmt "), Some(Kind::Audio(Sound::Wav)));
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
    fn knows_e_books_by_their_first_bytes_or_name() {
        let epub = b"PK\x03\x04\x0a\0\0\0\0\0\0\0\0\0\0\0\0\0\x14\0\0\0\x14\0\0\0\x08\0\0\0mimetypeapplication/epub+zip";
        assert_eq!(kind(epub, false), Some(Kind::Book(book::Format::Epub)));
        let mut mobi = b"Alice_s_Adventures_in_Wonderlan\0".to_vec();
        mobi.resize(60, 0);
        mobi.extend(b"BOOKMOBI\0\0\0\0");
        assert_eq!(kind(&mobi, false), Some(Kind::Book(book::Format::Mobi)));
        let fb2 = "\u{feff}<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<!-- made by hand -->\n\
            <FictionBook xmlns=\"http://www.gribuser.ru/xml/fictionbook/2.0\">";
        assert_eq!(
            kind(fb2.as_bytes(), false),
            Some(Kind::Book(book::Format::Fb2))
        );
        // Other XML is still text.
        assert_eq!(
            kind(b"<?xml version=\"1.0\"?><notes/>", true),
            Some(Kind::Text)
        );
        // An EPUB that doesn't start by saying so is known by its name.
        assert_eq!(kind(b"PK\x03\x04", false), None);
        assert_eq!(
            named("Alice.EPUB", b"PK\x03\x04", None),
            Some(Kind::Book(book::Format::Epub))
        );
        assert_eq!(named("Alice.zip", b"PK\x03\x04", None), None);
        assert_eq!(named("Alice.epub", b"%PDF-1.4", None), None);
    }

    #[test]
    fn knows_fonts_by_their_first_bytes() {
        let tables = |magic: &[u8]| {
            let mut head = magic.to_vec();
            head.extend([0, 12, 0, 128, 0, 3, 0, 32]);
            head.extend(b"GDEF\0\0\0\0");
            head
        };
        let font = |head: &[u8]| match kind(head, false) {
            Some(Kind::Font(font)) => Some(font),
            _ => None,
        };
        assert_eq!(font(&tables(b"\0\x01\0\0")), Some(Font::TrueType));
        assert_eq!(font(&tables(b"true")), Some(Font::TrueType));
        assert_eq!(font(&tables(b"OTTO")), Some(Font::OpenType));
        assert_eq!(font(b"ttcf\0\x01\0\0"), Some(Font::Collection));
        assert_eq!(font(b"wOFF\0\x01\0\0"), Some(Font::Woff));
        assert_eq!(font(b"wOF2OTTO"), Some(Font::Woff2));
        assert_eq!(font(b"%!PS-AdobeFont-1.0: NimbusRoman"), Some(Font::Type1));
        assert_eq!(font(b"%!FontType1-1.0: Example"), Some(Font::Type1));
        assert_eq!(
            font(b"\x80\x01\x10\x17\0\0%!PS-AdobeFont-1.0: Times"),
            Some(Font::Type1)
        );
        // Other files that start the same way aren't fonts.
        assert_eq!(font(b"\0\x01\0\0\0\0\0\0\0\0\0\0\0\0\0\0"), None);
        assert_eq!(font(b"true story, told in text"), None);
        assert_eq!(font(b"%!PS-Adobe-3.0 a PostScript page"), None);
        assert!(Font::Woff2.browser_draws());
        assert!(!Font::Collection.browser_draws());
        assert!(!Font::Type1.browser_draws());
        assert_eq!(
            named("Nemo.CBZ", b"PK\x03\x04", None),
            Some(Kind::Book(book::Format::Comic))
        );
    }

    #[test]
    fn escapes_values_in_ffmpeg_filters() {
        assert_eq!(filter_value("/srv/data/ab/cd"), "/srv/data/ab/cd");
        assert_eq!(filter_value("a:b"), "a\\\\:b");
        assert_eq!(filter_value("it's"), "it\\\\\\'s");
        assert_eq!(filter_value("a,b[c];"), "a\\,b\\[c\\]\\;");
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
            sox: "/nowhere/sox".into(),
            ..Tools::installed("/nowhere/accordion")
        };
        assert_eq!(
            pictures_unavailable(&missing).unwrap(),
            "Camera raw photos, SVG drawings, and DjVu books show an icon instead. Install \
             libraw-bin, librsvg2-bin, and djvulibre-bin with apt to give them thumbnails and \
             previews. Some older sound files, such as Yamaha TX16W samples, can't play. \
             Install sox with apt to play them."
        );
        let one = Tools {
            simple_dcraw: "/usr/bin/true".into(),
            rsvg_convert: "/usr/bin/true".into(),
            sox: "/usr/bin/true".into(),
            ..missing.clone()
        };
        assert_eq!(
            pictures_unavailable(&one).unwrap(),
            "DjVu books show an icon instead. Install djvulibre-bin with apt to give them \
             thumbnails and previews."
        );
        let sounds = Tools {
            simple_dcraw: "/usr/bin/true".into(),
            rsvg_convert: "/usr/bin/true".into(),
            ddjvu: "/usr/bin/true".into(),
            ..missing
        };
        assert_eq!(
            pictures_unavailable(&sounds).unwrap(),
            "Some older sound files, such as Yamaha TX16W samples, can't play. Install sox \
             with apt to play them."
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
    fn reads_a_pictures_size_as_its_shown() {
        let size = |output: &[u8]| picture_size(output).map(|facts| (facts.width, facts.height));
        assert_eq!(size(b"4032\n3024\n1\n"), Some((Some(4032), Some(3024))));
        // A phone's portrait photo, kept sideways with a flag to turn it.
        assert_eq!(size(b"4032\n3024\n6\n"), Some((Some(3024), Some(4032))));
        assert_eq!(size(b"4032\n3024\n3\n"), Some((Some(4032), Some(3024))));
        assert_eq!(size(b"640\n480\n"), Some((Some(640), Some(480))));
        assert_eq!(size(b"wide\n480\n1\n"), None);
        assert_eq!(size(b""), None);
    }

    #[test]
    fn reads_a_videos_size_and_length() {
        let portrait = br#"{"streams": [{"width": 1920, "height": 1080,
            "side_data_list": [{"rotation": -90}]}], "format": {"duration": "12.5"}}"#;
        assert_eq!(
            stream_facts(portrait, 1.0).unwrap(),
            Facts {
                width: Some(1080),
                height: Some(1920),
                seconds: Some(12.5),
                ..Facts::default()
            }
        );
        let upside_down = br#"{"streams": [{"width": 1920, "height": 1080,
            "side_data_list": [{"rotation": 180}]}], "format": {}}"#;
        let facts = stream_facts(upside_down, 1.0).unwrap();
        assert_eq!(
            (facts.width, facts.height, facts.seconds),
            (Some(1920), Some(1080), None)
        );
        let picture = br#"{"streams": [{"width": 300, "height": 200}], "format": {}}"#;
        assert_eq!(stream_facts(picture, 1.0).unwrap().width, Some(300));
        let bare = br#"{"streams": [{"width": 1280, "height": 720,
            "r_frame_rate": "24000/1001", "nb_read_packets": "4389"}], "format": {}}"#;
        let seconds = stream_facts(bare, 1.0).unwrap().seconds.unwrap();
        assert!((seconds - 183.06).abs() < 0.01, "{seconds}");
        let h264 = br#"{"streams": [{"r_frame_rate": "60/1", "nb_read_packets": "60"}],
            "format": {}}"#;
        assert_eq!(stream_facts(h264, 2.0).unwrap().seconds, Some(2.0));
        let uncounted = br#"{"streams": [{"r_frame_rate": "0/0"}], "format": {}}"#;
        assert_eq!(stream_facts(uncounted, 1.0).unwrap().seconds, None);
        let none = br#"{"streams": [], "format": {"duration": "N/A"}}"#;
        assert_eq!(stream_facts(none, 1.0).unwrap(), Facts::default());
        assert!(stream_facts(b"not json", 1.0).is_err());
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
    fn knows_sound_files_by_their_first_bytes() {
        let sound = |head: &[u8]| match kind(head, false) {
            Some(Kind::Audio(sound)) => Some(sound),
            _ => None,
        };
        assert_eq!(sound(b"ID3\x04\0\0\0\0\0#TSSE"), Some(Sound::Mpeg));
        assert_eq!(sound(b"fLaC\0\0\0\x22\x12\0"), Some(Sound::Flac));
        assert_eq!(
            sound(b"OggS\0\x02\0\0\0\0\0\0\0\0\x01\x1evorbis"),
            Some(Sound::Ogg)
        );
        assert_eq!(sound(b"RIFFr\xb4\x1c\x01WAVEfmt "), Some(Sound::Wav));
        assert_eq!(sound(b"RF64\xff\xff\xff\xffWAVEds64"), Some(Sound::Wav));
        assert_eq!(
            sound(b"riff\x2e\x91\xcf\x11\xa5\xd6\x28\xdb\x04\xc1\0\0\x2e\x91\x1c\x01"),
            Some(Sound::Wave64)
        );
        assert_eq!(sound(b"FORM\x01\x1c\xb4ZAIFFCOMM"), Some(Sound::Aiff));
        assert_eq!(sound(b"FORM\0\x8ek\\8SVXVHDR"), Some(Sound::Iff));
        assert_eq!(sound(b"FORM\x01\x1c\xd6PMAUDMHDR"), Some(Sound::Iff));
        assert_eq!(sound(b"FRM8\0\0\0\0\0\0\x10\0DSD FVER"), Some(Sound::Iff));
        assert_eq!(sound(b"DSD \x1c\0\0\0\0\0\0\0"), Some(Sound::Dsf));
        assert_eq!(sound(b"MAC \x96\x0f\0\0"), Some(Sound::Ape));
        assert_eq!(sound(b"wvpk\xb4v\0\0"), Some(Sound::WavPack));
        assert_eq!(sound(b"TTA1\x01\0\x02\0"), Some(Sound::Tta));
        assert_eq!(sound(b".snd\0\0\0 \x01\x1c"), Some(Sound::Au));
        assert_eq!(sound(b"caff\0\x01\0\0desc"), Some(Sound::Caf));
        assert_eq!(sound(b"#!AMR\n\x3c\x91\x17\x16"), Some(Sound::Amr));
        assert_eq!(sound(b".ra\xfd\0\x04\0\0"), Some(Sound::RealAudio));
        assert_eq!(sound(b"Creative Voice File\x1a\x1a\0"), Some(Sound::Voc));
        assert_eq!(sound(b"d\xa3\x01\0\0D,G"), Some(Sound::Ircam));
        assert_eq!(sound(b"NIST_1A\n   1024\n\0\x01\x02"), Some(Sound::Nist));
        assert_eq!(
            sound(b"2BIT\0\0\0\0\0\0\0\0\xff\xff\0\x10"),
            Some(Sound::Avr)
        );
        assert_eq!(sound(b"PVF1\n2 44100 16\n\x80\0\x7f"), Some(Sound::Pvf));
        assert_eq!(sound(b"\x7f\xfe\x80\x01\xfc<\x7f\xf0"), Some(Sound::Dts));
        assert_eq!(sound(b"Extended Module: tune\x1a"), Some(Sound::Module));
        assert_eq!(sound(b"IMPMtune\0\0\0\0"), Some(Sound::Module));
        assert_eq!(sound(b"\0\0\0\x18ftypM4A \0\0\0\0"), Some(Sound::Mp4));
        let mut hcom = vec![0; 132];
        hcom[65..69].copy_from_slice(b"FSSD");
        hcom[128..132].copy_from_slice(b"HCOM");
        assert_eq!(sound(&hcom), Some(Sound::Hcom));
        let mut s3m = vec![0; 48];
        s3m[44..48].copy_from_slice(b"SCRM");
        assert_eq!(sound(&s3m), Some(Sound::Module));

        // Ogg with Theora is a video, and RealMedia may be either.
        assert_eq!(
            kind(b"OggS\0\x02\0\0\0\0\0\0\0\0\x01\x1e\x80theora", false),
            Some(Kind::Video(Container::Ogg))
        );
        assert_eq!(
            kind(b".RMF\0\0\0\x12\0\0", false),
            Some(Kind::Video(Container::RealMedia))
        );
        // Text that starts with the same letters stays text.
        for text in [
            &b"2BIT is how many bits\n"[..],
            b"PVF1\nis the first version\n",
            b"IMPMENTATION notes\n",
            b"NIST_1A\nheader only\n",
            b"#!AMR\nnot really\n",
            b"MAC address of the router\n",
            b"ID3 tags hold a song's name\n",
            b"caffeine free\n",
            b"fLaC is a typo\n",
        ] {
            assert_eq!(kind(text, true), Some(Kind::Text), "{text:?}");
        }
    }

    #[test]
    fn knows_sound_files_without_a_signature_by_name() {
        let mp2 = b"\xff\xfd\xe0\x04\x99ffwf";
        assert_eq!(kind(mp2, false), None);
        assert_eq!(named("song.mp2", mp2, None), Some(Kind::Audio(Sound::Mpeg)));
        assert_eq!(
            named("song.mp3", b"\xff\xfb\x90\x64\0", None),
            Some(Kind::Audio(Sound::Mpeg))
        );
        // A bit rate of all ones isn't one.
        assert_eq!(named("song.mp3", b"\xff\xfb\xf0\x64\0", None), None);
        assert_eq!(
            named("song.aac", b"\xff\xf1P\x80\x03\xff", None),
            Some(Kind::Audio(Sound::Adts))
        );
        assert_eq!(
            named("film.ac3", b"\x0bw4\x0fT@C\xe1", None),
            Some(Kind::Audio(Sound::Ac3))
        );
        assert_eq!(
            named("film.ec3", b"\x0bw\x01\x7f\x3f\x86\xff", None),
            Some(Kind::Audio(Sound::Eac3))
        );
        assert_eq!(named("notes.mp3", b"\x0bw4\x0fT@C\xe1", None), None);
        let mut module = vec![0; 1084];
        module[1080..].copy_from_slice(b"M.K.");
        assert_eq!(
            named("tune.mod", &module, None),
            Some(Kind::Audio(Sound::Module))
        );
        module[1080..].copy_from_slice(b"8CHN");
        assert_eq!(
            named("tune.MOD", &module, None),
            Some(Kind::Audio(Sound::Module))
        );
        module[1080..].copy_from_slice(b"none");
        assert_eq!(named("tune.mod", &module, None), None);
    }

    #[test]
    fn knows_older_sound_files_only_sox_reads() {
        // SoX's own headers, as it writes them.
        let sndt = b"SOUND\x1a\0\0\x805G\0\0\0\0\0\x805G\0D\xac\0\0\n\0\x04\0Processed by SoX";
        assert_eq!(kind(sndt, false), Some(Kind::Sox(Sox::Sndt)));
        let txw = b"LM8953\0\0\0\0\0\0\0\0\0\0\0\0\x7f\x7f\x7f\x7f\xc9\x02";
        assert_eq!(kind(txw, false), Some(Kind::Sox(Sox::Txw)));
        assert_eq!(kind(b"SOUND CHECK at 9", true), Some(Kind::Text));
        for (head, format) in [
            (&b" paf\0\0\0\0\0\0\0\0\0\0\xacD"[..], Sox::Paris),
            (b"fap \0\0\0\0\x01\0\0\0D\xac\0\0", Sox::Paris),
            (
                b"SOUND SAMPLE DATA 2.1 Converted using Sox.\0\x01",
                Sox::SampleVision,
            ),
            (b"ALawSoundFile**\0\x0f\x10\0\x0c", Sox::Psion),
        ] {
            assert_eq!(kind(head, false), Some(Kind::Sox(format)), "{format:?}");
        }
        // A family tree named `.paf` stays as it is.
        assert_eq!(named("family.paf", b"\x03\x81\x02\xff", None), None);

        let htk = b"\0G5\x80\0\0\0\xe3\0\x02\0\0\xff\xff\0\0";
        assert_eq!(named("speech.htk", htk, None), Some(Kind::Sox(Sox::Htk)));
        // Speech features, not sound.
        let features = b"\0\0\x01\0\0\x01\x86\xa0\0\x34\0\x06";
        assert_eq!(named("speech.htk", features, None), None);
        assert_eq!(
            named("call.vox", b"\x08\x08\x80\x08", None),
            Some(Kind::Sox(Sox::Vox))
        );
        assert_eq!(named("castle.vox", b"VOX \x96\0\0\0MAIN", None), None);

        let sndr = b"\0\0\x44\xac\x0a\0\x04\0\x80\x80\x81\x80";
        assert_eq!(named("bell.sndr", sndr, None), Some(Kind::Sox(Sox::Sndr)));
        assert_eq!(named("bell.sndr", b"\x80\x80\x81\x80", None), None);

        let mut dvms = vec![0u8; 140];
        dvms[..13].copy_from_slice(b"/home/vagrant");
        dvms[38..54].copy_from_slice(b"Processed by SoX");
        let sum = dvms[..117]
            .iter()
            .fold(0u16, |sum, byte| sum.wrapping_add(u16::from(*byte)));
        dvms[118..120].copy_from_slice(&sum.to_le_bytes());
        assert_eq!(named("call.dvms", &dvms, None), Some(Kind::Sox(Sox::Dvms)));
        dvms[118] ^= 1;
        assert_eq!(named("call.dvms", &dvms, None), None);
        // A header of zeros adds up to zero, but isn't one.
        assert_eq!(named("call.dvms", &[0; 140], None), None);

        let mut ring = vec![0u8; 600];
        ring[..4].copy_from_slice(&32768u32.to_be_bytes());
        ring[16..24].copy_from_slice(b"ring.bin");
        assert_eq!(named("ring.gsrt", &ring, None), Some(Kind::Sox(Sox::Gsrt)));
        assert_eq!(named("ring.gsrt", &[0; 600], None), None);

        // Formats without a header are known by their name alone.
        let noise = b"\x58\xa6\xa5\x59\x5a\xa6\x65\x66";
        for (name, format) in [
            ("track.cdda", Sox::Cdda),
            ("radio.cvsd", Sox::Cvsd),
            ("radio.CVU", Sox::Cvu),
            ("radio.cvs", Sox::Cvsd),
            ("clip.fssd", Sox::Fssd),
            ("clip.sou", Sox::Fssd),
            ("take.sd2", Sox::Sd2),
        ] {
            assert_eq!(named(name, noise, None), Some(Kind::Sox(format)), "{name}");
        }
        // CorelDRAW's `.cdr` isn't CD audio, and `.ima` is mostly floppy
        // disk images, though SoX reads both as sound.
        assert_eq!(named("logo.cdr", noise, None), None);
        assert_eq!(named("disk.ima", noise, None), None);
    }

    fn video_tracks(codec: &str, pixels: &str, sound: Option<&str>) -> Tracks {
        Tracks {
            picture: Some(Picture {
                codec: codec.into(),
                pixels: pixels.into(),
                hdr: false,
            }),
            sound: sound.map(|codec| Track {
                codec: codec.into(),
                rate: 48000,
                channels: 6,
            }),
            video: true,
            ..Tracks::default()
        }
    }

    #[test]
    fn knows_videos_from_cameras_tv_and_without_a_container() {
        let mxf = b"\x06\x0e\x2b\x34\x02\x05\x01\x01\x0d\x01\x02\x01\x01\x02\x04\x00";
        assert_eq!(kind(mxf, false), Some(Kind::Video(Container::Mxf)));
        let mut wtv = WTV.to_vec();
        wtv.extend_from_slice(b"\x8c\xc3\xd2\xc2");
        assert_eq!(kind(&wtv, false), Some(Kind::Video(Container::Wtv)));

        let hevc = b"\0\0\0\x01\x40\x01\x0c\x01\xff\xff";
        assert_eq!(kind(hevc, false), None);
        assert_eq!(
            named("clip.hevc", hevc, None),
            Some(Kind::Video(Container::Hevc))
        );
        assert_eq!(
            named("clip.h265", hevc, None),
            Some(Kind::Video(Container::Hevc))
        );
        // Not HEVC's video settings.
        assert_eq!(named("clip.hevc", b"\0\0\0\x01\x67\x64", None), None);
        let h264 = b"\0\0\0\x01\x67\x64\x00\x1f";
        assert_eq!(
            named("clip.h264", h264, None),
            Some(Kind::Video(Container::H264))
        );
        assert_eq!(
            named("clip.h264", b"\0\0\x01\x09\xf0", None),
            Some(Kind::Video(Container::H264))
        );
        assert_eq!(named("notes.h264", b"#include", None), None);
        let dv = b"\x1f\x07\x00\x3f\xf8\x78\x78\x78";
        assert_eq!(named("tape.dv", dv, None), Some(Kind::Video(Container::Dv)));

        // A Motion JPEG stream starts as a photo does, so its name says.
        let jpeg = b"\xff\xd8\xff\xe0\0\x10JFIF";
        assert_eq!(kind(jpeg, false), Some(Kind::Image));
        assert_eq!(
            named("camera.mjpeg", jpeg, Some(Kind::Image)),
            Some(Kind::Video(Container::Mjpeg))
        );
        assert_eq!(
            named("photo.jpg", jpeg, Some(Kind::Image)),
            Some(Kind::Image)
        );
    }

    #[test]
    fn plays_copies_or_converts_each_video() {
        let copy = |webm, copy_sound| {
            Some(Recipe {
                webm,
                copy_picture: true,
                copy_sound,
            })
        };
        let convert = |copy_sound| {
            Some(Recipe {
                webm: false,
                copy_picture: false,
                copy_sound,
            })
        };
        let h264 = video_tracks("h264", "yuv420p", Some("aac"));
        // Browsers play these as they are.
        assert_eq!(recipe("clip.mp4", Container::Mov, &h264), None);
        assert_eq!(recipe("IMG_0001.MOV", Container::Mov, &h264), None);
        let vp9 = video_tracks("vp9", "yuv420p", Some("opus"));
        assert_eq!(recipe("talk.webm", Container::Matroska, &vp9), None);
        assert_eq!(
            recipe(
                "silent.mp4",
                Container::Mov,
                &video_tracks("h264", "yuvj420p", None)
            ),
            None
        );

        // The same codecs in another container are repackaged.
        assert_eq!(
            recipe("film.mkv", Container::Matroska, &h264),
            copy(false, true)
        );
        assert_eq!(
            recipe("talk.mkv", Container::Matroska, &vp9),
            copy(true, true)
        );
        assert_eq!(recipe("old.flv", Container::Flv, &h264), copy(false, true));
        // A camcorder's AC-3 sound is converted, and its picture copied.
        let camcorder = video_tracks("h264", "yuv420p", Some("ac3"));
        assert_eq!(
            recipe("00001.MTS", Container::MpegTs, &camcorder),
            copy(false, false)
        );
        assert_eq!(
            recipe("clip.mp4", Container::Mov, &camcorder),
            copy(false, false)
        );
        // AAC can't go in WebM.
        let av1 = video_tracks("av1", "yuv420p10le", Some("aac"));
        assert_eq!(
            recipe("clip.mkv", Container::Matroska, &av1),
            copy(true, false)
        );

        // Pictures not every browser plays are converted.
        let hevc = video_tracks("hevc", "yuv420p10le", Some("aac"));
        assert_eq!(recipe("IMG_0002.MOV", Container::Mov, &hevc), convert(true));
        let hi10 = video_tracks("h264", "yuv420p10le", Some("aac"));
        assert_eq!(
            recipe("anime.mkv", Container::Matroska, &hi10),
            convert(true)
        );
        let divx = video_tracks("mpeg4", "yuv420p", Some("mp3"));
        assert_eq!(recipe("movie.avi", Container::Avi, &divx), convert(true));
        let wmv = video_tracks("wmv3", "yuv420p", Some("wmav2"));
        assert_eq!(recipe("home.wmv", Container::Asf, &wmv), convert(false));
        // A WebM name doesn't make H.264 play as WebM.
        assert_eq!(
            recipe("odd.webm", Container::Matroska, &h264),
            copy(false, true)
        );
        // Only videos have a recipe.
        assert_eq!(recipe("song.mp4", Container::Mov, &Tracks::default()), None);
    }

    #[test]
    fn tells_ffmpeg_how_to_make_each_preview() {
        let text = |args: Vec<OsString>| {
            args.iter()
                .map(|arg| arg.to_string_lossy().into_owned())
                .collect::<Vec<_>>()
                .join(" ")
        };
        let file = Path::new("/data/blob");
        let out = Path::new("/scratch/video");
        let camcorder = video_tracks("h264", "yuv420p", Some("ac3"));
        let copied = text(video_args(
            file,
            "mpegts",
            "h264,ac3",
            &camcorder,
            &Recipe {
                webm: false,
                copy_picture: true,
                copy_sound: false,
            },
            out,
        ));
        assert!(
            copied.starts_with("-nostdin -v error -threads 4 -protocol_whitelist file -f mpegts")
        );
        assert!(copied.contains("-codec_whitelist h264,ac3 -i /data/blob"));
        assert!(copied.contains("-c:v copy"), "{copied}");
        // Surround sound is mixed down to stereo.
        assert!(
            copied.contains("-c:a aac -b:a 128k -ac 2 -ar 48000"),
            "{copied}"
        );
        assert!(copied.ends_with("-movflags +faststart -f mp4 /scratch/video"));

        let mut hdr = video_tracks("hevc", "yuv420p10le", Some("aac"));
        hdr.picture.as_mut().unwrap().hdr = true;
        let converted = text(video_args(
            file,
            "mov",
            "hevc,aac",
            &hdr,
            &Recipe {
                webm: false,
                copy_picture: false,
                copy_sound: true,
            },
            out,
        ));
        assert!(
            converted.contains("-c:v libx264 -preset veryfast -crf 23"),
            "{converted}"
        );
        assert!(
            converted.contains("bwdif=deint=interlaced,scale="),
            "{converted}"
        );
        assert!(converted.contains("tonemap=hable"), "{converted}");
        assert!(converted.contains("-fpsmax 60"), "{converted}");
        assert!(converted.contains("-c:a copy"), "{converted}");

        let webm = text(video_args(
            file,
            "matroska",
            "av1,aac",
            &video_tracks("av1", "yuv420p", Some("aac")),
            &Recipe {
                webm: true,
                copy_picture: true,
                copy_sound: false,
            },
            out,
        ));
        assert!(
            webm.contains("-c:a libopus -b:a 128k -ac 2 -ar 48000"),
            "{webm}"
        );
        assert!(webm.ends_with("-f webm /scratch/video"), "{webm}");
        // A video without sound has no sound settings.
        let silent = text(video_args(
            file,
            "mov",
            "h264",
            &video_tracks("mpeg4", "yuv420p", None),
            &Recipe {
                webm: false,
                copy_picture: false,
                copy_sound: true,
            },
            out,
        ));
        assert!(!silent.contains("-c:a"), "{silent}");
    }

    #[test]
    fn removes_the_least_recently_watched_videos_first() {
        let video = |byte: u8| ContentHash([byte; 32]);
        // Oldest first, as the database lists them.
        let videos = [(video(1), 300), (video(2), 500), (video(3), 200)];
        assert_eq!(over_limit(&videos, 900, 1000, None), vec![]);
        assert_eq!(over_limit(&videos, 1100, 1000, None), vec![video(1)]);
        assert_eq!(
            over_limit(&videos, 1500, 1000, None),
            vec![video(1), video(2)]
        );
        // The one just made stays, even if it's the oldest.
        assert_eq!(
            over_limit(&videos, 1100, 1000, Some(video(1))),
            vec![video(2)]
        );
    }

    #[test]
    fn names_when_videos_are_converted() {
        for choice in [
            VideoConversion::Upload,
            VideoConversion::Opened,
            VideoConversion::Never,
        ] {
            assert_eq!(VideoConversion::parse(choice.as_str()), Some(choice));
        }
        assert_eq!(VideoConversion::parse("always"), None);
        let automatic = VideoConversion::automatic();
        assert_eq!(
            automatic == VideoConversion::Upload,
            cores() >= UPLOAD_CORES
        );
    }

    #[test]
    fn reads_what_ffprobe_says_about_tracks() {
        let song = br#"{"streams": [
            {"codec_name": "mp3", "codec_type": "audio", "sample_rate": "44100", "channels": 2,
             "disposition": {"attached_pic": 0}},
            {"codec_name": "mjpeg", "codec_type": "video", "disposition": {"attached_pic": 1}}
        ], "format": {"duration": "105.772948"}}"#;
        assert_eq!(
            tracks(song).unwrap(),
            Tracks {
                sound: Some(Track {
                    codec: "mp3".into(),
                    rate: 44100,
                    channels: 2
                }),
                picture: None,
                cover: true,
                video: false,
                seconds: Some(105.772948),
            }
        );
        let film = br#"{"streams": [
            {"codec_name": "h264", "codec_type": "video", "pix_fmt": "yuv420p10le",
             "color_transfer": "arib-std-b67", "disposition": {"attached_pic": 0}},
            {"codec_name": "ac3", "codec_type": "audio", "sample_rate": "48000", "channels": 6,
             "disposition": {"attached_pic": 0}}
        ], "format": {}}"#;
        let film = tracks(film).unwrap();
        assert!(film.video && !film.cover);
        assert_eq!(
            film.picture,
            Some(Picture {
                codec: "h264".into(),
                pixels: "yuv420p10le".into(),
                hdr: true,
            })
        );
        assert_eq!(film.sound.unwrap().channels, 6);
        assert_eq!(tracks(br#"{"streams": []}"#).unwrap(), Tracks::default());
        assert!(tracks(b"not json").is_err());
    }

    #[test]
    fn converts_only_what_browsers_cant_play() {
        assert!(plays("Song.MP3", "mp3"));
        assert!(plays("Song.m4a", "aac"));
        assert!(plays("Ringtone.m4r", "aac"));
        assert!(plays("Song.opus", "opus"));
        assert!(plays("Song.oga", "flac"));
        assert!(plays("Take 1.wav", "pcm_s16le"));
        // Apple Lossless, MP2 named as MP3, and telephone WAV files aren't
        // what browsers play, nor any name Drive doesn't send as sound.
        assert!(!plays("Song.m4a", "alac"));
        assert!(!plays("Song.mp3", "mp2"));
        assert!(!plays("Call.wav", "adpcm_ms"));
        assert!(!plays("Speech.spx", "speex"));
        assert!(!plays("Song.ogg", "speex"));
        assert!(!plays("Ambisonic.amb", "pcm_s16le"));
        assert!(!plays("Song.wma", "wmav2"));
        assert!(!plays("Song", "mp3"));
    }

    #[test]
    fn keeps_a_sample_rate_aac_has() {
        assert_eq!(rate(44100), 44100);
        assert_eq!(rate(8000), 8000);
        assert_eq!(rate(22050), 22050);
        // More than people hear, and DSD, at 64 times a CD's rate.
        assert_eq!(rate(96000), 48000);
        assert_eq!(rate(2_822_400), 44100);
        assert_eq!(rate(352_800), 44100);
        assert_eq!(rate(6000), 48000);
        assert_eq!(rate(0), 48000);
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

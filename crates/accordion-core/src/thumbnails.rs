//! Thumbnails: a small picture of each photo, PDF, and video that's stored,
//! and the first lines of each text file, for Drive's grid now and Photos,
//! Mail, and Docs later (decisions 14 and 15 in the `accordion` repo's
//! `docs/decisions`).
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
//! PDF's first page, and ffmpeg for a frame from a video. ffmpeg is optional:
//! without it, videos have none, and they get another try once it's
//! installed. The server only reads a file's first bytes, to decide
//! whether there's anything to decode, and to take a text file's first lines,
//! which needs no decoder. Thumbnails keep no metadata from the file, such as
//! where a photo was taken.
//!
//! A file that fails isn't tried again until [`WORKER`] changes, such as
//! when an update makes thumbnails of more kinds of file.

use std::ffi::OsString;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use accordion_sandbox::{Job, Limits, Sandbox};
use rusqlite::Row;
use tokio::sync::watch;

use crate::db::{self, Database, Migration};
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
pub const WORKER: u32 = 3;

/// The longer side of a thumbnail, in pixels.
const SIZE: u32 = 480;

/// Larger files get no thumbnail.
const MAX_FILE: u64 = 200 * 1024 * 1024;

/// HEIC and AVIF files are decoded whole, so larger ones get no thumbnail.
const MAX_HEIF_PIXELS: u64 = 24_000_000;

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

/// The decoders ffmpeg may use for video: the codecs families' videos have.
const VIDEO_DECODERS: &str = "h264,hevc,vp8,vp9,av1,libdav1d,mpeg4,mpeg2video,mjpeg,prores";

/// ffprobe opens a decoder for every track, and stops at one it may not use,
/// so it may also use the decoders for the audio in those videos. ffmpeg
/// skips the audio.
const AUDIO_DECODERS: &str = "aac,mp3float,mp3,opus,libopus,vorbis,libvorbis,ac3,eac3,\
                              flac,alac,pcm_s16le,pcm_s16be,pcm_s24le,pcm_s24be,pcm_s32le,\
                              pcm_f32le,pcm_bluray";

/// Turns an HDR frame's colors into ordinary ones. It runs after the frame
/// is shrunk, since a whole 4K frame takes more memory than the limit.
const TONE_MAP: &str = "zscale=t=linear:npl=100,format=gbrpf32le,zscale=p=bt709,\
                        tonemap=hable:desat=0,zscale=t=bt709:m=bt709:r=tv,format=yuv420p";

/// Why a video has no thumbnail when ffmpeg isn't installed. Videos with
/// this reason get another try once it is.
const NO_FFMPEG: &str = "ffmpeg isn't installed";

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
    /// Optional: without ffmpeg, videos have no thumbnails.
    pub ffmpeg: PathBuf,
    pub ffprobe: PathBuf,
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
            ffmpeg: "/usr/bin/ffmpeg".into(),
            ffprobe: "/usr/bin/ffprobe".into(),
        }
    }

    fn has_ffmpeg(&self) -> bool {
        self.ffmpeg.is_file() && self.ffprobe.is_file()
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

/// How thumbnails are doing, for the health page.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ThumbnailStatus {
    /// Why no thumbnails are made, such as a missing program.
    pub unavailable: Option<String>,
    /// Why videos have no thumbnails, such as ffmpeg not being installed.
    pub videos: Option<String>,
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
enum Kind {
    /// JPEG, PNG, GIF, WebP, or TIFF.
    Image,
    /// HEIC or AVIF, which are decoded whole.
    Heif,
    Pdf,
    Video(Container),
    /// UTF-8 text, such as notes, Markdown, CSV, or code.
    Text,
}

/// The video formats families have. ffmpeg reads each with only the one
/// demuxer, so it never tries its others.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Container {
    /// QuickTime and MP4: `.mov`, `.mp4`, `.m4v`, and `.3gp`.
    Mov,
    /// `.mkv` and `.webm`.
    Matroska,
    Avi,
    /// MPEG transport streams, such as a camcorder's `.mts` files.
    MpegTs,
}

impl Container {
    fn format(self) -> &'static str {
        match self {
            Self::Mov => "mov",
            Self::Matroska => "matroska",
            Self::Avi => "avi",
            Self::MpegTs => "mpegts",
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
        // Thumbnails from a run that stopped partway.
        for entry in std::fs::read_dir(dir.join("partial"))? {
            let _ = std::fs::remove_file(entry?.path());
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
                    ..ThumbnailStatus::default()
                }),
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

    fn path(&self, hash: ContentHash, kind: ThumbnailKind) -> PathBuf {
        self.inner
            .dir
            .join(blobs::shard(hash))
            .with_extension(kind.extension())
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
        self.inner.status.send_if_modified(|status| {
            let changed =
                (status.waiting, status.failed, &status.videos) != (waiting, failed, &videos);
            status.waiting = waiting;
            status.failed = failed;
            status.videos = videos;
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
                )
            })
            .await?;
        Ok(())
    }

    /// Forgets contents that are gone from storage, and removes their
    /// thumbnails. Also forgets thumbnails that are missing from the cache,
    /// such as after a restore, since backups leave the cache out, and
    /// videos skipped without ffmpeg once it's installed, so they're made
    /// again.
    async fn tidy(&self) -> Result<(), Error> {
        if self.inner.tools.has_ffmpeg() {
            self.inner
                .db
                .write(|tx| {
                    tx.execute(
                        "DELETE FROM thumbnails WHERE state = 'none' AND reason = ?1",
                        [NO_FFMPEG],
                    )
                })
                .await?;
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
            for kind in [ThumbnailKind::Picture, ThumbnailKind::Text] {
                match std::fs::remove_file(self.path(hash, kind)) {
                    Err(err) if err.kind() != io::ErrorKind::NotFound => return Err(err.into()),
                    _ => {}
                }
            }
        }
        Ok(())
    }

    async fn make(&self, hash: ContentHash) -> Outcome {
        let file = self.inner.storage.contents_file(hash);
        let kind = match sniff(&file) {
            // Only the first lines are kept, so text files of any size have one.
            Ok(Some((Kind::Text, _, head))) => {
                return self.done(hash, ThumbnailKind::Text, &text(&head));
            }
            // ffmpeg reads a video's index and a few frames, not all of it.
            Ok(Some((kind, size, _))) if size > MAX_FILE && !matches!(kind, Kind::Video(_)) => {
                return Outcome::None("over 200 MB");
            }
            Ok(Some((kind, _, _))) => kind,
            Ok(None) => return Outcome::None("not a kind of file with thumbnails"),
            Err(err) => return Outcome::Failed(format!("couldn't read the file: {err}")),
        };
        let made = match kind {
            Kind::Image => self.thumbnail(std::fs::File::open(&file)).await,
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
        let mut png = self.frame(file, container, start, hdr).await?;
        if png.is_empty() && start > 0.0 {
            // The video is shorter than it says.
            png = self.frame(file, container, 0.0, hdr).await?;
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

    /// One frame from `start` seconds into a video, shrunk, as a PNG. Empty
    /// if there's no frame there.
    async fn frame(
        &self,
        file: &Path,
        container: Container,
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
            "-f",
            container.format(),
            "-codec_whitelist",
            VIDEO_DECODERS,
            "-ss",
            &start,
            "-i",
        ]
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
fn sniff(file: &Path) -> io::Result<Option<(Kind, u64, Vec<u8>)>> {
    let opened = std::fs::File::open(file)?;
    let size = opened.metadata()?.len();
    let mut head = Vec::with_capacity(HEAD);
    opened.take(HEAD as u64).read_to_end(&mut head)?;
    let whole = head.len() as u64 == size;
    Ok(kind(&head, whole).map(|kind| (kind, size, head)))
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
    // Transport stream packets start with 0x47 every 188 bytes, or every
    // 192 after a timestamp, as camcorders write them.
    let packets =
        |first: usize, size: usize| (0..3).all(|n| head.get(first + n * size) == Some(&0x47));
    (packets(0, 188) || packets(4, 192)).then_some(Container::MpegTs)
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

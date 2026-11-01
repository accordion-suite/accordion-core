//! Thumbnails: a small picture of each photo and PDF that's stored, for
//! Drive's grid now and Photos, Mail, and Docs later (decision 14 in the
//! `accordion` repo's `docs/decisions`).
//!
//! Each thumbnail is a WebP, at most 480 pixels on its longer side, named by
//! the hash of the contents it shows, so copies and unchanged versions share
//! one. They're kept in `cache/thumbnails/` in the data folder, which backups
//! leave out, because they can be made again. After a restore, the worker
//! finds them missing and makes them again.
//!
//! [`Thumbnails::run`] makes them in the background, one at a time, newest
//! files first. The decoding happens in a sandboxed worker
//! ([`accordion_sandbox`]): libvips for images, and poppler's `pdftoppm` for
//! a PDF's first page. The server only reads a file's first bytes, to decide
//! whether there's anything to decode. Thumbnails keep no metadata from the
//! file, such as where a photo was taken.
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
pub const WORKER: u32 = 1;

/// The longer side of a thumbnail, in pixels.
const SIZE: u32 = 480;

/// Larger files get no thumbnail.
const MAX_FILE: u64 = 200 * 1024 * 1024;

/// HEIC and AVIF files are decoded whole, so larger ones get no thumbnail.
const MAX_HEIF_PIXELS: u64 = 24_000_000;

/// What a worker may use for each thumbnail.
const LIMITS: Limits = Limits {
    memory: 1024 * 1024 * 1024,
    cpu: Duration::from_secs(20),
    time: Duration::from_secs(30),
    output: 10 * 1024 * 1024,
};

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
        }
    }
}

/// How thumbnails are doing, for the health page.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ThumbnailStatus {
    /// Why no thumbnails are made, such as a missing program.
    pub unavailable: Option<String>,
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
                    ..ThumbnailStatus::default()
                }),
            }),
        })
    }

    pub fn status(&self) -> watch::Receiver<ThumbnailStatus> {
        self.inner.status.subscribe()
    }

    /// The thumbnail of the contents with `hash`, a WebP file, if there is
    /// one yet.
    pub fn find(&self, hash: ContentHash) -> Option<PathBuf> {
        let path = self.path(hash);
        path.is_file().then_some(path)
    }

    fn path(&self, hash: ContentHash) -> PathBuf {
        self.inner
            .dir
            .join(blobs::shard(hash))
            .with_extension("webp")
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
        self.inner.status.send_if_modified(|status| {
            let changed = (status.waiting, status.failed) != (waiting, failed);
            status.waiting = waiting;
            status.failed = failed;
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
    /// such as after a restore, since backups leave the cache out, so
    /// they're made again.
    async fn tidy(&self) -> Result<(), Error> {
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
            .filter(|hash| !self.path(*hash).is_file())
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
            match std::fs::remove_file(self.path(hash)) {
                Err(err) if err.kind() != io::ErrorKind::NotFound => return Err(err.into()),
                _ => {}
            }
        }
        Ok(())
    }

    async fn make(&self, hash: ContentHash) -> Outcome {
        let file = self.inner.storage.contents_file(hash);
        let kind = match sniff(&file) {
            Ok(Some((_, size))) if size > MAX_FILE => return Outcome::None("over 200 MB"),
            Ok(Some((kind, _))) => kind,
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
        };
        match made {
            Ok(webp) => match self.keep(hash, &webp) {
                Ok(()) => Outcome::Ready,
                Err(err) => Outcome::Failed(format!("couldn't save the thumbnail: {err}")),
            },
            Err(problem) => Outcome::Failed(problem),
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
        let page = tempfile::tempfile_in(self.inner.dir.join("partial"))
            .and_then(|mut page| {
                io::Write::write_all(&mut page, &png)?;
                io::Seek::rewind(&mut page)?;
                Ok(page)
            })
            .map_err(|err| format!("couldn't keep the page: {err}"))?;
        self.thumbnail(Ok(page)).await
    }

    /// Saves a thumbnail in the cache, whole or not at all.
    fn keep(&self, hash: ContentHash, webp: &[u8]) -> io::Result<()> {
        let path = self.path(hash);
        std::fs::create_dir_all(path.parent().expect("a thumbnail is in a folder"))?;
        let mut partial = tempfile::NamedTempFile::new_in(self.inner.dir.join("partial"))?;
        io::Write::write_all(&mut partial, webp)?;
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

/// What kind of file this is, from its first bytes, and its size.
fn sniff(file: &Path) -> io::Result<Option<(Kind, u64)>> {
    let mut opened = std::fs::File::open(file)?;
    let size = opened.metadata()?.len();
    let mut head = [0u8; 16];
    let read = opened.read(&mut head)?;
    Ok(kind(&head[..read]).map(|kind| (kind, size)))
}

fn kind(head: &[u8]) -> Option<Kind> {
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
    None
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
        assert_eq!(kind(b"\xff\xd8\xff\xe0\0\x10JFIF"), Some(Kind::Image));
        assert_eq!(kind(b"\x89PNG\r\n\x1a\n\0\0"), Some(Kind::Image));
        assert_eq!(kind(b"GIF89a\x01\0"), Some(Kind::Image));
        assert_eq!(kind(b"RIFF\0\0\0\0WEBPVP8 "), Some(Kind::Image));
        assert_eq!(kind(b"II*\0\x08\0\0\0"), Some(Kind::Image));
        assert_eq!(kind(b"\0\0\0\x18ftypheic\0\0\0\0"), Some(Kind::Heif));
        assert_eq!(kind(b"\0\0\0\x1cftypavif\0\0\0\0"), Some(Kind::Heif));
        assert_eq!(kind(b"%PDF-1.7\n"), Some(Kind::Pdf));
        // A video in the same box format isn't a picture.
        assert_eq!(kind(b"\0\0\0\x18ftypmp42\0\0\0\0"), None);
        assert_eq!(kind(b"RIFF\0\0\0\0WAVEfmt "), None);
        assert_eq!(kind(b"PK\x03\x04"), None);
        assert_eq!(kind(b""), None);
    }
}

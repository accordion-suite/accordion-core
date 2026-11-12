//! The text inside stored files, for search (decision 20 in the `accordion`
//! repo's `docs/decisions`).
//!
//! [`Texts::run`] reads each file's text in the background, one at a time,
//! newest files first, and keeps it in an SQLite full-text index (FTS5) in
//! `cache/search.db` in the data folder. Text is kept by the hash of the
//! contents it came from, as thumbnails are, so copies share it, and the
//! index never says who can open what: a search finds the contents that
//! match, and storage finds the items with those contents that the person
//! can open. Backups leave the cache out, so after a restore the index is
//! made again.
//!
//! Plain text is read by the server itself, which needs no decoder. Other
//! files are untrusted, so they're read in the sandbox ([`accordion_sandbox`]):
//! poppler's `pdftotext` for PDFs, and a sandboxed copy of the server for
//! Word, Excel, PowerPoint, and OpenDocument files. Older Office files are
//! read from the PDF preview LibreOffice makes for their thumbnails.
//!
//! The first [`MAX_TEXT`] bytes of each file's text are kept.

pub mod office;

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
use crate::sql_now;
use crate::storage::{ContentHash, Storage};
use crate::thumbnails::{self, Kind, Thumbnails};

const OWNER: &str = "texts";

static MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    name: "Create texts",
    sql: "
    -- What happened to each stored file's contents. Contents with no row
    -- are waiting to be read.
    CREATE TABLE texts (
        hash BLOB PRIMARY KEY CHECK (length(hash) = 32),
        -- ready: the text is in the index. none: there's no text to read, or
        -- the file is too large. failed: the worker couldn't read it.
        state TEXT NOT NULL CHECK (state IN ('ready', 'none', 'failed')),
        -- The worker that decided, so a newer one tries again.
        worker INTEGER NOT NULL,
        -- Why there's none, or what went wrong.
        reason TEXT,
        made_at TEXT NOT NULL
    ) STRICT, WITHOUT ROWID;

    CREATE INDEX texts_failed ON texts (state) WHERE state = 'failed';
",
}];

const INDEX_OWNER: &str = "index";

static INDEX_MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    name: "Create the full-text index",
    sql: "
    -- The contents whose text is indexed, numbered for the index.
    CREATE TABLE contents (
        id INTEGER PRIMARY KEY,
        hash BLOB NOT NULL UNIQUE CHECK (length(hash) = 32)
    ) STRICT;

    -- Each one's text, by its number. Case and accents are ignored.
    CREATE VIRTUAL TABLE words USING fts5 (
        text,
        tokenize = 'unicode61 remove_diacritics 2'
    );
",
}];

/// The version of what reads text. Raise it when it can read files it
/// couldn't before, so they get another try.
pub const WORKER: u32 = 1;

/// The most text kept from each file, about 500 pages.
pub const MAX_TEXT: usize = 1024 * 1024;

/// Larger files aren't read, except plain text, of which only the start is.
const MAX_FILE: u64 = 200 * 1024 * 1024;

/// What a worker may use for each file.
const LIMITS: Limits = Limits {
    memory: 512 * 1024 * 1024,
    cpu: Duration::from_secs(30),
    time: Duration::from_secs(60),
    output: MAX_TEXT as u64,
};

/// Why an older Office file has no text yet: it's read from its preview,
/// which the thumbnail worker makes when LibreOffice is installed.
const NO_PREVIEW: &str = "waiting for the document's preview";

/// How many waiting files each look at the database fetches.
const BATCH: usize = 20;

/// How often the index loses the text of contents that are gone, and how
/// long the worker waits for a new file before looking anyway.
const TIDY_EVERY: Duration = Duration::from_secs(60 * 60);

/// Marks the start and end of matching words in a line of text. Text is
/// kept without control characters, so these never appear in it.
const MARK_START: char = '\u{1}';
const MARK_END: char = '\u{2}';

/// How many words a line of text around a match has.
const SNIPPET_WORDS: u32 = 16;

/// The programs that read text.
#[derive(Clone, Debug)]
pub struct Tools {
    /// The program that answers `sandbox` and [`office::COMMAND`]: the
    /// server itself.
    pub launcher: PathBuf,
    pub pdftotext: PathBuf,
}

impl Tools {
    /// The programs from the system's packages, with `launcher` as the
    /// sandbox.
    pub fn installed(launcher: impl Into<PathBuf>) -> Self {
        Self {
            launcher: launcher.into(),
            pdftotext: "/usr/bin/pdftotext".into(),
        }
    }
}

/// How reading text is doing, for the health page.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TextStatus {
    /// Why no text is read, such as a missing program.
    pub unavailable: Option<String>,
    /// Files waiting to be read.
    pub waiting: u64,
    /// Files the worker couldn't read.
    pub failed: u64,
    /// Why older Office files have no text, such as LibreOffice not being
    /// installed. Their text comes from their previews.
    pub documents: Option<String>,
    /// The last error from the database or the index, if the last round
    /// had one.
    pub last_error: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Db(#[from] db::Error),

    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),

    #[error("Accordion couldn't open its search index: {0}")]
    Io(#[from] io::Error),
}

/// What a search looks for: words and phrases, as someone typed them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Words(Vec<Term>);

/// One word, or a phrase in quotes.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Term {
    text: String,
    phrase: bool,
}

impl Words {
    /// The words in what someone typed. Text in double quotes is a phrase.
    /// Words are taken as words, never as commands.
    pub fn parse(typed: &str) -> Self {
        let mut terms = Vec::new();
        for (index, part) in typed.split('"').enumerate() {
            // Every other part is inside quotes. A quote with no partner
            // still starts a phrase, up to the end.
            if index % 2 == 1 {
                if part.chars().any(char::is_alphanumeric) {
                    terms.push(Term {
                        text: part.split_whitespace().collect::<Vec<_>>().join(" "),
                        phrase: true,
                    });
                }
            } else {
                terms.extend(
                    part.split_whitespace()
                        .filter(|word| word.chars().any(char::is_alphanumeric))
                        .map(|word| Term {
                            text: word.to_owned(),
                            phrase: false,
                        }),
                );
            }
        }
        Self(terms)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Whether every word is in `name`, ignoring case and accents, and any
    /// marks around it, such as the `*` in `budget*`.
    pub fn in_name(&self, name: &str) -> bool {
        let name = fold(name);
        self.0.iter().all(|term| {
            let word = term.text.trim_matches(|c: char| !c.is_alphanumeric());
            name.contains(&fold(word))
        })
    }

    /// The words as an FTS5 query: each word and phrase quoted, so nothing
    /// in it is taken as a command, and the last word matching the start of
    /// words, so results appear while someone is still typing. A start
    /// shorter than [`MIN_START`] matches only the whole word: "s" would
    /// match most of the index, which took over a minute on a Raspberry Pi.
    fn query(&self) -> String {
        let last = self.0.len().saturating_sub(1);
        self.0
            .iter()
            .enumerate()
            .map(|(index, term)| {
                let quoted = format!("\"{}\"", term.text.replace('"', "\"\""));
                if index == last && !term.phrase && starts_long_enough(&term.text) {
                    format!("{quoted}*")
                } else {
                    quoted
                }
            })
            .collect::<Vec<_>>()
            .join(" ")
    }
}

/// The fewest letters or digits a word needs to also match words that start
/// with it.
const MIN_START: usize = 3;

/// Whether the last part of `word` the index sees as a word is long enough
/// to match as a start. The index splits "it's" into "it" and "s", and only
/// "s" would match as a start.
fn starts_long_enough(word: &str) -> bool {
    use unicode_normalization::char::is_combining_mark;
    // Accents typed as separate marks belong to the letter before them.
    word.split(|c: char| !c.is_alphanumeric() && !is_combining_mark(c))
        .rfind(|part| !part.is_empty())
        .is_some_and(|part| part.chars().filter(|&c| !is_combining_mark(c)).count() >= MIN_START)
}

/// `text` in lowercase, without accents, for comparing names.
pub(crate) fn fold(text: &str) -> String {
    use unicode_normalization::UnicodeNormalization;
    use unicode_normalization::char::is_combining_mark;
    text.nfd()
        .filter(|&c| !is_combining_mark(c))
        .flat_map(char::to_lowercase)
        .collect()
}

/// The contents whose text matched a search, with how well.
#[derive(Clone, Debug, Default)]
pub struct Matched(HashMap<ContentHash, f64>);

impl Matched {
    /// How well the contents with `hash` matched, lower being better, or
    /// `None` if they didn't.
    pub fn rank(&self, hash: ContentHash) -> Option<f64> {
        self.0.get(&hash).copied()
    }
}

/// A line of text around where a search matched, in pieces.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Piece {
    pub text: String,
    /// Whether this piece is a word that matched.
    pub matched: bool,
}

/// The text service. Cloning it is cheap.
#[derive(Clone)]
pub struct Texts {
    inner: Arc<Inner>,
}

struct Inner {
    db: Database,
    /// `cache/search.db`.
    index: Database,
    storage: Storage,
    thumbnails: Thumbnails,
    tools: Tools,
    sandbox: Sandbox,
    status: watch::Sender<TextStatus>,
}

/// What became of one file.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Ready(String),
    None(&'static str),
    Failed(String),
}

impl Texts {
    /// Starts the service, with its index in `cache/search.db` under
    /// `data_dir`. Without the sandbox or `pdftotext`, it starts anyway and
    /// reads only plain text, and [`Texts::status`] says why.
    pub async fn open(
        db: Database,
        storage: Storage,
        thumbnails: Thumbnails,
        data_dir: &Path,
        tools: Tools,
    ) -> Result<Self, Error> {
        db.migrate(OWNER, MIGRATIONS).await?;
        let cache = data_dir.join("cache");
        std::fs::create_dir_all(&cache)?;
        let path = cache.join("search.db");
        let existed = path.exists();
        let index = match open_index(&path).await {
            Ok(index) => index,
            // It can be made again, so a damaged one is started afresh.
            Err(err) if existed => {
                tracing::warn!(%err, "starting the search index again");
                for suffix in ["", "-wal", "-shm"] {
                    let mut name = path.clone().into_os_string();
                    name.push(suffix);
                    match std::fs::remove_file(&name) {
                        Err(err) if err.kind() != io::ErrorKind::NotFound => {
                            return Err(err.into());
                        }
                        _ => {}
                    }
                }
                open_index(&path).await?
            }
            Err(err) => return Err(err),
        };
        // A new index, such as after a restore, has none of the text read
        // before. A newer worker may manage what an older one couldn't.
        let fresh = !existed || index_is_empty(&index).await?;
        db.write(move |tx| {
            if fresh {
                tx.execute("DELETE FROM texts WHERE state = 'ready'", [])?;
            }
            tx.execute(
                "DELETE FROM texts WHERE state != 'ready' AND worker < ?1",
                [WORKER],
            )
        })
        .await?;
        let unavailable = unavailable(&tools);
        if let Some(reason) = &unavailable {
            tracing::warn!("only plain text is searched: {reason}");
        }
        Ok(Self {
            inner: Arc::new(Inner {
                db,
                index,
                storage,
                thumbnails,
                sandbox: Sandbox::new(&tools.launcher),
                tools,
                status: watch::Sender::new(TextStatus {
                    unavailable,
                    ..TextStatus::default()
                }),
            }),
        })
    }

    pub fn status(&self) -> watch::Receiver<TextStatus> {
        self.inner.status.subscribe()
    }

    /// Reads text until `stopped` finishes: the newest waiting files
    /// first, then each new file as it's saved.
    pub async fn run(self, stopped: impl Future<Output = ()>) {
        let mut stopped = std::pin::pin!(stopped);
        let mut saved = self.inner.storage.saved();
        // Older Office files wait for their previews.
        let mut previews = self.inner.thumbnails.status();
        let mut tidied_at: Option<tokio::time::Instant> = None;
        loop {
            // Before looking, so a file saved after the look wakes the
            // worker below.
            saved.mark_unchanged();
            previews.mark_unchanged();
            if tidied_at.is_none_or(|at| at.elapsed() >= TIDY_EVERY) {
                let result = self.tidy().await;
                self.note(result.err());
                tidied_at = Some(tokio::time::Instant::now());
            }
            let result = self.previews_made().await;
            self.note(result.err());
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
                    outcome = self.read(*hash) => outcome,
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
                    _ = previews.changed() => {}
                    () = tokio::time::sleep(TIDY_EVERY) => {}
                }
            }
        }
    }

    /// Reads the text of the contents with `hash` now, whether or not
    /// they're waiting, and records how it went. Returns whether there's
    /// text.
    pub async fn make_now(&self, hash: ContentHash) -> Result<bool, Error> {
        let outcome = self.read(hash).await;
        let ready = matches!(outcome, Outcome::Ready(_));
        self.record(hash, outcome).await?;
        Ok(ready)
    }

    /// The contents whose text has every one of `words`.
    pub async fn matching(&self, words: &Words) -> Result<Matched, Error> {
        if words.is_empty() {
            return Ok(Matched::default());
        }
        let query = words.query();
        self.inner
            .index
            .read(move |conn| {
                let mut statement = conn.prepare(
                    "SELECT c.hash, bm25(words) FROM words
                     JOIN contents c ON c.id = words.rowid
                     WHERE words MATCH ?1",
                )?;
                let ranks = statement
                    .query_map([&query], |row| Ok((hash(row, 0)?, row.get::<_, f64>(1)?)))?
                    .collect::<Result<HashMap<_, _>, _>>()?;
                Ok(Matched(ranks))
            })
            .await
    }

    /// A line of text from each of `hashes` around where `words` matched.
    /// Contents the words don't match have none.
    pub async fn snippets(
        &self,
        words: &Words,
        hashes: Vec<ContentHash>,
    ) -> Result<HashMap<ContentHash, Vec<Piece>>, Error> {
        if words.is_empty() || hashes.is_empty() {
            return Ok(HashMap::new());
        }
        let query = words.query();
        self.inner
            .index
            .read(move |conn| {
                let mut id_of = conn.prepare("SELECT id FROM contents WHERE hash = ?1")?;
                let mut hash_of = HashMap::new();
                for hash in hashes {
                    let id: Option<i64> = id_of
                        .query_row([&hash.0[..]], |row| row.get(0))
                        .optional()?;
                    if let Some(id) = id {
                        hash_of.insert(id, hash);
                    }
                }
                if hash_of.is_empty() {
                    return Ok(HashMap::new());
                }
                let ids = format!(
                    "[{}]",
                    hash_of
                        .keys()
                        .map(i64::to_string)
                        .collect::<Vec<_>>()
                        .join(",")
                );
                // One pass over the matches. Looking up each content on
                // its own made FTS5 read every match again each time, which
                // took 5 seconds for a common word on a Raspberry Pi. The
                // `+` keeps SQLite from turning the list into lookups.
                let mut statement = conn.prepare(&format!(
                    "SELECT rowid, snippet(words, 0, char({}), char({}), '…', {SNIPPET_WORDS})
                     FROM words
                     WHERE words MATCH ?1
                       AND +rowid IN (SELECT value FROM json_each(?2))",
                    u32::from(MARK_START),
                    u32::from(MARK_END),
                ))?;
                let mut snippets = HashMap::new();
                let mut rows = statement.query((&query, &ids))?;
                while let Some(row) = rows.next()? {
                    let id: i64 = row.get(0)?;
                    let line: String = row.get(1)?;
                    if let Some(hash) = hash_of.get(&id) {
                        snippets.insert(*hash, pieces(&line));
                    }
                }
                Ok(snippets)
            })
            .await
    }

    fn note(&self, err: Option<Error>) {
        if let Some(err) = &err {
            tracing::warn!(%err, "reading text for search ran into a problem");
        }
        self.inner.status.send_if_modified(|status| {
            let last_error = err.as_ref().map(ToString::to_string);
            let changed = status.last_error != last_error;
            status.last_error = last_error;
            changed
        });
    }

    /// The newest stored contents that a file has now, with no decision
    /// yet. Older versions aren't searched.
    async fn waiting(&self, limit: usize) -> Result<Vec<ContentHash>, Error> {
        self.inner
            .db
            .read(move |conn| {
                conn.prepare(&format!(
                    "SELECT b.hash FROM storage_blobs b
                     WHERE {WAITING}
                     ORDER BY b.created_at DESC
                     LIMIT ?1"
                ))?
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
                    &format!(
                        "SELECT
                           (SELECT count(*) FROM storage_blobs b WHERE {WAITING}),
                           (SELECT count(*) FROM texts WHERE state = 'failed')"
                    ),
                    [],
                    |row| Ok((row.get::<_, i64>(0)? as u64, row.get::<_, i64>(1)? as u64)),
                )
                .map_err(Error::from)
            })
            .await?;
        let documents = self.inner.thumbnails.status().borrow().documents.clone();
        self.inner.status.send_if_modified(|status| {
            let changed =
                (status.waiting, status.failed, &status.documents) != (waiting, failed, &documents);
            status.waiting = waiting;
            status.failed = failed;
            status.documents = documents;
            changed
        });
        Ok(())
    }

    async fn record(&self, hash: ContentHash, outcome: Outcome) -> Result<(), Error> {
        let (state, reason) = match outcome {
            Outcome::Ready(text) => {
                // The index first, so the text is there before it's said
                // to be. If the server stops between the two, it's read
                // again.
                self.inner
                    .index
                    .write(move |tx| {
                        tx.execute(
                            "INSERT INTO contents (hash) VALUES (?1) ON CONFLICT DO NOTHING",
                            [&hash.0[..]],
                        )?;
                        let id: i64 = tx.query_row(
                            "SELECT id FROM contents WHERE hash = ?1",
                            [&hash.0[..]],
                            |row| row.get(0),
                        )?;
                        tx.execute("DELETE FROM words WHERE rowid = ?1", [id])?;
                        tx.execute(
                            "INSERT INTO words (rowid, text) VALUES (?1, ?2)",
                            (id, text),
                        )?;
                        Ok::<_, rusqlite::Error>(())
                    })
                    .await?;
                ("ready", None)
            }
            Outcome::None(reason) => ("none", Some(reason.to_owned())),
            Outcome::Failed(problem) => {
                tracing::info!(contents = %hash, %problem, "couldn't read a file's text");
                ("failed", Some(problem))
            }
        };
        self.inner
            .db
            .write(move |tx| {
                tx.execute(
                    concat!(
                        "INSERT INTO texts (hash, state, worker, reason, made_at)
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

    /// Lets older Office files whose previews are ready be read.
    async fn previews_made(&self) -> Result<(), Error> {
        self.inner
            .db
            .write(|tx| {
                tx.execute(
                    "DELETE FROM texts
                     WHERE state = 'none' AND reason = ?1
                       AND hash IN (SELECT hash FROM thumbnails WHERE state = 'ready')",
                    [NO_PREVIEW],
                )
            })
            .await?;
        Ok(())
    }

    /// Forgets contents that are gone from storage, and removes their text.
    async fn tidy(&self) -> Result<(), Error> {
        let gone = self
            .inner
            .db
            .write(|tx| {
                tx.prepare(
                    "DELETE FROM texts
                     WHERE NOT EXISTS (SELECT 1 FROM storage_blobs b WHERE b.hash = texts.hash)
                     RETURNING hash, state",
                )?
                .query_map([], |row| Ok((hash(row, 0)?, row.get::<_, String>(1)?)))?
                .filter_map(|row| match row {
                    Ok((hash, state)) => (state == "ready").then_some(Ok(hash)),
                    Err(err) => Some(Err(err)),
                })
                .collect::<Result<Vec<_>, _>>()
            })
            .await?;
        if !gone.is_empty() {
            self.inner
                .index
                .write(move |tx| {
                    let mut find = tx.prepare("SELECT id FROM contents WHERE hash = ?1")?;
                    for hash in &gone {
                        let Some(id) = find
                            .query_row([&hash.0[..]], |row| row.get::<_, i64>(0))
                            .optional()?
                        else {
                            continue;
                        };
                        tx.execute("DELETE FROM words WHERE rowid = ?1", [id])?;
                        tx.execute("DELETE FROM contents WHERE id = ?1", [id])?;
                    }
                    Ok::<_, rusqlite::Error>(())
                })
                .await?;
        }
        Ok(())
    }

    async fn read(&self, hash: ContentHash) -> Outcome {
        let file = self.inner.storage.contents_file(hash);
        let (kind, size, head) = match thumbnails::sniff(&file) {
            Ok(sniffed) => sniffed,
            Err(err) => return Outcome::Failed(format!("couldn't read the file: {err}")),
        };
        // A document is known by its name as well as its first bytes.
        let kind = if thumbnails::FORMATS
            .iter()
            .any(|format| head.starts_with(format.signature))
        {
            let name = self
                .inner
                .db
                .read(move |conn| thumbnails::newest_name(conn, hash))
                .await;
            match name {
                Ok(name) => name
                    .and_then(|name| thumbnails::format(&name, &head))
                    .map(Kind::Document)
                    .or(kind),
                Err(err) => {
                    return Outcome::Failed(format!("couldn't read the file's name: {err}"));
                }
            }
        } else {
            kind
        };
        let available = self.inner.status.borrow().unavailable.is_none();
        let text = match kind {
            // Only the start is read, so text files of any size have text.
            Some(Kind::Text) => plain_text(&file),
            Some(Kind::Pdf | Kind::Document(_)) if !available => {
                return Outcome::None("the sandbox or pdftotext isn't available");
            }
            Some(Kind::Pdf | Kind::Document(_)) if size > MAX_FILE => {
                return Outcome::None("over 200 MB");
            }
            Some(Kind::Pdf) => self.pdf(&file).await,
            Some(Kind::Document(format)) if office::EXTENSIONS.contains(&format.extension) => {
                self.office(&file, format.extension).await
            }
            Some(Kind::Document(_)) => match self.inner.thumbnails.preview(hash) {
                Some(preview) => self.pdf(&preview.path).await,
                None => return Outcome::None(NO_PREVIEW),
            },
            _ => return Outcome::None("not a kind of file with text"),
        };
        match text {
            Ok(text) => {
                let text = clean(&text);
                if text.trim().is_empty() {
                    // Such as a scanned PDF, which has only pictures.
                    Outcome::None("no text in it")
                } else {
                    Outcome::Ready(text)
                }
            }
            Err(problem) => Outcome::Failed(problem),
        }
    }

    /// A PDF's text, from poppler.
    async fn pdf(&self, file: &Path) -> Result<String, String> {
        let input =
            std::fs::File::open(file).map_err(|err| format!("couldn't open the file: {err}"))?;
        let args = ["-q", "-enc", "UTF-8", "-", "-"].map(OsString::from);
        let output = self.work(&self.inner.tools.pdftotext, &args, input).await?;
        Ok(lossy(&output))
    }

    /// An Office file's text, from a sandboxed copy of the server.
    async fn office(&self, file: &Path, extension: &str) -> Result<String, String> {
        let input =
            std::fs::File::open(file).map_err(|err| format!("couldn't open the file: {err}"))?;
        let args = [office::COMMAND, extension].map(OsString::from);
        let output = self.work(&self.inner.tools.launcher, &args, input).await?;
        Ok(lossy(&output))
    }

    /// Runs a program in the sandbox, keeping the first [`MAX_TEXT`] bytes
    /// it writes.
    async fn work(
        &self,
        program: &Path,
        args: &[OsString],
        input: std::fs::File,
    ) -> Result<Vec<u8>, String> {
        self.inner
            .sandbox
            .run_up_to(Job {
                program,
                args,
                env: &[("MALLOC_ARENA_MAX", "2")],
                input,
                read: &[],
                write: &[],
                sockets: None,
                limits: LIMITS,
            })
            .await
            .map_err(|err| err.to_string())
    }
}

/// Files with contents that a file has now and that have no decision yet,
/// as `b`.
const WAITING: &str = "b.damage IS NULL
    AND EXISTS (SELECT 1 FROM storage_versions v
                WHERE v.blob = b.hash
                  AND v.number = (SELECT max(number) FROM storage_versions
                                  WHERE item_id = v.item_id))
    AND NOT EXISTS (SELECT 1 FROM texts t WHERE t.hash = b.hash)";

async fn open_index(path: &Path) -> Result<Database, Error> {
    let index = Database::open(path)?;
    index.migrate(INDEX_OWNER, INDEX_MIGRATIONS).await?;
    Ok(index)
}

async fn index_is_empty(index: &Database) -> Result<bool, Error> {
    index
        .read(|conn| {
            conn.query_row("SELECT NOT EXISTS (SELECT 1 FROM contents)", [], |row| {
                row.get(0)
            })
            .map_err(Error::from)
        })
        .await
}

/// Why only plain text can be read here, if that's the case.
fn unavailable(tools: &Tools) -> Option<String> {
    if let Err(err) = accordion_sandbox::check() {
        return Some(format!("{err}."));
    }
    (!tools.pdftotext.is_file())
        .then(|| "pdftotext isn't installed. Install poppler-utils with apt.".to_owned())
}

/// The start of a text file, up to [`MAX_TEXT`] bytes. Reading text needs
/// no decoder, so it skips the sandbox, as text thumbnails do.
fn plain_text(file: &Path) -> Result<String, String> {
    let mut bytes = Vec::new();
    std::fs::File::open(file)
        .and_then(|opened| opened.take(MAX_TEXT as u64).read_to_end(&mut bytes))
        .map_err(|err| format!("couldn't read the file: {err}"))?;
    Ok(lossy(&bytes))
}

/// `bytes` as text, without a character cut off at the end, and with any
/// that aren't UTF-8 replaced.
fn lossy(bytes: &[u8]) -> String {
    let end = match std::str::from_utf8(bytes) {
        Ok(_) => bytes.len(),
        Err(err) if err.error_len().is_none() => err.valid_up_to(),
        Err(_) => bytes.len(),
    };
    String::from_utf8_lossy(&bytes[..end]).into_owned()
}

/// Text as it's kept: without a byte order mark or control characters,
/// such as the page breaks `pdftotext` writes, and no longer than
/// [`MAX_TEXT`].
fn clean(text: &str) -> String {
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);
    let mut kept: String = text
        .chars()
        .map(|c| {
            if c.is_control() && !matches!(c, '\n' | '\t') {
                ' '
            } else {
                c
            }
        })
        .collect();
    let mut end = kept.len().min(MAX_TEXT);
    while !kept.is_char_boundary(end) {
        end -= 1;
    }
    kept.truncate(end);
    kept
}

/// A line from FTS5's `snippet`, split at the marks around matching words.
fn pieces(line: &str) -> Vec<Piece> {
    let mut pieces = Vec::new();
    let mut matched = false;
    let mut text = String::new();
    for c in line.chars() {
        let mark = match c {
            MARK_START => Some(true),
            MARK_END => Some(false),
            // Lines run together, as they do in search results.
            '\n' | '\t' => {
                if !text.ends_with(' ') {
                    text.push(' ');
                }
                None
            }
            c => {
                text.push(c);
                None
            }
        };
        if let Some(next) = mark {
            if !text.is_empty() {
                pieces.push(Piece {
                    text: std::mem::take(&mut text),
                    matched,
                });
            }
            matched = next;
        }
    }
    if !text.is_empty() {
        pieces.push(Piece { text, matched });
    }
    pieces
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
    fn takes_words_as_words() {
        let words = Words::parse(r#"  tax "Tax return"  AND budg* - "#);
        assert_eq!(words.query(), r#""tax" "Tax return" "AND" "budg*"*"#);
        // A quote with no partner still makes a phrase, and quotes inside
        // a word are doubled, as FTS5 wants.
        assert_eq!(Words::parse(r#"say"hi"#).query(), r#""say" "hi""#);
        assert!(Words::parse(r#" " - * "#).is_empty());
    }

    #[test]
    fn matches_starts_of_words_from_three_letters() {
        assert_eq!(Words::parse("s").query(), r#""s""#);
        assert_eq!(Words::parse("st").query(), r#""st""#);
        assert_eq!(Words::parse("ste").query(), r#""ste"*"#);
        assert_eq!(Words::parse("tax st").query(), r#""tax" "st""#);
        // The index splits "it's" into "it" and "s", so only "s" would
        // match as a start.
        assert_eq!(Words::parse("it's").query(), r#""it's""#);
        assert_eq!(Words::parse("2026-q1").query(), r#""2026-q1""#);
        assert_eq!(Words::parse("2026-qtr").query(), r#""2026-qtr"*"#);
        // Letters count, not bytes or separate accent marks.
        assert_eq!(Words::parse("né").query(), r#""né""#);
        assert_eq!(Words::parse("ne\u{301}e").query(), "\"ne\u{301}e\"*");
        assert_eq!(Words::parse("日本語").query(), r#""日本語"*"#);
    }

    #[test]
    fn finds_words_in_names_ignoring_case_and_accents() {
        let words = Words::parse("resume CAFE");
        assert!(words.in_name("Café Résumé.docx"));
        assert!(!words.in_name("Résumé.docx"));
        assert!(Words::parse(r#""2025 tax""#).in_name("Our 2025 Taxes.pdf"));
        assert!(!Words::parse(r#""tax 2025""#).in_name("Our 2025 Taxes.pdf"));
        assert!(Words::default().in_name("anything"));
    }

    #[test]
    fn keeps_text_without_control_characters() {
        assert_eq!(
            clean("\u{feff}Page one\u{c}Page two\r\n"),
            "Page one Page two \n"
        );
        let long = "é".repeat(MAX_TEXT);
        assert_eq!(clean(&long).len(), MAX_TEXT);
        assert_eq!(lossy(b"Caf\xc3"), "Caf");
        assert_eq!(lossy(b"Caf\xe9 au lait"), "Caf\u{fffd} au lait");
    }

    #[test]
    fn splits_snippets_at_the_marks() {
        let line = "…the \u{1}budget\u{2} for\nthe \u{1}lake\u{2}";
        let pieces = pieces(line);
        let found: Vec<(&str, bool)> = pieces
            .iter()
            .map(|piece| (piece.text.as_str(), piece.matched))
            .collect();
        assert_eq!(
            found,
            [
                ("…the ", false),
                ("budget", true),
                (" for the ", false),
                ("lake", true)
            ]
        );
    }
}

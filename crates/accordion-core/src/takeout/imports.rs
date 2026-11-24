//! The import job (decision 34): reads an export's archives in batches,
//! hands each item to the module that converts its product, keeps what no
//! module converts in **Unconverted**, and writes a report.
//!
//! Imports run one at a time, in the background, and pick up where they
//! stopped after a restart. A table records what each person's imports
//! brought, by product and by the key the product gave each item, so
//! importing again, even from a newer export, skips what's already here.
//!
//! Each item is stored, then recorded, so a server that stops between the
//! two brings that one item twice. Everything else comes over once.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::pin::{Pin, pin};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use accordion_types::{ItemId, UserId};
use rusqlite::{Connection, OptionalExtension};
use serde_json::{Value, json};
use tokio::sync::Notify;
use uuid::Uuid;

use super::{FolderFound, Plan, Take, Takeout, Unpacked, UnpackedFile};
use crate::db::{self, Database, Migration};
use crate::events::{Action, Actor, Events, NewEvent, Via};
use crate::live::{self, Hint};
use crate::sql_now;
use crate::storage::{self, ContentHash, Dates, Item, ItemKind, MAX_NAME_LEN, Storage};

const OWNER: &str = "takeout";

static MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    name: "Create Takeout's import tables",
    sql: "
    -- One row for each import someone started.
    CREATE TABLE takeout_imports (
        id BLOB PRIMARY KEY,
        user_id BLOB NOT NULL,
        -- The archives, in the export's order, as JSON: each part's item
        -- and the hash of its contents when the import started, so a part
        -- that changes stops the import rather than mixing two exports.
        parts TEXT NOT NULL,
        -- The products that may claim folders, as JSON, so every batch
        -- plans as the first did, even after a module is turned on or off.
        products TEXT NOT NULL,
        -- What to bring, as JSON: products and folders no product claims.
        take TEXT NOT NULL,
        state TEXT NOT NULL CHECK (state IN ('running', 'canceling', 'done', 'canceled', 'failed')),
        -- How many items it takes, once the first pass has counted them.
        items INTEGER,
        -- The item the next batch starts at, and how many are handled.
        next INTEGER NOT NULL DEFAULT 0,
        done INTEGER NOT NULL DEFAULT 0,
        -- What came over, as JSON, for the report.
        counts TEXT NOT NULL DEFAULT '{}',
        -- Report lines written, and those past the limit, left out.
        lines INTEGER NOT NULL DEFAULT 0,
        dropped INTEGER NOT NULL DEFAULT 0,
        started_at TEXT NOT NULL,
        finished_at TEXT,
        error TEXT,
        report_id BLOB
    ) STRICT;
    CREATE INDEX takeout_imports_user ON takeout_imports (user_id, started_at);
    CREATE INDEX takeout_imports_running ON takeout_imports (started_at)
        WHERE state IN ('running', 'canceling');

    -- What each person's imports brought, so importing again skips it.
    CREATE TABLE takeout_imported (
        user_id BLOB NOT NULL,
        -- The product, or 'other' for a file no product claims.
        product TEXT NOT NULL,
        key TEXT NOT NULL,
        import_id BLOB NOT NULL,
        -- Where it was in that import, which tells a batch started again
        -- after a restart from two items with one key in one export.
        position INTEGER NOT NULL,
        -- What the module stored, when it says.
        item_id BLOB,
        PRIMARY KEY (user_id, product, key)
    ) STRICT, WITHOUT ROWID;

    -- The report's lines, kept until the import ends.
    CREATE TABLE takeout_report (
        import_id BLOB NOT NULL REFERENCES takeout_imports (id) ON DELETE CASCADE,
        line INTEGER NOT NULL,
        heading TEXT NOT NULL,
        text TEXT NOT NULL,
        PRIMARY KEY (import_id, line)
    ) STRICT, WITHOUT ROWID;
",
}];

/// How much one batch unpacks. Each batch reads a `.tgz` from its start, so
/// bigger batches mean fewer passes, and more room taken while one waits.
pub const BATCH_BYTES: u64 = 512 << 20;

/// The most archives one export can have.
pub const MAX_PARTS: usize = 100;

/// The most lines a report lists. The rest are counted.
const MAX_REPORT_LINES: i64 = 20_000;

/// The folder in My Drive for archives, reports, and **Unconverted**.
pub const FOLDER: &str = "Google Takeout";
/// The folder in [`FOLDER`] for files no module converts.
pub const UNCONVERTED: &str = "Unconverted";

/// The report's own headings. Modules add theirs between "Not converted"
/// and "Names that were shortened".
const NOT_CONVERTED: &str = "Not converted";
const SHORTENED: &str = "Names that were shortened";
const ALREADY: &str = "Brought by an earlier import";
const LEFT_OUT: &str = "Left out of the archives";

/// How long the job waits after a database error before trying again.
const RETRY_AFTER: Duration = Duration::from_secs(60);

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Converts and stores one product, such as Keep's notes. A module adds one
/// with [`Imports::add_importer`] when it starts, beside the [`super::Product`]
/// that finds the product's items in the sandbox.
pub trait Importer: Send + Sync + 'static {
    /// The product it stores, as its [`super::Product::id`].
    fn product(&self) -> &'static str;

    /// The app that converts it, such as "Notes", for when it's off.
    fn app(&self) -> &'static str;

    /// What the report says came over, in Markdown, such as "21 notes from
    /// Keep, in Notes".
    fn brought(&self, count: u64) -> String;

    /// The item's name in the report, in Markdown, such as a note's title.
    /// The item is untrusted, so [`escape`] anything taken from it.
    fn label(&self, item: &Unpacked) -> String {
        escape(&item.key)
    }

    /// Checks and stores one item, returning what it became when there's
    /// one item to name. Anything it says goes in the report through
    /// `importing`.
    fn store<'a>(
        &'a self,
        item: &'a Unpacked,
        importing: &'a Importing,
    ) -> BoxFuture<'a, Result<Option<ItemId>, ItemError>>;
}

/// Why an item didn't come over.
#[derive(Debug, PartialEq, Eq)]
pub enum ItemError {
    /// The item can't be converted. Its files go to **Unconverted**, and
    /// the report says why, in Markdown.
    NotConverted(String),
    /// The import can't go on, such as when the person's storage is full.
    /// The report says why.
    Stop(String),
}

impl From<storage::Error> for ItemError {
    fn from(err: storage::Error) -> Self {
        match err {
            storage::Error::QuotaFull
            | storage::Error::UserNotFound
            | storage::Error::Io(_)
            | storage::Error::Database(_)
            | storage::Error::Identity(_) => Self::Stop(err.to_string()),
            err => Self::NotConverted(escape(&err.to_string())),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Choose the export's archives to import.")]
    NoParts,

    #[error("An export can have up to {MAX_PARTS} archives. Make a new export with bigger ones.")]
    TooManyParts,

    #[error(
        "That archive isn't in your Drive. It may have been moved to the trash or deleted. \
         Choose the archives again."
    )]
    PartNotFound,

    #[error("That's a folder, not an archive. Choose the export's .zip or .tgz files.")]
    PartNotAFile,

    #[error("Choose at least one thing to import.")]
    NothingChosen,

    #[error(
        "Accordion can't convert that now. The app that does may have been turned off. \
         Load the page again, then try again."
    )]
    UnknownProduct(String),

    #[error("An import is already running. Wait for it to finish, or cancel it.")]
    Busy,

    #[error("That import isn't running.")]
    NotRunning,

    #[error(transparent)]
    Read(#[from] super::Error),

    #[error(transparent)]
    Storage(#[from] storage::Error),

    #[error(transparent)]
    Database(#[from] db::Error),
}

impl From<rusqlite::Error> for Error {
    fn from(err: rusqlite::Error) -> Self {
        Self::Database(err.into())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    Running,
    /// Canceled while running. It stops after the item it's on, and writes
    /// its report.
    Canceling,
    Done,
    Canceled,
    /// It stopped, and [`Import::error`] says why.
    Failed,
}

impl State {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Canceling => "canceling",
            Self::Done => "done",
            Self::Canceled => "canceled",
            Self::Failed => "failed",
        }
    }

    fn from_str(text: &str) -> Option<Self> {
        [
            Self::Running,
            Self::Canceling,
            Self::Done,
            Self::Canceled,
            Self::Failed,
        ]
        .into_iter()
        .find(|state| state.as_str() == text)
    }
}

/// An import someone started.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Import {
    pub id: Uuid,
    pub state: State,
    /// How many items it takes, once it has counted them.
    pub items: Option<u64>,
    /// How many items it has handled so far.
    pub done: u64,
    pub started_at: String,
    pub finished_at: Option<String>,
    /// Why it stopped, when it failed.
    pub error: Option<String>,
    /// Its report, once it's saved.
    pub report: Option<ItemId>,
}

/// Imports from Google Takeout. Cloning it is cheap.
#[derive(Clone)]
pub struct Imports {
    inner: Arc<Inner>,
}

struct Inner {
    db: Database,
    storage: Storage,
    events: Events,
    takeout: Takeout,
    importers: Mutex<BTreeMap<&'static str, Arc<dyn Importer>>>,
    wake: Notify,
    batch_bytes: u64,
}

/// Keeps an importer added until it's dropped, such as when its module is
/// turned off.
#[must_use = "the importer is removed when this is dropped"]
pub struct Registered {
    inner: Weak<Inner>,
    importer: Arc<dyn Importer>,
}

impl Drop for Registered {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.upgrade() {
            let mut importers = inner.importers.lock().expect("importers lock");
            let product = self.importer.product();
            if importers
                .get(product)
                .is_some_and(|known| Arc::ptr_eq(known, &self.importer))
            {
                importers.remove(product);
            }
        }
    }
}

impl Imports {
    /// Creates the import tables, reading archives with `takeout`.
    pub async fn open(
        db: Database,
        storage: Storage,
        events: Events,
        takeout: Takeout,
    ) -> Result<Self, Error> {
        db.migrate(OWNER, MIGRATIONS).await?;
        Ok(Self {
            inner: Arc::new(Inner {
                db,
                storage,
                events,
                takeout,
                importers: Mutex::new(BTreeMap::new()),
                wake: Notify::new(),
                batch_bytes: BATCH_BYTES,
            }),
        })
    }

    /// The same imports, unpacking `bytes` at a time, for tests that want
    /// many batches. Call it before anything else.
    pub fn with_batch_bytes(self, bytes: u64) -> Self {
        let inner = Arc::try_unwrap(self.inner)
            .unwrap_or_else(|_| panic!("set the batch size before using the imports"));
        Self {
            inner: Arc::new(Inner {
                batch_bytes: bytes,
                ..inner
            }),
        }
    }

    /// Adds a module's importer until the returned guard is dropped.
    pub fn add_importer(&self, importer: Arc<dyn Importer>) -> Registered {
        self.inner
            .importers
            .lock()
            .expect("importers lock")
            .insert(importer.product(), Arc::clone(&importer));
        Registered {
            inner: Arc::downgrade(&self.inner),
            importer,
        }
    }

    /// The products that can be converted now, in order.
    pub fn products(&self) -> Vec<&'static str> {
        self.inner
            .importers
            .lock()
            .expect("importers lock")
            .keys()
            .copied()
            .collect()
    }

    fn importer(&self, product: &str) -> Option<Arc<dyn Importer>> {
        self.inner
            .importers
            .lock()
            .expect("importers lock")
            .get(product)
            .cloned()
    }

    /// Lists what `person`'s archives hold, without unpacking anything.
    pub async fn plan(&self, person: UserId, parts: &[ItemId]) -> Result<Plan, Error> {
        let parts = self.parts(person, parts).await?;
        let paths = self.paths(&parts);
        Ok(self.inner.takeout.plan(&self.products(), &paths).await?)
    }

    /// How much more `person` can store, or `None` with no limit.
    pub async fn room(&self, person: UserId) -> Result<Option<u64>, Error> {
        let usage = self.inner.storage.usage(person).await?;
        Ok(usage
            .quota
            .map(|quota| quota.saturating_sub(usage.used + usage.uploading)))
    }

    /// Starts importing what `take` names from `person`'s archives. It runs
    /// in the background: its first pass counts the items and checks
    /// there's room for them before anything is stored.
    pub async fn start(
        &self,
        person: UserId,
        parts: &[ItemId],
        take: &[Take],
    ) -> Result<Uuid, Error> {
        if take.is_empty() {
            return Err(Error::NothingChosen);
        }
        let products = self.products();
        for take in take {
            if let Take::Product(product) = take
                && !products.contains(&product.as_str())
            {
                return Err(Error::UnknownProduct(product.clone()));
            }
        }
        let parts = self.parts(person, parts).await?;
        let id = Uuid::now_v7();
        let parts = Value::Array(
            parts
                .iter()
                .map(|(item, hash)| json!({ "item": item.to_string(), "hash": hash.to_hex() }))
                .collect(),
        );
        let take = takes_json(take);
        let products = json!(products);
        self.inner
            .db
            .write(move |tx| {
                let busy: bool = tx.query_row(
                    "SELECT EXISTS (SELECT 1 FROM takeout_imports
                     WHERE user_id = ?1 AND state IN ('running', 'canceling'))",
                    [person.as_uuid()],
                    |row| row.get(0),
                )?;
                if busy {
                    return Err(Error::Busy);
                }
                tx.execute(
                    concat!(
                        "INSERT INTO takeout_imports
                         (id, user_id, parts, products, take, state, started_at)
                         VALUES (?1, ?2, ?3, ?4, ?5, 'running', ",
                        sql_now!(),
                        ")"
                    ),
                    (
                        id,
                        person.as_uuid(),
                        parts.to_string(),
                        products.to_string(),
                        take.to_string(),
                    ),
                )?;
                db::hint(Hint::person(live::TAKEOUT_IMPORTS, person));
                Ok(())
            })
            .await?;
        self.inner.wake.notify_one();
        Ok(id)
    }

    /// Cancels `person`'s import. It stops after the item it's on, and
    /// writes its report.
    pub async fn cancel(&self, person: UserId, id: Uuid) -> Result<(), Error> {
        let changed = self
            .inner
            .db
            .write(move |tx| {
                let changed = tx.execute(
                    "UPDATE takeout_imports SET state = 'canceling'
                     WHERE id = ?1 AND user_id = ?2 AND state = 'running'",
                    (id, person.as_uuid()),
                )?;
                if changed > 0 {
                    db::hint(Hint::person(live::TAKEOUT_IMPORTS, person));
                }
                Ok::<_, Error>(changed)
            })
            .await?;
        if changed == 0 {
            return Err(Error::NotRunning);
        }
        self.inner.wake.notify_one();
        Ok(())
    }

    /// `person`'s imports, newest first.
    pub async fn list(&self, person: UserId, limit: usize) -> Result<Vec<Import>, Error> {
        self.inner
            .db
            .read(move |conn| {
                let mut statement = conn.prepare(
                    "SELECT id, state, items, done, started_at, finished_at, error, report_id
                     FROM takeout_imports WHERE user_id = ?1
                     ORDER BY started_at DESC LIMIT ?2",
                )?;
                let imports = statement
                    .query_map((person.as_uuid(), limit as i64), |row| {
                        let state: String = row.get(1)?;
                        Ok(Import {
                            id: row.get(0)?,
                            state: State::from_str(&state).unwrap_or(State::Failed),
                            items: row.get::<_, Option<i64>>(2)?.map(count),
                            done: count(row.get(3)?),
                            started_at: row.get(4)?,
                            finished_at: row.get(5)?,
                            error: row.get(6)?,
                            report: row.get::<_, Option<Uuid>>(7)?.map(ItemId::from_uuid),
                        })
                    })?
                    .collect::<Result<_, _>>()?;
                Ok(imports)
            })
            .await
    }

    /// Runs imports, one at a time, until `stopped` fires, starting with
    /// any that a restart interrupted.
    pub async fn run(self, stopped: impl Future<Output = ()>) {
        let mut stopped = pin!(stopped);
        loop {
            let next = match self.next_running().await {
                Ok(next) => next,
                Err(err) => {
                    tracing::error!(%err, "couldn't look for imports to run");
                    tokio::select! {
                        () = tokio::time::sleep(RETRY_AFTER) => continue,
                        () = &mut stopped => return,
                    }
                }
            };
            let Some(job) = next else {
                tokio::select! {
                    () = self.inner.wake.notified() => continue,
                    () = &mut stopped => return,
                }
            };
            let id = job.id;
            match self.run_one(job, stopped.as_mut()).await {
                Ok(Ran::Ended) => {}
                Ok(Ran::Stopped) => return,
                Err(err) => {
                    tracing::error!(%err, %id, "an import stopped");
                    tokio::select! {
                        () = tokio::time::sleep(RETRY_AFTER) => {}
                        () = &mut stopped => return,
                    }
                }
            }
        }
    }

    async fn next_running(&self) -> Result<Option<Job>, Error> {
        self.inner
            .db
            .read(|conn| {
                let row = conn
                    .query_row(
                        "SELECT id, user_id, parts, products, take, state, items, next, counts
                         FROM takeout_imports WHERE state IN ('running', 'canceling')
                         ORDER BY started_at LIMIT 1",
                        [],
                        |row| {
                            Ok((
                                row.get::<_, Uuid>(0)?,
                                row.get::<_, Uuid>(1)?,
                                row.get::<_, String>(2)?,
                                row.get::<_, String>(3)?,
                                row.get::<_, String>(4)?,
                                row.get::<_, String>(5)?,
                                row.get::<_, Option<i64>>(6)?.map(count),
                                count(row.get(7)?),
                                row.get::<_, String>(8)?,
                            ))
                        },
                    )
                    .optional()?;
                Ok(row.map(
                    |(id, user, parts, products, take, state, items, next, counts)| Job {
                        id,
                        person: UserId::from_uuid(user),
                        parts: read_parts(&parts),
                        products: serde_json::from_str(&products).unwrap_or_default(),
                        take: read_takes(&take),
                        canceling: state == "canceling",
                        items,
                        next,
                        counts: serde_json::from_str(&counts).unwrap_or_default(),
                    },
                ))
            })
            .await
    }

    /// Runs one import until it ends, or the server stops.
    async fn run_one(
        &self,
        mut job: Job,
        mut stopped: Pin<&mut impl Future<Output = ()>>,
    ) -> Result<Ran, Error> {
        let importing = Importing::new(self.inner.storage.clone(), job.person);
        if job.canceling {
            return self.finish(&job, &importing, State::Canceled, None).await;
        }
        let ending = match self.import(&mut job, &importing, stopped.as_mut()).await {
            Ok(Some(ending)) => ending,
            Ok(None) => return Ok(Ran::Stopped),
            Err(err) => Ending::Failed(err.to_string()),
        };
        match ending {
            Ending::Done => self.finish(&job, &importing, State::Done, None).await,
            Ending::Canceled => self.finish(&job, &importing, State::Canceled, None).await,
            Ending::Failed(why) => {
                self.finish(&job, &importing, State::Failed, Some(why))
                    .await
            }
        }
    }

    /// Brings the import's items, batch by batch. `None` when the server
    /// is stopping, leaving the import to start again from its last batch.
    async fn import(
        &self,
        job: &mut Job,
        importing: &Importing,
        mut stopped: Pin<&mut impl Future<Output = ()>>,
    ) -> Result<Option<Ending>, Error> {
        for take in &job.take {
            if let Take::Product(product) = take
                && self.importer(product).is_none()
            {
                return Ok(Some(Ending::Failed(format!(
                    "Accordion can't convert {} now. The app that does may have been turned \
                     off. Ask your admin to turn it on, then import the same archives again. \
                     What already came over is skipped.",
                    escape(product)
                ))));
            }
        }
        let Some(paths) = self.job_paths(job).await? else {
            return Ok(Some(Ending::Failed(
                "An archive was changed or deleted during the import. Import the archives \
                 again. What already came over is skipped."
                    .to_owned(),
            )));
        };
        let (owned, take) = (job.products.clone(), job.take.clone());
        let products: Vec<&str> = owned.iter().map(String::as_str).collect();

        if job.items.is_none() {
            let plan = tokio::select! {
                plan = self.inner.takeout.plan(&products, &paths) => plan,
                () = stopped.as_mut() => return Ok(None),
            };
            let plan = match plan {
                Ok(plan) => plan,
                Err(err) => return Ok(Some(Ending::Failed(escape(&err.to_string())))),
            };
            let taken: Vec<&FolderFound> = plan
                .folders
                .iter()
                .filter(|folder| take.iter().any(|take| takes(take, folder)))
                .collect();
            let items: u64 = taken.iter().map(|folder| folder.items).sum();
            let bytes: u64 = taken.iter().map(|folder| folder.bytes).sum();
            if let Some(room) = self.room(job.person).await?
                && bytes > room
            {
                return Ok(Some(Ending::Failed(format!(
                    "There isn't enough space in your storage. The import needs {}, and {} is \
                     left. Delete some files and empty the trash, or ask your admin for more \
                     space, then import again.",
                    size(bytes),
                    size(room)
                ))));
            }
            for skipped in &plan.skipped {
                importing.report(
                    LEFT_OUT,
                    format!("`{}`: {}", code(&skipped.path), escape(&skipped.why)),
                );
            }
            let more = plan.skipped_count.saturating_sub(plan.skipped.len() as u64);
            if more > 0 {
                importing.report(LEFT_OUT, format!("{more} more, not listed"));
            }
            let lines = importing.take_lines();
            let id = job.id;
            let person = job.person;
            self.inner
                .db
                .write(move |tx| {
                    tx.execute(
                        "UPDATE takeout_imports SET items = ?2 WHERE id = ?1",
                        (id, items as i64),
                    )?;
                    add_lines(tx, id, &lines)?;
                    db::hint(Hint::person(live::TAKEOUT_IMPORTS, person));
                    Ok::<_, Error>(())
                })
                .await?;
            job.items = Some(items);
        }

        let mut from = Some(job.next);
        while let Some(start) = from {
            let batch = tokio::select! {
                batch = self.inner.takeout.unpack(
                    &products, &paths, &take, start, self.inner.batch_bytes,
                ) => batch,
                () = stopped.as_mut() => return Ok(None),
            };
            let batch = match batch {
                Ok(batch) => batch,
                Err(err) => return Ok(Some(Ending::Failed(escape(&err.to_string())))),
            };
            for (offset, item) in batch.items.iter().enumerate() {
                if is_ready(stopped.as_mut()).await {
                    return Ok(None);
                }
                if self.canceling(job.id).await? {
                    return Ok(Some(Ending::Canceled));
                }
                let position = start + offset as u64;
                if let Some(why) = self.bring(job, importing, item, position).await? {
                    return Ok(Some(Ending::Failed(why)));
                }
            }
            from = batch.next;
            let id = job.id;
            let next = from.unwrap_or_else(|| job.items.unwrap_or_default());
            self.inner
                .db
                .write(move |tx| {
                    tx.execute(
                        "UPDATE takeout_imports SET next = ?2 WHERE id = ?1",
                        (id, next as i64),
                    )?;
                    Ok::<_, Error>(())
                })
                .await?;
            job.next = next;
        }
        Ok(Some(Ending::Done))
    }

    /// Brings one item, unless an earlier import did. `Some` with why when
    /// the import has to stop.
    async fn bring(
        &self,
        job: &mut Job,
        importing: &Importing,
        item: &Unpacked,
        position: u64,
    ) -> Result<Option<String>, Error> {
        let (product, key) = match &item.product {
            Some(product) => (product.clone(), item.key.clone()),
            None => {
                let size: u64 = item.files.iter().map(|file| file.size).sum();
                (
                    "other".to_owned(),
                    format!("{}/{} {size}", item.folder, item.key),
                )
            }
        };
        let importer = item.product.as_deref().and_then(|id| self.importer(id));
        let label = match &importer {
            Some(importer) => importer.label(item),
            None => format!("`{}`", code(&format!("{}/{}", item.folder, item.key))),
        };

        let person = job.person;
        let (lookup_product, lookup_key) = (product.clone(), key.clone());
        let earlier: Option<(Uuid, i64)> = self
            .inner
            .db
            .read(move |conn| {
                conn.query_row(
                    "SELECT import_id, position FROM takeout_imported
                     WHERE user_id = ?1 AND product = ?2 AND key = ?3",
                    (person.as_uuid(), &lookup_product, &lookup_key),
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
            })
            .await?;
        let outcome = match earlier {
            // This import brought it before the server restarted.
            Some((import, at)) if import == job.id && count(at) == position => return Ok(None),
            // Two items with one key in one export both come over.
            Some((import, _)) if import == job.id => None,
            Some(_) => {
                importing.report(ALREADY, label.clone());
                Some(Outcome::Already)
            }
            None => None,
        };
        let outcome = match outcome {
            Some(outcome) => outcome,
            None => match (&item.product, importer) {
                (None, _) => {
                    for file in &item.files {
                        if let Err(err) = importing.keep(file).await {
                            return Ok(Some(stop_reason(err)));
                        }
                    }
                    Outcome::Kept(item.folder.clone())
                }
                (Some(_), None) => {
                    return Ok(Some(format!(
                        "Accordion can't convert {} now. The app that does may have been \
                         turned off. Ask your admin to turn it on, then import the same \
                         archives again. What already came over is skipped.",
                        escape(&product)
                    )));
                }
                (Some(_), Some(importer)) => match importer.store(item, importing).await {
                    Ok(stored) => Outcome::Brought(stored),
                    Err(ItemError::NotConverted(why)) => {
                        importing.report(NOT_CONVERTED, format!("{label}: {why}"));
                        for file in &item.files {
                            if let Err(err) = importing.keep(file).await {
                                return Ok(Some(stop_reason(err)));
                            }
                        }
                        Outcome::NotConverted
                    }
                    Err(ItemError::Stop(why)) => {
                        self.save_lines(job.id, importing).await?;
                        return Ok(Some(why));
                    }
                },
            },
        };

        let counted = match &outcome {
            Outcome::Brought(_) => format!("brought/{product}"),
            Outcome::NotConverted => format!("not_converted/{product}"),
            Outcome::Kept(folder) => format!("kept/{folder}"),
            Outcome::Already => "already".to_owned(),
        };
        *job.counts.entry(counted).or_default() += 1;
        let counts = serde_json::to_string(&job.counts).unwrap_or_default();
        let record = match outcome {
            Outcome::Already => None,
            Outcome::Brought(stored) => Some(stored),
            Outcome::NotConverted | Outcome::Kept(_) => Some(None),
        };
        let lines = importing.take_lines();
        let id = job.id;
        self.inner
            .db
            .write(move |tx| {
                if let Some(stored) = record {
                    tx.execute(
                        "INSERT OR IGNORE INTO takeout_imported
                         (user_id, product, key, import_id, position, item_id)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                        (
                            person.as_uuid(),
                            &product,
                            &key,
                            id,
                            position as i64,
                            stored.map(|item| item.as_uuid()),
                        ),
                    )?;
                }
                tx.execute(
                    "UPDATE takeout_imports SET done = done + 1, counts = ?2 WHERE id = ?1",
                    (id, &counts),
                )?;
                add_lines(tx, id, &lines)?;
                db::hint(Hint::person(live::TAKEOUT_IMPORTS, person));
                Ok::<_, Error>(())
            })
            .await?;
        Ok(None)
    }

    async fn save_lines(&self, id: Uuid, importing: &Importing) -> Result<(), Error> {
        let lines = importing.take_lines();
        self.inner
            .db
            .write(move |tx| add_lines(tx, id, &lines))
            .await
    }

    async fn canceling(&self, id: Uuid) -> Result<bool, Error> {
        self.inner
            .db
            .read(move |conn| {
                let state: Option<String> = conn
                    .query_row(
                        "SELECT state FROM takeout_imports WHERE id = ?1",
                        [id],
                        |row| row.get(0),
                    )
                    .optional()?;
                Ok(state.as_deref() != Some("running"))
            })
            .await
    }

    /// Writes the report, records how the import ended, and rings the bell.
    async fn finish(
        &self,
        job: &Job,
        importing: &Importing,
        state: State,
        error: Option<String>,
    ) -> Result<Ran, Error> {
        self.save_lines(job.id, importing).await?;
        let report = match self
            .write_report(job, importing, state, error.as_deref())
            .await
        {
            Ok(report) => Some(report),
            Err(err) => {
                tracing::error!(%err, id = %job.id, "couldn't save an import's report");
                None
            }
        };
        let id = job.id;
        let person = job.person;
        let stored_error = error.clone();
        self.inner
            .db
            .write(move |tx| {
                tx.execute(
                    concat!(
                        "UPDATE takeout_imports SET state = ?2, error = ?3, report_id = ?4,
                         finished_at = ",
                        sql_now!(),
                        " WHERE id = ?1"
                    ),
                    (
                        id,
                        state.as_str(),
                        &stored_error,
                        report.map(|item| item.as_uuid()),
                    ),
                )?;
                tx.execute("DELETE FROM takeout_report WHERE import_id = ?1", [id])?;
                db::hint(Hint::person(live::TAKEOUT_IMPORTS, person));
                Ok::<_, Error>(())
            })
            .await?;
        let brought: u64 = job
            .counts
            .iter()
            .filter(|(what, _)| what.starts_with("brought/") || what.starts_with("kept/"))
            .map(|(_, count)| count)
            .sum();
        let mut details = json!({ "state": state.as_str(), "brought": brought });
        if let Some(error) = &error {
            details["error"] = json!(error);
        }
        let event = match report {
            Some(report) => NewEvent::about(Action::ImportFinished, report, person),
            None => NewEvent::new(Action::ImportFinished),
        };
        if let Err(err) = self
            .inner
            .events
            .record(Actor::SERVER, event.other(person).details(details))
            .await
        {
            tracing::error!(%err, "couldn't record an import's end");
        }
        Ok(Ran::Ended)
    }

    async fn write_report(
        &self,
        job: &Job,
        importing: &Importing,
        state: State,
        error: Option<&str>,
    ) -> Result<ItemId, Error> {
        let id = job.id;
        let (started, lines, dropped) = self
            .inner
            .db
            .read(move |conn| {
                let (started, dropped): (String, i64) = conn.query_row(
                    "SELECT started_at, dropped FROM takeout_imports WHERE id = ?1",
                    [id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?;
                let lines: Vec<(String, String)> = conn
                    .prepare(
                        "SELECT heading, text FROM takeout_report
                         WHERE import_id = ?1 ORDER BY line",
                    )?
                    .query_map([id], |row| Ok((row.get(0)?, row.get(1)?)))?
                    .collect::<Result<_, _>>()?;
                Ok::<_, Error>((started, lines, dropped))
            })
            .await?;
        let text = self.report_text(job, state, error, &started, &lines, dropped);
        let folder = importing.takeout_folder().await?;
        let date = local_time(&now(), "%Y-%m-%d");
        let report = importing
            .create_file(
                folder,
                &format!("Takeout import {date}.md"),
                text.as_bytes(),
                None,
            )
            .await?;
        Ok(report.id)
    }

    fn report_text(
        &self,
        job: &Job,
        state: State,
        error: Option<&str>,
        started: &str,
        lines: &[(String, String)],
        dropped: i64,
    ) -> String {
        let mut text = String::from("# Import from Google Takeout\n\n");
        text.push_str(&format!(
            "Started {}. Ended {}.\n\n",
            local_time(started, "%B %-d, %Y, %H:%M"),
            local_time(&now(), "%B %-d, %Y, %H:%M"),
        ));
        match state {
            State::Canceled => text.push_str(
                "You canceled it before it finished. Import the same archives again to bring \
                 the rest. What already came over is skipped.\n\n",
            ),
            State::Failed => {
                text.push_str("It stopped before it finished. ");
                text.push_str(error.unwrap_or("Accordion couldn't tell why."));
                text.push_str("\n\n");
            }
            _ => {}
        }

        text.push_str("## What came over\n\n");
        let mut any = false;
        let mut kept = false;
        for (what, &count) in &job.counts {
            let line = if let Some(product) = what.strip_prefix("brought/") {
                match self.importer(product) {
                    Some(importer) => importer.brought(count),
                    None => format!("{count} from {}", escape(product)),
                }
            } else if let Some(folder) = what.strip_prefix("kept/") {
                kept = true;
                format!(
                    "{count} {} from {}, kept in **{UNCONVERTED}**",
                    if count == 1 { "file" } else { "files" },
                    escape(folder)
                )
            } else if what.starts_with("not_converted/") {
                kept = true;
                continue;
            } else {
                continue;
            };
            any = true;
            text.push_str(&format!("- {line}\n"));
        }
        if !any {
            text.push_str("Nothing new came over.\n");
        }
        if let Some(&already) = job.counts.get("already") {
            text.push_str(&format!(
                "\n{already} {} brought by an earlier import, so {} skipped.\n",
                if already == 1 {
                    "item was"
                } else {
                    "items were"
                },
                if already == 1 { "it was" } else { "they were" },
            ));
        }
        if kept {
            text.push_str(&format!(
                "\nFiles Accordion didn't convert are in **{FOLDER}** > **{UNCONVERTED}**, in \
                 their folders from the archive.\n"
            ));
        }

        // Core's headings around the modules' own, which keep their order.
        let mut headings: Vec<&str> = vec![NOT_CONVERTED];
        for (heading, _) in lines {
            if ![NOT_CONVERTED, SHORTENED, ALREADY, LEFT_OUT].contains(&heading.as_str())
                && !headings.contains(&heading.as_str())
            {
                headings.push(heading);
            }
        }
        headings.extend([SHORTENED, ALREADY, LEFT_OUT]);
        for heading in headings {
            let mut section = lines.iter().filter(|(of, _)| of == heading).peekable();
            if section.peek().is_none() {
                continue;
            }
            text.push_str(&format!("\n## {}\n\n", escape(heading)));
            for (_, line) in section {
                text.push_str(&format!("- {line}\n"));
            }
        }
        if dropped > 0 {
            text.push_str(&format!(
                "\nThe report lists up to {MAX_REPORT_LINES} lines. {dropped} more weren't \
                 listed.\n"
            ));
        }
        text
    }

    /// `person`'s parts, each with its current contents, checked: theirs,
    /// files, and not in the trash.
    async fn parts(
        &self,
        person: UserId,
        parts: &[ItemId],
    ) -> Result<Vec<(ItemId, ContentHash)>, Error> {
        if parts.is_empty() {
            return Err(Error::NoParts);
        }
        if parts.len() > MAX_PARTS {
            return Err(Error::TooManyParts);
        }
        let mut found = Vec::new();
        for &id in parts {
            let item = self.inner.storage.item(id).await?;
            let Some(item) = item.filter(|item| item.owner == person) else {
                return Err(Error::PartNotFound);
            };
            if !self.inner.storage.outside_trash(id).await? {
                return Err(Error::PartNotFound);
            }
            let ItemKind::File { blob, .. } = item.kind else {
                return Err(Error::PartNotAFile);
            };
            found.push((id, blob.hash));
        }
        Ok(found)
    }

    fn paths(&self, parts: &[(ItemId, ContentHash)]) -> Vec<PathBuf> {
        parts
            .iter()
            .map(|&(_, hash)| self.inner.storage.contents_file(hash))
            .collect()
    }

    /// The job's parts, if each still holds what it did when it started.
    async fn job_paths(&self, job: &Job) -> Result<Option<Vec<PathBuf>>, Error> {
        if job.parts.is_empty() {
            return Ok(None);
        }
        for &(id, hash) in &job.parts {
            let current = self.inner.storage.item(id).await?;
            let same = matches!(
                current,
                Some(Item { kind: ItemKind::File { blob, .. }, .. }) if blob.hash == hash
            );
            if !same {
                return Ok(None);
            }
        }
        Ok(Some(self.paths(&job.parts)))
    }
}

/// An import being run.
struct Job {
    id: Uuid,
    person: UserId,
    parts: Vec<(ItemId, ContentHash)>,
    products: Vec<String>,
    take: Vec<Take>,
    canceling: bool,
    items: Option<u64>,
    next: u64,
    /// What came over: `brought/<product>`, `not_converted/<product>`,
    /// `kept/<folder>`, and `already`.
    counts: BTreeMap<String, u64>,
}

enum Ran {
    /// The import ended, and its report is written.
    Ended,
    /// The server is stopping. The import starts again from its last batch.
    Stopped,
}

enum Ending {
    Done,
    Canceled,
    Failed(String),
}

enum Outcome {
    Brought(Option<ItemId>),
    NotConverted,
    Kept(String),
    Already,
}

/// What a module's importer uses while it stores an item: the person's
/// storage, the report, and **Unconverted**.
pub struct Importing {
    person: UserId,
    storage: Storage,
    lines: Mutex<Vec<(String, String)>>,
    /// Folders found or made, by their parent and name.
    folders: tokio::sync::Mutex<HashMap<(ItemId, String), ItemId>>,
}

impl Importing {
    fn new(storage: Storage, person: UserId) -> Self {
        Self {
            person,
            storage: storage.acting_as(Actor::person(person, Via::Server)),
            lines: Mutex::new(Vec::new()),
            folders: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Who's importing.
    pub fn person(&self) -> UserId {
        self.person
    }

    /// Storage, acting as the person importing.
    pub fn storage(&self) -> &Storage {
        &self.storage
    }

    /// Adds `line`, in Markdown, to the report under `heading`. Headings
    /// keep the order they're first used in.
    pub fn report(&self, heading: &str, line: impl Into<String>) {
        self.lines
            .lock()
            .expect("report lock")
            .push((heading.to_owned(), line.into()));
    }

    fn take_lines(&self) -> Vec<(String, String)> {
        std::mem::take(&mut *self.lines.lock().expect("report lock"))
    }

    /// **Google Takeout** in the person's My Drive.
    pub async fn takeout_folder(&self) -> Result<ItemId, storage::Error> {
        let top = self.storage.top_folder(self.person).await?;
        self.folder(top, FOLDER).await
    }

    /// The folder `name` in `parent`, made if it isn't there. A file with
    /// that name gets a folder with a number beside it.
    pub async fn folder(&self, parent: ItemId, name: &str) -> Result<ItemId, storage::Error> {
        let name = usable(name);
        let mut folders = self.folders.lock().await;
        if let Some(&id) = folders.get(&(parent, name.clone())) {
            return Ok(id);
        }
        let (fitted, _) = fit(&name, "", false);
        let found = self.storage.find(parent, vec![fitted]).await?;
        let id = match found {
            Some(item) if item.kind == ItemKind::Folder => item.id,
            _ => self.create_folder(parent, &name, None).await?.id,
        };
        folders.insert((parent, name), id);
        Ok(id)
    }

    /// Makes the folder `name` in `parent`, numbered if the name is taken,
    /// and shortened if it's too long, which the report lists.
    pub async fn create_folder(
        &self,
        parent: ItemId,
        name: &str,
        dates: Option<Dates>,
    ) -> Result<Item, storage::Error> {
        self.save(parent, name, Contents::Folder, dates).await
    }

    /// Saves a new file `name` in `parent`, holding `contents`, with a
    /// number before its extension if the name is taken, such as
    /// `Budget (2).xlsx`, and shortened if it's too long, which the report
    /// lists.
    pub async fn create_file(
        &self,
        parent: ItemId,
        name: &str,
        contents: &[u8],
        dates: Option<Dates>,
    ) -> Result<Item, storage::Error> {
        self.save(parent, name, Contents::Bytes(contents), dates)
            .await
    }

    /// Saves a new file as [`Importing::create_file`] does, from an
    /// unpacked file, without reading it all into memory.
    pub async fn create_file_from(
        &self,
        parent: ItemId,
        name: &str,
        file: &UnpackedFile,
        dates: Option<Dates>,
    ) -> Result<Item, storage::Error> {
        self.save(parent, name, Contents::File(&file.file), dates)
            .await
    }

    async fn save(
        &self,
        parent: ItemId,
        name: &str,
        contents: Contents<'_>,
        dates: Option<Dates>,
    ) -> Result<Item, storage::Error> {
        let name = usable(name);
        let file = !matches!(contents, Contents::Folder);
        for number in 1u32.. {
            let (fitted, cut) = fit(&name, &numbered(number), file);
            let storage = &self.storage;
            let made = match (&contents, dates) {
                (Contents::Folder, Some(dates)) => {
                    storage.create_folder_dated(parent, &fitted, dates).await
                }
                (Contents::Folder, None) => storage.create_folder(parent, &fitted).await,
                (Contents::Bytes(bytes), Some(dates)) => {
                    storage
                        .create_file_dated(parent, &fitted, *bytes, dates)
                        .await
                }
                (Contents::Bytes(bytes), None) => {
                    storage.create_file(parent, &fitted, *bytes).await
                }
                (Contents::File(path), dates) => {
                    let opened = tokio::fs::File::open(path).await?;
                    match dates {
                        Some(dates) => {
                            storage
                                .create_file_dated(parent, &fitted, opened, dates)
                                .await
                        }
                        None => storage.create_file(parent, &fitted, opened).await,
                    }
                }
            };
            match made {
                Err(storage::Error::NameTaken { .. }) => continue,
                Ok(item) => {
                    if cut {
                        self.shortened(&name, &fitted);
                    }
                    return Ok(item);
                }
                Err(err) => return Err(err),
            }
        }
        unreachable!("some number is free")
    }

    /// Keeps `file` in **Unconverted**, in its folder from the archive, and
    /// says why in the report, in Markdown.
    pub async fn unconverted(&self, file: &UnpackedFile, why: &str) -> Result<(), ItemError> {
        self.keep(file).await?;
        self.report(
            NOT_CONVERTED,
            format!("`{}`: {why}", code(&inside_export(&file.path).join("/"))),
        );
        Ok(())
    }

    /// Lists a name that was shortened to fit, such as a title cut to
    /// make a file's name, under "Names that were shortened".
    pub fn shortened(&self, name: &str, fitted: &str) {
        self.report(
            SHORTENED,
            format!("`{}` became `{}`", code(name), code(fitted)),
        );
    }

    /// Keeps `file` in **Unconverted**, in its folder from the archive,
    /// without a line in the report, such as the original of a file that
    /// was converted.
    pub async fn keep(&self, file: &UnpackedFile) -> Result<(), ItemError> {
        let mut names = inside_export(&file.path);
        let Some(name) = names.pop() else {
            return Err(ItemError::NotConverted("its path is empty".to_owned()));
        };
        let mut folder = self
            .folder(self.takeout_folder().await?, UNCONVERTED)
            .await?;
        for part in names {
            folder = self.folder(folder, &part).await?;
        }
        self.save(folder, &name, Contents::File(&file.file), None)
            .await?;
        Ok(())
    }
}

/// What [`Importing::save`] saves.
enum Contents<'a> {
    Folder,
    Bytes(&'a [u8]),
    /// An unpacked file, opened again for each name it tries.
    File(&'a std::path::Path),
}

fn stop_reason(err: ItemError) -> String {
    match err {
        ItemError::NotConverted(why) | ItemError::Stop(why) => why,
    }
}

/// The names in an archive path after the export's own top folder, such as
/// `["Keep", "photo.jpg"]` for `Takeout/Keep/photo.jpg`, each usable.
fn inside_export(path: &str) -> Vec<String> {
    path.split('/')
        .filter(|part| !part.is_empty() && *part != "." && *part != "..")
        .skip(1)
        .map(usable)
        .collect()
}

/// `name` with what storage refuses replaced.
fn usable(name: &str) -> String {
    let name: String = name
        .chars()
        .map(|c| if c == '/' || c == '\0' { '_' } else { c })
        .collect();
    match name.as_str() {
        "" | "." | ".." => "_".to_owned(),
        _ => name,
    }
}

/// ` (2)` and so on for `number`, or nothing for 1.
fn numbered(number: u32) -> String {
    match number {
        1 => String::new(),
        number => format!(" ({number})"),
    }
}

/// `name` with `suffix` added before its extension, if it `has_extension`,
/// cut short so it fits [`MAX_NAME_LEN`], and whether it had to be cut.
fn fit(name: &str, suffix: &str, has_extension: bool) -> (String, bool) {
    let (stem, extension) = match name.rfind('.') {
        Some(dot)
            if has_extension && dot > 0 && name.len() - dot <= 16 && !name[dot..].contains(' ') =>
        {
            name.split_at(dot)
        }
        _ => (name, ""),
    };
    let room = MAX_NAME_LEN.saturating_sub(suffix.len() + extension.len());
    let mut end = stem.len().min(room);
    while !stem.is_char_boundary(end) {
        end -= 1;
    }
    (
        format!("{}{suffix}{extension}", &stem[..end]),
        end < stem.len(),
    )
}

/// `text` with Markdown's special characters escaped, and on one line, for
/// names from an archive in the report.
pub fn escape(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '\\' | '`' | '*' | '_' | '[' | ']' | '<' | '>' | '#' | '|' | '~' | '!' | '&' => {
                escaped.push('\\');
                escaped.push(c);
            }
            '\n' | '\r' | '\t' => escaped.push(' '),
            c if c.is_control() => {}
            c => escaped.push(c),
        }
    }
    escaped
}

/// `text` for inside backticks: on one line, with backticks replaced, since
/// Markdown doesn't escape inside code.
fn code(text: &str) -> String {
    text.chars()
        .map(|c| match c {
            '`' => '\'',
            c if c.is_control() => ' ',
            c => c,
        })
        .collect()
}

fn takes(take: &Take, folder: &FolderFound) -> bool {
    match take {
        Take::Product(product) => folder.product.as_deref() == Some(product.as_str()),
        Take::Other(name) => folder.product.is_none() && &folder.folder == name,
    }
}

fn takes_json(take: &[Take]) -> Value {
    Value::Array(
        take.iter()
            .map(|take| match take {
                Take::Product(id) => json!({ "product": id }),
                Take::Other(folder) => json!({ "other": folder }),
            })
            .collect(),
    )
}

fn read_takes(text: &str) -> Vec<Take> {
    let value: Value = serde_json::from_str(text).unwrap_or_default();
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|take| {
            if let Some(product) = take.get("product").and_then(Value::as_str) {
                Some(Take::Product(product.to_owned()))
            } else {
                take.get("other")
                    .and_then(Value::as_str)
                    .map(|folder| Take::Other(folder.to_owned()))
            }
        })
        .collect()
}

fn read_parts(text: &str) -> Vec<(ItemId, ContentHash)> {
    let value: Value = serde_json::from_str(text).unwrap_or_default();
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|part| {
            let item = Uuid::parse_str(part.get("item")?.as_str()?).ok()?;
            let hash = ContentHash::from_hex(part.get("hash")?.as_str()?)?;
            Some((ItemId::from_uuid(item), hash))
        })
        .collect()
}

fn add_lines(conn: &Connection, id: Uuid, lines: &[(String, String)]) -> Result<(), Error> {
    if lines.is_empty() {
        return Ok(());
    }
    let written: i64 = conn.query_row(
        "SELECT lines FROM takeout_imports WHERE id = ?1",
        [id],
        |row| row.get(0),
    )?;
    let room = usize::try_from(MAX_REPORT_LINES - written).unwrap_or(0);
    let mut statement = conn.prepare(
        "INSERT INTO takeout_report (import_id, line, heading, text) VALUES (?1, ?2, ?3, ?4)",
    )?;
    for (number, (heading, text)) in lines.iter().take(room).enumerate() {
        statement.execute((id, written + number as i64, heading, text))?;
    }
    let added = lines.len().min(room) as i64;
    let dropped = lines.len() as i64 - added;
    conn.execute(
        "UPDATE takeout_imports SET lines = lines + ?2, dropped = dropped + ?3 WHERE id = ?1",
        (id, added, dropped),
    )?;
    Ok(())
}

/// Whether `future` has finished, without waiting for it.
async fn is_ready(future: Pin<&mut impl Future<Output = ()>>) -> bool {
    tokio::select! {
        biased;
        () = future => true,
        () = std::future::ready(()) => false,
    }
}

/// A count from the database, which keeps them as signed numbers.
fn count(value: i64) -> u64 {
    u64::try_from(value).unwrap_or_default()
}

fn now() -> String {
    crate::time::format(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default(),
    )
}

/// A time in the database's format, in the server's time zone.
fn local_time(at: &str, format: &str) -> String {
    match at.parse::<jiff::Timestamp>() {
        Ok(at) => at
            .to_zoned_datetime(jiff::tz::TimeZone::system())
            .strftime(format)
            .to_string(),
        Err(_) => at.to_owned(),
    }
}

/// `bytes` for people, such as "2.1 GB".
fn size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["bytes", "KB", "MB", "GB", "TB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1000.0 && unit < UNITS.len() - 1 {
        value /= 1000.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} bytes")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fits_long_names_keeping_their_extension() {
        let long = format!("{}.xlsx", "a".repeat(300));
        let (fitted, cut) = fit(&long, " (2)", true);
        assert_eq!(fitted.len(), MAX_NAME_LEN);
        assert!(cut && fitted.ends_with("a (2).xlsx"));
        let fitted = |name, suffix, extension| fit(name, suffix, extension).0;
        assert_eq!(fitted("Budget.xlsx", " (2)", true), "Budget (2).xlsx");
        assert_eq!(fitted("Trip.photos", " (2)", false), "Trip.photos (2)");
        assert_eq!(fitted(".hidden", " (2)", true), ".hidden (2)");
        assert_eq!(
            fitted("Read me. Please", " (2)", true),
            "Read me. Please (2)"
        );
        assert_eq!(
            fit("Budget.xlsx", "", true),
            ("Budget.xlsx".to_owned(), false)
        );
        let wide = "é".repeat(200);
        assert!(fitted(&wide, "", true).len() <= MAX_NAME_LEN);
    }

    #[test]
    fn escapes_markdown_in_names() {
        assert_eq!(
            escape("[x](javascript:y) *b*"),
            "\\[x\\](javascript:y) \\*b\\*"
        );
        assert_eq!(escape("two\nlines"), "two lines");
        assert_eq!(code("a`b\nc"), "a'b c");
    }

    #[test]
    fn finds_paths_inside_the_export() {
        assert_eq!(
            inside_export("Takeout/Mail/Settings/filters.xml"),
            ["Mail", "Settings", "filters.xml"]
        );
        assert_eq!(inside_export("Takeout/Keep/../x"), ["Keep", "x"]);
    }
}

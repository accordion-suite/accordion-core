//! Importing from Google Takeout (decision 34): reading the archives.
//!
//! The archives are someone else's file format, so a copy of the server
//! reads them in the sandbox, as [`reader::COMMAND`], and the server only
//! ever handles its plain, checked answers. Reading happens in two kinds of
//! pass over the export's parts:
//!
//! - **Plan:** lists every entry, finds each product by what's in its
//!   files, and says what each product folder holds. Nothing is unpacked.
//! - **Unpack:** unpacks one batch of items, up to a number of bytes, into
//!   a job folder, and says where the next batch starts. Each batch plans
//!   again, which gives the same items in the same order, so an import that
//!   stops can start again from any batch.
//!
//! Modules convert their own products. Each gives the reader a [`Product`],
//! which runs in the sandbox, and stores what comes out itself. Folders no
//! product claims, such as Gmail before Mail exists, come out file by file,
//! for **Unconverted**.

pub mod archive;
pub mod imports;
pub mod reader;

use std::collections::HashMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use accordion_sandbox::{Folder, Job, Limits, Sandbox, Workers};
use serde_json::{Value, json};

pub use imports::{Importer, Importing, Imports, ItemError, Registered, escape};
pub use reader::COMMAND;

/// A product a module converts, such as Keep. It runs in the sandbox, on
/// entries whose names and sizes have been checked, but whose contents
/// haven't: anything read from them is untrusted.
pub trait Product: Sync {
    /// Its ID in plans and batches, such as `keep`.
    fn id(&self) -> &'static str;

    /// Whether to read the file at `path` (inside a product folder, such
    /// as `Groceries.json`) while planning. Only files up to 4 MB are read.
    fn wants(&self, path: &str) -> bool;

    /// The items in `folder`, if it's this product's. The folder's name
    /// follows the account's language, so only what's in it can say.
    fn plan(&self, folder: &ProductFolder) -> Option<Vec<Planned>>;
}

/// One top-level folder of an export, such as `Takeout/Keep`.
pub struct ProductFolder<'a> {
    /// Its name, such as `Keep`, in the account's language.
    pub name: &'a str,
    pub entries: &'a [FolderEntry],
    small: &'a HashMap<usize, Vec<u8>>,
}

impl ProductFolder<'_> {
    /// The contents of a small file the product asked for, if it was read.
    pub fn read(&self, entry: &FolderEntry) -> Option<&[u8]> {
        self.small.get(&entry.id).map(Vec::as_slice)
    }
}

/// A file or folder inside a product folder.
#[derive(Clone, Debug)]
pub struct FolderEntry {
    /// Which entry of the export it is, for [`PlannedFile::entry`].
    pub id: usize,
    /// Its path inside the product folder, such as `Trip/photo.jpg`.
    pub path: String,
    pub size: u64,
    pub folder: bool,
}

/// One item a product plans to bring, such as a note with its pictures.
#[derive(Clone, Debug)]
pub struct Planned {
    /// What tells this item apart from others in later exports, so an
    /// import run again skips it, such as a note's creation time and title.
    pub key: String,
    /// What the module stores, in its own shape. The server's side of the
    /// module checks it again.
    pub fields: Value,
    pub files: Vec<PlannedFile>,
}

/// A file that comes with an item.
#[derive(Clone, Debug)]
pub struct PlannedFile {
    /// What the file is to the item, in the module's own words, such as
    /// `attachment`.
    pub role: String,
    /// The entry's [`FolderEntry::id`].
    pub entry: usize,
}

/// What may be read: the module's products, or a folder no product claims.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Take {
    Product(String),
    /// A folder no product claims, by its name, whose files come out one
    /// by one for **Unconverted**.
    Other(String),
}

/// What a plan pass found.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Plan {
    pub parts: Vec<PartFound>,
    pub folders: Vec<FolderFound>,
    /// Entries left out, up to [`reader::MAX_SKIPPED_LISTED`] of them.
    pub skipped: Vec<SkippedEntry>,
    /// How many entries were left out in all.
    pub skipped_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartFound {
    /// `zip` or `tgz`.
    pub format: String,
    pub entries: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FolderFound {
    /// Its name in the archive, such as `Keep`.
    pub folder: String,
    /// The product that claimed it, or `None` for one no product knows.
    pub product: Option<String>,
    pub items: u64,
    pub bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkippedEntry {
    pub path: String,
    pub why: String,
}

/// One batch of unpacked items. Its files are removed when it's dropped.
pub struct Batch {
    pub items: Vec<Unpacked>,
    /// Where the next batch starts, or `None` after the last.
    pub next: Option<u64>,
    _folder: Folder,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Unpacked {
    /// The product folder it came from, such as `Keep`.
    pub folder: String,
    /// The product, or `None` for a file from a folder no product knows.
    pub product: Option<String>,
    pub key: String,
    pub fields: Value,
    pub files: Vec<UnpackedFile>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnpackedFile {
    pub role: String,
    /// Its path in the archive, such as `Takeout/Keep/photo.jpg`.
    pub path: String,
    /// Where it was unpacked, until the batch is dropped.
    pub file: PathBuf,
    pub size: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The reader explained why, such as a part that isn't an archive.
    #[error("{0}")]
    Unreadable(String),
    /// The sandbox couldn't run the reader, or it gave a wrong answer.
    #[error("Accordion couldn't read the archives: {0}")]
    Failed(String),
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::Failed(err.to_string())
    }
}

/// The programs that read archives.
#[derive(Clone, Debug)]
pub struct Tools {
    /// The program that answers `sandbox` and [`COMMAND`]: the server
    /// itself.
    pub launcher: PathBuf,
    /// The worker service, which runs workers as users of their own
    /// (decision 29). Without it, the server runs them itself.
    pub workers: Option<Workers>,
}

impl Tools {
    pub fn installed(launcher: impl Into<PathBuf>) -> Self {
        Self {
            launcher: launcher.into(),
            workers: None,
        }
    }
}

/// What reading an export may use. A big `.tgz` is read from its start on
/// every pass, which takes a while on a Raspberry Pi.
const LIMITS: Limits = Limits {
    memory: 1 << 30,
    cpu: Duration::from_secs(3 * 60 * 60),
    time: Duration::from_secs(4 * 60 * 60),
    output: 128 << 20,
};

/// The longest text the server takes from the reader for a path, key, or
/// role. Fields are the module's to check.
const MAX_TEXT: usize = 8192;

/// Reads Takeout's archives in the sandbox.
#[derive(Clone)]
pub struct Takeout {
    inner: Arc<Inner>,
}

struct Inner {
    sandbox: Sandbox,
    launcher: PathBuf,
    /// Where job folders go without the worker service.
    scratch: PathBuf,
}

impl Takeout {
    /// Reads with `tools`, making job folders in `scratch` when there's no
    /// worker service.
    pub fn new(tools: Tools, scratch: impl Into<PathBuf>) -> Self {
        let sandbox = Sandbox::new(&tools.launcher).with_workers(tools.workers.as_ref());
        Self {
            inner: Arc::new(Inner {
                sandbox,
                launcher: tools.launcher,
                scratch: scratch.into(),
            }),
        }
    }

    /// Lists what the export's `parts` hold, without unpacking anything.
    /// The parts are stored files, in the order the export numbered them.
    /// Only `products` may claim folders.
    pub async fn plan(&self, products: &[&str], parts: &[PathBuf]) -> Result<Plan, Error> {
        let mut args = vec![OsString::from(COMMAND), "plan".into(), list(products)];
        args.extend(parts.iter().map(|part| part.as_os_str().to_owned()));
        let output = self.run(&args, parts, None).await?;
        let line = output
            .split(|&byte| byte == b'\n')
            .find(|line| !line.is_empty())
            .ok_or_else(|| Error::Failed("the reader gave no plan".to_owned()))?;
        let value: Value =
            serde_json::from_slice(line).map_err(|err| Error::Failed(err.to_string()))?;
        read_plan(&value).ok_or_else(|| Error::Failed("the reader's plan was malformed".to_owned()))
    }

    /// Unpacks the items of `take` from item `from` on, up to `budget`
    /// bytes, but always at least one item. `products` must be the ones the
    /// plan used, so the items come in the same order.
    pub async fn unpack(
        &self,
        products: &[&str],
        parts: &[PathBuf],
        take: &[Take],
        from: u64,
        budget: u64,
    ) -> Result<Batch, Error> {
        let folder = self.inner.sandbox.folder(&self.inner.scratch).await?;
        let take = Value::Array(
            take.iter()
                .map(|take| match take {
                    Take::Product(id) => json!({ "product": id }),
                    Take::Other(folder) => json!({ "other": folder }),
                })
                .collect(),
        );
        let mut args = vec![
            OsString::from(COMMAND),
            "unpack".into(),
            list(products),
            take.to_string().into(),
            from.to_string().into(),
            budget.to_string().into(),
            folder.path().as_os_str().to_owned(),
        ];
        args.extend(parts.iter().map(|part| part.as_os_str().to_owned()));
        let output = self.run(&args, parts, Some(&folder)).await?;
        let mut items = Vec::new();
        let mut next = None;
        let mut ended = false;
        for line in output
            .split(|&byte| byte == b'\n')
            .filter(|line| !line.is_empty())
        {
            let bad = || Error::Failed("the reader's batch was malformed".to_owned());
            if ended {
                return Err(bad());
            }
            let value: Value = serde_json::from_slice(line).map_err(|_| bad())?;
            if let Some(end) = value.get("next") {
                next = match end {
                    Value::Null => None,
                    end => Some(end.as_u64().ok_or_else(bad)?),
                };
                ended = true;
                continue;
            }
            items.push(read_unpacked(&value, folder.path()).ok_or_else(bad)?);
        }
        if !ended {
            return Err(Error::Failed("the reader's batch was cut short".to_owned()));
        }
        if next.is_some_and(|next| next <= from) {
            return Err(Error::Failed("the reader went backwards".to_owned()));
        }
        Ok(Batch {
            items,
            next,
            _folder: folder,
        })
    }

    async fn run(
        &self,
        args: &[OsString],
        parts: &[PathBuf],
        folder: Option<&Folder>,
    ) -> Result<Vec<u8>, Error> {
        self.inner
            .sandbox
            .run(Job {
                program: &self.inner.launcher,
                args,
                env: &[("MALLOC_ARENA_MAX", "2")],
                input: std::fs::File::open("/dev/null")?,
                read: parts,
                folder,
                sockets: None,
                limits: LIMITS,
            })
            .await
            .map_err(|err| match err {
                // The reader says why it stopped on its error output.
                accordion_sandbox::RunError::Failed { message, .. }
                    if !message.trim().is_empty() =>
                {
                    Error::Unreadable(message.trim().to_owned())
                }
                err => Error::Failed(err.to_string()),
            })
    }
}

/// Products' IDs for the reader, separated by commas.
fn list(products: &[&str]) -> OsString {
    assert!(
        products
            .iter()
            .all(|id| !id.is_empty() && id.bytes().all(|b| b.is_ascii_lowercase())),
        "a product's ID is lowercase letters"
    );
    products.join(",").into()
}

fn text(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)?
        .as_str()
        .filter(|text| text.len() <= MAX_TEXT)
        .map(str::to_owned)
}

fn read_plan(value: &Value) -> Option<Plan> {
    let parts = value
        .get("parts")?
        .as_array()?
        .iter()
        .map(|part| {
            Some(PartFound {
                format: text(part, "format").filter(|format| format == "zip" || format == "tgz")?,
                entries: part.get("entries")?.as_u64()?,
            })
        })
        .collect::<Option<_>>()?;
    let folders = value
        .get("folders")?
        .as_array()?
        .iter()
        .map(|folder| {
            Some(FolderFound {
                folder: text(folder, "folder")?,
                product: match folder.get("product")? {
                    Value::Null => None,
                    _ => Some(text(folder, "product")?),
                },
                items: folder.get("items")?.as_u64()?,
                bytes: folder.get("bytes")?.as_u64()?,
            })
        })
        .collect::<Option<_>>()?;
    let skipped = value
        .get("skipped")?
        .as_array()?
        .iter()
        .take(reader::MAX_SKIPPED_LISTED)
        .map(|skipped| {
            Some(SkippedEntry {
                path: text(skipped, "path")?,
                why: text(skipped, "why")?,
            })
        })
        .collect::<Option<_>>()?;
    Some(Plan {
        parts,
        folders,
        skipped,
        skipped_count: value.get("skipped_count")?.as_u64()?,
    })
}

/// An item from the reader, with its files checked: each is a plain file
/// the reader made in `folder`, named by a number, holding what it says.
fn read_unpacked(value: &Value, folder: &Path) -> Option<Unpacked> {
    let files = value
        .get("files")?
        .as_array()?
        .iter()
        .map(|file| {
            let name = text(file, "name")?;
            if name.is_empty() || !name.bytes().all(|byte| byte.is_ascii_digit()) {
                return None;
            }
            let path = folder.join(&name);
            let size = file.get("size")?.as_u64()?;
            let made = path.symlink_metadata().ok()?;
            if !made.is_file() || made.len() != size {
                return None;
            }
            Some(UnpackedFile {
                role: text(file, "role")?,
                path: text(file, "path")?,
                file: path,
                size,
            })
        })
        .collect::<Option<_>>()?;
    Some(Unpacked {
        folder: text(value, "folder")?,
        product: match value.get("product")? {
            Value::Null => None,
            _ => Some(text(value, "product")?),
        },
        key: text(value, "key")?,
        fields: value.get("fields")?.clone(),
        files,
    })
}

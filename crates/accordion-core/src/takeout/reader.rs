//! The reader: a copy of the server, run in the sandbox as
//! `<server> takeout-read`, that reads an export's parts (decision 34).
//!
//! - `takeout-read plan <part>...` writes one line of JSON: the parts, each
//!   top-level folder with the product that claimed it, its items, and
//!   their size, and the entries left out.
//! - `takeout-read unpack <take> <from> <budget> <folder> <part>...` unpacks
//!   the items `take` names (a JSON list of `{"product": id}` and
//!   `{"other": folder}`), from item `from`, up to `budget` bytes, into
//!   `folder`. It writes one line of JSON per item, then `{"next": …}`.
//!
//! When it can't read the parts, it says why on its error output, in words
//! the import's report can show, and stops with status 1.

use std::collections::{BTreeMap, HashSet};
use std::ffi::OsString;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};

use super::archive::{Export, Skipped};
use super::{FolderEntry, Planned, PlannedFile, Product, ProductFolder};

pub const COMMAND: &str = "takeout-read";

/// How many skipped entries a plan lists by name. The rest are counted.
pub const MAX_SKIPPED_LISTED: usize = 1000;

pub fn main(args: impl Iterator<Item = OsString>, products: &[&dyn Product]) -> i32 {
    let args: Vec<OsString> = args.collect();
    let mut out = std::io::stdout().lock();
    let result = match args.first().and_then(|command| command.to_str()) {
        Some("plan") => plan(&args[1..], products, &mut out),
        Some("unpack") => unpack(&args[1..], products, &mut out),
        _ => Err(
            "Usage: takeout-read plan <part>... | unpack <take> <from> <budget> <folder> <part>..."
                .to_owned(),
        ),
    };
    match result.and_then(|()| out.flush().map_err(|err| err.to_string())) {
        Ok(()) => 0,
        Err(message) => {
            eprintln!("{message}");
            1
        }
    }
}

/// One top-level folder of the export and what's planned from it.
struct Planning {
    name: String,
    product: Option<&'static str>,
    items: Vec<Planned>,
}

/// The export's parts, opened and planned.
struct Read {
    export: Export,
    folders: Vec<Planning>,
    skipped: Vec<Skipped>,
}

fn read(parts: &[OsString], products: &[&dyn Product]) -> Result<Read, String> {
    if parts.is_empty() {
        return Err("No archives were given.".to_owned());
    }
    let paths: Vec<PathBuf> = parts.iter().map(PathBuf::from).collect();
    let paths: Vec<&Path> = paths.iter().map(PathBuf::as_path).collect();
    let wants = |path: &str, _size: u64| {
        inside_folder(path)
            .is_some_and(|(_, inner)| products.iter().any(|product| product.wants(inner)))
    };
    let export = Export::open(&paths, &wants).map_err(|err| sentence(&err.to_string()))?;
    let (folders, skipped) = plan_folders(&export, products);
    Ok(Read {
        export,
        folders,
        skipped,
    })
}

/// The product folder an entry is in and its path inside it, for a path
/// like `Takeout/Keep/Groceries.json`.
fn inside_folder(path: &str) -> Option<(&str, &str)> {
    let mut parts = path.splitn(3, '/');
    let _top = parts.next()?;
    let folder = parts.next()?;
    let inner = parts.next()?;
    Some((folder, inner))
}

/// Groups the export's entries by top-level folder and lets each product
/// claim the ones it knows, in the order the products are given. The
/// folders' order, and each product's items, stay the same on every pass.
fn plan_folders(export: &Export, products: &[&dyn Product]) -> (Vec<Planning>, Vec<Skipped>) {
    let mut skipped = export.skipped.clone();
    let top = export
        .entries
        .first()
        .map(|entry| entry.path.split('/').next().unwrap_or_default().to_owned());
    let mut groups: BTreeMap<&str, Vec<FolderEntry>> = BTreeMap::new();
    for entry in &export.entries {
        let mut parts = entry.path.splitn(3, '/');
        let first = parts.next().unwrap_or_default();
        if Some(first) != top.as_deref() {
            skipped.push(Skipped {
                path: entry.path.clone(),
                why: "it's outside the export's folder",
            });
            continue;
        }
        let (Some(folder), inner) = (parts.next(), parts.next()) else {
            continue;
        };
        let Some(inner) = inner else {
            if !entry.folder {
                skipped.push(Skipped {
                    path: entry.path.clone(),
                    why: "it isn't in a product's folder",
                });
            }
            continue;
        };
        groups.entry(folder).or_default().push(FolderEntry {
            id: entry.id,
            path: inner.to_owned(),
            size: entry.size,
            folder: entry.folder,
        });
    }
    let folders = groups
        .into_iter()
        .map(|(name, entries)| {
            let folder = ProductFolder {
                name,
                entries: &entries,
                small: &export.small,
            };
            let claimed = products
                .iter()
                .find_map(|product| product.plan(&folder).map(|items| (product.id(), items)));
            let (product, items) = match claimed {
                Some((id, items)) => (Some(id), items),
                None => (None, file_by_file(&entries)),
            };
            Planning {
                name: name.to_owned(),
                product,
                items,
            }
        })
        .collect();
    (folders, skipped)
}

/// A folder no product claims, as one item per file.
fn file_by_file(entries: &[FolderEntry]) -> Vec<Planned> {
    entries
        .iter()
        .filter(|entry| !entry.folder)
        .map(|entry| Planned {
            key: entry.path.clone(),
            fields: json!({}),
            files: vec![PlannedFile {
                role: "file".to_owned(),
                entry: entry.id,
            }],
        })
        .collect()
}

/// The bytes an item's files take, each counted once.
fn item_bytes(export: &Export, item: &Planned) -> u64 {
    let ids: HashSet<usize> = item.files.iter().map(|file| file.entry).collect();
    ids.into_iter()
        .filter_map(|id| export.entries.get(id))
        .map(|entry| entry.size)
        .sum()
}

fn plan(args: &[OsString], products: &[&dyn Product], out: &mut impl Write) -> Result<(), String> {
    let read = read(args, products)?;
    let folders: Vec<Value> = read
        .folders
        .iter()
        .map(|folder| {
            json!({
                "folder": folder.name,
                "product": folder.product,
                "items": folder.items.len(),
                "bytes": folder.items.iter().map(|item| item_bytes(&read.export, item)).sum::<u64>(),
            })
        })
        .collect();
    let parts: Vec<Value> = read
        .export
        .parts()
        .map(|(format, entries)| json!({ "format": format.name(), "entries": entries }))
        .collect();
    let skipped: Vec<Value> = read
        .skipped
        .iter()
        .take(MAX_SKIPPED_LISTED)
        .map(|skipped| json!({ "path": skipped.path, "why": skipped.why }))
        .collect();
    let plan = json!({
        "parts": parts,
        "folders": folders,
        "skipped": skipped,
        "skipped_count": read.skipped.len(),
    });
    writeln!(out, "{plan}").map_err(|err| err.to_string())
}

fn unpack(
    args: &[OsString],
    products: &[&dyn Product],
    out: &mut impl Write,
) -> Result<(), String> {
    let usage =
        || "Usage: takeout-read unpack <take> <from> <budget> <folder> <part>...".to_owned();
    let [take, from, budget, folder, parts @ ..] = args else {
        return Err(usage());
    };
    let take: Value = take
        .to_str()
        .and_then(|take| serde_json::from_str(take).ok())
        .ok_or_else(usage)?;
    let number = |arg: &OsString| arg.to_str().and_then(|arg| arg.parse::<u64>().ok());
    let (from, budget) = (
        number(from).ok_or_else(usage)?,
        number(budget).ok_or_else(usage)?,
    );
    let folder = PathBuf::from(folder);
    let taken = |planning: &Planning| {
        take.as_array().is_some_and(|take| {
            take.iter().any(|take| match planning.product {
                Some(id) => take.get("product").and_then(Value::as_str) == Some(id),
                None => take.get("other").and_then(Value::as_str) == Some(planning.name.as_str()),
            })
        })
    };

    let mut read = read(parts, products)?;
    let all: Vec<(&Planning, &Planned)> = read
        .folders
        .iter()
        .filter(|planning| taken(planning))
        .flat_map(|planning| planning.items.iter().map(move |item| (planning, item)))
        .collect();
    let start = usize::try_from(from).unwrap_or(usize::MAX).min(all.len());
    let mut batch = Vec::new();
    let mut bytes = 0u64;
    for &(planning, item) in &all[start..] {
        let size = item_bytes(&read.export, item);
        if !batch.is_empty() && bytes.saturating_add(size) > budget {
            break;
        }
        bytes = bytes.saturating_add(size);
        batch.push((planning, item));
    }
    let next = start + batch.len();
    let ids: HashSet<usize> = batch
        .iter()
        .flat_map(|(_, item)| item.files.iter().map(|file| file.entry))
        .collect();
    let lines: Vec<Value> = batch
        .iter()
        .map(|(planning, item)| {
            let files: Vec<Value> = item
                .files
                .iter()
                .filter_map(|file| {
                    let entry = read.export.entries.get(file.entry)?;
                    Some(json!({
                        "role": file.role,
                        "path": entry.path,
                        "name": entry.id.to_string(),
                        "size": entry.size,
                    }))
                })
                .collect();
            json!({
                "folder": planning.name,
                "product": planning.product,
                "key": item.key,
                "fields": item.fields,
                "files": files,
            })
        })
        .collect();
    read.export
        .unpack(&ids, &folder)
        .map_err(|err| sentence(&err.to_string()))?;
    for line in lines {
        writeln!(out, "{line}").map_err(|err| err.to_string())?;
    }
    let next = (next < all.len()).then_some(next);
    writeln!(out, "{}", json!({ "next": next })).map_err(|err| err.to_string())
}

/// `message` as a sentence for the report: capitalized, with a period.
fn sentence(message: &str) -> String {
    let mut chars = message.chars();
    let Some(first) = chars.next() else {
        return String::new();
    };
    let mut sentence: String = first.to_uppercase().chain(chars).collect();
    if !sentence.ends_with('.') {
        sentence.push('.');
    }
    sentence
}

//! The sandbox launcher on its own, for the thumbnail and text tests. On a
//! server, the server's own program is the launcher. It also reads the text
//! of Office files and the covers and details of e-books, as the server's
//! program does, and reads Takeout archives with a made-up product, for the
//! tests.

use accordion_core::takeout::{self, Planned, PlannedFile, Product, ProductFolder};
use accordion_core::{texts, thumbnails};
use serde_json::{Value, json};

fn main() {
    let mut args = std::env::args_os().skip(1);
    match args.next() {
        Some(command) if command == texts::office::COMMAND => {
            std::process::exit(texts::office::main(args, texts::MAX_TEXT));
        }
        Some(command) if command == thumbnails::book::COMMAND => {
            std::process::exit(thumbnails::book::main(args));
        }
        Some(command) if command == takeout::COMMAND => {
            std::process::exit(takeout::reader::main(args, &[&Notes]));
        }
        _ => accordion_sandbox::main(),
    }
}

/// A product shaped like Keep, for the tests: each `.json` file with a
/// `createdTimestampUsec` is a note, with its attachments beside it.
struct Notes;

impl Product for Notes {
    fn id(&self) -> &'static str {
        "notes"
    }

    fn wants(&self, path: &str) -> bool {
        path.ends_with(".json")
    }

    fn plan(&self, folder: &ProductFolder) -> Option<Vec<Planned>> {
        let mut items = Vec::new();
        for entry in folder
            .entries
            .iter()
            .filter(|entry| entry.path.ends_with(".json"))
        {
            let Some(note) = folder
                .read(entry)
                .and_then(|json| serde_json::from_slice::<Value>(json).ok())
            else {
                continue;
            };
            let Some(created) = note.get("createdTimestampUsec").and_then(Value::as_u64) else {
                continue;
            };
            let title = note
                .get("title")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let mut files = vec![PlannedFile {
                role: "note".to_owned(),
                entry: entry.id,
            }];
            let mut missing = Vec::new();
            for attachment in note
                .get("attachments")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let Some(name) = attachment.get("filePath").and_then(Value::as_str) else {
                    continue;
                };
                match folder.entries.iter().find(|other| other.path == name) {
                    Some(other) => files.push(PlannedFile {
                        role: "attachment".to_owned(),
                        entry: other.id,
                    }),
                    None => missing.push(name.to_owned()),
                }
            }
            items.push(Planned {
                key: format!("{created} {title}"),
                fields: json!({ "title": title, "missing": missing }),
                files,
            });
        }
        (!items.is_empty()).then_some(items)
    }
}

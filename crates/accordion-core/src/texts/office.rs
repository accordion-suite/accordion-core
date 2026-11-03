//! Reads the text inside Word, Excel, PowerPoint, and OpenDocument files.
//! Each is a zip file of XML, so this needs no LibreOffice. It runs in the
//! sandbox, as `<server> office-text <extension>`, with the file on standard
//! input, because the files are untrusted (decision 1).

use std::io::{self, BufReader, Read, Seek, Write};
use std::os::fd::AsFd;

use quick_xml::Reader;
use quick_xml::events::Event;

/// The command the server's program answers, after which comes the file's
/// extension.
pub const COMMAND: &str = "office-text";

/// The formats this reads, by extension.
pub(crate) const EXTENSIONS: [&str; 6] = ["docx", "xlsx", "pptx", "odt", "ods", "odp"];

/// The most XML read from one part of a file, so a small file that expands
/// to a huge one stops here instead.
const MAX_PART: u64 = 256 * 1024 * 1024;

/// Runs the command: reads the file on standard input, and writes its text
/// to standard output, up to `limit` bytes. Returns the exit code.
pub fn main(mut args: impl Iterator<Item = std::ffi::OsString>, limit: usize) -> i32 {
    let Some(extension) = args.next().and_then(|arg| arg.into_string().ok()) else {
        eprintln!("Usage: {COMMAND} <extension>");
        return 2;
    };
    // Standard input is the stored file itself, so it can be read in any
    // order, as a zip file has to be.
    let file = io::stdin().as_fd().try_clone_to_owned();
    let file = match file.map(std::fs::File::from) {
        Ok(file) => file,
        Err(err) => {
            eprintln!("couldn't read the file: {err}");
            return 1;
        }
    };
    match text(&extension, file, limit) {
        Ok(text) => match io::stdout().lock().write_all(text.as_bytes()) {
            Ok(()) => 0,
            Err(err) => {
                eprintln!("couldn't write the text: {err}");
                1
            }
        },
        Err(problem) => {
            eprintln!("{problem}");
            1
        }
    }
}

/// How a format keeps its text.
#[derive(Clone, Copy)]
enum Layout {
    /// Office Open XML, from Word, Excel, and PowerPoint: text is inside
    /// `t` elements, and paragraphs are `p`.
    Open,
    /// OpenDocument: all text inside the `body` element is the document's.
    Odf,
}

/// The text of the file in `file`, a format named by `extension`, up to
/// `limit` bytes.
pub(crate) fn text(
    extension: &str,
    file: impl Read + Seek,
    limit: usize,
) -> Result<String, String> {
    let mut archive =
        zip::ZipArchive::new(file).map_err(|err| format!("isn't a zip file: {err}"))?;
    let (layout, parts): (Layout, Vec<String>) = match extension {
        "docx" => (Layout::Open, vec!["word/document.xml".into()]),
        "xlsx" => {
            // Most cells' text is shared, and the sheets have the rest.
            let mut parts = vec!["xl/sharedStrings.xml".into()];
            parts.extend(numbered(&archive, "xl/worksheets/sheet"));
            (Layout::Open, parts)
        }
        "pptx" => (Layout::Open, numbered(&archive, "ppt/slides/slide")),
        "odt" | "ods" | "odp" => (Layout::Odf, vec!["content.xml".into()]),
        _ => return Err(format!("can't read text from .{extension} files")),
    };
    let mut text = String::new();
    for part in parts {
        let entry = match archive.by_name(&part) {
            Ok(entry) => entry,
            // A workbook without shared text, for example.
            Err(zip::result::ZipError::FileNotFound) => continue,
            Err(err) => return Err(format!("couldn't read {part}: {err}")),
        };
        read_part(
            BufReader::new(entry.take(MAX_PART)),
            layout,
            &mut text,
            limit,
        )
        .map_err(|err| format!("couldn't read {part}: {err}"))?;
        if text.len() >= limit {
            break;
        }
    }
    let mut end = text.len().min(limit);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
    Ok(text)
}

/// The parts named `<prefix><number>.xml`, in order, such as each slide.
fn numbered<R: Read + Seek>(archive: &zip::ZipArchive<R>, prefix: &str) -> Vec<String> {
    let mut found: Vec<(u32, String)> = archive
        .file_names()
        .filter_map(|name| {
            let name = name.ok()?;
            let number = name
                .strip_prefix(prefix)?
                .strip_suffix(".xml")?
                .parse()
                .ok()?;
            Some((number, name.into_owned()))
        })
        .collect();
    found.sort();
    found.into_iter().map(|(_, name)| name).collect()
}

/// Adds the text of one XML part to `text`, until it holds `limit` bytes.
fn read_part(
    input: impl io::BufRead,
    layout: Layout,
    text: &mut String,
    limit: usize,
) -> Result<(), quick_xml::Error> {
    let mut reader = Reader::from_reader(input);
    let mut buf = Vec::new();
    // How many elements deep inside text, a `t` or the `body`.
    let mut inside = 0usize;
    let is_text = |name: &str| match layout {
        Layout::Open => name == "t",
        Layout::Odf => name == "body",
    };
    while text.len() < limit {
        match reader.read_event_into(&mut buf)? {
            Event::Start(element) => {
                if inside > 0 || is_text(element.local_name().into_inner()) {
                    inside += 1;
                }
            }
            Event::End(element) => {
                inside = inside.saturating_sub(1);
                if matches!(element.local_name().into_inner(), "p" | "h" | "si") {
                    text.push('\n');
                }
            }
            Event::Empty(element) => match element.local_name().into_inner() {
                "tab" => text.push('\t'),
                "br" | "cr" | "line-break" => text.push('\n'),
                "s" if matches!(layout, Layout::Odf) && inside > 0 => text.push(' '),
                _ => {}
            },
            Event::Text(words) if inside > 0 => text.push_str(&words.into_inner()),
            Event::CData(words) if inside > 0 => text.push_str(&words.into_inner()),
            Event::GeneralRef(reference) if inside > 0 => {
                if let Some(character) = reference.resolve_char_ref()? {
                    text.push(character);
                } else {
                    let name = reference.into_inner();
                    if let Some(entity) = quick_xml::escape::resolve_predefined_entity(&name) {
                        text.push_str(entity);
                    }
                }
            }
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }
    Ok(())
}

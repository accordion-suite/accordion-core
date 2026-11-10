//! Reads an e-book's cover and details: its title, authors, description,
//! and table of contents. EPUB is a zip file of XML, FictionBook is one XML
//! file, and Mobipocket and Kindle books are Palm databases, so this needs
//! no e-book program. It runs in the sandbox, as
//! `<server> book <format> cover|details`, with the book on standard input,
//! because the files are untrusted (decision 1).
//!
//! Details are plain text. A book's own HTML never leaves the sandbox, so
//! none of it reaches the browser.

use std::borrow::Cow;
use std::collections::HashMap;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::fd::AsFd;

use base64ct::{Base64, Encoding};
use quick_xml::Reader;
use quick_xml::events::{BytesStart, Event};

/// The command the server's program answers, after which come the book's
/// format and what to write: `cover` or `details`.
pub const COMMAND: &str = "book";

/// The formats this reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Format {
    /// EPUB, and Apple Books' `.ibooks`, which is EPUB too.
    Epub,
    /// Mobipocket and Kindle books: `.mobi`, `.azw`, `.azw3`, and `.prc`.
    Mobi,
    /// FictionBook, `.fb2`.
    Fb2,
}

impl Format {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Epub => "epub",
            Self::Mobi => "mobi",
            Self::Fb2 => "fb2",
        }
    }

    fn parse(text: &str) -> Option<Self> {
        [Self::Epub, Self::Mobi, Self::Fb2]
            .into_iter()
            .find(|format| format.as_str() == text)
    }
}

/// The most read of one XML part of an EPUB, so a small file that expands
/// to a huge one stops here instead.
const MAX_PART: u64 = 16 * 1024 * 1024;

/// The largest cover read. Larger ones are left out.
const MAX_COVER: u64 = 32 * 1024 * 1024;

/// The most of a FictionBook read. Its pictures are inside it, so it's
/// larger than its text.
const MAX_FB2: u64 = 64 * 1024 * 1024;

/// The most elements read from one XML part.
const MAX_ELEMENTS: usize = 200_000;

/// The most entries kept from a table of contents.
const MAX_CONTENTS: usize = 500;

/// How deep the table of contents goes. Deeper entries show at this depth.
const MAX_DEPTH: u32 = 5;

/// The most characters kept of a title, an author, or an entry.
const MAX_LINE: usize = 300;

/// The most characters kept of a description.
const MAX_DESCRIPTION: usize = 5000;

const MAX_AUTHORS: usize = 20;

/// What the viewer shows of a book.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Details {
    pub(crate) title: Option<String>,
    pub(crate) authors: Vec<String>,
    /// Paragraphs, with `\n` between them.
    pub(crate) description: Option<String>,
    pub(crate) contents: Vec<Entry>,
}

/// One entry in a table of contents.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Entry {
    pub(crate) title: String,
    /// 0 for a top-level entry, such as a part or a chapter.
    pub(crate) depth: u32,
}

impl Details {
    pub(crate) fn to_json(&self) -> Vec<u8> {
        let contents: Vec<serde_json::Value> = self
            .contents
            .iter()
            .map(|entry| serde_json::json!({ "title": entry.title, "depth": entry.depth }))
            .collect();
        serde_json::to_vec(&serde_json::json!({
            "title": self.title,
            "authors": self.authors,
            "description": self.description,
            "contents": contents,
        }))
        .expect("JSON of strings and numbers")
    }
}

#[derive(Debug, Default)]
pub(crate) struct Book {
    pub(crate) details: Details,
    /// A JPEG, PNG, GIF, or WebP, which the worker shrinks.
    pub(crate) cover: Option<Vec<u8>>,
}

/// Runs the command: reads the book on standard input, and writes its
/// cover or its details as JSON to standard output. A book without a cover
/// writes nothing. Returns the exit code.
pub fn main(mut args: impl Iterator<Item = std::ffi::OsString>) -> i32 {
    let mut next = || args.next().and_then(|arg| arg.into_string().ok());
    let (Some(format), Some(part)) = (next().as_deref().and_then(Format::parse), next()) else {
        eprintln!("Usage: {COMMAND} epub|mobi|fb2 cover|details");
        return 2;
    };
    if part != "cover" && part != "details" {
        eprintln!("Usage: {COMMAND} epub|mobi|fb2 cover|details");
        return 2;
    }
    // Standard input is the stored file itself, so it can be read in any
    // order, as a zip file has to be.
    let file = match io::stdin().as_fd().try_clone_to_owned() {
        Ok(file) => std::fs::File::from(file),
        Err(err) => {
            eprintln!("couldn't read the file: {err}");
            return 1;
        }
    };
    let book = match read(format, file) {
        Ok(book) => book,
        Err(problem) => {
            eprintln!("{problem}");
            return 1;
        }
    };
    let output = if part == "cover" {
        book.cover.unwrap_or_default()
    } else {
        book.details.to_json()
    };
    match io::stdout().lock().write_all(&output) {
        Ok(()) => 0,
        Err(err) => {
            eprintln!("couldn't write the {part}: {err}");
            1
        }
    }
}

pub(crate) fn read(format: Format, file: impl Read + Seek) -> Result<Book, String> {
    match format {
        Format::Epub => epub(file),
        Format::Mobi => mobi(file),
        Format::Fb2 => fb2(file),
    }
}

/// Whether `bytes` are a picture libvips reads with the loaders it trusts.
fn picture(bytes: &[u8]) -> bool {
    bytes.starts_with(b"\xff\xd8\xff")
        || bytes.starts_with(b"\x89PNG\r\n\x1a\n")
        || bytes.starts_with(b"GIF87a")
        || bytes.starts_with(b"GIF89a")
        || (bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WEBP"))
}

// EPUB

/// One file in an EPUB's manifest.
#[derive(Debug, Default)]
struct Item {
    id: String,
    /// Its path in the zip file.
    path: String,
    media_type: String,
    properties: String,
}

fn epub(file: impl Read + Seek) -> Result<Book, String> {
    let mut archive =
        zip::ZipArchive::new(file).map_err(|err| format!("isn't a zip file: {err}"))?;
    let container = part(&mut archive, "META-INF/container.xml")?
        .ok_or("has no META-INF/container.xml, which every EPUB has")?;
    let package_path = parse(&container)
        .find("rootfile")
        .and_then(|rootfile| rootfile.attribute("full-path"))
        .map(|path| resolve("", path))
        .ok_or("doesn't say where its package file is")?;
    let package = part(&mut archive, &package_path)?
        .ok_or_else(|| format!("doesn't have its package file, {package_path}"))?;
    let package = parse(&package);
    let base = package_path
        .rsplit_once('/')
        .map_or("", |(folder, _)| folder);

    let mut details = Details::default();
    let metadata = package.find("metadata");
    // EPUB 3 gives creators their roles in separate elements.
    let mut roles = HashMap::new();
    for meta in metadata.iter().flat_map(|metadata| metadata.all("meta")) {
        if meta.attribute("property") == Some("role")
            && let Some(id) = meta
                .attribute("refines")
                .and_then(|id| id.strip_prefix('#'))
        {
            roles.insert(id.to_owned(), meta.text());
        }
    }
    if let Some(metadata) = metadata {
        details.title = metadata.find("title").and_then(|title| line(&title.text()));
        let creators: Vec<(String, Option<String>)> = metadata
            .all("creator")
            .into_iter()
            .filter_map(|creator| {
                let role = creator.attribute("role").map(str::to_owned).or_else(|| {
                    creator
                        .attribute("id")
                        .and_then(|id| roles.get(id).cloned())
                });
                Some((line(&creator.text())?, role))
            })
            .collect();
        // Illustrators and editors are creators too, but the authors come
        // first.
        let authors: Vec<String> = creators
            .iter()
            .filter(|(_, role)| role.as_deref().is_none_or(|role| role.trim() == "aut"))
            .map(|(name, _)| name.clone())
            .collect();
        details.authors = if authors.is_empty() {
            creators.into_iter().map(|(name, _)| name).collect()
        } else {
            authors
        };
        details.authors.dedup();
        details.authors.truncate(MAX_AUTHORS);
        details.description = metadata
            .find("description")
            .and_then(|description| paragraphs(&plain(&description.text())));
    }

    let items: Vec<Item> = package
        .find("manifest")
        .map(|manifest| manifest.all("item"))
        .unwrap_or_default()
        .into_iter()
        .map(|item| Item {
            id: item.attribute("id").unwrap_or_default().to_owned(),
            path: resolve(base, item.attribute("href").unwrap_or_default()),
            media_type: item.attribute("media-type").unwrap_or_default().to_owned(),
            properties: item.attribute("properties").unwrap_or_default().to_owned(),
        })
        .collect();
    let has =
        |item: &Item, property: &str| item.properties.split_whitespace().any(|p| p == property);

    // EPUB 3's navigation document, or else EPUB 2's NCX.
    if let Some(nav) = items.iter().find(|item| has(item, "nav"))
        && let Some(nav) = part(&mut archive, &nav.path)?
    {
        details.contents = nav_contents(&parse(&nav));
    }
    if details.contents.is_empty() {
        let toc = package
            .find("spine")
            .and_then(|spine| spine.attribute("toc"));
        let ncx = items
            .iter()
            .find(|item| Some(item.id.as_str()) == toc)
            .or_else(|| {
                items
                    .iter()
                    .find(|item| item.media_type == "application/x-dtbncx+xml")
            });
        if let Some(ncx) = ncx
            && let Some(ncx) = part(&mut archive, &ncx.path)?
        {
            details.contents = ncx_contents(&parse(&ncx));
        }
    }

    // EPUB 3 marks the cover, and EPUB 2 names it in the metadata. Older
    // books only call it a cover.
    let named = metadata
        .iter()
        .flat_map(|metadata| metadata.all("meta"))
        .find(|meta| meta.attribute("name") == Some("cover"))
        .and_then(|meta| meta.attribute("content"));
    let image = |item: &&Item| item.media_type.starts_with("image/");
    let cover = items
        .iter()
        .find(|item| has(item, "cover-image"))
        .or_else(|| {
            named.and_then(|named| {
                items
                    .iter()
                    .filter(image)
                    .find(|item| item.id == named || item.path == resolve(base, named))
            })
        })
        .or_else(|| {
            items.iter().filter(image).find(|item| {
                item.id.to_ascii_lowercase().contains("cover")
                    || item.path.to_ascii_lowercase().contains("cover")
            })
        });
    let cover = match cover {
        Some(cover) => entry(&mut archive, &cover.path, MAX_COVER)?.filter(|cover| picture(cover)),
        None => None,
    };
    Ok(Book { details, cover })
}

/// The name of the file at `path` in `archive`, which may differ in case,
/// as books made on Windows sometimes do.
fn find_name<R: Read + Seek>(archive: &zip::ZipArchive<R>, path: &str) -> Option<String> {
    if archive.index_for_name(path).is_some() {
        return Some(path.to_owned());
    }
    archive
        .file_names()
        .filter_map(Result::ok)
        .find(|name| name.eq_ignore_ascii_case(path))
        .map(Cow::into_owned)
}

/// Up to `limit` bytes of the file at `path` in `archive`, if it has one.
/// Larger files are left out.
fn entry<R: Read + Seek>(
    archive: &mut zip::ZipArchive<R>,
    path: &str,
    limit: u64,
) -> Result<Option<Vec<u8>>, String> {
    let Some(name) = find_name(archive, path) else {
        return Ok(None);
    };
    let entry = archive
        .by_name(&name)
        .map_err(|err| format!("couldn't read {name}: {err}"))?;
    let mut bytes = Vec::new();
    entry
        .take(limit + 1)
        .read_to_end(&mut bytes)
        .map_err(|err| format!("couldn't read {name}: {err}"))?;
    Ok((bytes.len() as u64 <= limit).then_some(bytes))
}

/// The XML file at `path` in `archive`, as text.
fn part<R: Read + Seek>(
    archive: &mut zip::ZipArchive<R>,
    path: &str,
) -> Result<Option<String>, String> {
    Ok(entry(archive, path, MAX_PART)?.map(|bytes| unicode(&bytes)))
}

/// The path in the zip file of `href`, a link from a file in `folder`.
fn resolve(folder: &str, href: &str) -> String {
    let href = href.split('#').next().unwrap_or_default();
    let href = unpercent(href);
    let mut parts: Vec<&str> = if href.starts_with('/') {
        Vec::new()
    } else {
        folder.split('/').filter(|part| !part.is_empty()).collect()
    };
    for part in href.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            part => parts.push(part),
        }
    }
    parts.join("/")
}

fn unpercent(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = |at: usize| bytes.get(at).and_then(|b| (*b as char).to_digit(16));
        if bytes[i] == b'%'
            && let (Some(high), Some(low)) = (hex(i + 1), hex(i + 2))
        {
            out.push((high * 16 + low) as u8);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The table of contents in an EPUB 3 navigation document.
fn nav_contents(document: &Element) -> Vec<Entry> {
    let navs = document.all("nav");
    let toc = navs
        .iter()
        .find(|nav| {
            nav.attribute("type")
                .is_some_and(|kind| kind.split_whitespace().any(|kind| kind == "toc"))
        })
        .or(navs.first());
    let mut contents = Vec::new();
    if let Some(list) = toc.and_then(|toc| toc.find("ol")) {
        nav_list(list, 0, &mut contents);
    }
    contents
}

fn nav_list(list: &Element, depth: u32, contents: &mut Vec<Entry>) {
    for item in list.elements().filter(|element| element.name == "li") {
        if contents.len() >= MAX_CONTENTS {
            return;
        }
        let label = item
            .elements()
            .find(|element| element.name == "a" || element.name == "span");
        if let Some(title) = label.and_then(|label| line(&label.text())) {
            contents.push(Entry {
                title,
                depth: depth.min(MAX_DEPTH),
            });
        }
        if let Some(inner) = item.elements().find(|element| element.name == "ol") {
            nav_list(inner, depth + 1, contents);
        }
    }
}

/// The table of contents in an EPUB 2 NCX file.
fn ncx_contents(document: &Element) -> Vec<Entry> {
    let mut contents = Vec::new();
    if let Some(map) = document.find("navMap") {
        ncx_points(map, 0, &mut contents);
    }
    contents
}

fn ncx_points(parent: &Element, depth: u32, contents: &mut Vec<Entry>) {
    for point in parent
        .elements()
        .filter(|element| element.name == "navPoint")
    {
        if contents.len() >= MAX_CONTENTS {
            return;
        }
        let title = point
            .elements()
            .find(|element| element.name == "navLabel")
            .and_then(|label| label.find("text"))
            .and_then(|text| line(&text.text()));
        if let Some(title) = title {
            contents.push(Entry {
                title,
                depth: depth.min(MAX_DEPTH),
            });
        }
        ncx_points(point, depth + 1, contents);
    }
}

// A small tree of an XML file, for EPUB's parts, which are small.

#[derive(Debug, Default)]
struct Element {
    /// The local name, without a namespace prefix.
    name: String,
    /// Local names and values.
    attributes: Vec<(String, String)>,
    children: Vec<Node>,
}

#[derive(Debug)]
enum Node {
    Element(Element),
    Text(String),
}

impl Element {
    fn attribute(&self, name: &str) -> Option<&str> {
        self.attributes
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }

    fn elements(&self) -> impl Iterator<Item = &Element> {
        self.children.iter().filter_map(|child| match child {
            Node::Element(element) => Some(element),
            Node::Text(_) => None,
        })
    }

    /// The first element inside this one named `name`, at any depth.
    fn find(&self, name: &str) -> Option<&Element> {
        self.elements().find_map(|element| {
            (element.name == name)
                .then_some(element)
                .or_else(|| element.find(name))
        })
    }

    /// Every element inside this one named `name`, at any depth, in order.
    fn all(&self, name: &str) -> Vec<&Element> {
        let mut found = Vec::new();
        self.collect(name, &mut found);
        found
    }

    fn collect<'a>(&'a self, name: &str, found: &mut Vec<&'a Element>) {
        for element in self.elements() {
            if element.name == name {
                found.push(element);
            }
            element.collect(name, found);
        }
    }

    /// All the text inside, with a space where an element ends.
    fn text(&self) -> String {
        let mut text = String::new();
        self.add_text(&mut text);
        text
    }

    fn add_text(&self, text: &mut String) {
        for child in &self.children {
            match child {
                Node::Text(words) => text.push_str(words),
                Node::Element(element) => {
                    element.add_text(text);
                    text.push(' ');
                }
            }
        }
    }
}

fn attributes(element: &BytesStart<'_>) -> Vec<(String, String)> {
    element
        .attributes()
        .with_checks(false)
        .filter_map(Result::ok)
        .map(|attribute| {
            let value = attribute
                .normalized_value(quick_xml::XmlVersion::default())
                .map(Cow::into_owned)
                .unwrap_or_else(|_| attribute.value.clone().into_owned());
            (attribute.key.local_name().into_inner().to_owned(), value)
        })
        .collect()
}

fn reader(xml: &str) -> Reader<&[u8]> {
    let mut reader = Reader::from_str(xml);
    // Books are often sloppy, so mismatched tags are sorted out here.
    let config = reader.config_mut();
    config.check_end_names = false;
    config.allow_unmatched_ends = true;
    config.allow_dangling_amp = true;
    reader
}

/// Adds a text event's words to `text`, and returns whether it was one.
fn add_words(event: &Event<'_>, text: &mut String) -> bool {
    match event {
        Event::Text(words) => text.push_str(&words.clone().into_inner()),
        Event::CData(words) => text.push_str(&words.clone().into_inner()),
        Event::GeneralRef(reference) => {
            if let Ok(Some(character)) = reference.resolve_char_ref() {
                text.push(character);
            } else {
                let name = reference.clone().into_inner();
                match quick_xml::escape::resolve_predefined_entity(&name) {
                    Some(entity) => text.push_str(entity),
                    // XHTML's named characters that books use most.
                    None => text.push_str(match &*name {
                        "nbsp" => " ",
                        "mdash" => "—",
                        "ndash" => "–",
                        "hellip" => "…",
                        "rsquo" => "’",
                        "lsquo" => "‘",
                        "rdquo" => "”",
                        "ldquo" => "“",
                        _ => "",
                    }),
                }
            }
        }
        _ => return false,
    }
    true
}

/// The tree of `xml`, as far as it reads. A broken file gives what came
/// before the break.
fn parse(xml: &str) -> Element {
    let mut reader = reader(xml);
    let mut open: Vec<Element> = vec![Element::default()];
    let mut count = 0;
    let close = |open: &mut Vec<Element>| {
        let element = open.pop().expect("the root stays");
        open.last_mut()
            .expect("the root stays")
            .children
            .push(Node::Element(element));
    };
    loop {
        let event = match reader.read_event() {
            Ok(Event::Eof) | Err(_) => break,
            Ok(event) => event,
        };
        let mut words = String::new();
        if add_words(&event, &mut words) {
            open.last_mut()
                .expect("the root stays")
                .children
                .push(Node::Text(words));
            continue;
        }
        let (start, empty) = match &event {
            Event::Start(start) => (Some(start), false),
            Event::Empty(start) => (Some(start), true),
            _ => (None, false),
        };
        if let Some(start) = start {
            count += 1;
            if count > MAX_ELEMENTS {
                break;
            }
            open.push(Element {
                name: start.local_name().into_inner().to_owned(),
                attributes: attributes(start),
                children: Vec::new(),
            });
            if empty {
                close(&mut open);
            }
            continue;
        }
        if let Event::End(end) = event {
            let name = end.local_name().into_inner();
            // An end tag closes its element and any left open inside it.
            // One that matches nothing is ignored.
            if let Some(at) = open.iter().rposition(|element| element.name == name)
                && at > 0
            {
                while open.len() > at {
                    close(&mut open);
                }
            }
        }
    }
    while open.len() > 1 {
        close(&mut open);
    }
    open.pop().expect("the root stays")
}

// Mobipocket and Kindle

fn u16_at(bytes: &[u8], at: usize) -> Option<u16> {
    Some(u16::from_be_bytes(bytes.get(at..at + 2)?.try_into().ok()?))
}

fn u32_at(bytes: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_be_bytes(bytes.get(at..at + 4)?.try_into().ok()?))
}

/// A Palm database's records, by where each starts and ends.
struct Records<R> {
    file: R,
    offsets: Vec<u64>,
    end: u64,
}

impl<R: Read + Seek> Records<R> {
    /// Record `index`, if it's at most `limit` bytes.
    fn get(&mut self, index: usize, limit: u64) -> Result<Option<Vec<u8>>, String> {
        let Some(&start) = self.offsets.get(index) else {
            return Ok(None);
        };
        let stop = self.offsets.get(index + 1).copied().unwrap_or(self.end);
        if stop < start || stop > self.end || stop - start > limit {
            return Ok(None);
        }
        let mut record = vec![0; (stop - start) as usize];
        self.file
            .seek(SeekFrom::Start(start))
            .and_then(|_| self.file.read_exact(&mut record))
            .map_err(|err| format!("couldn't read the book: {err}"))?;
        Ok(Some(record))
    }
}

/// The EXTH record types this reads.
const EXTH_AUTHOR: u32 = 100;
const EXTH_DESCRIPTION: u32 = 103;
const EXTH_COVER: u32 = 201;
const EXTH_THUMBNAIL: u32 = 202;
const EXTH_TITLE: u32 = 503;

/// Mobipocket's code page for Windows-1252. Others are UTF-8.
const CODE_PAGE_1252: u32 = 1252;

/// Reads a Mobipocket or Kindle book. Its details are in the EXTH header of
/// its first record, and its cover is one of its picture records. Its table
/// of contents is inside its compressed text, which this doesn't read.
fn mobi(mut file: impl Read + Seek) -> Result<Book, String> {
    let mut header = [0; 78];
    file.read_exact(&mut header)
        .map_err(|_| "is too short for a Mobipocket book")?;
    if header.get(60..68) != Some(b"BOOKMOBI") {
        return Err("isn't a Mobipocket book".into());
    }
    let count = usize::from(u16_at(&header, 76).unwrap_or(0));
    let mut table = vec![0; count * 8];
    file.read_exact(&mut table)
        .map_err(|_| "is cut off in its list of records")?;
    let end = file
        .seek(SeekFrom::End(0))
        .map_err(|err| format!("couldn't read the book: {err}"))?;
    let offsets = table
        .as_chunks::<8>()
        .0
        .iter()
        .map(|entry| u64::from(u32_at(entry, 0).unwrap_or(0)))
        .collect();
    let mut records = Records { file, offsets, end };
    let first = records
        .get(0, MAX_PART)?
        .ok_or("doesn't have its first record")?;
    if first.get(16..20) != Some(b"MOBI") {
        return Err("doesn't have a Mobipocket header".into());
    }
    let header_length = u32_at(&first, 20).unwrap_or(0) as usize;
    let utf8 = u32_at(&first, 28) != Some(CODE_PAGE_1252);
    let decode = |bytes: &[u8]| {
        if utf8 {
            String::from_utf8_lossy(bytes).into_owned()
        } else {
            windows_1252(bytes)
        }
    };
    let full_name = u32_at(&first, 84)
        .zip(u32_at(&first, 88))
        .and_then(|(at, length)| {
            first.get(at as usize..(at as usize).checked_add(length as usize)?)
        })
        .map(decode);
    let first_image = u32_at(&first, 108).filter(|index| *index != u32::MAX);
    let has_exth = u32_at(&first, 128).is_some_and(|flags| flags & 0x40 != 0);

    let mut details = Details::default();
    let (mut cover, mut thumbnail) = (None, None);
    let exth = 16 + header_length;
    if has_exth && first.get(exth..exth + 4) == Some(b"EXTH") {
        let count = u32_at(&first, exth + 8).unwrap_or(0);
        let mut at = exth + 12;
        for _ in 0..count.min(10_000) {
            let (Some(kind), Some(length)) = (u32_at(&first, at), u32_at(&first, at + 4)) else {
                break;
            };
            let length = length as usize;
            let Some(data) = first.get(at + 8..at.saturating_add(length)) else {
                break;
            };
            if length < 8 {
                break;
            }
            match kind {
                EXTH_AUTHOR => {
                    if let Some(author) = line(&decode(data)) {
                        details.authors.push(author);
                    }
                }
                EXTH_DESCRIPTION => details.description = paragraphs(&plain(&decode(data))),
                EXTH_COVER => cover = u32_at(data, 0),
                EXTH_THUMBNAIL => thumbnail = u32_at(data, 0),
                EXTH_TITLE => details.title = line(&decode(data)),
                _ => {}
            }
            at += length;
        }
    }
    details.authors.dedup();
    details.authors.truncate(MAX_AUTHORS);
    if details.title.is_none() {
        details.title = full_name.as_deref().and_then(line);
    }
    // Some books have only the small thumbnail Kindles show.
    let mut picture_record = None;
    for offset in [cover, thumbnail].into_iter().flatten() {
        let Some(index) = first_image.and_then(|first| first.checked_add(offset)) else {
            continue;
        };
        if let Some(record) = records.get(index as usize, MAX_COVER)?
            && picture(&record)
        {
            picture_record = Some(record);
            break;
        }
    }
    Ok(Book {
        details,
        cover: picture_record,
    })
}

// FictionBook

/// What's being read from a FictionBook.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Field {
    Title,
    /// First, middle, and last name, and nickname.
    Name(usize),
    Annotation,
    Contents(u32),
    Cover,
}

/// Reads a FictionBook: one XML file with the book's details in its
/// `description`, its chapters as nested `section`s, and its pictures as
/// base64 in `binary` elements.
fn fb2(file: impl Read) -> Result<Book, String> {
    let mut bytes = Vec::new();
    file.take(MAX_FB2)
        .read_to_end(&mut bytes)
        .map_err(|err| format!("couldn't read the book: {err}"))?;
    let xml = declared(&bytes)?;
    let mut reader = reader(&xml);
    let mut details = Details::default();
    let mut open: Vec<String> = Vec::new();
    let mut cover_id: Option<String> = None;
    let mut cover: Option<Vec<u8>> = None;
    let mut name: [String; 4] = Default::default();
    // What's being read, how deep its element is, and its text so far.
    let mut reading: Option<(Field, usize)> = None;
    let mut text = String::new();
    // How deep in the book's own body's sections, and whether in the body.
    // Other bodies are notes and comments.
    let mut sections = 0u32;
    let mut body: Option<usize> = None;
    let mut bodies = 0;
    loop {
        let event = match reader.read_event() {
            Ok(Event::Eof) | Err(_) => break,
            Ok(event) => event,
        };
        if reading.is_some() && add_words(&event, &mut text) {
            if text.len() > (MAX_COVER as usize) * 4 / 3 + 4096 {
                break;
            }
            continue;
        }
        let (start, empty) = match &event {
            Event::Start(start) => (Some(start), false),
            Event::Empty(start) => (Some(start), true),
            _ => (None, false),
        };
        if let Some(start) = start {
            let element = start.local_name().into_inner();
            let inside = |name: &str| open.iter().any(|open| open == name);
            let parent = open.last().map(String::as_str);
            let depth = open.len();
            let info = inside("title-info");
            match element {
                "book-title" if info => reading = Some((Field::Title, depth)),
                "author" if info && !inside("author") => name = Default::default(),
                "first-name" | "middle-name" | "last-name" | "nickname"
                    if info && inside("author") =>
                {
                    let part = ["first-name", "middle-name", "last-name", "nickname"]
                        .iter()
                        .position(|part| *part == element)
                        .expect("one of the four");
                    reading = Some((Field::Name(part), depth));
                }
                "annotation" if info => reading = Some((Field::Annotation, depth)),
                "image" if info && inside("coverpage") && cover_id.is_none() => {
                    cover_id = attributes(start)
                        .into_iter()
                        .find(|(key, _)| key == "href")
                        .map(|(_, href)| href.trim_start_matches('#').to_owned());
                }
                "body" if !empty => {
                    bodies += 1;
                    if bodies == 1 || attributes(start).iter().all(|(key, _)| key != "name") {
                        body = body.or(Some(depth));
                    }
                }
                "section" if body.is_some() && !empty => sections += 1,
                "title" if body.is_some() && parent == Some("section") && reading.is_none() => {
                    reading = Some((Field::Contents(sections.saturating_sub(1)), depth));
                }
                "binary"
                    if cover.is_none()
                        && cover_id.is_some()
                        && attributes(start)
                            .iter()
                            .any(|(key, id)| key == "id" && Some(id) == cover_id.as_ref()) =>
                {
                    reading = Some((Field::Cover, depth));
                }
                _ => {}
            }
            if empty {
                if matches!(reading, Some((_, at)) if at == depth) {
                    reading = None;
                    text.clear();
                }
            } else {
                open.push(element.to_owned());
            }
            continue;
        }
        let Event::End(end) = event else {
            continue;
        };
        let element = end.local_name().into_inner();
        let Some(at) = open.iter().rposition(|open| open == element) else {
            continue;
        };
        // Paragraphs in a description, and lines in a chapter's title.
        if element == "p" {
            match reading {
                Some((Field::Annotation, _)) => text.push('\n'),
                Some(_) => text.push(' '),
                None => {}
            }
        }
        while open.len() > at {
            let closed = open.pop().expect("at least one is open");
            let depth = open.len();
            if let Some((field, start)) = reading
                && start == depth
            {
                reading = None;
                let words = std::mem::take(&mut text);
                match field {
                    Field::Title => details.title = details.title.take().or_else(|| line(&words)),
                    Field::Name(part) => name[part] = words,
                    Field::Annotation => details.description = paragraphs(&words),
                    Field::Contents(level) => {
                        if details.contents.len() < MAX_CONTENTS
                            && let Some(title) = line(&words)
                        {
                            details.contents.push(Entry {
                                title,
                                depth: level.min(MAX_DEPTH),
                            });
                        }
                    }
                    Field::Cover => {
                        let base64: String =
                            words.chars().filter(|c| !c.is_ascii_whitespace()).collect();
                        cover = Base64::decode_vec(&base64)
                            .ok()
                            .filter(|bytes| picture(bytes));
                    }
                }
            }
            match closed.as_str() {
                "author" if open.iter().any(|open| open == "title-info") => {
                    let [first, middle, last, nickname] = &name;
                    let full = [first, middle, last]
                        .iter()
                        .map(|part| part.trim())
                        .filter(|part| !part.is_empty())
                        .collect::<Vec<_>>()
                        .join(" ");
                    let full = if full.is_empty() {
                        nickname.clone()
                    } else {
                        full
                    };
                    if let Some(author) = line(&full)
                        && details.authors.len() < MAX_AUTHORS
                        && !details.authors.contains(&author)
                    {
                        details.authors.push(author);
                    }
                }
                "section" if body.is_some() => sections = sections.saturating_sub(1),
                "body" if body == Some(depth) => body = None,
                _ => {}
            }
        }
        // Everything that's shown comes before the pictures, so reading
        // stops once the cover is found.
        if cover.is_some() && body.is_none() && bodies > 0 {
            break;
        }
    }
    Ok(Book { details, cover })
}

// Text

/// `bytes` as text, from UTF-8 or UTF-16, which every EPUB part is in.
fn unicode(bytes: &[u8]) -> String {
    if let Some(utf16) = utf16(bytes) {
        return utf16;
    }
    let bytes = bytes.strip_prefix(b"\xef\xbb\xbf").unwrap_or(bytes);
    String::from_utf8_lossy(bytes).into_owned()
}

fn utf16(bytes: &[u8]) -> Option<String> {
    let big = if bytes.starts_with(b"\xfe\xff") {
        true
    } else if bytes.starts_with(b"\xff\xfe") {
        false
    } else {
        return None;
    };
    let units = bytes[2..].as_chunks::<2>().0.iter().map(|pair| {
        if big {
            u16::from_be_bytes(*pair)
        } else {
            u16::from_le_bytes(*pair)
        }
    });
    Some(
        char::decode_utf16(units)
            .map(|c| c.unwrap_or(char::REPLACEMENT_CHARACTER))
            .collect(),
    )
}

/// An XML file as text, in the encoding its declaration names. Many
/// FictionBooks are in Windows-1251, for Russian.
fn declared(bytes: &[u8]) -> Result<String, String> {
    if let Some(utf16) = utf16(bytes) {
        return Ok(utf16);
    }
    let bytes = bytes.strip_prefix(b"\xef\xbb\xbf").unwrap_or(bytes);
    let head = String::from_utf8_lossy(&bytes[..bytes.len().min(256)]).to_ascii_lowercase();
    let encoding = head
        .strip_prefix("<?xml")
        .and_then(|declaration| declaration.split("?>").next())
        .and_then(|declaration| declaration.split_once("encoding"))
        .and_then(|(_, rest)| {
            let rest = rest.trim_start().strip_prefix('=')?.trim_start();
            let quote = rest.chars().next().filter(|c| *c == '"' || *c == '\'')?;
            rest[1..].split(quote).next().map(str::to_owned)
        });
    match encoding.as_deref().map(str::trim) {
        None | Some("utf-8" | "utf8") => Ok(String::from_utf8_lossy(bytes).into_owned()),
        Some("windows-1251" | "cp1251") => Ok(windows_1251(bytes)),
        Some("windows-1252" | "cp1252" | "iso-8859-1" | "latin1" | "us-ascii") => {
            Ok(windows_1252(bytes))
        }
        Some(other) => Err(format!("is in {other}, which Accordion doesn't read")),
    }
}

/// Windows-1252's characters from 0x80 to 0x9F, where it differs from
/// Latin-1. Its five unused bytes are U+FFFD.
const WINDOWS_1252: [char; 32] = [
    '€', '\u{fffd}', '‚', 'ƒ', '„', '…', '†', '‡', 'ˆ', '‰', 'Š', '‹', 'Œ', '\u{fffd}', 'Ž',
    '\u{fffd}', '\u{fffd}', '‘', '’', '“', '”', '•', '–', '—', '˜', '™', 'š', '›', 'œ', '\u{fffd}',
    'ž', 'Ÿ',
];

fn windows_1252(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|&byte| match byte {
            0x80..=0x9f => WINDOWS_1252[usize::from(byte - 0x80)],
            _ => char::from(byte),
        })
        .collect()
}

/// Windows-1251's characters from 0x80 to 0xBF. From 0xC0, it's А to я in
/// order.
const WINDOWS_1251: [char; 64] = [
    'Ђ', 'Ѓ', '‚', 'ѓ', '„', '…', '†', '‡', '€', '‰', 'Љ', '‹', 'Њ', 'Ќ', 'Ћ', 'Џ', //
    'ђ', '‘', '’', '“', '”', '•', '–', '—', '\u{fffd}', '™', 'љ', '›', 'њ', 'ќ', 'ћ', 'џ', //
    '\u{a0}', 'Ў', 'ў', 'Ј', '¤', 'Ґ', '¦', '§', 'Ё', '©', 'Є', '«', '¬', '\u{ad}', '®',
    'Ї', //
    '°', '±', 'І', 'і', 'ґ', 'µ', '¶', '·', 'ё', '№', 'є', '»', 'ј', 'Ѕ', 'ѕ', 'ї',
];

fn windows_1251(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|&byte| match byte {
            0x80..=0xbf => WINDOWS_1251[usize::from(byte - 0x80)],
            0xc0..=0xff => char::from_u32(0x410 + u32::from(byte - 0xc0)).expect("Cyrillic"),
            _ => char::from(byte),
        })
        .collect()
}

/// `text` on one line, with its spaces evened out and without control
/// characters, cut to [`MAX_LINE`] characters. `None` if it's empty.
fn line(text: &str) -> Option<String> {
    let words: Vec<&str> = text
        .split(|c: char| c.is_whitespace() || c.is_control())
        .filter(|word| !word.is_empty())
        .collect();
    if words.is_empty() {
        return None;
    }
    Some(cut(words.join(" "), MAX_LINE))
}

/// `text` as paragraphs, each on one line, with `\n` between them, cut to
/// [`MAX_DESCRIPTION`] characters. `None` if it's empty.
fn paragraphs(text: &str) -> Option<String> {
    let lines: Vec<String> = text
        .lines()
        .filter_map(|paragraph| {
            let words: Vec<&str> = paragraph
                .split(|c: char| c.is_whitespace() || c.is_control())
                .filter(|word| !word.is_empty())
                .collect();
            (!words.is_empty()).then(|| words.join(" "))
        })
        .collect();
    if lines.is_empty() {
        return None;
    }
    Some(cut(lines.join("\n"), MAX_DESCRIPTION))
}

fn cut(mut text: String, most: usize) -> String {
    if let Some((at, _)) = text.char_indices().nth(most) {
        text.truncate(at);
        text.truncate(text.trim_end().len());
        text.push('…');
    }
    text
}

/// The text of `html`, a description that may have HTML in it, with a new
/// line where each paragraph ends. Its tags are dropped, not run.
fn plain(html: &str) -> String {
    const BREAKS: [&str; 12] = [
        "p",
        "br",
        "div",
        "li",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "tr",
        "blockquote",
    ];
    let mut text = String::with_capacity(html.len());
    let mut rest = html;
    while let Some(at) = rest.find(['<', '&']) {
        text.push_str(&rest[..at]);
        rest = &rest[at..];
        if rest.starts_with('<') {
            let Some(end) = rest.find('>') else {
                rest = "";
                break;
            };
            let closing = rest[1..end].starts_with('/');
            let name: String = rest[1..end]
                .trim_start_matches('/')
                .chars()
                .take_while(char::is_ascii_alphanumeric)
                .collect::<String>()
                .to_ascii_lowercase();
            if BREAKS.contains(&name.as_str()) {
                text.push('\n');
            }
            rest = &rest[end + 1..];
            // A script's or a style's words aren't the description's.
            if (name == "script" || name == "style") && !closing {
                let close = format!("</{name}");
                rest = rest
                    .to_ascii_lowercase()
                    .find(&close)
                    .map_or("", |at| &rest[at..]);
            }
        } else {
            let end = rest[1..]
                .find(';')
                .filter(|end| *end <= 10)
                .map(|end| end + 1);
            let character = end.and_then(|end| {
                let name = &rest[1..end];
                match name {
                    "amp" => Some('&'),
                    "lt" => Some('<'),
                    "gt" => Some('>'),
                    "quot" => Some('"'),
                    "apos" => Some('\''),
                    "nbsp" => Some(' '),
                    _ => name
                        .strip_prefix("#x")
                        .or_else(|| name.strip_prefix("#X"))
                        .and_then(|hex| u32::from_str_radix(hex, 16).ok())
                        .or_else(|| name.strip_prefix('#').and_then(|n| n.parse().ok()))
                        .and_then(char::from_u32),
                }
            });
            match (character, end) {
                (Some(character), Some(end)) => {
                    text.push(character);
                    rest = &rest[end + 1..];
                }
                _ => {
                    text.push('&');
                    rest = &rest[1..];
                }
            }
        }
    }
    text.push_str(rest);
    text
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    /// The first bytes of a JPEG, which is all the reader looks at.
    const JPEG: &[u8] = b"\xff\xd8\xff\xe0\0\x10JFIF\0cover";

    /// An EPUB of `files`, each a path and its contents, after the
    /// `mimetype` file every EPUB starts with.
    fn epub(files: &[(&str, &[u8])]) -> Vec<u8> {
        let mut zip = zip::ZipWriter::new(Cursor::new(Vec::new()));
        let stored = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);
        zip.start_file("mimetype", stored).unwrap();
        zip.write_all(b"application/epub+zip").unwrap();
        let deflated = zip::write::SimpleFileOptions::default();
        for (path, contents) in files {
            zip.start_file(*path, deflated).unwrap();
            zip.write_all(contents).unwrap();
        }
        zip.finish().unwrap().into_inner()
    }

    fn container(package: &str) -> String {
        format!(
            r#"<?xml version="1.0"?>
<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
  <rootfiles><rootfile full-path="{package}" media-type="application/oebps-package+xml"/></rootfiles>
</container>"#
        )
    }

    fn entry(title: &str, depth: u32) -> Entry {
        Entry {
            title: title.into(),
            depth,
        }
    }

    #[test]
    fn reads_an_epub_3s_cover_and_details() {
        let package = r##"<?xml version="1.0" encoding="UTF-8"?>
<package xmlns="http://www.idpf.org/2007/opf" version="3.0" unique-identifier="id">
  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
    <dc:title>The   Garden
      Party</dc:title>
    <dc:creator id="author">Katherine Mansfield</dc:creator>
    <dc:creator id="artist">Marie Laurencin</dc:creator>
    <meta property="role" refines="#author" scheme="marc:relators">aut</meta>
    <meta property="role" refines="#artist" scheme="marc:relators">ill</meta>
    <dc:description>&lt;p&gt;Stories &amp;amp; sketches.&lt;/p&gt;&lt;p&gt;Her third book.&lt;/p&gt;</dc:description>
  </metadata>
  <manifest>
    <item id="nav" href="Text/nav.xhtml" media-type="application/xhtml+xml" properties="nav"/>
    <item id="front" href="Images/front%20cover.jpg" media-type="image/jpeg" properties="cover-image"/>
    <item id="other" href="Images/cover.png" media-type="image/png"/>
  </manifest>
  <spine><itemref idref="nav"/></spine>
</package>"##;
        let nav = r##"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml" xmlns:epub="http://www.idpf.org/2007/ops">
<body>
  <nav epub:type="landmarks"><ol><li><a href="#">Landmark</a></li></ol></nav>
  <nav epub:type="toc" id="toc"><h1>Contents</h1>
    <ol>
      <li><a href="one.xhtml">The Garden&nbsp;Party</a>
        <ol>
          <li><a href="one.xhtml#i"><span>I</span></a></li>
          <li><span>II</span></li>
        </ol>
      </li>
      <li><a href="two.xhtml">The Daughters of the Late Colonel<br/></a></li>
    </ol>
  </nav>
</body>
</html>"##;
        let book = epub(&[
            (
                "META-INF/container.xml",
                container("OEBPS/content.opf").as_bytes(),
            ),
            ("OEBPS/content.opf", package.as_bytes()),
            ("OEBPS/Text/nav.xhtml", nav.as_bytes()),
            ("OEBPS/Images/front cover.jpg", JPEG),
            ("OEBPS/Images/cover.png", b"\x89PNG\r\n\x1a\nother"),
        ]);
        let book = read(Format::Epub, Cursor::new(book)).unwrap();
        assert_eq!(book.cover.as_deref(), Some(JPEG));
        assert_eq!(
            book.details,
            Details {
                title: Some("The Garden Party".into()),
                // The illustrator isn't an author.
                authors: vec!["Katherine Mansfield".into()],
                description: Some("Stories & sketches.\nHer third book.".into()),
                contents: vec![
                    entry("The Garden Party", 0),
                    entry("I", 1),
                    entry("II", 1),
                    entry("The Daughters of the Late Colonel", 0),
                ],
            }
        );
    }

    #[test]
    fn reads_an_epub_2s_ncx_and_the_cover_its_metadata_names() {
        let package = r#"<?xml version="1.0" encoding="UTF-8"?>
<package xmlns="http://www.idpf.org/2007/opf" xmlns:opf="http://www.idpf.org/2007/opf" version="2.0">
  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
    <dc:title>Middlemarch</dc:title>
    <dc:creator opf:role="aut">George Eliot</dc:creator>
    <dc:creator opf:role="edt">Rosemary Ashton</dc:creator>
    <meta name="cover" content="picture"/>
  </metadata>
  <manifest>
    <item id="ncx" href="toc.ncx" media-type="application/x-dtbncx+xml"/>
    <item id="picture" href="images/Front.JPG" media-type="image/jpeg"/>
  </manifest>
  <spine toc="ncx"/>
</package>"#;
        let ncx = r#"<?xml version="1.0" encoding="UTF-8"?>
<ncx xmlns="http://www.daisy.org/z3986/2005/ncx/" version="2005-1">
  <navMap>
    <navPoint id="a"><navLabel><text>Book I. Miss Brooke</text></navLabel><content src="a.html"/>
      <navPoint id="b"><navLabel><text>Chapter I</text></navLabel><content src="b.html"/></navPoint>
    </navPoint>
    <navPoint id="c"><navLabel><text>Finale</text></navLabel><content src="c.html"/></navPoint>
  </navMap>
</ncx>"#;
        // Made on Windows, so the picture's name differs in case.
        let book = epub(&[
            (
                "META-INF/container.xml",
                container("content.opf").as_bytes(),
            ),
            ("content.opf", package.as_bytes()),
            ("toc.ncx", ncx.as_bytes()),
            ("Images/front.jpg", JPEG),
        ]);
        let book = read(Format::Epub, Cursor::new(book)).unwrap();
        assert_eq!(book.cover.as_deref(), Some(JPEG));
        assert_eq!(book.details.title.as_deref(), Some("Middlemarch"));
        assert_eq!(book.details.authors, ["George Eliot"]);
        assert_eq!(
            book.details.contents,
            [
                entry("Book I. Miss Brooke", 0),
                entry("Chapter I", 1),
                entry("Finale", 0)
            ]
        );
    }

    #[test]
    fn leaves_out_a_cover_that_isnt_a_picture_or_is_missing() {
        let package = r#"<package version="3.0"><metadata><title>No cover</title></metadata>
<manifest><item id="c" href="cover.svg" media-type="image/svg+xml" properties="cover-image"/></manifest>
</package>"#;
        let book = epub(&[
            ("META-INF/container.xml", container("p.opf").as_bytes()),
            ("p.opf", package.as_bytes()),
            (
                "cover.svg",
                b"<svg xmlns='http://www.w3.org/2000/svg'><script/></svg>",
            ),
        ]);
        let book = read(Format::Epub, Cursor::new(book)).unwrap();
        assert_eq!(book.cover, None);
        assert_eq!(book.details.title.as_deref(), Some("No cover"));

        let broken = epub(&[("META-INF/container.xml", b"<container/>")]);
        assert!(read(Format::Epub, Cursor::new(broken)).is_err());
        assert!(read(Format::Epub, Cursor::new(b"PK\x03\x04 not really".to_vec())).is_err());
    }

    /// `text` in Windows-1251, for the Russian in these tests.
    fn in_1251(text: &str) -> Vec<u8> {
        text.chars()
            .map(|c| match c {
                'А'..='я' => (c as u32 - 0x410 + 0xc0) as u8,
                'Ё' => 0xa8,
                'ё' => 0xb8,
                '«' => 0xab,
                '»' => 0xbb,
                _ => u8::try_from(c as u32).expect("ASCII"),
            })
            .collect()
    }

    #[test]
    fn reads_a_fictionbook_in_windows_1251() {
        let cover = Base64::encode_string(JPEG);
        let book = format!(
            r##"<?xml version="1.0" encoding="windows-1251"?>
<FictionBook xmlns="http://www.gribuser.ru/xml/fictionbook/2.0" xmlns:l="http://www.w3.org/1999/xlink">
<description>
  <title-info>
    <genre>prose_rus_classic</genre>
    <author><first-name>Антон</first-name><middle-name>Павлович</middle-name><last-name>Чехов</last-name></author>
    <author><nickname>Редактор</nickname></author>
    <book-title>Рассказы</book-title>
    <annotation><p>Ранние  рассказы.</p><p>«Толстый и тонкий».</p></annotation>
    <coverpage><image l:href="#cover.jpg"/></coverpage>
  </title-info>
  <document-info><author><nickname>Somebody else</nickname></author></document-info>
</description>
<body>
  <title><p>Рассказы</p></title>
  <section><title><p>Часть первая</p></title>
    <section><title><p>Толстый</p><p>и тонкий</p></title><p>Text.</p></section>
    <section><p>A section without a title.</p></section>
  </section>
  <section><title><p>Часть вторая</p></title></section>
</body>
<body name="notes"><section><title><p>1</p></title><p>A note.</p></section></body>
<binary id="other.png" content-type="image/png">iVBORw0KGgo=</binary>
<binary id="cover.jpg" content-type="image/jpeg">
{}
</binary>
</FictionBook>"##,
            cover
        );
        let book = read(Format::Fb2, Cursor::new(in_1251(&book))).unwrap();
        assert_eq!(book.cover.as_deref(), Some(JPEG));
        assert_eq!(
            book.details,
            Details {
                title: Some("Рассказы".into()),
                authors: vec!["Антон Павлович Чехов".into(), "Редактор".into()],
                description: Some("Ранние рассказы.\n«Толстый и тонкий».".into()),
                contents: vec![
                    entry("Часть первая", 0),
                    entry("Толстый и тонкий", 1),
                    entry("Часть вторая", 0),
                ],
            }
        );
    }

    #[test]
    fn says_which_encodings_it_cant_read() {
        let book = br#"<?xml version="1.0" encoding="koi8-r"?><FictionBook/>"#;
        assert!(read(Format::Fb2, Cursor::new(book.to_vec())).is_err());
        let book = "\u{feff}<?xml version='1.0'?><FictionBook><description><title-info>\
            <book-title>Plain</book-title></title-info></description></FictionBook>";
        let book = read(Format::Fb2, Cursor::new(book.as_bytes().to_vec())).unwrap();
        assert_eq!(book.details.title.as_deref(), Some("Plain"));
        assert_eq!(book.cover, None);
    }

    /// A Mobipocket book: a Palm database whose first record has the
    /// headers, with `exth` records, and whose third is a picture.
    fn mobi(code_page: u32, name: &[u8], exth: &[(u32, &[u8])]) -> Vec<u8> {
        let mut extended = Vec::new();
        for (kind, data) in exth {
            extended.extend(kind.to_be_bytes());
            extended.extend((data.len() as u32 + 8).to_be_bytes());
            extended.extend(*data);
        }
        let mut header = vec![0; 16];
        let mut mobi = vec![0; 232];
        mobi[0..4].copy_from_slice(b"MOBI");
        mobi[4..8].copy_from_slice(&232u32.to_be_bytes());
        mobi[12..16].copy_from_slice(&code_page.to_be_bytes());
        // The first picture is the second record.
        mobi[92..96].copy_from_slice(&1u32.to_be_bytes());
        mobi[112..116].copy_from_slice(&0x50u32.to_be_bytes());
        header.extend(mobi);
        header.extend(b"EXTH");
        header.extend((extended.len() as u32 + 12).to_be_bytes());
        header.extend((exth.len() as u32).to_be_bytes());
        header.extend(extended);
        let name_at = header.len() as u32;
        header.extend(name);
        header[84..88].copy_from_slice(&name_at.to_be_bytes());
        header[88..92].copy_from_slice(&(name.len() as u32).to_be_bytes());
        let records: [&[u8]; 3] = [&header, b"\x89PNG\r\n\x1a\nlogo", JPEG];

        let mut book = vec![0; 78];
        book[..12].copy_from_slice(b"Short_title\0");
        book[60..68].copy_from_slice(b"BOOKMOBI");
        book[76..78].copy_from_slice(&3u16.to_be_bytes());
        let mut at = 78 + 8 * records.len() as u32 + 2;
        for record in records {
            book.extend(at.to_be_bytes());
            book.extend([0; 4]);
            at += record.len() as u32;
        }
        book.extend([0; 2]);
        for record in records {
            book.extend(record);
        }
        book
    }

    #[test]
    fn reads_a_mobipocket_books_cover_and_details() {
        let book = mobi(
            65001,
            b"Full name",
            &[
                (EXTH_AUTHOR, "Émile Zola".as_bytes()),
                (EXTH_DESCRIPTION, b"<div>Paris, 1860s.</div>"),
                (EXTH_COVER, &1u32.to_be_bytes()),
                (EXTH_TITLE, "Thérèse Raquin".as_bytes()),
            ],
        );
        let book = read(Format::Mobi, Cursor::new(book)).unwrap();
        assert_eq!(book.cover.as_deref(), Some(JPEG));
        assert_eq!(
            book.details,
            Details {
                title: Some("Thérèse Raquin".into()),
                authors: vec!["Émile Zola".into()],
                description: Some("Paris, 1860s.".into()),
                contents: Vec::new(),
            }
        );

        // Older books are in Windows-1252, and some have only a full name.
        let book = mobi(1252, b"Th\xe9r\xe8se", &[(EXTH_AUTHOR, b"\xc9mile Zola")]);
        let book = read(Format::Mobi, Cursor::new(book)).unwrap();
        assert_eq!(book.cover, None);
        assert_eq!(book.details.title.as_deref(), Some("Thérèse"));
        assert_eq!(book.details.authors, ["Émile Zola"]);

        // A cover past the last record is left out.
        let book = mobi(65001, b"Name", &[(EXTH_COVER, &7u32.to_be_bytes())]);
        assert_eq!(read(Format::Mobi, Cursor::new(book)).unwrap().cover, None);
        assert!(read(Format::Mobi, Cursor::new(vec![0; 100])).is_err());
    }

    #[test]
    fn resolves_links_inside_a_book() {
        assert_eq!(
            resolve("OEBPS/Text", "../Images/a%20b.jpg#top"),
            "OEBPS/Images/a b.jpg"
        );
        assert_eq!(resolve("OEBPS", "./nav.xhtml"), "OEBPS/nav.xhtml");
        assert_eq!(resolve("OEBPS", "/cover.jpg"), "cover.jpg");
        assert_eq!(resolve("", "../../escape.jpg"), "escape.jpg");
    }

    #[test]
    fn keeps_descriptions_and_titles_as_plain_text() {
        let html = "<p>One &amp; two&#8230;</p><p>Three<br/>four &#x2014; five</p>\
            <script>alert(1)</script><style>p { color: red }</style><p>A &bogus; <b>end</b></p>";
        assert_eq!(
            paragraphs(&plain(html)).as_deref(),
            Some("One & two…\nThree\nfour — five\nA &bogus; end")
        );
        assert_eq!(line(" \u{7}a\n\tb "), Some("a b".into()));
        assert_eq!(line(" \n "), None);
        let long = line(&"word ".repeat(100)).unwrap();
        assert_eq!(long.chars().count(), 300);
        assert!(long.ends_with("word…"));
    }

    #[test]
    fn writes_details_as_json() {
        let details = Details {
            title: Some("A \"quoted\" title".into()),
            authors: vec!["A".into()],
            description: None,
            contents: vec![entry("One", 0)],
        };
        let json: serde_json::Value = serde_json::from_slice(&details.to_json()).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "title": "A \"quoted\" title",
                "authors": ["A"],
                "description": null,
                "contents": [{ "title": "One", "depth": 0 }],
            })
        );
    }
}

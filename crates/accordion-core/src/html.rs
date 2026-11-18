//! The text a person sees in an HTML page, for search and text thumbnails.
//!
//! An HTML file is text, but its tags, styles, and scripts aren't what
//! anyone reads or searches for. A Docs document (decision 30) also keeps
//! its co-editing state in a data block, a `<script>` that browsers skip,
//! which can be far longer than its text. This reads the page as a browser
//! shows it: the text of its elements, a line for each block, without
//! scripts, styles, or the page's title.
//!
//! Only html5ever's tokenizer runs, which is memory-safe Rust and builds no
//! tree, so it reads untrusted pages in the server as plain text is read.

use std::borrow::Cow;
use std::cell::{Cell, RefCell};
use std::io::{self, Read};

use html5ever::TokenizerResult;
use html5ever::tendril::StrTendril;
use html5ever::tokenizer::states::RawKind;
use html5ever::tokenizer::{
    BufferQueue, Tag, TagKind, Token, TokenSink, TokenSinkResult, Tokenizer, TokenizerOpts,
};
use serde_json::{Map, Value, json};

/// How much of the file is read at a time.
const CHUNK: usize = 64 * 1024;

/// Whether a file that starts with `head` is an HTML page, by its doctype
/// or its `<html>` tag.
pub(crate) fn is_html(head: &[u8]) -> bool {
    let head = head.strip_prefix(b"\xef\xbb\xbf").unwrap_or(head);
    let start = head
        .iter()
        .position(|byte| !byte.is_ascii_whitespace())
        .unwrap_or(head.len());
    let head = &head[start..];
    [&b"<!doctype html"[..], b"<html"].iter().any(|prefix| {
        head.get(..prefix.len())
            .is_some_and(|start| start.eq_ignore_ascii_case(prefix))
            && head
                .get(prefix.len())
                .is_none_or(|next| next.is_ascii_whitespace() || *next == b'>')
    })
}

/// Whether a file named `name` is an HTML page by its extension, as a
/// fragment someone saved may have no doctype or `<html>` tag.
pub(crate) fn is_html_name(name: &str) -> bool {
    name.rsplit_once('.').is_some_and(|(_, extension)| {
        ["htm", "html", "xhtml"]
            .iter()
            .any(|page| extension.eq_ignore_ascii_case(page))
    })
}

/// The version of the page thumbnails [`Page::thumbnail`] writes. Raise it
/// when what they hold changes in a way apps must know about.
pub(crate) const PAGE_VERSION: u32 = 1;

/// The page `file` as a browser shows it, reading at most `max_read` bytes
/// of it. Reading stops once its text has `max_text` bytes or `max_lines`
/// lines.
pub(crate) fn read(
    file: impl Read,
    max_read: u64,
    max_text: usize,
    max_lines: usize,
) -> io::Result<Page> {
    let tokenizer = Tokenizer::new(
        Sink {
            out: RefCell::new(Out::new(max_text, max_lines)),
            hidden: RefCell::new(None),
            template: Cell::new(0),
        },
        TokenizerOpts::default(),
    );
    let queue = BufferQueue::default();
    let mut file = file.take(max_read);
    let mut buffer = vec![0; CHUNK];
    // The bytes of a character a chunk cut off, put back before the next.
    let mut carried = Vec::new();
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        carried.extend_from_slice(&buffer[..read]);
        let end = match std::str::from_utf8(&carried) {
            Ok(_) => carried.len(),
            Err(err) if err.error_len().is_none() => err.valid_up_to(),
            Err(_) => carried.len(),
        };
        let chunk = String::from_utf8_lossy(&carried[..end]).into_owned();
        carried.drain(..end);
        queue.push_back(StrTendril::from(chunk));
        feed(&tokenizer, &queue);
        if tokenizer.sink.out.borrow().full {
            return Ok(tokenizer.sink.out.borrow_mut().finish());
        }
    }
    if !carried.is_empty() {
        queue.push_back(StrTendril::from(
            String::from_utf8_lossy(&carried).into_owned(),
        ));
        feed(&tokenizer, &queue);
    }
    tokenizer.end();
    Ok(tokenizer.sink.out.borrow_mut().finish())
}

/// The visible text of the page `file`, as [`read`] reads it.
pub(crate) fn text(
    file: impl Read,
    max_read: u64,
    max_text: usize,
    max_lines: usize,
) -> io::Result<String> {
    read(file, max_read, max_text, max_lines).map(|page| page.text())
}

/// Tokenizes everything in `queue`.
fn feed(tokenizer: &Tokenizer<Sink>, queue: &BufferQueue) {
    while !matches!(tokenizer.feed(queue), TokenizerResult::Done) {}
}

/// A page's lines as a browser shows them.
#[derive(Debug)]
pub(crate) struct Page {
    lines: Vec<Line>,
}

impl Page {
    /// The page as plain text, a line for each line, with list items'
    /// bullets and numbers and checklists' boxes.
    pub(crate) fn text(&self) -> String {
        let lines: Vec<String> = self.lines.iter().map(Line::text).collect();
        lines.join("\n")
    }

    /// The page as a page thumbnail: JSON with each line's kind and styled
    /// text, which apps draw with their own styles, so it follows their
    /// light or dark look.
    pub(crate) fn thumbnail(&self) -> Vec<u8> {
        let lines: Vec<Value> = self.lines.iter().map(Line::json).collect();
        serde_json::to_vec(&json!({ "version": PAGE_VERSION, "lines": lines }))
            .expect("JSON values always write")
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
enum LineKind {
    #[default]
    Paragraph,
    /// A heading, from 1 to 6.
    Heading(u8),
    /// A list item, with a bullet or a number.
    Item,
    /// A checklist item, and whether it's checked.
    Check(bool),
    /// A line of preformatted text, such as code.
    Code,
    /// A table row.
    Row,
}

#[derive(Debug, Default)]
struct Line {
    kind: LineKind,
    /// How many lists the item is inside, past the first.
    depth: usize,
    /// A list item's bullet or number, such as "•" or "3.".
    mark: Option<String>,
    /// Whether it's quoted.
    quote: bool,
    /// The text: a row's cells, or a single cell.
    cells: Vec<Vec<Run>>,
}

impl Line {
    fn is_empty(&self) -> bool {
        self.cells.iter().all(Vec::is_empty)
    }

    fn text(&self) -> String {
        let mut text = "  ".repeat(self.depth);
        match (self.kind, &self.mark) {
            (LineKind::Item, Some(mark)) => {
                text.push_str(mark);
                text.push(' ');
            }
            (LineKind::Check(true), _) => text.push_str("☑ "),
            (LineKind::Check(false), _) => text.push_str("☐ "),
            _ => {}
        }
        let cells: Vec<String> = self
            .cells
            .iter()
            .map(|cell| cell.iter().map(|run| run.text.as_str()).collect())
            .collect();
        text.push_str(&cells.join(" "));
        text
    }

    fn json(&self) -> Value {
        let mut line = Map::new();
        let kind = match self.kind {
            LineKind::Paragraph => "paragraph",
            LineKind::Heading(_) => "heading",
            LineKind::Item => "item",
            LineKind::Check(_) => "check",
            LineKind::Code => "code",
            LineKind::Row => "row",
        };
        line.insert("kind".into(), kind.into());
        match self.kind {
            LineKind::Heading(level) => {
                line.insert("level".into(), level.into());
            }
            LineKind::Check(checked) => {
                line.insert("checked".into(), checked.into());
            }
            LineKind::Item => {
                line.insert("mark".into(), self.mark.clone().unwrap_or_default().into());
            }
            _ => {}
        }
        if self.depth > 0 {
            line.insert("depth".into(), self.depth.into());
        }
        if self.quote {
            line.insert("quote".into(), true.into());
        }
        let runs = |cell: &Vec<Run>| Value::Array(cell.iter().map(Run::json).collect());
        if self.kind == LineKind::Row {
            line.insert(
                "cells".into(),
                Value::Array(self.cells.iter().map(runs).collect()),
            );
        } else {
            let cell = self.cells.first().map(runs);
            line.insert("runs".into(), cell.unwrap_or(Value::Array(Vec::new())));
        }
        Value::Object(line)
    }
}

/// Text with the same styles.
#[derive(Debug)]
struct Run {
    text: String,
    /// The styles, as bits for each of [`STYLES`].
    styles: u8,
}

/// The styles a run can have, by their names in a page thumbnail, and the
/// elements that give them.
const STYLES: [(&str, &[&str]); 6] = [
    ("bold", &["b", "strong", "th"]),
    ("italic", &["i", "em", "cite"]),
    ("underline", &["u", "ins"]),
    ("strike", &["s", "strike", "del"]),
    ("code", &["code", "kbd", "samp"]),
    ("link", &["a"]),
];

/// The styles that draw a line under or through text: underline, strike,
/// and link.
const LINED: u8 = 1 << 2 | 1 << 3 | 1 << 5;

impl Run {
    fn json(&self) -> Value {
        let mut run = Map::new();
        run.insert("text".into(), self.text.clone().into());
        for (index, (name, _)) in STYLES.iter().enumerate() {
            if self.styles & (1 << index) != 0 {
                run.insert((*name).into(), true.into());
            }
        }
        Value::Object(run)
    }
}

struct Sink {
    out: RefCell<Out>,
    /// The element whose contents are skipped, such as a script, until it
    /// ends.
    hidden: RefCell<Option<String>>,
    /// How many `<template>` elements are open, whose contents aren't shown.
    template: Cell<usize>,
}

impl TokenSink for Sink {
    type Handle = ();

    fn process_token(&self, token: Token, _line: u64) -> TokenSinkResult<()> {
        if self.out.borrow().full {
            return TokenSinkResult::Continue;
        }
        match token {
            Token::TagToken(tag) => return self.tag(&tag),
            Token::CharacterTokens(text)
                if self.hidden.borrow().is_none() && self.template.get() == 0 =>
            {
                self.out.borrow_mut().push(&text);
            }
            _ => {}
        }
        TokenSinkResult::Continue
    }
}

impl Sink {
    fn tag(&self, tag: &Tag) -> TokenSinkResult<()> {
        let name: &str = &tag.name;
        let hidden = self.hidden.borrow().clone();
        if let Some(hidden) = hidden {
            // Only the end of the hidden element itself shows text again.
            if tag.kind == TagKind::EndTag && hidden == name {
                *self.hidden.borrow_mut() = None;
            }
            return TokenSinkResult::Continue;
        }
        let start = tag.kind == TagKind::StartTag;
        if name == "template" {
            let open = self.template.get();
            self.template.set(if start {
                open + 1
            } else {
                open.saturating_sub(1)
            });
            return TokenSinkResult::Continue;
        }
        // Elements whose contents aren't markup are read as a browser reads
        // them, so a `<` in a script doesn't start a tag.
        let raw = match name {
            "script" => Some(RawKind::ScriptData),
            "style" | "noscript" | "noframes" | "iframe" | "xmp" => Some(RawKind::Rawtext),
            "title" => Some(RawKind::Rcdata),
            "textarea" => {
                if start && !tag.self_closing {
                    self.out.borrow_mut().block();
                    return TokenSinkResult::RawData(RawKind::Rcdata);
                }
                None
            }
            _ => None,
        };
        if let Some(raw) = raw {
            if start && !tag.self_closing {
                *self.hidden.borrow_mut() = Some(name.to_owned());
                return TokenSinkResult::RawData(raw);
            }
            return TokenSinkResult::Continue;
        }
        if self.template.get() > 0 {
            return TokenSinkResult::Continue;
        }
        let mut out = self.out.borrow_mut();
        // A link only looks like one with an address.
        let styled = name != "a" || !start || attribute(tag, "href").is_some();
        if let Some(index) = STYLES
            .iter()
            .position(|(_, elements)| elements.contains(&name))
            && styled
            && !tag.self_closing
        {
            let open = &mut out.styles[index];
            *open = if start {
                *open + 1
            } else {
                open.saturating_sub(1)
            };
        }
        match (name, start) {
            ("br", _) if out.row => out.space = true,
            ("br", _) => out.line_break(),
            ("pre", true) => {
                out.block();
                out.pre += 1;
                out.pre_started = true;
            }
            ("pre", false) => {
                out.pre = out.pre.saturating_sub(1);
                out.block();
            }
            ("h1" | "h2" | "h3" | "h4" | "h5" | "h6", _) => {
                out.block();
                out.heading = start.then(|| name.as_bytes()[1] - b'0');
            }
            ("blockquote", _) => {
                out.block();
                out.quote = if start {
                    out.quote + 1
                } else {
                    out.quote.saturating_sub(1)
                };
            }
            ("ul", true) => {
                out.block();
                out.lists.push(None);
            }
            ("ol", true) => {
                out.block();
                let first = attribute(tag, "start")
                    .and_then(|start| start.trim().parse().ok())
                    .unwrap_or(1);
                out.lists.push(Some(first));
            }
            ("ul" | "ol", false) => {
                out.lists.pop();
                out.mark = None;
                out.block();
            }
            ("li", false) => {
                out.mark = None;
                out.block();
            }
            ("li", true) => {
                out.block();
                let mark = match out.lists.last_mut() {
                    Some(Some(number)) => {
                        let mark = format!("{number}.");
                        *number += 1;
                        mark
                    }
                    _ => "•".to_owned(),
                };
                out.mark = Some(Pending::Item(mark));
            }
            ("input", true) if attribute(tag, "type").is_some_and(is_checkbox) => {
                let checked = attribute(tag, "checked").is_some();
                // A checklist's item shows its box instead of a bullet.
                if out.line.cells.is_empty() {
                    out.mark = Some(Pending::Check(checked));
                } else {
                    out.push(if checked { "☑ " } else { "☐ " });
                }
            }
            ("tr", _) => {
                out.block();
                out.row = start;
            }
            ("td" | "th", true) if out.row => out.cell(),
            (name, _) if is_block(name) => {
                // A paragraph in a table cell stays in its row.
                if out.row {
                    out.space = true;
                } else {
                    out.block();
                }
            }
            _ => {}
        }
        TokenSinkResult::Continue
    }
}

/// What the next line starts with, once it has text.
enum Pending {
    Item(String),
    Check(bool),
}

/// The page so far.
#[derive(Default)]
struct Out {
    lines: Vec<Line>,
    /// The line being written, whitespace collapsed. It has no cells until
    /// it has text.
    line: Line,
    /// The bytes of text so far, counting a newline between lines.
    size: usize,
    max_text: usize,
    max_lines: usize,
    full: bool,
    /// Whether a space goes before the next word on the line.
    space: bool,
    /// A bullet, number, or checkbox for the next line.
    mark: Option<Pending>,
    /// For each open list, the next number, or `None` for bullets.
    lists: Vec<Option<u64>>,
    /// How many `<pre>` elements are open, which keep their whitespace.
    pre: usize,
    /// Whether nothing came yet after `<pre>`, whose first newline isn't
    /// shown.
    pre_started: bool,
    /// The level of the heading the text is in.
    heading: Option<u8>,
    /// How many quotes are open.
    quote: usize,
    /// Whether the text is in a table row.
    row: bool,
    /// How many elements giving each of [`STYLES`] are open.
    styles: [usize; STYLES.len()],
}

impl Out {
    fn new(max_text: usize, max_lines: usize) -> Self {
        Out {
            max_text,
            max_lines,
            ..Out::default()
        }
    }

    fn push(&mut self, text: &str) {
        let text = if std::mem::take(&mut self.pre_started) {
            text.strip_prefix('\n').unwrap_or(text)
        } else {
            text
        };
        if self.pre > 0 {
            for (index, piece) in text.split('\n').enumerate() {
                if index > 0 {
                    self.line_break();
                }
                if !piece.is_empty() {
                    self.word(piece.trim_end_matches('\r'));
                }
            }
            return;
        }
        let collapses = |c: char| c.is_whitespace() && c != '\u{a0}';
        if text.starts_with(collapses) {
            self.space = true;
        }
        let mut first = true;
        for word in text.split(collapses).filter(|word| !word.is_empty()) {
            if !first {
                self.space = true;
            }
            self.word(word);
            first = false;
        }
        if text.ends_with(collapses) {
            self.space = true;
        }
    }

    /// The styles the text has now, as bits for each of [`STYLES`].
    fn styles(&self) -> u8 {
        let mut styles = 0;
        for (index, open) in self.styles.iter().enumerate() {
            // Preformatted text is a code line already.
            let code = STYLES[index].0 == "code";
            if *open > 0 && !(code && self.pre > 0) {
                styles |= 1 << index;
            }
        }
        styles
    }

    /// Starts the line, once it has text, taking its kind from where the
    /// text is.
    fn start_line(&mut self) {
        let mut line = Line {
            cells: vec![Vec::new()],
            quote: self.quote > 0,
            ..Line::default()
        };
        line.kind = match (self.pre, self.heading, self.mark.take()) {
            (1.., _, _) => LineKind::Code,
            (_, Some(level), _) => LineKind::Heading(level),
            (_, _, Some(Pending::Item(mark))) => {
                line.mark = Some(mark);
                LineKind::Item
            }
            (_, _, Some(Pending::Check(checked))) => LineKind::Check(checked),
            _ if self.row => LineKind::Row,
            _ => LineKind::Paragraph,
        };
        if matches!(line.kind, LineKind::Item | LineKind::Check(_)) {
            line.depth = self.lists.len().saturating_sub(1);
        }
        self.line = line;
    }

    /// Starts a table cell.
    fn cell(&mut self) {
        if self.line.cells.is_empty() {
            self.start_line();
        } else {
            self.line.cells.push(Vec::new());
        }
        self.space = false;
    }

    fn word(&mut self, word: &str) {
        if self.full {
            return;
        }
        if self.line.cells.is_empty() {
            self.start_line();
        }
        let styles = self.styles();
        let cell = self
            .line
            .cells
            .last_mut()
            .expect("a started line has a cell");
        let mut word = Cow::Borrowed(word);
        if std::mem::take(&mut self.space)
            && let Some(last) = cell.last_mut()
        {
            // A space shows no line under or through it, so it goes with
            // text that has none.
            if last.styles == styles || last.styles & LINED == 0 {
                last.text.push(' ');
            } else if styles & LINED == 0 {
                word = Cow::Owned(format!(" {word}"));
            } else {
                cell.push(Run {
                    text: " ".to_owned(),
                    styles: 0,
                });
            }
            self.size += 1;
        }
        self.size += word.len();
        match cell.last_mut() {
            Some(last) if last.styles == styles => last.text.push_str(&word),
            _ => cell.push(Run {
                text: word.into_owned(),
                styles,
            }),
        }
        if self.size >= self.max_text {
            self.end_line();
            self.full = true;
        }
    }

    /// Ends the line, if it has text, as a block element does.
    fn block(&mut self) {
        if self.line.is_empty() {
            self.line = Line::default();
        } else {
            self.end_line();
        }
        self.space = false;
    }

    /// Ends the line even if it's empty, as `<br>` does.
    fn line_break(&mut self) {
        self.end_line();
        self.space = false;
    }

    fn end_line(&mut self) {
        let line = std::mem::take(&mut self.line);
        // Nothing shows before the first line with text.
        if self.full || (self.lines.is_empty() && line.is_empty()) {
            return;
        }
        if !self.lines.is_empty() {
            self.size += 1;
        }
        self.lines.push(line);
        if self.lines.len() >= self.max_lines || self.size >= self.max_text {
            self.full = true;
        }
    }

    /// The page, once the last line ends.
    fn finish(&mut self) -> Page {
        self.block();
        // Blank lines at the end, such as from a final `<br>`, show nothing.
        while self.lines.last().is_some_and(Line::is_empty) {
            self.lines.pop();
        }
        Page {
            lines: std::mem::take(&mut self.lines),
        }
    }
}

fn attribute<'a>(tag: &'a Tag, name: &str) -> Option<&'a str> {
    tag.attrs
        .iter()
        .find(|attr| &*attr.name.local == name)
        .map(|attr| &*attr.value)
}

fn is_checkbox(kind: &str) -> bool {
    kind.trim().eq_ignore_ascii_case("checkbox")
}

/// Elements that start and end a line of their own.
fn is_block(name: &str) -> bool {
    matches!(
        name,
        "address"
            | "article"
            | "aside"
            | "blockquote"
            | "body"
            | "caption"
            | "dd"
            | "details"
            | "div"
            | "dl"
            | "dt"
            | "fieldset"
            | "figcaption"
            | "figure"
            | "footer"
            | "form"
            | "h1"
            | "h2"
            | "h3"
            | "h4"
            | "h5"
            | "h6"
            | "head"
            | "header"
            | "hr"
            | "legend"
            | "li"
            | "main"
            | "nav"
            | "option"
            | "p"
            | "section"
            | "summary"
            | "table"
            | "tr"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read(page: &str) -> String {
        text(page.as_bytes(), u64::MAX, usize::MAX, usize::MAX).unwrap()
    }

    #[test]
    fn knows_pages_by_their_start() {
        assert!(is_html(b"<!DOCTYPE html>\n<html>"));
        assert!(is_html(b"\xef\xbb\xbf\n  <!doctype HTML><p>"));
        assert!(is_html(b"<html lang=\"en\">"));
        assert!(is_html(b"<HTML>"));
        assert!(is_html(
            b"<!DOCTYPE html>\n<html data-accordion-docs=\"1\">"
        ));
        assert!(!is_html(b"<htmlx>"));
        assert!(!is_html(b"<div>Not a page</div>"));
        assert!(!is_html(b"Notes about <html> tags"));
        assert!(!is_html(b"# Heading\n"));
        assert!(is_html_name("Info Page.HTML"));
        assert!(is_html_name("old.htm"));
        assert!(!is_html_name("notes.txt"));
        assert!(!is_html_name("html"));
    }

    #[test]
    fn reads_what_a_browser_shows() {
        let page = "<!DOCTYPE html>\n<html><head><title>The title</title>\
            <style>p{color:red}</style><script>if (a < b) { x(\"</p>\") }</script></head>\
            <body><h1>Trip   to\n  Lisbon</h1><p>Fly on <b>Friday</b>, back &amp; rested.</p>\
            <!-- a comment --><template><p>Hidden</p></template>\
            <div>Line one<br>Line two</div><textarea>Typed &lt;here&gt;</textarea></body></html>";
        assert_eq!(
            read(page),
            "Trip to Lisbon\nFly on Friday, back & rested.\nLine one\nLine two\nTyped <here>"
        );
    }

    #[test]
    fn skips_a_documents_data_block() {
        let page = "<!DOCTYPE html>\n<html data-accordion-docs=\"1\">\n<head>\n\
            <meta charset=\"utf-8\">\n<title>Plan</title>\n<style>main{margin:0}</style>\n\
            </head>\n<body>\n<main>\n<h1>Plan</h1>\n<p>Book the hall</p>\n</main>\n\
            <script type=\"application/x-accordion-docs\">{\"version\":1,\"state\":\"AQKw+</p>==\"}</script>\n\
            </body>\n</html>\n";
        assert_eq!(read(page), "Plan\nBook the hall");
    }

    #[test]
    fn marks_lists_and_checklists() {
        let page = "<html><ul><li>Bread</li><li><p>Milk</p><ul><li>Oat</li></ul></li></ul>\
            <ol start=\"3\"><li>Third</li><li>Fourth</li></ol>\
            <ul class=\"checklist\"><li data-checked=\"true\"><input type=\"checkbox\" checked disabled>\n\
            <p>Done</p></li><li><input type=\"checkbox\" disabled>\n<p>To do</p></li></ul>\
            <table><tr><th>Day</th><th>Who</th></tr><tr><td>Mon</td><td>Sam</td></tr></table>";
        assert_eq!(
            read(page),
            "• Bread\n• Milk\n  • Oat\n3. Third\n4. Fourth\n☑ Done\n☐ To do\nDay Who\nMon Sam"
        );
    }

    #[test]
    fn keeps_each_lines_kind_and_styles() {
        let page = "<!DOCTYPE html>\n<html><body><main>\
            <h2>Trip to <em>Lisbon</em></h2>\
            <p>Fly <b>on   Friday</b>, see <a href=\"https://example.com\">the plan</a> and \
            <code>gate 4</code>.<br>Back <s>Monday</s> <u>Sunday</u>.</p>\
            <ol start=\"2\"><li><p>Pack</p><ul><li>Shoes</li></ul></li></ol>\
            <ul class=\"checklist\"><li data-checked=\"true\"><input type=\"checkbox\" checked>\n<p>Book</p></li></ul>\
            <blockquote><p>Quoted</p></blockquote>\
            <table><tr><th><p>Day</p></th><td><p>Mon</p><p>Tue</p></td></tr></table>\
            <pre><code>x = 1</code></pre></main></body></html>";
        let page = super::read(page.as_bytes(), u64::MAX, usize::MAX, usize::MAX).unwrap();
        let json: Value = serde_json::from_slice(&page.thumbnail()).unwrap();
        assert_eq!(
            json,
            json!({
                "version": 1,
                "lines": [
                    {"kind": "heading", "level": 2, "runs": [
                        {"text": "Trip to "}, {"text": "Lisbon", "italic": true}]},
                    {"kind": "paragraph", "runs": [
                        {"text": "Fly "}, {"text": "on Friday", "bold": true}, {"text": ", see "},
                        {"text": "the plan", "link": true}, {"text": " and "},
                        {"text": "gate 4", "code": true}, {"text": "."}]},
                    {"kind": "paragraph", "runs": [
                        {"text": "Back "}, {"text": "Monday", "strike": true}, {"text": " "},
                        {"text": "Sunday", "underline": true}, {"text": "."}]},
                    {"kind": "item", "mark": "2.", "runs": [{"text": "Pack"}]},
                    {"kind": "item", "mark": "•", "depth": 1, "runs": [{"text": "Shoes"}]},
                    {"kind": "check", "checked": true, "runs": [{"text": "Book"}]},
                    {"kind": "paragraph", "quote": true, "runs": [{"text": "Quoted"}]},
                    {"kind": "row", "cells": [
                        [{"text": "Day", "bold": true}], [{"text": "Mon Tue"}]]},
                    {"kind": "code", "runs": [{"text": "x = 1"}]},
                ]
            })
        );
    }

    #[test]
    fn keeps_preformatted_text() {
        assert_eq!(
            read("<html><pre>\nfn main() {\n    go();\n}</pre><p>After</p>"),
            "fn main() {\n    go();\n}\nAfter"
        );
    }

    #[test]
    fn stops_at_the_limits() {
        let page = format!("<html>{}", "<p>Line</p>".repeat(100));
        let lines = text(page.as_bytes(), u64::MAX, usize::MAX, 3).unwrap();
        assert_eq!(lines, "Line\nLine\nLine");
        let short = text(page.as_bytes(), u64::MAX, 12, usize::MAX).unwrap();
        assert!(short.len() <= 16, "{short:?}");
        let read_part = text(page.as_bytes(), 6 + 11 * 2, usize::MAX, usize::MAX).unwrap();
        assert_eq!(read_part, "Line\nLine");
    }

    #[test]
    fn keeps_characters_split_between_chunks() {
        let mut page = String::from("<html><p>");
        page.push_str(&"a".repeat(CHUNK - page.len() - 1));
        page.push_str("é</p>");
        assert!(read(&page).ends_with("aé"));
    }
}

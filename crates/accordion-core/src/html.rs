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

use std::cell::{Cell, RefCell};
use std::io::{self, Read};

use html5ever::TokenizerResult;
use html5ever::tendril::StrTendril;
use html5ever::tokenizer::states::RawKind;
use html5ever::tokenizer::{
    BufferQueue, Tag, TagKind, Token, TokenSink, TokenSinkResult, Tokenizer, TokenizerOpts,
};

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

/// The visible text of the page `file`, reading at most `max_read` bytes of
/// it. Reading stops once the text has `max_text` bytes or `max_lines`
/// lines.
pub(crate) fn text(
    file: impl Read,
    max_read: u64,
    max_text: usize,
    max_lines: usize,
) -> io::Result<String> {
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

/// Tokenizes everything in `queue`.
fn feed(tokenizer: &Tokenizer<Sink>, queue: &BufferQueue) {
    while !matches!(tokenizer.feed(queue), TokenizerResult::Done) {}
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
        match (name, start) {
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
                        let mark = format!("{number}. ");
                        *number += 1;
                        mark
                    }
                    _ => "• ".to_owned(),
                };
                out.mark = Some(mark);
            }
            ("input", true) if attribute(tag, "type").is_some_and(is_checkbox) => {
                let mark = if attribute(tag, "checked").is_some() {
                    "☑ "
                } else {
                    "☐ "
                };
                // A checklist's item shows its box instead of a bullet.
                if out.line.is_empty() {
                    out.mark = Some(mark.to_owned());
                } else {
                    out.push(mark);
                }
            }
            ("td" | "th", false) => out.space = true,
            (name, _) if is_block(name) => out.block(),
            _ => {}
        }
        TokenSinkResult::Continue
    }
}

/// The text so far.
#[derive(Default)]
struct Out {
    text: String,
    /// The line being written, whitespace collapsed, added to `text` when it
    /// ends.
    line: String,
    lines: usize,
    max_text: usize,
    max_lines: usize,
    full: bool,
    /// Whether a space goes before the next word on the line.
    space: bool,
    /// A bullet, number, or checkbox for the line's first word.
    mark: Option<String>,
    /// For each open list, the next number, or `None` for bullets.
    lists: Vec<Option<u64>>,
    /// How many `<pre>` elements are open, which keep their whitespace.
    pre: usize,
    /// Whether nothing came yet after `<pre>`, whose first newline isn't
    /// shown.
    pre_started: bool,
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
        let mut words = text.split(|c: char| c.is_whitespace() && c != '\u{a0}');
        if text.starts_with(|c: char| c.is_whitespace() && c != '\u{a0}') {
            self.space = true;
        }
        let ends_with_space = text.ends_with(|c: char| c.is_whitespace() && c != '\u{a0}');
        let mut first = true;
        for word in words.by_ref().filter(|word| !word.is_empty()) {
            if !first {
                self.space = true;
            }
            self.word(word);
            first = false;
        }
        if ends_with_space {
            self.space = true;
        }
    }

    fn word(&mut self, word: &str) {
        if self.full {
            return;
        }
        if self.line.is_empty() {
            if let Some(mark) = self.mark.take() {
                let depth = self.lists.len().saturating_sub(1);
                self.line.push_str(&"  ".repeat(depth));
                self.line.push_str(&mark);
            }
        } else if self.space {
            self.line.push(' ');
        }
        self.space = false;
        self.line.push_str(word);
        if self.text.len() + self.line.len() >= self.max_text {
            self.end_line();
            self.full = true;
        }
    }

    /// Ends the line, if it has anything on it, as a block element does.
    fn block(&mut self) {
        if !self.line.is_empty() {
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
        // Nothing shows before the first line with text.
        if self.full || (self.lines == 0 && self.line.is_empty()) {
            return;
        }
        if self.lines > 0 {
            self.text.push('\n');
        }
        self.text.push_str(&self.line);
        self.line.clear();
        self.lines += 1;
        if self.lines >= self.max_lines || self.text.len() >= self.max_text {
            self.full = true;
        }
    }

    /// The text, once the last line ends.
    fn finish(&mut self) -> String {
        self.block();
        // Blank lines at the end, such as from a final `<br>`, show nothing.
        let trimmed = self.text.trim_end_matches('\n').len();
        self.text.truncate(trimmed);
        std::mem::take(&mut self.text)
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

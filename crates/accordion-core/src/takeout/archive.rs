//! The parts of one Takeout export, read in the sandbox (decision 34).
//!
//! An export is one or more archives, each a `.zip` or a `.tgz`, told apart
//! by their first bytes rather than their names. Every entry's name is
//! checked as it's listed, and nothing is ever unpacked by its name: an
//! unpacked file is named after its place in the export's list. That also
//! keeps two entries apart when Takeout gave them the same shortened path,
//! as its `.tgz` parts do.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;

/// The most entries an export may have, across its parts.
pub(crate) const MAX_ENTRIES: usize = 500_000;

/// The longest path an entry may have, in bytes.
const MAX_PATH: usize = 4096;

/// The largest file a product may read while planning, such as a note's
/// JSON. Larger ones are left unread.
pub(crate) const MAX_SMALL: u64 = 4 << 20;

/// The most that small files may hold together while planning.
const MAX_SMALL_TOTAL: u64 = 256 << 20;

/// How a part is packed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Format {
    Zip,
    Tgz,
}

impl Format {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Format::Zip => "zip",
            Format::Tgz => "tgz",
        }
    }
}

enum Reader {
    Zip(Box<zip::ZipArchive<File>>),
    /// A `.tgz` can only be read from the start, so each pass rewinds it.
    Tgz(File),
}

struct Part {
    reader: Reader,
    format: Format,
    entries: usize,
}

/// One file or folder in the export.
#[derive(Clone, Debug)]
pub struct Entry {
    /// Its place in the export's list, which also names its unpacked file.
    pub id: usize,
    part: usize,
    /// Its place in its part: zip's index, or its place in the tar stream.
    index: usize,
    /// Its path in the archive, such as `Takeout/Keep/Groceries.json`.
    pub path: String,
    pub size: u64,
    pub folder: bool,
}

/// An entry left out, and why.
#[derive(Clone, Debug)]
pub struct Skipped {
    pub path: String,
    pub why: &'static str,
}

/// The parts of one export, listed together.
pub(crate) struct Export {
    parts: Vec<Part>,
    pub entries: Vec<Entry>,
    pub skipped: Vec<Skipped>,
    /// The small files products asked for, by entry ID.
    pub small: HashMap<usize, Vec<u8>>,
}

/// Why an export couldn't be read.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Problem {
    #[error("part {part} isn't a .zip or .tgz archive")]
    NotAnArchive { part: usize },
    #[error("part {part} is damaged: {message}")]
    Damaged { part: usize, message: String },
    #[error("the export has more than {MAX_ENTRIES} files and folders")]
    TooManyEntries,
    #[error("couldn't read part {part}: {source}")]
    Io {
        part: usize,
        #[source]
        source: io::Error,
    },
}

impl Export {
    /// Lists every entry in `parts`, in order, and reads the small files
    /// `wants` asks for, by their paths.
    pub(crate) fn open(
        parts: &[&Path],
        wants: &dyn Fn(&str, u64) -> bool,
    ) -> Result<Self, Problem> {
        let mut export = Export {
            parts: Vec::new(),
            entries: Vec::new(),
            skipped: Vec::new(),
            small: HashMap::new(),
        };
        let mut small_total = 0;
        for (number, path) in parts.iter().enumerate() {
            let io = |source| Problem::Io {
                part: number,
                source,
            };
            let mut file = File::open(path).map_err(io)?;
            let mut magic = [0; 4];
            let read = read_up_to(&mut file, &mut magic).map_err(io)?;
            file.seek(SeekFrom::Start(0)).map_err(io)?;
            let format = match &magic[..read] {
                [b'P', b'K', 3, 4] | [b'P', b'K', 5, 6] => Format::Zip,
                [0x1f, 0x8b, ..] => Format::Tgz,
                _ => return Err(Problem::NotAnArchive { part: number }),
            };
            let reader = match format {
                Format::Zip => {
                    let zip = zip::ZipArchive::new(file).map_err(|err| Problem::Damaged {
                        part: number,
                        message: err.to_string(),
                    })?;
                    Reader::Zip(Box::new(zip))
                }
                Format::Tgz => Reader::Tgz(file),
            };
            export.parts.push(Part {
                reader,
                format,
                entries: 0,
            });
            export.list(number, wants, &mut small_total)?;
        }
        Ok(export)
    }

    /// How each part is packed, and how many entries it has.
    pub(crate) fn parts(&self) -> impl Iterator<Item = (Format, usize)> + '_ {
        self.parts.iter().map(|part| (part.format, part.entries))
    }

    fn list(
        &mut self,
        number: usize,
        wants: &dyn Fn(&str, u64) -> bool,
        small_total: &mut u64,
    ) -> Result<(), Problem> {
        let damaged = |message: String| Problem::Damaged {
            part: number,
            message,
        };
        let mut found = Vec::new();
        let mut reads = Vec::new();
        match &mut self.parts[number].reader {
            Reader::Zip(zip) => {
                for index in 0..zip.len() {
                    let file = zip
                        .by_index_raw(index)
                        .map_err(|err| damaged(err.to_string()))?;
                    let why = if file.encrypted() {
                        Some("it's encrypted")
                    } else if file.is_symlink() {
                        Some("it's a link")
                    } else {
                        None
                    };
                    found.push((
                        index,
                        file.name_raw().to_vec(),
                        file.size(),
                        file.is_dir(),
                        why,
                    ));
                }
            }
            Reader::Tgz(file) => {
                file.seek(SeekFrom::Start(0))
                    .map_err(|source| Problem::Io {
                        part: number,
                        source,
                    })?;
                let gz = flate2::read::MultiGzDecoder::new(BufReader::new(&*file));
                let mut tar = tar::Archive::new(gz);
                let entries = tar.entries().map_err(|err| damaged(err.to_string()))?;
                for (index, entry) in entries.enumerate() {
                    let mut entry = entry.map_err(|err| damaged(err.to_string()))?;
                    let kind = entry.header().entry_type();
                    let raw = entry.path_bytes().into_owned();
                    let size = entry.size();
                    let (folder, why) = if kind.is_dir() {
                        (true, None)
                    } else if kind.is_file() || kind.is_contiguous() {
                        (false, None)
                    } else {
                        (false, Some("it's a link or a device"))
                    };
                    // A `.tgz` can't go back for a file later, so small ones
                    // are read on the way past.
                    let wanted = why.is_none()
                        && !folder
                        && size <= MAX_SMALL
                        && *small_total + size <= MAX_SMALL_TOTAL
                        && std::str::from_utf8(&raw).is_ok_and(|path| wants(path, size));
                    if wanted {
                        let mut contents = Vec::with_capacity(size as usize);
                        (&mut entry)
                            .take(size + 1)
                            .read_to_end(&mut contents)
                            .map_err(|err| damaged(err.to_string()))?;
                        if contents.len() as u64 == size {
                            *small_total += size;
                            reads.push((index, contents));
                        }
                    }
                    found.push((index, raw, size, folder, why));
                }
            }
        }
        let mut read_now: HashMap<usize, Vec<u8>> = reads.into_iter().collect();
        let mut zip_reads = Vec::new();
        for (index, raw, size, folder, why) in found {
            let path = match why.map_or_else(|| checked_path(&raw), Err) {
                Ok(path) => path,
                Err(why) => {
                    self.skipped.push(Skipped {
                        path: String::from_utf8_lossy(&raw).into_owned(),
                        why,
                    });
                    continue;
                }
            };
            if self.entries.len() >= MAX_ENTRIES {
                return Err(Problem::TooManyEntries);
            }
            let id = self.entries.len();
            if let Some(contents) = read_now.remove(&index) {
                self.small.insert(id, contents);
            } else if !folder
                && size <= MAX_SMALL
                && self.parts[number].format == Format::Zip
                && wants(&path, size)
                && *small_total + size <= MAX_SMALL_TOTAL
            {
                zip_reads.push((id, index, size));
            }
            self.entries.push(Entry {
                id,
                part: number,
                index,
                path,
                size,
                folder,
            });
            self.parts[number].entries += 1;
        }
        for (id, index, size) in zip_reads {
            let mut contents = Vec::with_capacity(size as usize);
            if self.copy_zip(number, index, size, &mut contents).is_ok() {
                *small_total += size;
                self.small.insert(id, contents);
            }
        }
        Ok(())
    }

    /// Unpacks the entries in `ids` into `folder`, each named after its ID.
    pub(crate) fn unpack(&mut self, ids: &HashSet<usize>, folder: &Path) -> Result<(), Problem> {
        for number in 0..self.parts.len() {
            let wanted: HashMap<usize, (usize, u64)> = self
                .entries
                .iter()
                .filter(|entry| entry.part == number && !entry.folder && ids.contains(&entry.id))
                .map(|entry| (entry.index, (entry.id, entry.size)))
                .collect();
            if wanted.is_empty() {
                continue;
            }
            let io = |source| Problem::Io {
                part: number,
                source,
            };
            let damaged = |message: String| Problem::Damaged {
                part: number,
                message,
            };
            match self.parts[number].format {
                Format::Zip => {
                    for (&index, &(id, size)) in &wanted {
                        let mut out = new_file(folder, id).map_err(io)?;
                        self.copy_zip(number, index, size, &mut out)?;
                    }
                }
                Format::Tgz => {
                    let Reader::Tgz(file) = &mut self.parts[number].reader else {
                        unreachable!("a .tgz part has a .tgz reader");
                    };
                    file.seek(SeekFrom::Start(0)).map_err(io)?;
                    let gz = flate2::read::MultiGzDecoder::new(BufReader::new(&*file));
                    let mut tar = tar::Archive::new(gz);
                    let entries = tar.entries().map_err(|err| damaged(err.to_string()))?;
                    let mut left = wanted.len();
                    for (index, entry) in entries.enumerate() {
                        let Some(&(id, size)) = wanted.get(&index) else {
                            continue;
                        };
                        let entry = entry.map_err(|err| damaged(err.to_string()))?;
                        let mut out = new_file(folder, id).map_err(io)?;
                        copy_exactly(entry, size, &mut out)
                            .map_err(|err| damaged(err.to_string()))?;
                        left -= 1;
                        if left == 0 {
                            break;
                        }
                    }
                    if left > 0 {
                        return Err(damaged("it ended before every file".to_owned()));
                    }
                }
            }
        }
        Ok(())
    }

    fn copy_zip(
        &mut self,
        number: usize,
        index: usize,
        size: u64,
        out: &mut dyn Write,
    ) -> Result<(), Problem> {
        let damaged = |message: String| Problem::Damaged {
            part: number,
            message,
        };
        let Reader::Zip(zip) = &mut self.parts[number].reader else {
            unreachable!("a .zip part has a zip reader");
        };
        let file = zip
            .by_index(index)
            .map_err(|err| damaged(err.to_string()))?;
        copy_exactly(file, size, out).map_err(|err| damaged(err.to_string()))
    }
}

/// Copies `reader` to `out`, which must hold exactly `size` bytes, as the
/// archive's listing said. A file that unpacks to more, such as in a zip
/// bomb, stops one byte past.
fn copy_exactly(reader: impl Read, size: u64, out: &mut dyn Write) -> io::Result<()> {
    let copied = io::copy(&mut reader.take(size + 1), out)?;
    if copied != size {
        return Err(io::Error::other(format!(
            "a file held {copied} bytes where its listing said {size}"
        )));
    }
    Ok(())
}

fn new_file(folder: &Path, id: usize) -> io::Result<File> {
    File::options()
        .write(true)
        .create_new(true)
        .open(folder.join(id.to_string()))
}

fn read_up_to(file: &mut File, buffer: &mut [u8]) -> io::Result<usize> {
    let mut read = 0;
    while read < buffer.len() {
        match file.read(&mut buffer[read..])? {
            0 => break,
            count => read += count,
        }
    }
    Ok(read)
}

/// The entry's path, if it's safe: UTF-8, relative, with no `.` or `..`,
/// and not too long. A folder's trailing `/` is dropped.
fn checked_path(raw: &[u8]) -> Result<String, &'static str> {
    let path = std::str::from_utf8(raw).map_err(|_| "its name isn't UTF-8")?;
    if path.len() > MAX_PATH {
        return Err("its name is too long");
    }
    if path.contains('\0') {
        return Err("its name has a null character");
    }
    let path = path.strip_suffix('/').unwrap_or(path);
    if path.is_empty() || path.starts_with('/') {
        return Err("its path leaves its folder");
    }
    // Windows tools write `\` between folders, so it counts here too.
    if path
        .split(['/', '\\'])
        .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err("its path leaves its folder");
    }
    Ok(path.to_owned())
}

#[cfg(test)]
mod tests {
    use super::{checked_path, copy_exactly};

    #[test]
    fn stops_a_file_that_holds_more_than_its_listing_says() {
        let mut out = Vec::new();
        assert!(copy_exactly(&[1; 10][..], 10, &mut out).is_ok());
        let mut out = Vec::new();
        // As a zip bomb's would, listed small and unpacking to more.
        assert!(copy_exactly(&[1; 1000][..], 10, &mut out).is_err());
        assert_eq!(out.len(), 11);
        assert!(copy_exactly(&[1; 5][..], 10, &mut Vec::new()).is_err());
    }

    #[test]
    fn refuses_paths_that_leave_their_folder() {
        assert_eq!(
            checked_path(b"Takeout/Keep/a.json"),
            Ok("Takeout/Keep/a.json".into())
        );
        assert_eq!(
            checked_path(b"Takeout/Drive/empty/"),
            Ok("Takeout/Drive/empty".into())
        );
        assert_eq!(
            checked_path("Takeout/買い物 🛒.json".as_bytes()),
            Ok("Takeout/買い物 🛒.json".into())
        );
        for bad in [
            &b"/etc/passwd"[..],
            b"../outside",
            b"Takeout/../../outside",
            b"Takeout/./a",
            b"Takeout//a",
            b"Takeout\\..\\outside",
            b"",
            b"a\0b",
            b"\xff\xfe",
        ] {
            assert!(checked_path(bad).is_err(), "{bad:?}");
        }
    }
}

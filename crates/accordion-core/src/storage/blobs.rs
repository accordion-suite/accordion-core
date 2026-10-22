//! File contents, stored once each and named by their SHA-256 hash.
//!
//! A stored file is never changed. A new version is a new file, and identical
//! contents share one file. Each is written under `uploads/`, flushed to
//! disk, and only then renamed into `blobs/`, so a crash leaves either the
//! whole file or nothing under its hash.

use std::fmt;
use std::io::{self, Read};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use accordion_types::UploadId;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

pub(super) const BUFFER_LEN: usize = 64 * 1024;

/// The SHA-256 hash of a file's contents.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ContentHash(pub [u8; 32]);

impl ContentHash {
    pub fn to_hex(self) -> String {
        self.0.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    /// Reads 64 lowercase hex digits, as [`ContentHash::to_hex`] writes them.
    pub fn from_hex(hex: &str) -> Option<Self> {
        if hex.len() != 64 || !hex.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
            return None;
        }
        let mut bytes = [0u8; 32];
        for (byte, pair) in bytes.iter_mut().zip(hex.as_bytes().chunks(2)) {
            *byte = u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok()?;
        }
        Some(Self(bytes))
    }
}

impl fmt::Display for ContentHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl fmt::Debug for ContentHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ContentHash({self})")
    }
}

/// Stored contents: their hash and size in bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Blob {
    pub hash: ContentHash,
    pub size: u64,
}

/// What's wrong with stored contents that failed the integrity check.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Problem {
    /// The file is gone from the disk.
    Missing,
    /// The file no longer matches its hash, so the disk changed it.
    Changed,
}

impl Problem {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::Changed => "changed",
        }
    }

    pub(crate) fn parse(text: &str) -> Option<Self> {
        match text {
            "missing" => Some(Self::Missing),
            "changed" => Some(Self::Changed),
            _ => None,
        }
    }
}

/// Contents written under `uploads/` and flushed to disk, but not yet
/// stored under their hash.
pub(crate) struct Received {
    pub path: PathBuf,
    pub blob: Blob,
}

pub(crate) struct Blobs {
    blobs: PathBuf,
    uploads: PathBuf,
}

impl Blobs {
    /// Uses `blobs/` and `uploads/` under `dir`, creating them if needed.
    pub(crate) fn open(dir: &Path) -> io::Result<Self> {
        let blobs = dir.join("blobs").join("sha256");
        let uploads = dir.join("uploads");
        std::fs::create_dir_all(&blobs)?;
        std::fs::create_dir_all(&uploads)?;
        Ok(Self { blobs, uploads })
    }

    pub(crate) fn uploads(&self) -> &Path {
        &self.uploads
    }

    /// Where the contents with `hash` live, such as `blobs/sha256/3f/9a/3f9a…`.
    pub(crate) fn path(&self, hash: ContentHash) -> PathBuf {
        self.blobs.join(shard(hash))
    }

    /// Where a resumable upload collects its contents.
    pub(crate) fn upload_path(&self, id: UploadId) -> PathBuf {
        self.uploads
            .join(format!("{}.upload", id.as_uuid().simple()))
    }

    /// Writes everything `contents` gives to a new file under `uploads/`,
    /// hashing it on the way. [`Blobs::keep`] then stores it.
    pub(crate) async fn receive(
        &self,
        mut contents: impl AsyncRead + Unpin,
    ) -> io::Result<Received> {
        let path = self.uploads.join(format!("{}.part", random_name()));
        let result = async {
            let mut file = tokio::fs::File::create(&path).await?;
            let mut hasher = Sha256::new();
            let mut size = 0u64;
            let mut buffer = vec![0; BUFFER_LEN];
            loop {
                let read = contents.read(&mut buffer).await?;
                if read == 0 {
                    break;
                }
                hasher.update(&buffer[..read]);
                file.write_all(&buffer[..read]).await?;
                size += read as u64;
            }
            file.sync_all().await?;
            Ok(Blob {
                hash: ContentHash(hasher.finalize().into()),
                size,
            })
        }
        .await;
        match result {
            Ok(blob) => Ok(Received { path, blob }),
            Err(err) => {
                let _ = tokio::fs::remove_file(&path).await;
                Err(err)
            }
        }
    }

    /// Moves the finished upload at `upload` into `blobs/` under `hash`, or
    /// drops it if the same contents are already there. Returns whether it
    /// stored new contents. It blocks, so call it from `spawn_blocking`.
    pub(crate) fn keep(&self, upload: &Path, hash: ContentHash) -> io::Result<bool> {
        let path = self.path(hash);
        if path.exists() {
            std::fs::remove_file(upload)?;
            return Ok(false);
        }
        // Read-only, so nothing edits stored contents through a hard link in
        // the plain view.
        std::fs::set_permissions(upload, std::fs::Permissions::from_mode(0o400))?;
        let dir = path.parent().expect("blob paths have a parent");
        std::fs::create_dir_all(dir)?;
        std::fs::rename(upload, &path)?;
        // The rename only survives a power cut once the folder is flushed too.
        std::fs::File::open(dir)?.sync_all()?;
        Ok(true)
    }

    /// Deletes stored contents, if they're there. It blocks.
    pub(crate) fn remove(&self, hash: ContentHash) -> io::Result<()> {
        match std::fs::remove_file(self.path(hash)) {
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
            other => other,
        }
    }

    /// Every hash with a file under `blobs/`. It blocks.
    pub(crate) fn stored(&self) -> io::Result<Vec<ContentHash>> {
        let mut hashes = Vec::new();
        for first in folders_in(&self.blobs)? {
            for second in folders_in(&first)? {
                for file in std::fs::read_dir(second)? {
                    let name = file?.file_name();
                    if let Some(hash) = name.to_str().and_then(ContentHash::from_hex) {
                        hashes.push(hash);
                    }
                }
            }
        }
        Ok(hashes)
    }

    /// Files that [`Blobs::receive`] left under `uploads/` when the server
    /// stopped partway, untouched for longer than `age`. It blocks.
    pub(crate) fn abandoned(&self, age: Duration) -> io::Result<Vec<PathBuf>> {
        let now = SystemTime::now();
        let mut paths = Vec::new();
        for entry in std::fs::read_dir(&self.uploads)? {
            let entry = entry?;
            let path = entry.path();
            let modified = entry.metadata()?.modified()?;
            let old = now.duration_since(modified).is_ok_and(|idle| idle > age);
            if old && path.extension().is_some_and(|ext| ext == "part") {
                paths.push(path);
            }
        }
        Ok(paths)
    }

    /// Rehashes stored contents to find damage. It blocks.
    pub(crate) fn check(&self, blob: Blob) -> io::Result<Option<Problem>> {
        let file = match std::fs::File::open(self.path(blob.hash)) {
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Some(Problem::Missing)),
            other => other?,
        };
        let (hasher, size) = hash_reader(file, u64::MAX)?;
        let intact = size == blob.size && ContentHash(hasher.finalize().into()) == blob.hash;
        Ok((!intact).then_some(Problem::Changed))
    }
}

/// Where contents live under `blobs/sha256/`, such as `3f/9a/3f9a…`. Two
/// levels of folders keep each folder small.
pub(crate) fn shard(hash: ContentHash) -> PathBuf {
    let hex = hash.to_hex();
    [&hex[..2], &hex[2..4], &hex].iter().collect()
}

fn folders_in(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut folders = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            folders.push(entry.path());
        }
    }
    Ok(folders)
}

/// Hashes the first `len` bytes of the file at `path`, to pick up hashing a
/// resumable upload where it left off. It blocks.
pub(crate) fn hash_prefix(path: &Path, len: u64) -> io::Result<Sha256> {
    let (hasher, read) = hash_reader(std::fs::File::open(path)?, len)?;
    if read < len {
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    Ok(hasher)
}

/// Hashes up to `limit` bytes, and returns how many there were.
fn hash_reader(reader: impl Read, limit: u64) -> io::Result<(Sha256, u64)> {
    let mut reader = reader.take(limit);
    let mut hasher = Sha256::new();
    let mut size = 0u64;
    let mut buffer = vec![0; BUFFER_LEN];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            return Ok((hasher, size));
        }
        hasher.update(&buffer[..read]);
        size += read as u64;
    }
}

/// A name for a temporary file that nothing else will pick.
pub(crate) fn random_name() -> String {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).expect("the OS random number generator failed");
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn put(blobs: &Blobs, contents: &[u8]) -> (Blob, bool) {
        let received = blobs.receive(contents).await.unwrap();
        let stored = blobs.keep(&received.path, received.blob.hash).unwrap();
        (received.blob, stored)
    }

    #[tokio::test]
    async fn stores_identical_contents_once() {
        let dir = tempfile::tempdir().unwrap();
        let blobs = Blobs::open(dir.path()).unwrap();
        let (first, stored) = put(&blobs, b"hello").await;
        assert!(stored);
        let (second, stored) = put(&blobs, b"hello").await;
        assert!(!stored);
        assert_eq!(first, second);
        assert_eq!(first.size, 5);
        assert_eq!(
            first.hash.to_hex(),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
        assert_eq!(
            ContentHash::from_hex(&first.hash.to_hex()),
            Some(first.hash)
        );

        let path = blobs.path(first.hash);
        assert!(
            path.ends_with(
                "2c/f2/2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
            )
        );
        assert_eq!(std::fs::read(&path).unwrap(), b"hello");
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o400);
        assert_eq!(std::fs::read_dir(blobs.uploads()).unwrap().count(), 0);
        assert_eq!(blobs.stored().unwrap(), [first.hash]);
    }

    #[tokio::test]
    async fn finds_missing_and_changed_contents() {
        let dir = tempfile::tempdir().unwrap();
        let blobs = Blobs::open(dir.path()).unwrap();
        let (blob, _) = put(&blobs, b"hello").await;
        assert_eq!(blobs.check(blob).unwrap(), None);

        let path = blobs.path(blob.hash);
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::write(&path, b"jello").unwrap();
        assert_eq!(blobs.check(blob).unwrap(), Some(Problem::Changed));

        blobs.remove(blob.hash).unwrap();
        blobs.remove(blob.hash).unwrap();
        assert_eq!(blobs.check(blob).unwrap(), Some(Problem::Missing));
    }

    #[test]
    fn reads_only_hashes_it_wrote() {
        assert_eq!(ContentHash::from_hex("2cf2"), None);
        assert_eq!(ContentHash::from_hex(&"G".repeat(64)), None);
        assert_eq!(ContentHash::from_hex(&"A".repeat(64)), None);
        assert_eq!(
            ContentHash::from_hex(&"0f".repeat(32)),
            Some(ContentHash([0x0f; 32]))
        );
    }
}

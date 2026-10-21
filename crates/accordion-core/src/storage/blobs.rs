//! File contents, stored once each and named by their SHA-256 hash.
//!
//! A stored file is never changed. A new version is a new file, and identical
//! contents share one file. Each is written under `uploads/`, flushed to
//! disk, and only then renamed into `blobs/`, so a crash leaves either the
//! whole file or nothing under its hash.

use std::fmt;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

const BUFFER_LEN: usize = 64 * 1024;

/// The SHA-256 hash of a file's contents.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ContentHash(pub [u8; 32]);

impl ContentHash {
    pub fn to_hex(self) -> String {
        self.0.iter().map(|byte| format!("{byte:02x}")).collect()
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
        let hex = hash.to_hex();
        self.blobs.join(&hex[..2]).join(&hex[2..4]).join(hex)
    }

    /// Stores everything `contents` gives, unless identical contents are
    /// already stored.
    pub(crate) async fn put(&self, mut contents: impl AsyncRead + Unpin) -> io::Result<Blob> {
        let upload = self.uploads.join(format!("{}.part", random_name()));
        let result = async {
            let mut file = tokio::fs::File::create(&upload).await?;
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
            let blob = Blob {
                hash: ContentHash(hasher.finalize().into()),
                size,
            };
            let path = self.path(blob.hash);
            let upload = upload.clone();
            tokio::task::spawn_blocking(move || keep(&upload, &path))
                .await
                .expect("storing a file panicked")?;
            Ok(blob)
        }
        .await;
        if result.is_err() {
            let _ = tokio::fs::remove_file(&upload).await;
        }
        result
    }
}

/// A name for a temporary file that nothing else will pick.
pub(crate) fn random_name() -> String {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).expect("the OS random number generator failed");
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Moves a finished upload to `path`, or drops it if `path` already holds
/// the same contents.
fn keep(upload: &Path, path: &Path) -> io::Result<()> {
    if path.exists() {
        return std::fs::remove_file(upload);
    }
    // Read-only, so nothing edits stored contents through a hard link in the
    // plain view.
    std::fs::set_permissions(upload, std::fs::Permissions::from_mode(0o400))?;
    let dir = path.parent().expect("blob paths have a parent");
    std::fs::create_dir_all(dir)?;
    std::fs::rename(upload, path)?;
    // The rename only survives a power cut once the folder is flushed too.
    std::fs::File::open(dir)?.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stores_identical_contents_once() {
        let dir = tempfile::tempdir().unwrap();
        let blobs = Blobs::open(dir.path()).unwrap();
        let first = blobs.put(&b"hello"[..]).await.unwrap();
        let second = blobs.put(&b"hello"[..]).await.unwrap();
        assert_eq!(first, second);
        assert_eq!(first.size, 5);
        assert_eq!(
            first.hash.to_hex(),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
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
    }
}

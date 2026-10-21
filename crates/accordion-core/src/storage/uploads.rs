//! Resumable uploads, for large files over connections that can drop.
//!
//! An upload starts with its size, which counts toward the owner's quota
//! until it finishes, so two big uploads can't both squeeze into the last of
//! the space. Its contents collect in `uploads/<id>.upload`, flushed to disk
//! after every piece, so the file's length is always how much has safely
//! arrived. A client that loses its connection asks how much arrived and
//! sends the rest. Uploads with no progress for a day are removed.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use accordion_types::{ItemId, UploadId, UserId};
use rusqlite::{Connection, OptionalExtension};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

use super::blobs::{self, BUFFER_LEN, ContentHash, Received};
use super::{Blob, Error, Item, Storage, check_name, check_space, size_from, to_sql_size};
use crate::sql_now;

/// An upload in progress.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Upload {
    pub id: UploadId,
    pub owner: UserId,
    /// The whole file's size, given when the upload started.
    pub size: u64,
    /// How much has safely arrived. The next piece starts here.
    pub received: u64,
    pub created_at: String,
}

/// Each upload's hashing so far, kept between pieces so finishing doesn't
/// read the whole file again. After a restart, it's rebuilt from the file.
pub(super) type InProgress = Mutex<HashMap<UploadId, Arc<tokio::sync::Mutex<Option<Hashed>>>>>;

pub(super) struct Hashed {
    hasher: Sha256,
    len: u64,
}

impl Storage {
    /// Starts an upload of `size` bytes for `owner`, if it fits in their
    /// quota.
    pub async fn start_upload(&self, owner: UserId, size: u64) -> Result<Upload, Error> {
        self.inner
            .identity
            .user(owner)
            .await?
            .ok_or(Error::UserNotFound)?;
        let id = UploadId::generate();
        let path = self.inner.blobs.upload_path(id);
        tokio::fs::File::create(&path).await?;
        let _changes = self.inner.changes.lock().await;
        let result = self
            .inner
            .db
            .write(move |tx| {
                check_space(tx, owner, size, None)?;
                tx.execute(
                    concat!(
                        "INSERT INTO storage_uploads (id, owner_id, size, created_at, updated_at)
                         VALUES (?1, ?2, ?3, ",
                        sql_now!(),
                        ", ",
                        sql_now!(),
                        ")"
                    ),
                    (id.as_uuid(), owner.as_uuid(), to_sql_size(size)),
                )?;
                Ok(())
            })
            .await;
        if let Err(err) = result {
            let _ = tokio::fs::remove_file(&path).await;
            return Err(err);
        }
        self.upload(id).await
    }

    /// An upload's progress.
    pub async fn upload(&self, id: UploadId) -> Result<Upload, Error> {
        let mut upload = self
            .inner
            .db
            .read(move |conn| {
                conn.query_row(
                    "SELECT owner_id, size, created_at FROM storage_uploads WHERE id = ?1",
                    [id.as_uuid()],
                    |row| {
                        Ok(Upload {
                            id,
                            owner: UserId::from_uuid(row.get(0)?),
                            size: size_from(row, 1)?,
                            received: 0,
                            created_at: row.get(2)?,
                        })
                    },
                )
                .optional()?
                .ok_or(Error::UploadNotFound)
            })
            .await?;
        upload.received = match tokio::fs::metadata(self.inner.blobs.upload_path(id)).await {
            Ok(metadata) => metadata.len(),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Err(Error::UploadNotFound);
            }
            Err(err) => return Err(err.into()),
        };
        Ok(upload)
    }

    /// Adds the next piece of an upload, which must start at `offset`: what
    /// has arrived so far. If the connection drops partway, what arrived is
    /// kept, and [`Storage::upload`] says where to continue.
    pub async fn append(
        &self,
        id: UploadId,
        offset: u64,
        mut piece: impl AsyncRead + Unpin,
    ) -> Result<Upload, Error> {
        let hashed = self.hashed(id);
        let mut hashed = hashed.lock().await;
        let mut upload = self.found(id, self.upload(id).await)?;
        if offset != upload.received {
            return Err(Error::UploadOffset {
                expected: upload.received,
            });
        }
        let path = self.inner.blobs.upload_path(id);
        let mut state = match hashed.take() {
            Some(state) if state.len == upload.received => state,
            _ => self.rehash(id, upload.received).await?,
        };

        let mut file = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .await?;
        let mut buffer = vec![0; BUFFER_LEN];
        let copied = async {
            loop {
                let read = piece.read(&mut buffer).await?;
                if read == 0 {
                    return Ok(());
                }
                if state.len + read as u64 > upload.size {
                    return Err(Error::UploadTooLong { size: upload.size });
                }
                file.write_all(&buffer[..read]).await?;
                state.hasher.update(&buffer[..read]);
                state.len += read as u64;
            }
        }
        .await;
        file.sync_data().await?;

        // A failed write may have left part of a piece, which the hashing
        // doesn't cover. Then the next piece rehashes the file instead.
        upload.received = file.metadata().await?.len();
        if upload.received == state.len {
            *hashed = Some(state);
        }
        self.inner
            .db
            .write(move |tx| {
                tx.execute(
                    concat!(
                        "UPDATE storage_uploads SET updated_at = ",
                        sql_now!(),
                        " WHERE id = ?1"
                    ),
                    [id.as_uuid()],
                )?;
                Ok::<_, Error>(())
            })
            .await?;
        copied.map(|()| upload)
    }

    /// Saves a finished upload as the file `name` in `parent`, as a new
    /// version if a file with that name is already there.
    pub async fn finish_upload(
        &self,
        id: UploadId,
        parent: ItemId,
        name: &str,
    ) -> Result<Item, Error> {
        let name = check_name(name)?;
        let hashed = self.hashed(id);
        let mut hashed = hashed.lock().await;
        let upload = self.found(id, self.upload(id).await)?;
        if upload.received != upload.size {
            return Err(Error::UploadIncomplete {
                received: upload.received,
                size: upload.size,
            });
        }
        let state = match hashed.take() {
            Some(state) if state.len == upload.size => state,
            _ => self.rehash(id, upload.size).await?,
        };
        let received = Received {
            path: self.inner.blobs.upload_path(id),
            blob: Blob {
                hash: ContentHash(state.hasher.clone().finalize().into()),
                size: upload.size,
            },
        };
        let result = self.save(parent, name, &received, Some(id)).await;
        match result {
            Ok(_) => self.forget(id),
            // The upload stays, so the client can try again, such as with
            // another name.
            Err(_) => *hashed = Some(state),
        }
        result
    }

    /// Stops an upload and deletes what arrived.
    pub async fn cancel_upload(&self, id: UploadId) -> Result<(), Error> {
        let hashed = self.hashed(id);
        let _hashed = hashed.lock().await;
        let removed = self.remove_upload(id).await;
        self.forget(id);
        if removed? {
            Ok(())
        } else {
            Err(Error::UploadNotFound)
        }
    }

    /// Removes uploads with no progress for a day, apart from any that are
    /// receiving a piece right now. Returns how many it removed.
    pub(super) async fn remove_stalled_uploads(&self) -> Result<usize, Error> {
        let stalled: Vec<UploadId> = self
            .inner
            .db
            .read(|conn| {
                let ids = conn
                    .prepare(concat!(
                        "SELECT id FROM storage_uploads WHERE updated_at < ",
                        sql_now!("-1 day")
                    ))?
                    .query_map([], |row| Ok(UploadId::from_uuid(row.get(0)?)))?
                    .collect::<Result<_, _>>()?;
                Ok::<_, Error>(ids)
            })
            .await?;
        let mut removed = 0;
        for id in stalled {
            let hashed = self.hashed(id);
            let Ok(_hashed) = hashed.try_lock() else {
                continue;
            };
            if self.remove_upload(id).await? {
                removed += 1;
            }
            self.forget(id);
        }
        Ok(removed)
    }

    /// Deletes an upload's row and file. Returns whether it existed.
    async fn remove_upload(&self, id: UploadId) -> Result<bool, Error> {
        let deleted = self
            .inner
            .db
            .write(move |tx| {
                let deleted =
                    tx.execute("DELETE FROM storage_uploads WHERE id = ?1", [id.as_uuid()])?;
                Ok::<_, Error>(deleted > 0)
            })
            .await?;
        match tokio::fs::remove_file(self.inner.blobs.upload_path(id)).await {
            Err(err) if err.kind() != std::io::ErrorKind::NotFound => Err(err.into()),
            _ => Ok(deleted),
        }
    }

    /// Hashes what has arrived so far, to pick up after a restart.
    async fn rehash(&self, id: UploadId, len: u64) -> Result<Hashed, Error> {
        let path = self.inner.blobs.upload_path(id);
        let hasher = self
            .on_disk(move |_| blobs::hash_prefix(&path, len))
            .await?;
        Ok(Hashed { hasher, len })
    }

    /// The lock that keeps one piece of an upload going at a time.
    fn hashed(&self, id: UploadId) -> Arc<tokio::sync::Mutex<Option<Hashed>>> {
        let mut uploads = self
            .inner
            .uploads
            .lock()
            .expect("upload list lock poisoned");
        Arc::clone(uploads.entry(id).or_default())
    }

    fn forget(&self, id: UploadId) {
        self.inner
            .uploads
            .lock()
            .expect("upload list lock poisoned")
            .remove(&id);
    }

    /// Passes `result` on, forgetting `id` first if there's no such upload.
    fn found(&self, id: UploadId, result: Result<Upload, Error>) -> Result<Upload, Error> {
        if matches!(result, Err(Error::UploadNotFound)) {
            self.forget(id);
        }
        result
    }
}

/// Who started an upload.
pub(super) fn owner(conn: &Connection, id: UploadId) -> Result<UserId, Error> {
    conn.query_row(
        "SELECT owner_id FROM storage_uploads WHERE id = ?1",
        [id.as_uuid()],
        |row| Ok(UserId::from_uuid(row.get(0)?)),
    )
    .optional()?
    .ok_or(Error::UploadNotFound)
}

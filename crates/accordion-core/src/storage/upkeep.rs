//! Upkeep: emptying old trash and versions, removing contents nothing uses,
//! and checking stored contents for damage. [`Storage::maintain`] runs it
//! all in the background.

use std::collections::HashSet;
use std::time::Duration;

use rusqlite::params_from_iter;

use super::blobs::Problem;
use super::{Blob, ContentHash, Error, Storage, delete_subtree, hash_from, size_from};
use crate::sql_now;

/// SQL that's true for a `storage_blobs` row that no version and no module
/// uses.
macro_rules! unused {
    () => {
        "NOT EXISTS (SELECT 1 FROM storage_versions v WHERE v.blob = storage_blobs.hash)
         AND NOT EXISTS (SELECT 1 FROM storage_blob_refs r WHERE r.blob = storage_blobs.hash)"
    };
}

/// How often [`Storage::maintain`] runs a round of upkeep.
const ROUND_EVERY: Duration = Duration::from_secs(60 * 60);

/// Rounds between looks for stray files under `blobs/`. Only a crash or a
/// failed commit leaves them, so once a day is plenty.
const STRAYS_EVERY: u32 = 24;

/// How much the integrity check rehashes each round. At an hour a round,
/// that's about 700 GB a month, and a few seconds of work each hour.
const CHECK_PER_ROUND: u64 = 1024 * 1024 * 1024;

/// Rows each write handles, so upkeep never holds up other writes for long.
const BATCH: usize = 500;

/// How long deleted items and replaced versions are kept.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Retention {
    /// Days an item stays in the trash.
    pub trash_days: u32,
    /// Days an older version is kept after a newer one replaces it.
    pub version_days: u32,
}

impl Default for Retention {
    /// 30 days each, as the spec asks.
    fn default() -> Self {
        Self {
            trash_days: 30,
            version_days: 30,
        }
    }
}

/// What [`Storage::purge`] deleted for good.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Purged {
    /// Items that were in the trash, not counting what was inside them.
    pub items: usize,
    pub versions: usize,
}

/// What [`Storage::clean_up`] removed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Cleaned {
    /// Stored contents that nothing had used for a day.
    pub contents: usize,
    pub bytes: u64,
    /// Uploads with no progress for a day.
    pub uploads: usize,
}

/// What a round of the integrity check found.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Checked {
    pub contents: usize,
    pub bytes: u64,
    /// How many failed, in this round.
    pub damaged: usize,
}

/// Stored contents that failed the integrity check.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Damage {
    pub blob: ContentHash,
    pub problem: Problem,
    pub found_at: String,
}

/// How storage is doing, for the health page.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StorageStatus {
    /// The plain view is being recreated from the database.
    pub rebuilding_view: bool,
    /// Stored contents that failed the integrity check.
    pub damaged: u64,
    /// Why the last round of upkeep failed, if it did.
    pub last_error: Option<String>,
}

impl Storage {
    /// How storage is doing. It changes as upkeep runs.
    pub fn status(&self) -> tokio::sync::watch::Receiver<StorageStatus> {
        self.inner.status.subscribe()
    }

    /// Runs upkeep every hour until `stopped` completes. The first round
    /// also rebuilds the plain view, in case the server stopped partway
    /// through a change.
    pub async fn maintain(self, stopped: impl Future<Output = ()>) {
        let mut stopped = std::pin::pin!(stopped);
        let mut view_rebuilt = false;
        let mut round = 0u32;
        loop {
            let result = self.upkeep(&mut view_rebuilt, round).await;
            if let Err(err) = &result {
                tracing::warn!(%err, "storage upkeep failed");
            }
            self.inner
                .status
                .send_modify(|status| status.last_error = result.err().map(|err| err.to_string()));
            round = round.wrapping_add(1);
            if tokio::time::timeout(ROUND_EVERY, &mut stopped)
                .await
                .is_ok()
            {
                return;
            }
        }
    }

    async fn upkeep(&self, view_rebuilt: &mut bool, round: u32) -> Result<(), Error> {
        if !*view_rebuilt {
            self.rebuild_view().await?;
            *view_rebuilt = true;
        }
        if round.is_multiple_of(STRAYS_EVERY) {
            let strays = self.remove_strays().await?;
            if strays > 0 {
                tracing::info!(strays, "removed stored contents with no record");
            }
        }
        let purged = self.purge(Retention::default()).await?;
        let cleaned = self.clean_up().await?;
        let checked = self.check_stored(CHECK_PER_ROUND).await?;
        tracing::debug!(?purged, ?cleaned, ?checked, "storage upkeep done");
        Ok(())
    }

    /// Deletes for good what's been in the trash, and older versions that
    /// were replaced, longer than `retention` allows. Their contents are
    /// removed a day later, unless something else uses them.
    pub async fn purge(&self, retention: Retention) -> Result<Purged, Error> {
        let mut purged = Purged::default();
        let trash_cutoff = format!("-{} days", retention.trash_days);
        loop {
            let _changes = self.inner.changes.lock().await;
            let cutoff = trash_cutoff.clone();
            let items = self
                .inner
                .db
                .write(move |tx| {
                    let ids: Vec<accordion_types::ItemId> = tx
                        .prepare(
                            "SELECT id FROM storage_items
                             WHERE trashed_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?1)
                             LIMIT ?2",
                        )?
                        .query_map((&cutoff, BATCH as i64), |row| {
                            Ok(accordion_types::ItemId::from_uuid(row.get(0)?))
                        })?
                        .collect::<Result<_, _>>()?;
                    for &id in &ids {
                        delete_subtree(tx, id)?;
                    }
                    Ok::<_, Error>(ids.len())
                })
                .await?;
            purged.items += items;
            if items < BATCH {
                break;
            }
        }

        // A version goes once the version after it is old enough. The
        // current version has none after it, so it always stays.
        let version_cutoff = format!("-{} days", retention.version_days);
        loop {
            let _changes = self.inner.changes.lock().await;
            let cutoff = version_cutoff.clone();
            let versions = self
                .inner
                .db
                .write(move |tx| {
                    Ok::<_, Error>(tx.execute(
                        "DELETE FROM storage_versions WHERE id IN (
                             SELECT v.id FROM storage_versions v WHERE EXISTS (
                                 SELECT 1 FROM storage_versions newer
                                 WHERE newer.item_id = v.item_id AND newer.number > v.number
                                   AND newer.created_at
                                       < strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?1)
                             )
                             LIMIT ?2
                         )",
                        (&cutoff, BATCH as i64),
                    )?)
                })
                .await?;
            purged.versions += versions;
            if versions < BATCH {
                break;
            }
        }
        Ok(purged)
    }

    /// Removes stored contents that nothing has used for a day, uploads
    /// with no progress for a day, and pieces of uploads a crash left
    /// behind.
    ///
    /// The day's wait means a file deleted for good can still come back
    /// from the same contents uploaded again, without storing them twice.
    pub async fn clean_up(&self) -> Result<Cleaned, Error> {
        let mut cleaned = Cleaned {
            uploads: self.remove_stalled_uploads().await?,
            ..Cleaned::default()
        };
        let abandoned = self
            .on_disk(|blobs| blobs.abandoned(Duration::from_secs(24 * 60 * 60)))
            .await?;
        for path in abandoned {
            let _ = tokio::fs::remove_file(path).await;
        }

        {
            let _changes = self.inner.changes.lock().await;
            self.inner
                .db
                .write(|tx| {
                    tx.execute(
                        concat!(
                            "UPDATE storage_blobs SET unused_since = ",
                            sql_now!(),
                            " WHERE unused_since IS NULL AND ",
                            unused!()
                        ),
                        [],
                    )?;
                    // Saving contents clears this already. Clearing it here
                    // too means contents in use always get their full day
                    // once they're let go.
                    tx.execute(
                        concat!(
                            "UPDATE storage_blobs SET unused_since = NULL
                             WHERE unused_since IS NOT NULL AND NOT (",
                            unused!(),
                            ")"
                        ),
                        [],
                    )?;
                    Ok::<_, Error>(())
                })
                .await?;
        }
        loop {
            let _changes = self.inner.changes.lock().await;
            let gone: Vec<Blob> = self
                .inner
                .db
                .write(|tx| {
                    let gone: Vec<Blob> = tx
                        .prepare(concat!(
                            "SELECT hash, size FROM storage_blobs WHERE unused_since < ",
                            sql_now!("-1 day"),
                            " AND ",
                            unused!(),
                            " LIMIT ?1"
                        ))?
                        .query_map([BATCH as i64], |row| {
                            Ok(Blob {
                                hash: hash_from(row, 0)?,
                                size: size_from(row, 1)?,
                            })
                        })?
                        .collect::<Result<_, _>>()?;
                    let mut delete = tx.prepare("DELETE FROM storage_blobs WHERE hash = ?1")?;
                    for blob in &gone {
                        delete.execute([&blob.hash.0[..]])?;
                    }
                    Ok::<_, Error>(gone)
                })
                .await?;
            // The rows go first. If removing a file fails, it's a stray,
            // which a later round removes.
            let hashes: Vec<ContentHash> = gone.iter().map(|blob| blob.hash).collect();
            self.on_disk(move |blobs| {
                for hash in hashes {
                    blobs.remove(hash)?;
                }
                Ok(())
            })
            .await?;
            cleaned.contents += gone.len();
            cleaned.bytes += gone.iter().map(|blob| blob.size).sum::<u64>();
            if gone.len() < BATCH {
                break;
            }
        }
        Ok(cleaned)
    }

    /// Removes files under `blobs/` that the database has no record of,
    /// which a crash between storing contents and committing them leaves.
    /// Returns how many it removed.
    pub async fn remove_strays(&self) -> Result<usize, Error> {
        let stored = self.on_disk(|blobs| blobs.stored()).await?;
        let mut removed = 0;
        for chunk in stored.chunks(BATCH) {
            // Checked under the lock, so contents being stored right now
            // aren't mistaken for strays.
            let _changes = self.inner.changes.lock().await;
            let chunk = chunk.to_vec();
            let strays: Vec<ContentHash> = self
                .inner
                .db
                .read(move |conn| {
                    let placeholders = vec!["?"; chunk.len()].join(", ");
                    let known: HashSet<ContentHash> = conn
                        .prepare(&format!(
                            "SELECT hash FROM storage_blobs WHERE hash IN ({placeholders})"
                        ))?
                        .query_map(params_from_iter(chunk.iter().map(|hash| hash.0)), |row| {
                            hash_from(row, 0)
                        })?
                        .collect::<Result<_, _>>()?;
                    Ok::<_, Error>(
                        chunk
                            .into_iter()
                            .filter(|hash| !known.contains(hash))
                            .collect(),
                    )
                })
                .await?;
            removed += strays.len();
            self.on_disk(move |blobs| {
                for hash in strays {
                    blobs.remove(hash)?;
                }
                Ok(())
            })
            .await?;
        }
        Ok(removed)
    }

    /// Rehashes about `max_bytes` of stored contents, those checked longest
    /// ago first, and records any damage. Contents that pass again, such as
    /// after a restore from backup, are no longer counted as damaged.
    pub async fn check_stored(&self, max_bytes: u64) -> Result<Checked, Error> {
        let due: Vec<Blob> = self
            .inner
            .db
            .read(move |conn| {
                let mut statement = conn.prepare(
                    "SELECT hash, size FROM storage_blobs ORDER BY checked_at LIMIT 10000",
                )?;
                let mut rows = statement.query([])?;
                let mut due = Vec::new();
                let mut bytes = 0u64;
                // Always at least one, so a file bigger than `max_bytes` still
                // gets its turn.
                while bytes < max_bytes.max(1) {
                    let Some(row) = rows.next()? else { break };
                    let blob = Blob {
                        hash: hash_from(row, 0)?,
                        size: size_from(row, 1)?,
                    };
                    bytes = bytes.saturating_add(blob.size.max(1));
                    due.push(blob);
                }
                Ok::<_, Error>(due)
            })
            .await?;
        let results = self
            .on_disk(move |blobs| {
                due.into_iter()
                    .map(|blob| Ok((blob, blobs.check(blob)?)))
                    .collect::<std::io::Result<Vec<_>>>()
            })
            .await?;

        let checked = Checked {
            contents: results.len(),
            bytes: results.iter().map(|(blob, _)| blob.size).sum(),
            damaged: results
                .iter()
                .filter(|(_, problem)| problem.is_some())
                .count(),
        };
        for (blob, problem) in &results {
            if let Some(problem) = problem {
                tracing::error!(hash = %blob.hash, problem = problem.as_str(), "stored contents are damaged");
            }
        }
        let damaged = self
            .inner
            .db
            .write(move |tx| {
                let mut update = tx.prepare(concat!(
                    "UPDATE storage_blobs SET checked_at = ",
                    sql_now!(),
                    ", damage = ?2 WHERE hash = ?1"
                ))?;
                for (blob, problem) in &results {
                    update.execute((&blob.hash.0[..], problem.map(Problem::as_str)))?;
                }
                count_damaged(tx)
            })
            .await?;
        self.inner
            .status
            .send_modify(|status| status.damaged = damaged);
        Ok(checked)
    }

    /// Stored contents that failed the integrity check, most recent first.
    pub async fn damage(&self) -> Result<Vec<Damage>, Error> {
        self.inner
            .db
            .read(|conn| {
                let damage = conn
                    .prepare(
                        "SELECT hash, damage, checked_at FROM storage_blobs
                         WHERE damage IS NOT NULL ORDER BY checked_at DESC",
                    )?
                    .query_map([], |row| {
                        let problem: String = row.get(1)?;
                        Ok(Damage {
                            blob: hash_from(row, 0)?,
                            problem: Problem::parse(&problem).unwrap_or(Problem::Changed),
                            found_at: row.get(2)?,
                        })
                    })?
                    .collect::<Result<_, _>>()?;
                Ok(damage)
            })
            .await
    }
}

fn count_damaged(conn: &rusqlite::Connection) -> Result<u64, Error> {
    Ok(conn.query_row(
        "SELECT count(*) FROM storage_blobs WHERE damage IS NOT NULL",
        [],
        |row| size_from(row, 0),
    )?)
}

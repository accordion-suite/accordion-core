//! The list of changes that sync follows (decision 23 in the `accordion`
//! repo's `docs/decisions`).
//!
//! Every change to an item is recorded as it commits, for each person it
//! affects: its owner, and everyone it or a folder above it is shared with.
//! A device keeps a cursor, and asks for what changed after it. Each entry
//! is the item as it is now, or "gone" if the person can't open it any
//! more, so an item that changed five times since the device last asked
//! comes once.
//!
//! Changes are kept for 90 days. A device whose cursor is older than that,
//! or from before a backup was restored, starts over: it lists everything
//! again and compares it with what it has.

use std::collections::{HashMap, HashSet};

use accordion_types::{ItemId, UserId};
use rusqlite::{Connection, OptionalExtension};

use super::{Access, Error, Item, Storage, access, item, location, shares};
use crate::events::{Action, NewEvent};
use crate::sql_now;

/// The most entries in one page of changes or of a listing.
pub const PAGE: usize = 1000;

/// An item as one person sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SyncItem {
    pub item: Item,
    /// What the person may do with it.
    pub access: Access,
    /// The folder it's in, if the person can open that folder. `None` for
    /// their top folder, and for an item shared with them whose folder
    /// isn't, which goes in **Shared with me**.
    pub parent: Option<ItemId>,
    /// Everything inside it changed for the person too, such as when it was
    /// shared with them, so a device lists it again with
    /// [`Storage::sync_list`].
    pub whole: bool,
}

/// One entry in the list of changes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SyncChange {
    Changed(Box<SyncItem>),
    /// The person can't open it any more: it was deleted, moved to the
    /// trash, moved out of their reach, or unshared. Everything inside it
    /// went with it.
    Gone(ItemId),
}

/// A page of changes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Changes {
    pub changes: Vec<SyncChange>,
    /// Where to ask from next time.
    pub cursor: String,
    /// More changes are waiting, so ask again with `cursor` right away.
    pub more: bool,
}

/// Where a device starts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SyncStart {
    /// The cursor to follow changes from once everything is listed. It's
    /// taken before the listing, so nothing that changes during it is
    /// missed.
    pub cursor: String,
    /// The person's top folder, then each item shared with them that isn't
    /// inside another one they can open.
    pub roots: Vec<SyncItem>,
}

/// A page of what's inside a folder.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SyncPage {
    /// Everything inside, at any depth, outside the trash, by ID.
    pub items: Vec<SyncItem>,
    /// Where the next page starts, if there is one.
    pub next: Option<ItemId>,
}

impl Storage {
    /// Where a device starts syncing `user`'s items: a cursor, then the
    /// folders to list with [`Storage::sync_list`].
    pub async fn sync_start(&self, user: UserId) -> Result<SyncStart, Error> {
        self.top_folder(user).await?;
        self.inner
            .db
            .read(move |conn| {
                let cursor = cursor(conn)?;
                let mut roots = Vec::new();
                for id in shares::roots(conn, user)? {
                    if let Some(entry) = sync_item(conn, user, id, false)?
                        && entry.parent.is_none()
                    {
                        roots.push(entry);
                    }
                }
                Ok(SyncStart { cursor, roots })
            })
            .await
    }

    /// A page of everything inside `folder` that `user` can open, after
    /// the item `after`. Treat [`Error::NotFound`] as "gone".
    pub async fn sync_list(
        &self,
        user: UserId,
        folder: ItemId,
        after: Option<ItemId>,
    ) -> Result<SyncPage, Error> {
        self.inner
            .db
            .read(move |conn| {
                sync_item(conn, user, folder, false)?.ok_or(Error::NotFound)?;
                let ids: Vec<ItemId> = conn
                    .prepare(
                        "WITH RECURSIVE down (id) AS (
                             SELECT id FROM storage_items
                             WHERE parent_id = ?1 AND trashed_at IS NULL
                             UNION ALL
                             SELECT i.id FROM storage_items i JOIN down ON i.parent_id = down.id
                             WHERE i.trashed_at IS NULL
                         )
                         SELECT id FROM down WHERE ?2 IS NULL OR id > ?2 ORDER BY id LIMIT ?3",
                    )?
                    .query_map(
                        (folder.as_uuid(), after.map(|id| id.as_uuid()), PAGE as i64),
                        |row| Ok(ItemId::from_uuid(row.get(0)?)),
                    )?
                    .collect::<Result<_, _>>()?;
                let next = (ids.len() == PAGE).then(|| ids[PAGE - 1]);
                let mut items = Vec::with_capacity(ids.len());
                for id in ids {
                    items.extend(sync_item(conn, user, id, false)?);
                }
                Ok(SyncPage { items, next })
            })
            .await
    }

    /// What changed for `user` after `cursor`, a page at a time.
    /// [`Error::StartOver`] means the device lists everything again.
    pub async fn changes(&self, user: UserId, cursor: &str) -> Result<Changes, Error> {
        let (generation, after) = parse_cursor(cursor).ok_or(Error::StartOver)?;
        self.inner
            .db
            .read(move |conn| {
                let sync = state(conn)?;
                if generation != sync.generation
                    || after < sync.pruned_through
                    || after > sync.last_seq
                {
                    return Err(Error::StartOver);
                }
                let mut rows: Vec<(i64, ItemId, bool)> = conn
                    .prepare(
                        "SELECT seq, item_id, whole FROM storage_changes
                         WHERE user_id = ?1 AND seq > ?2 ORDER BY seq LIMIT ?3",
                    )?
                    .query_map((user.as_uuid(), after, PAGE as i64 + 1), |row| {
                        Ok((row.get(0)?, ItemId::from_uuid(row.get(1)?), row.get(2)?))
                    })?
                    .collect::<Result<_, _>>()?;
                let more = rows.len() > PAGE;
                rows.truncate(PAGE);
                // Without more to come, the cursor moves to the newest change
                // anyone has, so a device with nothing new keeps a cursor
                // that pruning won't pass for another 90 days.
                let upto = match rows.last() {
                    Some(&(seq, ..)) if more => seq,
                    _ => sync.last_seq,
                };

                // Each item once, where it last changed.
                let mut whole: HashMap<ItemId, bool> = HashMap::new();
                for &(_, id, all) in &rows {
                    *whole.entry(id).or_default() |= all;
                }
                let mut seen = HashSet::new();
                let mut order: Vec<ItemId> = rows
                    .iter()
                    .rev()
                    .filter(|(_, id, _)| seen.insert(*id))
                    .map(|&(_, id, _)| id)
                    .collect();
                order.reverse();

                let mut changes = Vec::with_capacity(order.len());
                for id in order {
                    changes.push(match sync_item(conn, user, id, whole[&id])? {
                        Some(entry) => SyncChange::Changed(Box::new(entry)),
                        None => SyncChange::Gone(id),
                    });
                }
                Ok(Changes {
                    changes,
                    cursor: format_cursor(&sync.generation, upto),
                    more,
                })
            })
            .await
    }

    /// Removes changes older than 90 days. Devices with a cursor from before
    /// then start over.
    pub async fn prune_changes(&self) -> Result<usize, Error> {
        self.inner
            .db
            .write(|tx| {
                tx.execute(
                    concat!(
                        "UPDATE storage_sync SET pruned_through = max(pruned_through, coalesce(
                             (SELECT max(seq) FROM storage_changes WHERE at < ",
                        sql_now!("-90 days"),
                        "), 0))"
                    ),
                    [],
                )?;
                Ok::<_, Error>(tx.execute(
                    concat!(
                        "DELETE FROM storage_changes WHERE at < ",
                        sql_now!("-90 days")
                    ),
                    [],
                )?)
            })
            .await
    }
}

/// Makes every device's cursor stop working, so each one starts over. For
/// a restored backup, where the list of changes went back in time.
pub(crate) fn forget_cursors(conn: &Connection) -> rusqlite::Result<()> {
    let exists = conn
        .query_row(
            "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'storage_sync'",
            [],
            |_| Ok(()),
        )
        .optional()?
        .is_some();
    if exists {
        conn.execute("UPDATE storage_sync SET generation = randomblob(16)", [])?;
    }
    Ok(())
}

/// Records the change an event describes, for everyone it affects. Called
/// for every event storage records, inside its transaction.
pub(super) fn record_event(conn: &Connection, event: &NewEvent) -> Result<(), Error> {
    let Some(id) = event.item else {
        return Ok(());
    };
    match event.action {
        Action::Added
        | Action::Edited
        | Action::Renamed
        | Action::Trashed
        | Action::VersionRestored => note(
            conn,
            id,
            reaching(conn, id)?.into_iter().map(|user| (user, false)),
        ),
        // What's inside was out of sight while it was in the trash.
        Action::Restored => note(
            conn,
            id,
            reaching(conn, id)?.into_iter().map(|user| (user, true)),
        ),
        Action::Moved => {
            let mut before: HashSet<UserId> = shared_directly(conn, id)?;
            if let Some(from) = event
                .details
                .get("from")
                .cloned()
                .and_then(|from| serde_json::from_value::<ItemId>(from).ok())
            {
                before.extend(reaching(conn, from)?);
            }
            let after: HashSet<UserId> = reaching(conn, id)?.into_iter().collect();
            // Those who can open it only now list everything inside it.
            // Those who could only before hear it's gone.
            let people = after
                .iter()
                .map(|&user| (user, !before.contains(&user)))
                .chain(before.difference(&after).map(|&user| (user, false)))
                .collect::<Vec<_>>();
            note(conn, id, people)
        }
        Action::Shared | Action::ShareChanged | Action::Unshared => match event.other {
            Some(other) => note(conn, id, [(other, true)]),
            None => Ok(()),
        },
        _ => Ok(()),
    }
}

/// Records that `id` is about to be deleted for good, for everyone who
/// could reach it.
pub(super) fn record_deleting(conn: &Connection, id: ItemId) -> Result<(), Error> {
    note(
        conn,
        id,
        reaching(conn, id)?.into_iter().map(|user| (user, false)),
    )
}

/// Records that everything `owner` shared is about to be deleted with
/// their account, for the people they shared it with.
pub(super) fn record_owner_deleting(conn: &Connection, owner: UserId) -> Result<(), Error> {
    let shares: Vec<(ItemId, UserId)> = conn
        .prepare(
            "SELECT s.item_id, s.user_id FROM storage_shares s
             JOIN storage_items i ON i.id = s.item_id WHERE i.owner_id = ?1",
        )?
        .query_map([owner.as_uuid()], |row| {
            Ok((
                ItemId::from_uuid(row.get(0)?),
                UserId::from_uuid(row.get(1)?),
            ))
        })?
        .collect::<Result<_, _>>()?;
    for (id, user) in shares {
        note(conn, id, [(user, false)])?;
    }
    Ok(())
}

/// Records a change to `id` for each person, `true` for those who need
/// everything inside it again.
pub(super) fn note(
    conn: &Connection,
    id: ItemId,
    people: impl IntoIterator<Item = (UserId, bool)>,
) -> Result<(), Error> {
    let mut each: HashMap<UserId, bool> = HashMap::new();
    for (user, whole) in people {
        *each.entry(user).or_default() |= whole;
    }
    if each.is_empty() {
        return Ok(());
    }
    let seq: i64 = conn.query_row(
        "UPDATE storage_sync SET last_seq = last_seq + 1 RETURNING last_seq",
        [],
        |row| row.get(0),
    )?;
    let mut insert = conn.prepare_cached(concat!(
        "INSERT INTO storage_changes (user_id, seq, item_id, whole, at) VALUES (?1, ?2, ?3, ?4, ",
        sql_now!(),
        ")"
    ))?;
    for (user, whole) in each {
        insert.execute((user.as_uuid(), seq, id.as_uuid(), whole))?;
    }
    Ok(())
}

/// The item as `user` sees it, or `None` if they can't open it. Items in
/// the trash count as gone, even to their owner: sync shows only what's
/// outside it.
fn sync_item(
    conn: &Connection,
    user: UserId,
    id: ItemId,
    whole: bool,
) -> Result<Option<SyncItem>, Error> {
    let Some(item) = item(conn, id)? else {
        return Ok(None);
    };
    if !location(conn, id)?.visible {
        return Ok(None);
    }
    let Some(level) = access(conn, user, id)? else {
        return Ok(None);
    };
    let parent = match item.parent {
        Some(parent) if item.owner == user || access(conn, user, parent)?.is_some() => Some(parent),
        _ => None,
    };
    Ok(Some(SyncItem {
        item,
        access: level,
        parent,
        whole,
    }))
}

/// Everyone who reaches `id`, in the trash or not: its owner, and whoever
/// it or a folder above it is shared with.
fn reaching(conn: &Connection, id: ItemId) -> Result<Vec<UserId>, Error> {
    let Some(owner) = conn
        .query_row(
            "SELECT owner_id FROM storage_items WHERE id = ?1",
            [id.as_uuid()],
            |row| Ok(UserId::from_uuid(row.get(0)?)),
        )
        .optional()?
    else {
        return Ok(Vec::new());
    };
    let mut people = vec![owner];
    let mut shared = conn.prepare_cached(
        "WITH RECURSIVE up (id, parent_id) AS (
             SELECT id, parent_id FROM storage_items WHERE id = ?1
             UNION ALL
             SELECT i.id, i.parent_id FROM storage_items i JOIN up ON i.id = up.parent_id
         )
         SELECT DISTINCT s.user_id FROM up JOIN storage_shares s ON s.item_id = up.id",
    )?;
    for user in shared.query_map([id.as_uuid()], |row| Ok(UserId::from_uuid(row.get(0)?)))? {
        let user = user?;
        if user != owner {
            people.push(user);
        }
    }
    Ok(people)
}

/// Who `id` itself is shared with, not counting folders above it.
fn shared_directly(conn: &Connection, id: ItemId) -> Result<HashSet<UserId>, Error> {
    let people = conn
        .prepare_cached("SELECT user_id FROM storage_shares WHERE item_id = ?1")?
        .query_map([id.as_uuid()], |row| Ok(UserId::from_uuid(row.get(0)?)))?
        .collect::<Result<_, _>>()?;
    Ok(people)
}

struct State {
    generation: Vec<u8>,
    last_seq: i64,
    pruned_through: i64,
}

fn state(conn: &Connection) -> Result<State, Error> {
    Ok(conn.query_row(
        "SELECT generation, last_seq, pruned_through FROM storage_sync WHERE id = 1",
        [],
        |row| {
            Ok(State {
                generation: row.get(0)?,
                last_seq: row.get(1)?,
                pruned_through: row.get(2)?,
            })
        },
    )?)
}

/// The cursor for everything up to now.
fn cursor(conn: &Connection) -> Result<String, Error> {
    let sync = state(conn)?;
    Ok(format_cursor(&sync.generation, sync.last_seq))
}

/// A cursor is the list's generation and a position in it, such as
/// `3f9a…c2.1042`. Devices treat it as opaque.
fn format_cursor(generation: &[u8], seq: i64) -> String {
    let hex: String = generation
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    format!("{hex}.{seq}")
}

fn parse_cursor(cursor: &str) -> Option<(Vec<u8>, i64)> {
    let (hex, seq) = cursor.split_once('.')?;
    if hex.len() % 2 != 0 || !hex.is_ascii() {
        return None;
    }
    let generation = (0..hex.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&hex[at..at + 2], 16).ok())
        .collect::<Option<Vec<u8>>>()?;
    Some((generation, seq.parse().ok()?))
}

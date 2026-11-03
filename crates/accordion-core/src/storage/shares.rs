//! Sharing: owners give other people access to a file or folder, and core
//! answers what each person may do with each item (decision 16).
//!
//! A share on a folder covers everything inside it, including what's added
//! later. Someone's access to an item is the highest level any share gives
//! them, on the item or a folder above it. Shared items still belong to the
//! owner of their tree, so what an editor adds counts toward the owner's
//! quota, and what they delete goes to the owner's trash.
//!
//! Only the owner sees an item in the trash. To everyone else, it's gone.

use accordion_types::{ItemId, UserId};
use std::collections::HashMap;

use rusqlite::{Connection, OptionalExtension, Row};

use super::{
    Error, FOUND_SELECT, Found, Item, SHARED_SELECT, Storage, found_from_row, item, item_from_row,
    location,
};
use crate::events::{self, Action, Actor, NewEvent};
use crate::sql_now;

/// What a share lets someone do.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ShareLevel {
    /// Open, download, and copy to their own Drive.
    Viewer,
    /// Also add, rename, replace, and move items inside the shared folder,
    /// restore versions, and move items to the trash.
    Editor,
}

impl ShareLevel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Viewer => "viewer",
            Self::Editor => "editor",
        }
    }

    fn from_column(row: &Row<'_>, index: usize) -> rusqlite::Result<Self> {
        match row.get_ref(index)?.as_str()? {
            "viewer" => Ok(Self::Viewer),
            "editor" => Ok(Self::Editor),
            other => Err(rusqlite::Error::FromSqlConversionFailure(
                index,
                rusqlite::types::Type::Text,
                format!("unknown share level {other:?}").into(),
            )),
        }
    }
}

/// What someone may do with an item, from least to most.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Access {
    Viewer,
    Editor,
    /// Everything, including sharing it, and the trash.
    Owner,
}

impl From<ShareLevel> for Access {
    fn from(level: ShareLevel) -> Self {
        match level {
            ShareLevel::Viewer => Self::Viewer,
            ShareLevel::Editor => Self::Editor,
        }
    }
}

/// One person's access to one item.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Share {
    pub item: ItemId,
    pub user: UserId,
    pub level: ShareLevel,
    /// Who shared it. The account may since have been deleted.
    pub shared_by: UserId,
    pub created_at: String,
}

/// An item shared with someone, with the share.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Shared {
    pub item: Item,
    pub share: Share,
}

/// One of an owner's items that they shared, with where it is and who has
/// it, for **Shared by me**.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SharedOut {
    pub found: Found,
    /// Everyone it's shared with directly, in the order they were added.
    pub shares: Vec<Share>,
}

/// A record of sharing, changing, or removing someone's access, for the
/// audit log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShareEvent {
    pub at: String,
    pub item: ItemId,
    pub user: UserId,
    /// `None` when their access was removed.
    pub level: Option<ShareLevel>,
    pub by: UserId,
}

impl Storage {
    /// The handle's actor, as `by`, who the caller says is sharing.
    fn sharer(&self, by: UserId) -> Actor {
        Actor {
            user: Some(by),
            ..self.actor
        }
    }

    /// What `user` may do with an item, or `None` if they can't see it at
    /// all. Treat `None` as "not found", so nobody learns what exists.
    pub async fn access(&self, user: UserId, id: ItemId) -> Result<Option<Access>, Error> {
        self.inner.db.read(move |conn| access(conn, user, id)).await
    }

    /// Gives `with` access to an item and everything inside it, or changes
    /// the access they have. The caller checks that `by` may share it.
    pub async fn share(
        &self,
        id: ItemId,
        with: UserId,
        level: ShareLevel,
        by: UserId,
    ) -> Result<Share, Error> {
        self.inner
            .identity
            .user(with)
            .await?
            .ok_or(Error::UserNotFound)?;
        let actor = self.sharer(by);
        self.inner
            .db
            .write(move |tx| {
                let item = item(tx, id)?.ok_or(Error::NotFound)?;
                if item.parent.is_none() {
                    return Err(Error::ShareTopFolder);
                }
                if !location(tx, id)?.visible {
                    return Err(Error::InTrash);
                }
                if item.owner == with {
                    return Err(Error::ShareWithOwner);
                }
                let before = share(tx, id, with)?;
                if before.as_ref().is_some_and(|share| share.level == level) {
                    return Ok(before.expect("checked above"));
                }
                tx.execute(
                    concat!(
                        "INSERT INTO storage_shares (item_id, user_id, level, shared_by, created_at)
                         VALUES (?1, ?2, ?3, ?4, ",
                        sql_now!(),
                        ") ON CONFLICT (item_id, user_id) DO UPDATE SET level = excluded.level"
                    ),
                    (id.as_uuid(), with.as_uuid(), level.as_str(), by.as_uuid()),
                )?;
                let action = if before.is_some() {
                    Action::ShareChanged
                } else {
                    Action::Shared
                };
                record(tx, actor, &item, with, action, Some(level))?;
                share(tx, id, with)?.ok_or(Error::NotFound)
            })
            .await
    }

    /// Takes away the access a share on this item gave `with`. They may
    /// still reach it through a share on a folder above it.
    pub async fn unshare(&self, id: ItemId, with: UserId, by: UserId) -> Result<(), Error> {
        let actor = self.sharer(by);
        self.inner
            .db
            .write(move |tx| {
                let removed = tx.execute(
                    "DELETE FROM storage_shares WHERE item_id = ?1 AND user_id = ?2",
                    (id.as_uuid(), with.as_uuid()),
                )?;
                if removed > 0
                    && let Some(item) = item(tx, id)?
                {
                    record(tx, actor, &item, with, Action::Unshared, None)?;
                }
                Ok(())
            })
            .await
    }

    /// Stops sharing an item with everyone it's shared with directly, and
    /// returns the shares it removed.
    pub async fn unshare_all(&self, id: ItemId, by: UserId) -> Result<Vec<Share>, Error> {
        let actor = self.sharer(by);
        self.inner
            .db
            .write(move |tx| {
                let item = item(tx, id)?.ok_or(Error::NotFound)?;
                let removed = tx
                    .prepare(
                        "DELETE FROM storage_shares WHERE item_id = ?1
                         RETURNING item_id, user_id, level, shared_by, created_at",
                    )?
                    .query_map([id.as_uuid()], |row| share_from_row(row, 0))?
                    .collect::<Result<Vec<_>, _>>()?;
                for share in &removed {
                    record(tx, actor, &item, share.user, Action::Unshared, None)?;
                }
                Ok(removed)
            })
            .await
    }

    /// Everyone an item is shared with directly, in the order they were
    /// added. Shares on the folders above it aren't included.
    pub async fn shares(&self, id: ItemId) -> Result<Vec<Share>, Error> {
        self.inner
            .db
            .read(move |conn| {
                let shares = conn
                    .prepare(&format!(
                        "SELECT {SHARE_COLUMNS} FROM storage_shares s
                         WHERE s.item_id = ?1 ORDER BY s.created_at, s.user_id"
                    ))?
                    .query_map([id.as_uuid()], |row| share_from_row(row, 0))?
                    .collect::<Result<_, _>>()?;
                Ok(shares)
            })
            .await
    }

    /// What others shared with `user` directly, outside the trash, most
    /// recently shared first.
    pub async fn shared_with(&self, user: UserId) -> Result<Vec<Shared>, Error> {
        self.inner
            .db
            .read(move |conn| {
                let mut statement = conn.prepare(&format!(
                    "{SHARED_SELECT} WHERE s.user_id = ?1 ORDER BY s.created_at DESC, i.name"
                ))?;
                let rows = statement.query_map([user.as_uuid()], |row| {
                    Ok(Shared {
                        item: item_from_row(row)?,
                        share: share_from_row(row, 11)?,
                    })
                })?;
                let mut shared = Vec::new();
                for row in rows {
                    let row = row?;
                    if location(conn, row.item.id)?.visible {
                        shared.push(row);
                    }
                }
                Ok(shared)
            })
            .await
    }

    /// The items `owner` shared directly with anyone, outside the trash,
    /// most recently shared first. What's inside a shared folder isn't
    /// listed unless it's shared on its own too.
    pub async fn shared_by(&self, owner: UserId) -> Result<Vec<SharedOut>, Error> {
        self.inner
            .db
            .read(move |conn| {
                let Some(top) = conn
                    .query_row(
                        "SELECT id FROM storage_items WHERE owner_id = ?1 AND parent_id IS NULL",
                        [owner.as_uuid()],
                        |row| row.get::<_, uuid::Uuid>(0),
                    )
                    .optional()?
                else {
                    return Ok(Vec::new());
                };
                let found = conn
                    .prepare(&format!(
                        "{FOUND_SELECT} WHERE i.id IN (SELECT item_id FROM storage_shares)"
                    ))?
                    .query_map([top], found_from_row)?
                    .collect::<Result<Vec<_>, _>>()?;
                let mut shares: HashMap<ItemId, Vec<Share>> = HashMap::new();
                let mut statement = conn.prepare(&format!(
                    "SELECT {SHARE_COLUMNS} FROM storage_shares s
                     JOIN storage_items i ON i.id = s.item_id
                     WHERE i.owner_id = ?1 ORDER BY s.created_at, s.user_id"
                ))?;
                for share in statement.query_map([owner.as_uuid()], |row| share_from_row(row, 0))? {
                    let share = share?;
                    shares.entry(share.item).or_default().push(share);
                }
                let mut out: Vec<SharedOut> = found
                    .into_iter()
                    .map(|found| SharedOut {
                        shares: shares.remove(&found.item.id).unwrap_or_default(),
                        found,
                    })
                    .collect();
                out.sort_by(|a, b| {
                    let latest = |out: &SharedOut| {
                        out.shares
                            .iter()
                            .map(|share| share.created_at.clone())
                            .max()
                    };
                    latest(b)
                        .cmp(&latest(a))
                        .then_with(|| a.found.item.name.cmp(&b.found.item.name))
                });
                Ok(out)
            })
            .await
    }

    /// Which of `owner`'s items they shared with anyone, such as for an
    /// icon on each.
    pub async fn shared_by_owner(&self, owner: UserId) -> Result<Vec<ItemId>, Error> {
        self.inner
            .db
            .read(move |conn| {
                let ids = conn
                    .prepare(
                        "SELECT DISTINCT s.item_id FROM storage_shares s
                         JOIN storage_items i ON i.id = s.item_id WHERE i.owner_id = ?1",
                    )?
                    .query_map([owner.as_uuid()], |row| Ok(ItemId::from_uuid(row.get(0)?)))?
                    .collect::<Result<_, _>>()?;
                Ok(ids)
            })
            .await
    }

    /// Every change to who could reach an item, oldest first.
    pub async fn share_history(&self, id: ItemId) -> Result<Vec<ShareEvent>, Error> {
        self.inner
            .db
            .read(move |conn| {
                let events = conn
                    .prepare(
                        "SELECT at, item_id, other_id, details ->> 'level', actor_id
                         FROM events_log
                         WHERE item_id = ?1 AND action IN ('shared', 'share_changed', 'unshared')
                         ORDER BY id",
                    )?
                    .query_map([id.as_uuid()], |row| {
                        Ok(ShareEvent {
                            at: row.get(0)?,
                            item: ItemId::from_uuid(row.get(1)?),
                            user: UserId::from_uuid(row.get(2)?),
                            level: match row.get_ref(3)?.as_str_or_null()? {
                                Some(_) => Some(ShareLevel::from_column(row, 3)?),
                                None => None,
                            },
                            by: UserId::from_uuid(row.get(4)?),
                        })
                    })?
                    .collect::<Result<_, _>>()?;
                Ok(events)
            })
            .await
    }
}

const SHARE_COLUMNS: &str = "s.item_id, s.user_id, s.level, s.shared_by, s.created_at";

fn share_from_row(row: &Row<'_>, start: usize) -> rusqlite::Result<Share> {
    Ok(Share {
        item: ItemId::from_uuid(row.get(start)?),
        user: UserId::from_uuid(row.get(start + 1)?),
        level: ShareLevel::from_column(row, start + 2)?,
        shared_by: UserId::from_uuid(row.get(start + 3)?),
        created_at: row.get(start + 4)?,
    })
}

fn share(conn: &Connection, id: ItemId, with: UserId) -> Result<Option<Share>, Error> {
    Ok(conn
        .query_row(
            &format!(
                "SELECT {SHARE_COLUMNS} FROM storage_shares s
                 WHERE s.item_id = ?1 AND s.user_id = ?2"
            ),
            (id.as_uuid(), with.as_uuid()),
            |row| share_from_row(row, 0),
        )
        .optional()?)
}

/// Records sharing `item` with `with`, changing their access to `level`,
/// or stopping.
fn record(
    conn: &Connection,
    actor: Actor,
    item: &Item,
    with: UserId,
    action: Action,
    level: Option<ShareLevel>,
) -> Result<(), Error> {
    let mut event = NewEvent::about(action, item.id, item.owner).other(with);
    if let Some(level) = level {
        event = event.details(serde_json::json!({ "level": level.as_str() }));
    }
    Ok(events::record(conn, actor, &event)?)
}

pub(crate) fn access(conn: &Connection, user: UserId, id: ItemId) -> Result<Option<Access>, Error> {
    let Some(item) = item(conn, id)? else {
        return Ok(None);
    };
    if item.owner == user {
        return Ok(Some(Access::Owner));
    }
    if !location(conn, id)?.visible {
        return Ok(None);
    }
    let mut levels = conn.prepare(
        "WITH RECURSIVE up (id, parent_id) AS (
             SELECT id, parent_id FROM storage_items WHERE id = ?1
             UNION ALL
             SELECT i.id, i.parent_id FROM storage_items i JOIN up ON i.id = up.parent_id
         )
         SELECT s.level FROM up JOIN storage_shares s ON s.item_id = up.id AND s.user_id = ?2",
    )?;
    let levels = levels
        .query_map((id.as_uuid(), user.as_uuid()), |row| {
            ShareLevel::from_column(row, 0)
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(levels.into_iter().max().map(Access::from))
}

/// Everyone who can open `id`: its owner, and while it's outside the
/// trash, whoever it or a folder above it is shared with.
pub(crate) fn people_with_access(conn: &Connection, id: ItemId) -> Result<Vec<UserId>, Error> {
    let Some(item) = item(conn, id)? else {
        return Ok(Vec::new());
    };
    let mut people = vec![item.owner];
    if location(conn, id)?.visible {
        let mut shared = conn.prepare(
            "WITH RECURSIVE up (id, parent_id) AS (
                 SELECT id, parent_id FROM storage_items WHERE id = ?1
                 UNION ALL
                 SELECT i.id, i.parent_id FROM storage_items i JOIN up ON i.id = up.parent_id
             )
             SELECT DISTINCT s.user_id FROM up JOIN storage_shares s ON s.item_id = up.id",
        )?;
        for user in shared.query_map([id.as_uuid()], |row| Ok(UserId::from_uuid(row.get(0)?)))? {
            let user = user?;
            if user != item.owner {
                people.push(user);
            }
        }
    }
    Ok(people)
}

/// Where everything `user` can reach starts: their top folder, if they have
/// one, and each item shared with them that's outside the trash.
pub(super) fn roots(conn: &Connection, user: UserId) -> Result<Vec<ItemId>, Error> {
    let mut roots: Vec<ItemId> = conn
        .prepare("SELECT id FROM storage_items WHERE owner_id = ?1 AND parent_id IS NULL")?
        .query_map([user.as_uuid()], |row| Ok(ItemId::from_uuid(row.get(0)?)))?
        .collect::<Result<_, _>>()?;
    let shared: Vec<ItemId> = conn
        .prepare("SELECT item_id FROM storage_shares WHERE user_id = ?1")?
        .query_map([user.as_uuid()], |row| Ok(ItemId::from_uuid(row.get(0)?)))?
        .collect::<Result<_, _>>()?;
    for id in shared {
        if location(conn, id)?.visible {
            roots.push(id);
        }
    }
    Ok(roots)
}

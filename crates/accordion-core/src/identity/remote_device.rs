//! Remote access devices: the phones and laptops each person lets reach the
//! server through its WireGuard tunnel (decision 21 in the `accordion`
//! repo's `docs/decisions`).
//!
//! Each device has its own key pair. Only the public key is stored: the
//! server makes the private key for the device's setup, shows it once, and
//! forgets it. Each device also gets one address in the tunnel's /24, kept
//! here as the last part of the address, so two devices never share one.
//!
//! The tunnel follows [`Identity::remote_devices_changed`], so removing a
//! device, or deleting or turning off its account, cuts it off at once.

use std::time::{SystemTime, UNIX_EPOCH};

use accordion_types::{RemoteDeviceId, UserId};
use rusqlite::{OptionalExtension, Row, Transaction};

use super::session::clean_device_name;
use super::{Error, Identity, User, exists, record, user_from_row};
use crate::events::{Action, NewEvent};
use crate::{sql_now, time};

/// The most devices each person can have.
pub const MAX_PER_PERSON: usize = 10;

/// The last part of each device's address in the tunnel's /24. The server
/// is `.1`.
const FIRST_HOST: u8 = 2;
const LAST_HOST: u8 = 254;

macro_rules! device_columns {
    () => {
        "d.id, d.user_id, d.name, d.public_key, d.host, d.home_address, d.port, d.created_at, \
         d.last_connected_at"
    };
}

/// One device, as the device lists show it.
///
/// Times are ISO 8601 in UTC.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteDevice {
    pub id: RemoteDeviceId,
    pub user_id: UserId,
    pub name: String,
    /// Its WireGuard public key.
    pub public_key: [u8; 32],
    /// The last part of its address in the tunnel's /24.
    pub host: u8,
    /// The server's home address that its setup sends through the tunnel.
    /// If the server's address changes, the device needs a new setup.
    pub home_address: String,
    /// The UDP port its setup connects to.
    pub port: u16,
    pub created_at: String,
    /// When it last connected from away, if it has.
    pub last_connected_at: Option<String>,
}

/// What a new device's setup says.
pub struct NewRemoteDevice<'a> {
    pub name: &'a str,
    pub public_key: [u8; 32],
    pub home_address: &'a str,
    pub port: u16,
}

impl Identity {
    /// Adds a device for `user_id`, with the lowest free address.
    ///
    /// Adding one is a sensitive action, so only allow it when the user has
    /// signed in recently, as for app passwords.
    pub async fn add_remote_device(
        &self,
        user_id: UserId,
        new: NewRemoteDevice<'_>,
    ) -> Result<RemoteDevice, Error> {
        let id = RemoteDeviceId::generate();
        let name = clean_device_name(new.name);
        let public_key = new.public_key.to_vec();
        let home_address = new.home_address.to_owned();
        let port = new.port;
        let actor = self.actor_or(user_id);
        let added = self
            .db
            .write(move |tx| {
                if !exists(tx, "id = ?1", [user_id.as_uuid()])? {
                    return Err(Error::UserNotFound);
                }
                let count: i64 = tx.query_row(
                    "SELECT count(*) FROM identity_remote_devices WHERE user_id = ?1",
                    [user_id.as_uuid()],
                    |row| row.get(0),
                )?;
                if usize::try_from(count).unwrap_or(usize::MAX) >= MAX_PER_PERSON {
                    return Err(Error::TooManyRemoteDevices);
                }
                let host = free_host(tx)?.ok_or(Error::NoRemoteAddressLeft)?;
                tx.execute(
                    concat!(
                        "INSERT INTO identity_remote_devices (id, user_id, name, public_key, \
                         host, home_address, port, created_at)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ",
                        sql_now!(),
                        ")"
                    ),
                    (
                        id.as_uuid(),
                        user_id.as_uuid(),
                        &name,
                        public_key,
                        host,
                        home_address,
                        port,
                    ),
                )?;
                record(
                    tx,
                    actor,
                    NewEvent::new(Action::RemoteDeviceAdded)
                        .other(user_id)
                        .details(serde_json::json!({ "device": name })),
                )?;
                Ok(tx.query_row(
                    concat!(
                        "SELECT ",
                        device_columns!(),
                        " FROM identity_remote_devices d WHERE d.id = ?1"
                    ),
                    [id.as_uuid()],
                    device_from_row,
                )?)
            })
            .await?;
        self.remote_devices_changed
            .send_modify(|version| *version += 1);
        Ok(added)
    }

    /// The user's devices, newest first.
    pub async fn remote_devices(&self, user_id: UserId) -> Result<Vec<RemoteDevice>, Error> {
        self.db
            .read(move |conn| {
                conn.prepare(concat!(
                    "SELECT ",
                    device_columns!(),
                    " FROM identity_remote_devices d WHERE d.user_id = ?1
                     ORDER BY d.created_at DESC, d.id DESC"
                ))?
                .query_map([user_id.as_uuid()], device_from_row)?
                .collect()
            })
            .await
            .map_err(Error::from)
    }

    /// Everyone's devices with their owners, for admins, by owner and then
    /// newest first.
    pub async fn all_remote_devices(&self) -> Result<Vec<(RemoteDevice, User)>, Error> {
        self.db
            .read(move |conn| {
                conn.prepare(concat!(
                    "SELECT ",
                    device_columns!(),
                    ", u.id, u.username, u.display_name, u.role
                     FROM identity_remote_devices d JOIN identity_users u ON u.id = d.user_id
                     ORDER BY u.display_name, d.created_at DESC, d.id DESC"
                ))?
                .query_map([], |row| {
                    Ok((device_from_row(row)?, user_from_row(row, 9)?))
                })?
                .collect()
            })
            .await
            .map_err(Error::from)
    }

    /// The devices that may connect: those of accounts that aren't turned
    /// off.
    pub async fn connectable_remote_devices(&self) -> Result<Vec<RemoteDevice>, Error> {
        self.db
            .read(move |conn| {
                conn.prepare(concat!(
                    "SELECT ",
                    device_columns!(),
                    " FROM identity_remote_devices d JOIN identity_users u ON u.id = d.user_id
                     WHERE u.turned_off_at IS NULL"
                ))?
                .query_map([], device_from_row)?
                .collect()
            })
            .await
            .map_err(Error::from)
    }

    /// Removes one of the user's devices, which cuts it off at once.
    /// Returns `false` if the user has no such device.
    ///
    /// Checking that the caller may do this is up to the caller: users can
    /// remove their own devices, and admins can remove anyone's.
    pub async fn remove_remote_device(
        &self,
        user_id: UserId,
        id: RemoteDeviceId,
    ) -> Result<bool, Error> {
        let actor = self.actor_or(user_id);
        let removed = self
            .db
            .write(move |tx| {
                let name: Option<String> = tx
                    .query_row(
                        "DELETE FROM identity_remote_devices WHERE user_id = ?1 AND id = ?2
                         RETURNING name",
                        (user_id.as_uuid(), id.as_uuid()),
                        |row| row.get(0),
                    )
                    .optional()?;
                let Some(name) = name else {
                    return Ok::<_, Error>(false);
                };
                record(
                    tx,
                    actor,
                    NewEvent::new(Action::RemoteDeviceRemoved)
                        .other(user_id)
                        .details(serde_json::json!({ "device": name })),
                )?;
                Ok(true)
            })
            .await?;
        if removed {
            self.remote_devices_changed
                .send_modify(|version| *version += 1);
        }
        Ok(removed)
    }

    /// Notes when devices last connected from away, given their public
    /// keys. Older times than the ones kept change nothing.
    pub async fn record_remote_connections(
        &self,
        connected: Vec<([u8; 32], SystemTime)>,
    ) -> Result<(), Error> {
        if connected.is_empty() {
            return Ok(());
        }
        let connected: Vec<([u8; 32], String)> = connected
            .into_iter()
            .map(|(key, at)| {
                (
                    key,
                    time::format(at.duration_since(UNIX_EPOCH).unwrap_or_default()),
                )
            })
            .collect();
        self.db
            .write(move |tx| {
                let mut update = tx.prepare(
                    "UPDATE identity_remote_devices SET last_connected_at = ?2
                     WHERE public_key = ?1
                       AND (last_connected_at IS NULL OR last_connected_at < ?2)",
                )?;
                for (key, at) in &connected {
                    update.execute((key.as_slice(), at))?;
                }
                Ok(())
            })
            .await
    }

    /// Changes whenever the devices that may connect change: one is added or
    /// removed, or an account is deleted, turned off, or turned back on.
    pub fn remote_devices_changed(&self) -> tokio::sync::watch::Receiver<u64> {
        self.remote_devices_changed.subscribe()
    }
}

/// The lowest address in the tunnel that no device has.
fn free_host(tx: &Transaction<'_>) -> rusqlite::Result<Option<u8>> {
    let mut taken = tx.prepare("SELECT host FROM identity_remote_devices ORDER BY host")?;
    let taken: Vec<u8> = taken
        .query_map([], |row| row.get(0))?
        .collect::<Result<_, _>>()?;
    Ok((FIRST_HOST..=LAST_HOST).find(|host| taken.binary_search(host).is_err()))
}

fn device_from_row(row: &Row<'_>) -> rusqlite::Result<RemoteDevice> {
    let key: Vec<u8> = row.get(3)?;
    Ok(RemoteDevice {
        id: RemoteDeviceId::from_uuid(row.get(0)?),
        user_id: UserId::from_uuid(row.get(1)?),
        name: row.get(2)?,
        public_key: key.try_into().map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                3,
                rusqlite::types::Type::Blob,
                "a public key isn't 32 bytes".into(),
            )
        })?,
        host: row.get(4)?,
        home_address: row.get(5)?,
        port: row.get(6)?,
        created_at: row.get(7)?,
        last_connected_at: row.get(8)?,
    })
}

#[cfg(test)]
mod tests {
    use accordion_types::Role;

    use super::*;
    use crate::db::Database;
    use crate::identity::NewUser;

    async fn start() -> (tempfile::TempDir, Identity, User, User) {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir.path().join("accordion.db")).unwrap();
        let identity = Identity::new(db).await.unwrap();
        let mut users = Vec::new();
        for (username, role) in [("alex", Role::Owner), ("sam", Role::User)] {
            users.push(
                identity
                    .create_user(NewUser {
                        username: username.into(),
                        display_name: username.into(),
                        role,
                        password: "correct horse battery".into(),
                    })
                    .await
                    .unwrap(),
            );
        }
        let sam = users.pop().unwrap();
        let alex = users.pop().unwrap();
        (dir, identity, alex, sam)
    }

    fn device(name: &str, key: u8) -> NewRemoteDevice<'_> {
        NewRemoteDevice {
            name,
            public_key: [key; 32],
            home_address: "192.168.1.20",
            port: 51820,
        }
    }

    #[tokio::test]
    async fn gives_each_device_its_own_address() {
        let (_dir, identity, alex, sam) = start().await;
        let mut changed = identity.remote_devices_changed();

        let phone = identity
            .add_remote_device(alex.id, device("Alex's phone", 1))
            .await
            .unwrap();
        let laptop = identity
            .add_remote_device(sam.id, device("Sam's laptop", 2))
            .await
            .unwrap();
        assert_eq!((phone.host, laptop.host), (2, 3));
        assert!(changed.has_changed().unwrap());
        changed.mark_unchanged();

        // A freed address is used again.
        assert!(
            identity
                .remove_remote_device(alex.id, phone.id)
                .await
                .unwrap()
        );
        assert!(changed.has_changed().unwrap());
        // Only the owner's ID removes it.
        assert!(
            !identity
                .remove_remote_device(alex.id, laptop.id)
                .await
                .unwrap()
        );
        let tablet = identity
            .add_remote_device(alex.id, device("Alex's tablet", 3))
            .await
            .unwrap();
        assert_eq!(tablet.host, 2);

        // The same key can't be added twice.
        assert!(
            identity
                .add_remote_device(sam.id, device("Again", 2))
                .await
                .is_err()
        );
        assert_eq!(identity.remote_devices(sam.id).await.unwrap(), [laptop]);
        assert_eq!(identity.all_remote_devices().await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn limits_each_person_to_ten() {
        let (_dir, identity, alex, _sam) = start().await;
        for key in 0..10 {
            identity
                .add_remote_device(alex.id, device("Phone", key))
                .await
                .unwrap();
        }
        assert!(matches!(
            identity
                .add_remote_device(alex.id, device("One more", 10))
                .await,
            Err(Error::TooManyRemoteDevices)
        ));
    }

    #[tokio::test]
    async fn cuts_off_accounts_that_are_turned_off_or_deleted() {
        let (_dir, identity, alex, sam) = start().await;
        identity
            .add_remote_device(alex.id, device("Alex's phone", 1))
            .await
            .unwrap();
        identity
            .add_remote_device(sam.id, device("Sam's phone", 2))
            .await
            .unwrap();
        let mut changed = identity.remote_devices_changed();
        changed.mark_unchanged();

        identity.turn_off_member(sam.id).await.unwrap();
        assert!(changed.has_changed().unwrap());
        let connectable = identity.connectable_remote_devices().await.unwrap();
        assert_eq!(connectable.len(), 1);
        assert_eq!(connectable[0].user_id, alex.id);

        identity.turn_on_member(sam.id).await.unwrap();
        assert_eq!(
            identity.connectable_remote_devices().await.unwrap().len(),
            2
        );

        changed.mark_unchanged();
        identity.delete_member(sam.id).await.unwrap();
        assert!(changed.has_changed().unwrap());
        assert_eq!(identity.all_remote_devices().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn keeps_the_latest_connection_time() {
        let (_dir, identity, alex, _sam) = start().await;
        identity
            .add_remote_device(alex.id, device("Alex's phone", 1))
            .await
            .unwrap();
        let at = |time: &str| vec![([1; 32], crate::time::parse(time).unwrap())];
        identity
            .record_remote_connections(at("2026-11-03T10:00:00.000Z"))
            .await
            .unwrap();
        identity
            .record_remote_connections(at("2026-11-03T09:00:00.000Z"))
            .await
            .unwrap();
        let devices = identity.remote_devices(alex.id).await.unwrap();
        assert_eq!(
            devices[0].last_connected_at.as_deref(),
            Some("2026-11-03T10:00:00.000Z")
        );
    }
}

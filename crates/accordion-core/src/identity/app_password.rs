//! App passwords, for standard clients such as WebDAV mounts and mail apps
//! (decision 3 in the `accordion` repo's `docs/decisions`).
//!
//! These protocols send a password with every request and can't ask for a
//! passkey or a code, so they never get the account password, which would
//! get around two-step sign-in. Each device gets its own random app password
//! instead. It works for one kind of protocol and can be deleted on its own.
//! The database stores a SHA-256 hash: safe because the password is random,
//! and cheap enough that a burst of WebDAV requests doesn't run Argon2 on
//! each one.

use std::fmt;

use accordion_types::{AppPasswordId, UserId};
use rusqlite::{OptionalExtension, Row};
use sha2::{Digest, Sha256};

use super::session::{bad_value, clean_device_name, last_used_precision};
use super::{Error, Identity, User, normalize_username, user_from_row};
use crate::{random, sql_now};

/// 16 lowercase letters is about 75 bits, far more than anyone can guess
/// through a server, and they can be typed on a phone without symbols.
const LEN: usize = 16;

macro_rules! app_password_columns {
    () => {
        "a.id, a.user_id, a.use_for, a.device_name, a.created_at, a.last_used_at"
    };
}

/// What an app password works for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AppPasswordUse {
    /// IMAP and SMTP.
    Mail,
    /// WebDAV, CalDAV, and CardDAV: files, calendars, and contacts.
    Dav,
}

impl AppPasswordUse {
    fn as_str(self) -> &'static str {
        match self {
            Self::Mail => "mail",
            Self::Dav => "dav",
        }
    }
}

/// A new app password. Show it to the user once, when it's created. Only its
/// hash is stored, so it can't be shown again.
pub struct AppPasswordSecret(String);

impl AppPasswordSecret {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The password in groups of four, such as `abcd efgh ijkl mnop`, which
    /// reads better off a screen. Clients can send it with or without the
    /// spaces.
    pub fn grouped(&self) -> String {
        random::grouped(&self.0, 4)
    }
}

impl fmt::Debug for AppPasswordSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AppPasswordSecret(..)")
    }
}

/// One app password, as the device list shows it.
///
/// Times are ISO 8601 in UTC.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppPassword {
    pub id: AppPasswordId,
    pub user_id: UserId,
    pub use_for: AppPasswordUse,
    pub device_name: String,
    pub created_at: String,
    /// `None` until the device first signs in with it. Updated at most once
    /// an hour.
    pub last_used_at: Option<String>,
}

/// The result of a successful app password check.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UsingAppPassword {
    pub user: User,
    pub app_password: AppPassword,
}

impl Identity {
    /// Creates an app password for one device.
    ///
    /// Creating one is a sensitive action, so only allow it when the user
    /// has signed in recently ([`SignedIn::recently_authenticated`]).
    ///
    /// [`SignedIn::recently_authenticated`]: super::SignedIn::recently_authenticated
    pub async fn create_app_password(
        &self,
        user_id: UserId,
        use_for: AppPasswordUse,
        device_name: &str,
    ) -> Result<(AppPasswordSecret, AppPassword), Error> {
        let (secret, hash) = new_secret();
        let id = AppPasswordId::generate();
        let device_name = clean_device_name(device_name);
        let created = self
            .db
            .write(move |tx| {
                let user_exists: bool = tx.query_row(
                    "SELECT EXISTS (SELECT 1 FROM identity_users WHERE id = ?1)",
                    [user_id.as_uuid()],
                    |row| row.get(0),
                )?;
                if !user_exists {
                    return Err(Error::UserNotFound);
                }
                tx.execute(
                    concat!(
                        "INSERT INTO identity_app_passwords (id, user_id, password_hash, \
                         use_for, device_name, created_at)
                         VALUES (?1, ?2, ?3, ?4, ?5, ",
                        sql_now!(),
                        ")"
                    ),
                    (
                        id.as_uuid(),
                        user_id.as_uuid(),
                        hash,
                        use_for.as_str(),
                        device_name,
                    ),
                )?;
                let created = tx.query_row(
                    concat!(
                        "SELECT ",
                        app_password_columns!(),
                        " FROM identity_app_passwords a WHERE a.id = ?1"
                    ),
                    [id.as_uuid()],
                    app_password_from_row,
                )?;
                Ok(created)
            })
            .await?;
        Ok((secret, created))
    }

    /// Checks a username and app password from a standard client. Returns
    /// `None` if they don't match, or if the app password is for the other
    /// kind of protocol. The account password never matches, even when it's
    /// right.
    ///
    /// Wrong tries aren't limited the way account passwords are
    /// ([`limits`](super::limits)), because nobody can guess 75 random bits
    /// one request at a time.
    pub async fn check_app_password(
        &self,
        username: &str,
        password: &str,
        use_for: AppPasswordUse,
    ) -> Result<Option<UsingAppPassword>, Error> {
        let (Ok(username), Some(hash)) = (normalize_username(username), parse_secret(password))
        else {
            return Ok(None);
        };
        let found = self
            .db
            .read(move |conn| {
                conn.query_row(
                    concat!(
                        "SELECT ",
                        app_password_columns!(),
                        ", u.id, u.username, u.display_name, u.role, \
                         a.last_used_at IS NULL OR a.last_used_at < ",
                        last_used_precision!(),
                        " FROM identity_app_passwords a JOIN identity_users u ON u.id = a.user_id
                         WHERE a.password_hash = ?1 AND u.username = ?2 AND a.use_for = ?3"
                    ),
                    (hash, username, use_for.as_str()),
                    |row| {
                        let using = UsingAppPassword {
                            app_password: app_password_from_row(row)?,
                            user: user_from_row(row, 6)?,
                        };
                        let stale: bool = row.get(10)?;
                        Ok((using, stale))
                    },
                )
                .optional()
            })
            .await?;

        let Some((using, stale)) = found else {
            return Ok(None);
        };
        if stale {
            let id = using.app_password.id;
            self.db
                .write(move |tx| {
                    tx.execute(
                        concat!(
                            "UPDATE identity_app_passwords SET last_used_at = ",
                            sql_now!(),
                            " WHERE id = ?1"
                        ),
                        [id.as_uuid()],
                    )
                })
                .await?;
        }
        Ok(Some(using))
    }

    /// The user's app passwords, newest first.
    pub async fn app_passwords(&self, user_id: UserId) -> Result<Vec<AppPassword>, Error> {
        self.db
            .read(move |conn| {
                conn.prepare(concat!(
                    "SELECT ",
                    app_password_columns!(),
                    " FROM identity_app_passwords a WHERE a.user_id = ?1
                     ORDER BY a.created_at DESC, a.id DESC"
                ))?
                .query_map([user_id.as_uuid()], app_password_from_row)?
                .collect()
            })
            .await
            .map_err(Error::from)
    }

    /// Deletes one of the user's app passwords, so its device can't sign in
    /// with it again. Returns `false` if the user has no such app password.
    ///
    /// Checking that the caller may do this is up to the caller: users can
    /// delete their own app passwords, and admins can delete anyone's.
    pub async fn delete_app_password(
        &self,
        user_id: UserId,
        id: AppPasswordId,
    ) -> Result<bool, Error> {
        let deleted = self
            .db
            .write(move |tx| {
                tx.execute(
                    "DELETE FROM identity_app_passwords WHERE user_id = ?1 AND id = ?2",
                    (user_id.as_uuid(), id.as_uuid()),
                )
            })
            .await?;
        Ok(deleted > 0)
    }
}

fn new_secret() -> (AppPasswordSecret, Vec<u8>) {
    let letters = random::letters(LEN);
    let hash = hash_secret(&letters);
    (AppPasswordSecret(letters), hash)
}

/// Returns the hash to look up, or `None` if `input` can't be an app
/// password.
fn parse_secret(input: &str) -> Option<Vec<u8>> {
    random::parse_letters(input, LEN).map(|letters| hash_secret(&letters))
}

fn hash_secret(letters: &str) -> Vec<u8> {
    Sha256::digest(letters.as_bytes()).to_vec()
}

fn app_password_from_row(row: &Row<'_>) -> rusqlite::Result<AppPassword> {
    Ok(AppPassword {
        id: AppPasswordId::from_uuid(row.get(0)?),
        user_id: UserId::from_uuid(row.get(1)?),
        use_for: match row.get_ref(2)?.as_str()? {
            "mail" => AppPasswordUse::Mail,
            "dav" => AppPasswordUse::Dav,
            other => return Err(bad_value(2, other)),
        },
        device_name: row.get(3)?,
        created_at: row.get(4)?,
        last_used_at: row.get(5)?,
    })
}

#[cfg(test)]
mod tests {
    use accordion_types::Role;

    use super::*;
    use crate::db::Database;
    use crate::identity::NewUser;

    async fn start() -> (tempfile::TempDir, Identity, User) {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir.path().join("accordion.db")).unwrap();
        let identity = Identity::new(db).await.unwrap();
        let alex = identity
            .create_user(NewUser {
                username: "alex".into(),
                display_name: "Alex Kim".into(),
                role: Role::Owner,
                password: "correct horse".into(),
            })
            .await
            .unwrap();
        (dir, identity, alex)
    }

    #[test]
    fn secrets_are_16_letters_and_parse_loosely() {
        let (secret, hash) = new_secret();
        assert_eq!(secret.as_str().len(), LEN);
        assert!(secret.as_str().bytes().all(|b| b.is_ascii_lowercase()));
        assert_eq!(format!("{secret:?}"), "AppPasswordSecret(..)");

        let grouped = secret.grouped();
        assert_eq!(grouped.len(), LEN + 3);
        assert_eq!(grouped.replace(' ', ""), secret.as_str());

        let shouted = secret.as_str().to_ascii_uppercase();
        let hyphens = grouped.replace(' ', "-");
        for same in [secret.as_str(), &grouped, &shouted, &hyphens] {
            assert_eq!(parse_secret(same).as_ref(), Some(&hash), "{same:?}");
        }
        for bad in [
            "",
            "abcd",
            &format!("{}a", secret.as_str()),
            "abcdefghijklmno1",
        ] {
            assert_eq!(parse_secret(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn secrets_use_the_whole_alphabet() {
        let mut seen = [false; 26];
        for _ in 0..100 {
            for b in new_secret().0.as_str().bytes() {
                seen[usize::from(b - b'a')] = true;
            }
        }
        assert!(seen.iter().all(|&seen| seen));
    }

    #[tokio::test]
    async fn an_app_password_works_only_for_its_own_protocols() {
        let (_dir, identity, alex) = start().await;
        let (secret, created) = identity
            .create_app_password(alex.id, AppPasswordUse::Dav, "  Alex's laptop\n")
            .await
            .unwrap();
        assert_eq!(created.device_name, "Alex's laptop");
        assert_eq!(created.last_used_at, None);

        let using = identity
            .check_app_password(" Alex ", &secret.grouped(), AppPasswordUse::Dav)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(using.user, alex);
        assert_eq!(using.app_password.id, created.id);

        // The check counted as a use.
        let listed = identity.app_passwords(alex.id).await.unwrap();
        assert!(listed[0].last_used_at.is_some());

        assert_eq!(
            identity
                .check_app_password("alex", secret.as_str(), AppPasswordUse::Mail)
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn the_account_password_and_other_users_never_match() {
        let (_dir, identity, alex) = start().await;
        let sam = identity
            .create_user(NewUser {
                username: "sam".into(),
                display_name: "Sam Kim".into(),
                role: Role::User,
                password: "battery staple".into(),
            })
            .await
            .unwrap();
        let (secret, _) = identity
            .create_app_password(alex.id, AppPasswordUse::Dav, "Laptop")
            .await
            .unwrap();

        for (username, password) in [
            ("alex", "correct horse"),
            ("sam", secret.as_str()),
            ("nobody", secret.as_str()),
            ("not a username", secret.as_str()),
        ] {
            assert_eq!(
                identity
                    .check_app_password(username, password, AppPasswordUse::Dav)
                    .await
                    .unwrap(),
                None,
                "{username}"
            );
        }
        assert_eq!(identity.app_passwords(sam.id).await.unwrap(), []);
    }

    #[tokio::test]
    async fn deleting_an_app_password_stops_it_working() {
        let (_dir, identity, alex) = start().await;
        let (first, first_created) = identity
            .create_app_password(alex.id, AppPasswordUse::Dav, "Laptop")
            .await
            .unwrap();
        let (second, _) = identity
            .create_app_password(alex.id, AppPasswordUse::Mail, "Phone")
            .await
            .unwrap();
        let names: Vec<String> = identity
            .app_passwords(alex.id)
            .await
            .unwrap()
            .into_iter()
            .map(|password| password.device_name)
            .collect();
        assert_eq!(names, ["Phone", "Laptop"]);

        assert!(
            !identity
                .delete_app_password(UserId::generate(), first_created.id)
                .await
                .unwrap()
        );
        assert!(
            identity
                .delete_app_password(alex.id, first_created.id)
                .await
                .unwrap()
        );
        assert_eq!(
            identity
                .check_app_password("alex", first.as_str(), AppPasswordUse::Dav)
                .await
                .unwrap(),
            None
        );
        assert!(
            identity
                .check_app_password("alex", second.as_str(), AppPasswordUse::Mail)
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn app_passwords_need_a_real_user() {
        let (_dir, identity, _alex) = start().await;
        let result = identity
            .create_app_password(UserId::generate(), AppPasswordUse::Dav, "Laptop")
            .await;
        assert!(matches!(result, Err(Error::UserNotFound)));
    }
}

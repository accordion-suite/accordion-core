//! Managing the people on a server: adding accounts, password links, roles,
//! names, turning accounts off, and deleting them.
//!
//! Admins never choose or see anyone else's password. A new account starts
//! without one, and its owner sets it from a one-time link the admin passes
//! on. A forgotten password works the same way: the admin makes a reset
//! link, which also signs the account out everywhere.
//!
//! Who may manage whom is the server's call. This module only keeps the
//! rules that must always hold, such as the owner's account staying the
//! owner's and staying on.

use std::fmt;

use accordion_types::{Role, UserId};
use base64ct::{Base64UrlUnpadded, Encoding};
use rusqlite::{OptionalExtension, Row, Transaction};
use sha2::{Digest, Sha256};

use super::session::{bad_value, delete_sessions};
use super::{
    Error, Identity, RESERVED_USERNAMES, User, check_password_rules, exists, insert_user,
    new_account, normalize_username, password, user_from_row, validate_display_name,
};
use crate::sql_now;

/// Marks the string as an Accordion password link token, so a leaked one is
/// recognizable in logs and to secret scanners.
const TOKEN_PREFIX: &str = "acp_";
const SECRET_LEN: usize = 32;

/// Whether a link hasn't expired, for a query that names the links table `l`.
macro_rules! link_live {
    () => {
        concat!("l.expires_at > ", sql_now!())
    };
}

/// What a password link is for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LinkPurpose {
    /// A new account's first password. Lasts seven days, since the person
    /// may not see the message right away.
    NewAccount,
    /// A forgotten password. Lasts a day, because the account can't sign in
    /// until it's used.
    Reset,
}

impl LinkPurpose {
    fn as_str(self) -> &'static str {
        match self {
            Self::NewAccount => "new_account",
            Self::Reset => "reset",
        }
    }

    fn from_column(row: &Row<'_>, column: usize) -> rusqlite::Result<Self> {
        match row.get_ref(column)?.as_str()? {
            "new_account" => Ok(Self::NewAccount),
            "reset" => Ok(Self::Reset),
            other => Err(bad_value(column, other)),
        }
    }
}

/// What a password link does to the account's app passwords.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AppPasswords {
    /// For a forgotten password: devices such as File Explorer stay
    /// connected while the person chooses a new one.
    Keep,
    /// For when someone else might know the password. They could have
    /// signed in and made an app password of their own, so every one goes,
    /// and each device has to connect again.
    Delete,
}

/// An unused password link. Times are ISO 8601 in UTC.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PasswordLink {
    pub purpose: LinkPurpose,
    pub expires_at: String,
}

/// The secret part of a password link. Give it to the admin once, to pass
/// on, and never log it: whoever has it can set the account's password.
pub struct LinkToken(String);

impl LinkToken {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for LinkToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("LinkToken(..)")
    }
}

/// A user account, as the admin's list of people shows it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Member {
    pub user: User,
    pub created_at: String,
    /// `false` until the person uses their link, and after a reset.
    pub has_password: bool,
    pub turned_off: bool,
    /// The account's unused, unexpired password link, if it has one.
    pub password_link: Option<PasswordLink>,
}

impl Identity {
    /// Everyone with an account: the owner first, then admins, then everyone
    /// else, each by name.
    pub async fn members(&self) -> Result<Vec<Member>, Error> {
        self.db
            .read(|conn| {
                conn.prepare(concat!(
                    "SELECT u.id, u.username, u.display_name, u.role, u.created_at,
                            u.password_hash IS NOT NULL, u.turned_off_at IS NOT NULL,
                            l.purpose, l.expires_at
                     FROM identity_users u
                     LEFT JOIN identity_password_links l ON l.user_id = u.id AND ",
                    link_live!(),
                    " ORDER BY CASE u.role WHEN 'owner' THEN 0 WHEN 'admin' THEN 1 ELSE 2 END,
                              u.display_name COLLATE NOCASE, u.username"
                ))?
                .query_map([], |row| {
                    let password_link = match row.get_ref(7)?.as_str_or_null()? {
                        Some(_) => Some(PasswordLink {
                            purpose: LinkPurpose::from_column(row, 7)?,
                            expires_at: row.get(8)?,
                        }),
                        None => None,
                    };
                    Ok(Member {
                        user: user_from_row(row, 0)?,
                        created_at: row.get(4)?,
                        has_password: row.get(5)?,
                        turned_off: row.get(6)?,
                        password_link,
                    })
                })?
                .collect()
            })
            .await
            .map_err(Error::from)
    }

    /// Adds an admin or standard account without a password, and returns a
    /// link for the person to set one.
    pub async fn add_member(
        &self,
        username: &str,
        display_name: &str,
        role: Role,
    ) -> Result<(User, LinkToken, PasswordLink), Error> {
        check_member_role(role)?;
        let user = new_account(username, display_name, role)?;
        let (token, hash) = new_token();
        let created = user.clone();
        let link = self
            .db
            .write(move |tx| -> Result<_, Error> {
                insert_user(tx, &user, None)?;
                Ok(insert_link(tx, user.id, &hash, LinkPurpose::NewAccount)?)
            })
            .await?;
        Ok((created, token, link))
    }

    /// Makes a new password link for an account, replacing any earlier one.
    ///
    /// For an account that has a password, this is a reset: it removes the
    /// password and signs the account out everywhere. `app_passwords` says
    /// whether the account's devices stay connected. For an account that
    /// never set one, it's a fresh copy of the first link.
    pub async fn make_password_link(
        &self,
        id: UserId,
        app_passwords: AppPasswords,
    ) -> Result<(LinkToken, PasswordLink), Error> {
        let (token, hash) = new_token();
        let (link, ended) = self
            .db
            .write(move |tx| {
                // A second link after a reset is still a reset, even though
                // the password is already gone.
                let (has_password, was_reset, turned_off): (bool, bool, bool) = tx
                    .query_row(
                        "SELECT password_hash IS NOT NULL,
                                EXISTS (SELECT 1 FROM identity_password_links
                                        WHERE user_id = ?1 AND purpose = 'reset'),
                                turned_off_at IS NOT NULL
                         FROM identity_users WHERE id = ?1",
                        [id.as_uuid()],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )
                    .optional()?
                    .ok_or(Error::UserNotFound)?;
                if turned_off {
                    return Err(Error::TurnedOff);
                }
                let mut ended = Vec::new();
                let purpose = if has_password {
                    tx.execute(
                        "UPDATE identity_users SET password_hash = NULL WHERE id = ?1",
                        [id.as_uuid()],
                    )?;
                    ended = delete_sessions(tx, "user_id = ?1", [id.as_uuid()])?;
                    LinkPurpose::Reset
                } else if was_reset {
                    LinkPurpose::Reset
                } else {
                    LinkPurpose::NewAccount
                };
                if app_passwords == AppPasswords::Delete {
                    tx.execute(
                        "DELETE FROM identity_app_passwords WHERE user_id = ?1",
                        [id.as_uuid()],
                    )?;
                }
                Ok((insert_link(tx, id, &hash, purpose)?, ended))
            })
            .await?;
        self.announce_ended(&ended);
        Ok((token, link))
    }

    /// Finds whose password link `token` is, if it's unused, unexpired, and
    /// the account is on.
    pub async fn check_password_link(
        &self,
        token: &str,
    ) -> Result<Option<(User, LinkPurpose)>, Error> {
        let Some(hash) = parse_token(token) else {
            return Ok(None);
        };
        self.db
            .read(move |conn| {
                conn.query_row(
                    concat!(
                        "SELECT u.id, u.username, u.display_name, u.role, l.purpose
                         FROM identity_password_links l JOIN identity_users u ON u.id = l.user_id
                         WHERE l.token_hash = ?1 AND u.turned_off_at IS NULL AND ",
                        link_live!()
                    ),
                    [hash],
                    |row| Ok((user_from_row(row, 0)?, LinkPurpose::from_column(row, 4)?)),
                )
                .optional()
            })
            .await
            .map_err(Error::from)
    }

    /// Sets the password from a link, and uses the link up.
    pub async fn use_password_link(&self, token: &str, password: String) -> Result<User, Error> {
        let hash = parse_token(token).ok_or(Error::LinkNotValid)?;
        check_password_rules(&password)?;
        let password_hash = password::hash(password).await?;
        self.db
            .write(move |tx| {
                let user = tx
                    .query_row(
                        concat!(
                            "DELETE FROM identity_password_links AS l
                             WHERE l.token_hash = ?1 AND ",
                            link_live!(),
                            " AND l.user_id IN
                                 (SELECT id FROM identity_users WHERE turned_off_at IS NULL)
                             RETURNING user_id"
                        ),
                        [&hash],
                        |row| row.get::<_, uuid::Uuid>(0),
                    )
                    .optional()?
                    .ok_or(Error::LinkNotValid)?;
                Ok(tx.query_row(
                    "UPDATE identity_users SET password_hash = ?2 WHERE id = ?1
                     RETURNING id, username, display_name, role",
                    (user, &password_hash),
                    |row| user_from_row(row, 0),
                )?)
            })
            .await
    }

    /// Makes an account an admin or a standard account.
    pub async fn set_role(&self, id: UserId, role: Role) -> Result<User, Error> {
        check_member_role(role)?;
        self.db
            .write(move |tx| {
                let user = current_user(tx, id)?;
                if user.role == Role::Owner {
                    return Err(Error::OwnerAccount);
                }
                tx.execute(
                    "UPDATE identity_users SET role = ?2 WHERE id = ?1",
                    (id.as_uuid(), role.as_str()),
                )?;
                Ok(User { role, ..user })
            })
            .await
    }

    /// Stops an account from signing in, and signs it out everywhere,
    /// including its app passwords. Its files and settings stay, for when
    /// it's turned back on.
    pub async fn turn_off_member(&self, id: UserId) -> Result<(), Error> {
        let ended = self
            .db
            .write(move |tx| {
                if current_user(tx, id)?.role == Role::Owner {
                    return Err(Error::OwnerAccount);
                }
                tx.execute(
                    concat!(
                        "UPDATE identity_users SET turned_off_at = ",
                        sql_now!(),
                        " WHERE id = ?1 AND turned_off_at IS NULL"
                    ),
                    [id.as_uuid()],
                )?;
                tx.execute(
                    "DELETE FROM identity_password_links WHERE user_id = ?1",
                    [id.as_uuid()],
                )?;
                Ok(delete_sessions(tx, "user_id = ?1", [id.as_uuid()])?)
            })
            .await?;
        self.announce_ended(&ended);
        Ok(())
    }

    /// Changes an account's username and the name people see.
    ///
    /// Everything else refers to the account by its ID, so only signing in
    /// changes: it takes the new username right away, including on devices
    /// that use app passwords. Storage's plain view names each person's
    /// folder after their username, so rebuild it after a new username.
    pub async fn rename_member(
        &self,
        id: UserId,
        username: &str,
        display_name: &str,
    ) -> Result<User, Error> {
        let username = normalize_username(username)?;
        if RESERVED_USERNAMES.contains(&username.as_str()) {
            return Err(Error::UsernameReserved);
        }
        let display_name = validate_display_name(display_name)?;
        self.db
            .write(move |tx| {
                let user = current_user(tx, id)?;
                if username != user.username && exists(tx, "username = ?1", [&username])? {
                    return Err(Error::UsernameTaken);
                }
                tx.execute(
                    "UPDATE identity_users SET username = ?2, display_name = ?3 WHERE id = ?1",
                    (id.as_uuid(), &username, &display_name),
                )?;
                Ok(User {
                    username,
                    display_name,
                    ..user
                })
            })
            .await
    }

    /// Deletes an account, and signs it out everywhere. Its sessions, app
    /// passwords, and password link go with it, and its username is free
    /// for someone else.
    ///
    /// Anything else that belongs to the account has to be moved or deleted
    /// first, such as its files, with `Storage::remove_owner`. Until then,
    /// the database refuses, so nothing is left behind pointing at an
    /// account that's gone.
    pub async fn delete_member(&self, id: UserId) -> Result<(), Error> {
        let ended = self
            .db
            .write(move |tx| {
                if current_user(tx, id)?.role == Role::Owner {
                    return Err(Error::OwnerAccount);
                }
                let ended = delete_sessions(tx, "user_id = ?1", [id.as_uuid()])?;
                tx.execute("DELETE FROM identity_users WHERE id = ?1", [id.as_uuid()])?;
                Ok(ended)
            })
            .await?;
        self.announce_ended(&ended);
        Ok(())
    }

    /// Lets a turned-off account sign in again.
    pub async fn turn_on_member(&self, id: UserId) -> Result<(), Error> {
        self.db
            .write(move |tx| {
                current_user(tx, id)?;
                tx.execute(
                    "UPDATE identity_users SET turned_off_at = NULL WHERE id = ?1",
                    [id.as_uuid()],
                )?;
                Ok(())
            })
            .await
    }
}

/// The owner comes from the setup wizard, and guests from shared links, so
/// admins only add and switch between these two.
fn check_member_role(role: Role) -> Result<(), Error> {
    match role {
        Role::Admin | Role::User => Ok(()),
        Role::Owner | Role::Guest => Err(Error::RoleNotAllowed),
    }
}

fn current_user(tx: &Transaction<'_>, id: UserId) -> Result<User, Error> {
    tx.query_row(
        "SELECT id, username, display_name, role FROM identity_users WHERE id = ?1",
        [id.as_uuid()],
        |row| user_from_row(row, 0),
    )
    .optional()?
    .ok_or(Error::UserNotFound)
}

fn insert_link(
    tx: &Transaction<'_>,
    user_id: UserId,
    hash: &[u8],
    purpose: LinkPurpose,
) -> rusqlite::Result<PasswordLink> {
    tx.query_row(
        concat!(
            "INSERT OR REPLACE INTO identity_password_links
                 (user_id, token_hash, purpose, created_at, expires_at)
             VALUES (?1, ?2, ?3, ",
            sql_now!(),
            ", CASE ?3 WHEN 'reset' THEN ",
            sql_now!("+1 day"),
            " ELSE ",
            sql_now!("+7 days"),
            " END)
             RETURNING expires_at"
        ),
        (user_id.as_uuid(), hash, purpose.as_str()),
        |row| {
            Ok(PasswordLink {
                purpose,
                expires_at: row.get(0)?,
            })
        },
    )
}

fn new_token() -> (LinkToken, Vec<u8>) {
    let mut secret = [0u8; SECRET_LEN];
    getrandom::fill(&mut secret).expect("the OS random number generator failed");
    let token = format!(
        "{TOKEN_PREFIX}{}",
        Base64UrlUnpadded::encode_string(&secret)
    );
    (LinkToken(token), Sha256::digest(secret).to_vec())
}

/// Returns the hash to look up, or `None` if `token` isn't a link token.
fn parse_token(token: &str) -> Option<Vec<u8>> {
    let encoded = token.trim().strip_prefix(TOKEN_PREFIX)?;
    let mut secret = [0u8; SECRET_LEN];
    let decoded = Base64UrlUnpadded::decode(encoded, &mut secret).ok()?;
    (decoded.len() == SECRET_LEN).then(|| Sha256::digest(secret).to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;
    use crate::identity::{
        AppPasswordSecret, AppPasswordUse, AuthMethod, Client, NewUser, SignInFrom,
    };

    const HOME: SignInFrom = SignInFrom {
        address: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        home_network: true,
    };

    async fn start() -> (tempfile::TempDir, Identity, User) {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir.path().join("accordion.db")).unwrap();
        let identity = Identity::new(db).await.unwrap();
        let owner = identity
            .create_user(NewUser {
                username: "alex".into(),
                display_name: "Alex Kim".into(),
                role: Role::Owner,
                password: "correct horse".into(),
            })
            .await
            .unwrap();
        (dir, identity, owner)
    }

    #[tokio::test]
    async fn new_members_set_their_own_password() {
        let (_dir, identity, _) = start().await;
        let (sam, token, link) = identity
            .add_member(" Sam ", "Sam Lee", Role::User)
            .await
            .unwrap();
        assert_eq!(sam.username, "sam");
        assert_eq!(link.purpose, LinkPurpose::NewAccount);
        assert!(token.as_str().starts_with(TOKEN_PREFIX));
        assert_eq!(format!("{token:?}"), "LinkToken(..)");

        // No password works until the link is used.
        let err = identity.check_password("sam", "", HOME).await.unwrap_err();
        assert!(matches!(err, Error::WrongCredentials));

        let members = identity.members().await.unwrap();
        let names: Vec<_> = members.iter().map(|m| m.user.username.as_str()).collect();
        assert_eq!(names, ["alex", "sam"]);
        assert!(!members[1].has_password);
        assert_eq!(members[1].password_link, Some(link));

        assert_eq!(
            identity.check_password_link(token.as_str()).await.unwrap(),
            Some((sam.clone(), LinkPurpose::NewAccount))
        );
        for bad in ["", "acp_", "acs_abc", "nonsense"] {
            assert_eq!(identity.check_password_link(bad).await.unwrap(), None);
        }
        let err = identity
            .use_password_link(token.as_str(), "short".into())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::PasswordTooShort));

        let used = identity
            .use_password_link(token.as_str(), "battery staple".into())
            .await
            .unwrap();
        assert_eq!(used, sam);
        assert!(
            identity
                .check_password("sam", "battery staple", HOME)
                .await
                .is_ok()
        );
        // A link works once.
        let err = identity
            .use_password_link(token.as_str(), "battery staple".into())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::LinkNotValid));
        let members = identity.members().await.unwrap();
        assert!(members[1].has_password);
        assert_eq!(members[1].password_link, None);
    }

    #[tokio::test]
    async fn checks_new_members() {
        let (_dir, identity, _) = start().await;
        for role in [Role::Owner, Role::Guest] {
            let err = identity
                .add_member("sam", "Sam Lee", role)
                .await
                .unwrap_err();
            assert!(matches!(err, Error::RoleNotAllowed), "{role}");
        }
        let err = identity
            .add_member("ALEX", "Alex Again", Role::User)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::UsernameTaken));
        let err = identity
            .add_member("shared", "Shared", Role::User)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::UsernameReserved));
        assert_eq!(identity.members().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_reset_signs_out_everywhere_until_the_link_is_used() {
        let (_dir, identity, _) = start().await;
        let (sam, token, _) = identity
            .add_member("sam", "Sam Lee", Role::User)
            .await
            .unwrap();
        identity
            .use_password_link(token.as_str(), "battery staple".into())
            .await
            .unwrap();
        let (session, _) = identity
            .create_session(sam.id, Client::Browser, "Firefox", AuthMethod::Password)
            .await
            .unwrap();
        let mut ended = identity.ended_sessions();

        let (old, _) = identity
            .make_password_link(sam.id, AppPasswords::Keep)
            .await
            .unwrap();
        let (token, link) = identity
            .make_password_link(sam.id, AppPasswords::Keep)
            .await
            .unwrap();
        assert_eq!(
            link.purpose,
            LinkPurpose::Reset,
            "a second link is still a reset"
        );
        assert!(ended.try_recv().is_ok());
        assert_eq!(
            identity.check_session(session.as_str()).await.unwrap(),
            None
        );
        let err = identity
            .check_password("sam", "battery staple", HOME)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::WrongCredentials));

        // Only the newest link works.
        assert_eq!(
            identity.check_password_link(old.as_str()).await.unwrap(),
            None
        );
        identity
            .use_password_link(token.as_str(), "new staple".into())
            .await
            .unwrap();
        assert!(
            identity
                .check_password("sam", "new staple", HOME)
                .await
                .is_ok()
        );

        let err = identity
            .make_password_link(UserId::generate(), AppPasswords::Keep)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::UserNotFound));
    }

    #[tokio::test]
    async fn a_reset_can_disconnect_every_device() {
        let (_dir, identity, _) = start().await;
        let (sam, token, _) = identity
            .add_member("sam", "Sam Lee", Role::User)
            .await
            .unwrap();
        identity
            .use_password_link(token.as_str(), "battery staple".into())
            .await
            .unwrap();
        let mut app_passwords = Vec::new();
        for device in ["Laptop", "Phone"] {
            let (secret, _) = identity
                .create_app_password(sam.id, AppPasswordUse::Dav, device)
                .await
                .unwrap();
            app_passwords.push(secret);
        }
        let works = async |secret: &AppPasswordSecret| {
            identity
                .check_app_password("sam", secret.as_str(), AppPasswordUse::Dav)
                .await
                .unwrap()
                .is_some()
        };

        identity
            .make_password_link(sam.id, AppPasswords::Keep)
            .await
            .unwrap();
        assert!(works(&app_passwords[0]).await);
        assert!(works(&app_passwords[1]).await);

        let (_, link) = identity
            .make_password_link(sam.id, AppPasswords::Delete)
            .await
            .unwrap();
        assert_eq!(link.purpose, LinkPurpose::Reset);
        assert!(!works(&app_passwords[0]).await);
        assert!(!works(&app_passwords[1]).await);
        assert!(identity.app_passwords(sam.id).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn links_expire() {
        let (_dir, identity, _) = start().await;
        let (sam, token, _) = identity
            .add_member("sam", "Sam Lee", Role::User)
            .await
            .unwrap();
        identity
            .db
            .write(move |tx| {
                tx.execute(
                    concat!(
                        "UPDATE identity_password_links SET expires_at = ",
                        sql_now!("-1 second"),
                        " WHERE user_id = ?1"
                    ),
                    [sam.id.as_uuid()],
                )
            })
            .await
            .unwrap();
        assert_eq!(
            identity.check_password_link(token.as_str()).await.unwrap(),
            None
        );
        let err = identity
            .use_password_link(token.as_str(), "battery staple".into())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::LinkNotValid));
        assert_eq!(identity.members().await.unwrap()[1].password_link, None);
    }

    #[tokio::test]
    async fn changes_roles_but_never_the_owners() {
        let (_dir, identity, owner) = start().await;
        let (sam, _, _) = identity
            .add_member("sam", "Sam Lee", Role::User)
            .await
            .unwrap();
        let sam = identity.set_role(sam.id, Role::Admin).await.unwrap();
        assert_eq!(sam.role, Role::Admin);
        assert_eq!(identity.user(sam.id).await.unwrap(), Some(sam.clone()));

        let err = identity.set_role(owner.id, Role::User).await.unwrap_err();
        assert!(matches!(err, Error::OwnerAccount));
        let err = identity.set_role(sam.id, Role::Owner).await.unwrap_err();
        assert!(matches!(err, Error::RoleNotAllowed));
        let err = identity
            .set_role(UserId::generate(), Role::User)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::UserNotFound));
    }

    #[tokio::test]
    async fn turned_off_accounts_cant_sign_in() {
        let (_dir, identity, owner) = start().await;
        let (sam, token, _) = identity
            .add_member("sam", "Sam Lee", Role::User)
            .await
            .unwrap();
        identity
            .use_password_link(token.as_str(), "battery staple".into())
            .await
            .unwrap();
        let (session, _) = identity
            .create_session(sam.id, Client::Browser, "Firefox", AuthMethod::Password)
            .await
            .unwrap();
        let (app_password, _) = identity
            .create_app_password(sam.id, AppPasswordUse::Dav, "Laptop")
            .await
            .unwrap();

        identity.turn_off_member(sam.id).await.unwrap();
        // Twice is fine.
        identity.turn_off_member(sam.id).await.unwrap();
        assert!(identity.members().await.unwrap()[1].turned_off);
        assert_eq!(
            identity.check_session(session.as_str()).await.unwrap(),
            None
        );
        assert!(
            identity
                .check_app_password("sam", app_password.as_str(), AppPasswordUse::Dav)
                .await
                .unwrap()
                .is_none()
        );
        // The right password says why; a wrong one doesn't.
        let err = identity
            .check_password("sam", "battery staple", HOME)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::TurnedOff));
        let err = identity
            .check_password("sam", "wrong horse", HOME)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::WrongCredentials));
        let err = identity
            .make_password_link(sam.id, AppPasswords::Keep)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::TurnedOff));

        identity.turn_on_member(sam.id).await.unwrap();
        assert!(
            identity
                .check_password("sam", "battery staple", HOME)
                .await
                .is_ok()
        );
        // App passwords come back with the account, so devices keep syncing.
        assert!(
            identity
                .check_app_password("sam", app_password.as_str(), AppPasswordUse::Dav)
                .await
                .unwrap()
                .is_some()
        );

        let err = identity.turn_off_member(owner.id).await.unwrap_err();
        assert!(matches!(err, Error::OwnerAccount));
    }

    #[tokio::test]
    async fn renames_accounts() {
        let (_dir, identity, owner) = start().await;
        let (sam, token, _) = identity
            .add_member("sam", "Sam Lee", Role::User)
            .await
            .unwrap();
        identity
            .use_password_link(token.as_str(), "battery staple".into())
            .await
            .unwrap();
        let (app_password, _) = identity
            .create_app_password(sam.id, AppPasswordUse::Dav, "Laptop")
            .await
            .unwrap();

        let renamed = identity
            .rename_member(sam.id, " Samuel ", "Samuel Lee")
            .await
            .unwrap();
        assert_eq!(renamed.username, "samuel");
        assert_eq!(renamed.display_name, "Samuel Lee");
        assert_eq!(renamed.role, Role::User);
        assert_eq!(identity.user(sam.id).await.unwrap(), Some(renamed));

        // Sign-ins take the new username, app passwords included.
        assert!(
            identity
                .check_password("samuel", "battery staple", HOME)
                .await
                .is_ok()
        );
        let err = identity
            .check_password("sam", "battery staple", HOME)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::WrongCredentials));
        assert!(
            identity
                .check_app_password("samuel", app_password.as_str(), AppPasswordUse::Dav)
                .await
                .unwrap()
                .is_some()
        );

        // Keeping the username and changing only the name is fine.
        let renamed = identity
            .rename_member(sam.id, "samuel", "Sam")
            .await
            .unwrap();
        assert_eq!(renamed.display_name, "Sam");
        // The owner can be renamed too. Who may do it is the server's call.
        identity
            .rename_member(owner.id, "alex", "Alex")
            .await
            .unwrap();

        let err = identity
            .rename_member(sam.id, "ALEX", "Sam")
            .await
            .unwrap_err();
        assert!(matches!(err, Error::UsernameTaken));
        let err = identity
            .rename_member(sam.id, "shared", "Sam")
            .await
            .unwrap_err();
        assert!(matches!(err, Error::UsernameReserved));
        let err = identity
            .rename_member(sam.id, "no spaces", "Sam")
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidUsername));
        let err = identity
            .rename_member(sam.id, "samuel", " ")
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidDisplayName));
        let err = identity
            .rename_member(UserId::generate(), "nobody", "Nobody")
            .await
            .unwrap_err();
        assert!(matches!(err, Error::UserNotFound));
    }

    #[tokio::test]
    async fn deletes_accounts_but_never_the_owners() {
        let (_dir, identity, owner) = start().await;
        let (sam, token, _) = identity
            .add_member("sam", "Sam Lee", Role::User)
            .await
            .unwrap();
        identity
            .use_password_link(token.as_str(), "battery staple".into())
            .await
            .unwrap();
        let (session, _) = identity
            .create_session(sam.id, Client::Browser, "Firefox", AuthMethod::Password)
            .await
            .unwrap();
        let (app_password, _) = identity
            .create_app_password(sam.id, AppPasswordUse::Dav, "Laptop")
            .await
            .unwrap();
        let mut ended = identity.ended_sessions();

        identity.delete_member(sam.id).await.unwrap();
        assert!(ended.try_recv().is_ok(), "open pages hear the sign-out");
        assert_eq!(identity.user(sam.id).await.unwrap(), None);
        assert_eq!(identity.members().await.unwrap().len(), 1);
        assert_eq!(
            identity.check_session(session.as_str()).await.unwrap(),
            None
        );
        assert!(
            identity
                .check_app_password("sam", app_password.as_str(), AppPasswordUse::Dav)
                .await
                .unwrap()
                .is_none()
        );
        let err = identity.delete_member(sam.id).await.unwrap_err();
        assert!(matches!(err, Error::UserNotFound));

        // The username is free again, for a new account with nothing of the
        // old one's.
        let (new_sam, _, _) = identity
            .add_member("sam", "Sam Park", Role::User)
            .await
            .unwrap();
        assert_ne!(new_sam.id, sam.id);
        assert!(identity.app_passwords(new_sam.id).await.unwrap().is_empty());

        let err = identity.delete_member(owner.id).await.unwrap_err();
        assert!(matches!(err, Error::OwnerAccount));
    }

    #[tokio::test]
    async fn turning_off_an_account_cancels_its_link() {
        let (_dir, identity, _) = start().await;
        let (sam, token, _) = identity
            .add_member("sam", "Sam Lee", Role::User)
            .await
            .unwrap();
        identity.turn_off_member(sam.id).await.unwrap();
        identity.turn_on_member(sam.id).await.unwrap();
        let err = identity
            .use_password_link(token.as_str(), "battery staple".into())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::LinkNotValid));
    }
}

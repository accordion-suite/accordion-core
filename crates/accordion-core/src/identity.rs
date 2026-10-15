//! Identity: user accounts, roles, and sign-in checks.
//!
//! This is the one place that knows who users are. Modules ask it, through
//! this API, and never read the `identity_` tables directly.

pub mod password;

use accordion_types::{Role, UserId};
use rusqlite::{Connection, OptionalExtension, Row};

use crate::db::{self, Database, Migration};

const OWNER: &str = "identity";

static MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    name: "Create users",
    sql: "
        CREATE TABLE identity_users (
            id BLOB PRIMARY KEY CHECK (length(id) = 16),
            username TEXT NOT NULL UNIQUE,
            display_name TEXT NOT NULL,
            role TEXT NOT NULL CHECK (role IN ('owner', 'admin', 'user', 'guest')),
            -- NULL until the user sets a password. Passkey-only accounts may
            -- never have one.
            password_hash TEXT,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
        ) STRICT, WITHOUT ROWID;

        -- Backs up the check in create_user: there's only ever one owner.
        CREATE UNIQUE INDEX identity_users_one_owner ON identity_users (role)
            WHERE role = 'owner';
    ",
}];

const MAX_USERNAME_LEN: usize = 32;
const MAX_DISPLAY_NAME_LEN: usize = 100;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(
        "Usernames can be up to {MAX_USERNAME_LEN} characters: lowercase letters and \
         numbers, with single periods, hyphens, or underscores between them."
    )]
    InvalidUsername,

    #[error("That username is already taken. Try a different one.")]
    UsernameTaken,

    #[error("Names can be up to {MAX_DISPLAY_NAME_LEN} characters and can't be blank.")]
    InvalidDisplayName,

    #[error("Passwords need at least {} characters.", password::MIN_LEN)]
    PasswordTooShort,

    #[error("Passwords can be up to {} characters.", password::MAX_LEN)]
    PasswordTooLong,

    #[error("This server already has an owner. Choose a different role.")]
    OwnerExists,

    #[error("That account doesn't exist.")]
    UserNotFound,

    /// Deliberately vague, so sign-in pages don't reveal which usernames exist.
    #[error("The username or password is incorrect.")]
    WrongCredentials,

    #[error(transparent)]
    Database(#[from] db::Error),

    #[error(transparent)]
    Hash(#[from] password::HashError),
}

impl From<rusqlite::Error> for Error {
    fn from(err: rusqlite::Error) -> Self {
        Self::Database(err.into())
    }
}

/// A user account.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct User {
    pub id: UserId,
    /// Lowercase ASCII, so it also works as an email address and an IMAP or
    /// WebDAV sign-in name. Users can change it, so refer to users by `id`.
    pub username: String,
    /// The name people see, in any language or script.
    pub display_name: String,
    pub role: Role,
}

/// The details needed to create an account.
pub struct NewUser {
    pub username: String,
    pub display_name: String,
    pub role: Role,
    pub password: String,
}

/// The identity service. Cloning it is cheap.
#[derive(Clone)]
pub struct Identity {
    db: Database,
}

impl Identity {
    /// Starts the identity service, creating or updating its tables first.
    pub async fn new(db: Database) -> Result<Self, Error> {
        db.migrate(OWNER, MIGRATIONS).await?;
        Ok(Self { db })
    }

    /// Whether the server has an owner yet. The setup wizard runs until it
    /// does.
    pub async fn has_owner(&self) -> Result<bool, Error> {
        self.db
            .read(|conn| {
                conn.query_row(
                    "SELECT EXISTS (SELECT 1 FROM identity_users WHERE role = 'owner')",
                    [],
                    |row| row.get(0),
                )
            })
            .await
            .map_err(Error::from)
    }

    pub async fn create_user(&self, new: NewUser) -> Result<User, Error> {
        let user = User {
            id: UserId::generate(),
            username: normalize_username(&new.username)?,
            display_name: validate_display_name(&new.display_name)?,
            role: new.role,
        };
        check_password_rules(&new.password)?;
        let hash = password::hash(new.password).await?;

        let created = user.clone();
        self.db
            .write(move |tx| {
                if user.role == Role::Owner && exists(tx, "role = 'owner'", ())? {
                    return Err(Error::OwnerExists);
                }
                if exists(tx, "username = ?1", [&user.username])? {
                    return Err(Error::UsernameTaken);
                }
                tx.execute(
                    "INSERT INTO identity_users (id, username, display_name, role, password_hash)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    (
                        user.id.as_uuid(),
                        &user.username,
                        &user.display_name,
                        user.role.as_str(),
                        &hash,
                    ),
                )?;
                Ok(())
            })
            .await?;
        Ok(created)
    }

    pub async fn user(&self, id: UserId) -> Result<Option<User>, Error> {
        self.db
            .read(move |conn| {
                conn.query_row(
                    "SELECT id, username, display_name, role FROM identity_users WHERE id = ?1",
                    [id.as_uuid()],
                    user_from_row,
                )
                .optional()
            })
            .await
            .map_err(Error::from)
    }

    /// Finds a user by username, ignoring case and surrounding spaces.
    pub async fn user_by_username(&self, username: &str) -> Result<Option<User>, Error> {
        let Ok(username) = normalize_username(username) else {
            return Ok(None);
        };
        self.db
            .read(move |conn| {
                conn.query_row(
                    "SELECT id, username, display_name, role FROM identity_users WHERE username = ?1",
                    [username],
                    user_from_row,
                )
                .optional()
            })
            .await
            .map_err(Error::from)
    }

    /// Checks a username and password, and returns the account if they match.
    ///
    /// Every failure returns [`Error::WrongCredentials`] after the same amount
    /// of work, whether the username exists or not.
    pub async fn check_password(&self, username: &str, password: &str) -> Result<User, Error> {
        let found = match normalize_username(username) {
            Ok(username) => {
                self.db
                    .read(move |conn| {
                        conn.query_row(
                            "SELECT id, username, display_name, role, password_hash
                             FROM identity_users WHERE username = ?1",
                            [username],
                            |row| Ok((user_from_row(row)?, row.get::<_, Option<String>>(4)?)),
                        )
                        .optional()
                    })
                    .await?
            }
            Err(_) => None,
        };
        let (user, hash) = match found {
            Some((user, hash)) => (Some(user), hash),
            None => (None, None),
        };
        // Overlong input can't match a stored hash. Hashing it anyway keeps
        // the timing the same as any other wrong password.
        let password = password.chars().take(password::MAX_LEN + 1).collect();
        match user {
            Some(user) if password::verify(password, hash).await => Ok(user),
            _ => Err(Error::WrongCredentials),
        }
    }

    pub async fn set_password(&self, id: UserId, password: String) -> Result<(), Error> {
        check_password_rules(&password)?;
        let hash = password::hash(password).await?;
        let changed = self
            .db
            .write(move |tx| {
                tx.execute(
                    "UPDATE identity_users SET password_hash = ?2 WHERE id = ?1",
                    (id.as_uuid(), hash),
                )
            })
            .await?;
        if changed == 0 {
            return Err(Error::UserNotFound);
        }
        Ok(())
    }
}

fn exists(
    conn: &Connection,
    condition: &str,
    params: impl rusqlite::Params,
) -> rusqlite::Result<bool> {
    conn.query_row(
        &format!("SELECT EXISTS (SELECT 1 FROM identity_users WHERE {condition})"),
        params,
        |row| row.get(0),
    )
}

fn user_from_row(row: &Row<'_>) -> rusqlite::Result<User> {
    let role: String = row.get(3)?;
    Ok(User {
        id: UserId::from_uuid(row.get(0)?),
        username: row.get(1)?,
        display_name: row.get(2)?,
        role: role.parse().map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(3, rusqlite::types::Type::Text, Box::new(err))
        })?,
    })
}

/// Usernames are lowercase ASCII letters and digits, with single periods,
/// hyphens, or underscores between them. That keeps every username valid as
/// the local part of an email address, and as a sign-in name in IMAP and
/// WebDAV clients, which handle other characters inconsistently.
fn normalize_username(input: &str) -> Result<String, Error> {
    let username = input.trim().to_ascii_lowercase();
    let bytes = username.as_bytes();
    let valid = !bytes.is_empty()
        && bytes.len() <= MAX_USERNAME_LEN
        && bytes.iter().all(|b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'-' | b'_')
        })
        && bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        && !bytes
            .windows(2)
            .any(|pair| !pair[0].is_ascii_alphanumeric() && !pair[1].is_ascii_alphanumeric());
    if valid {
        Ok(username)
    } else {
        Err(Error::InvalidUsername)
    }
}

fn validate_display_name(input: &str) -> Result<String, Error> {
    let name = input.trim();
    let len = name.chars().count();
    if len == 0 || len > MAX_DISPLAY_NAME_LEN || name.chars().any(char::is_control) {
        return Err(Error::InvalidDisplayName);
    }
    Ok(name.to_owned())
}

/// Counts characters, not bytes, so a password in any script gets the same
/// limits.
fn check_password_rules(password: &str) -> Result<(), Error> {
    let len = password.chars().count();
    if len < password::MIN_LEN {
        Err(Error::PasswordTooShort)
    } else if len > password::MAX_LEN {
        Err(Error::PasswordTooLong)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn start() -> (tempfile::TempDir, Identity) {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir.path().join("accordion.db")).unwrap();
        (dir, Identity::new(db).await.unwrap())
    }

    fn new_user(username: &str, role: Role) -> NewUser {
        NewUser {
            username: username.into(),
            display_name: "Alex Kim".into(),
            role,
            password: "correct horse".into(),
        }
    }

    #[test]
    fn username_rules() {
        assert_eq!(normalize_username("  Alex.Kim ").unwrap(), "alex.kim");
        for ok in ["a", "al", "alex-kim", "alex_kim2", "2alex", &"a".repeat(32)] {
            assert!(normalize_username(ok).is_ok(), "{ok:?} should be valid");
        }
        for bad in [
            "",
            " ",
            ".alex",
            "alex.",
            "alex..kim",
            "alex.-kim",
            "alex kim",
            "álex",
            "alex@home",
            &"a".repeat(33),
        ] {
            assert!(
                normalize_username(bad).is_err(),
                "{bad:?} should be invalid"
            );
        }
    }

    #[test]
    fn display_name_rules() {
        assert_eq!(
            validate_display_name("  Zoë Müller ").unwrap(),
            "Zoë Müller"
        );
        assert!(validate_display_name("   ").is_err());
        assert!(validate_display_name("Alex\nKim").is_err());
        assert!(validate_display_name(&"é".repeat(101)).is_err());
    }

    #[test]
    fn password_rules_count_characters() {
        assert!(matches!(
            check_password_rules("seven77"),
            Err(Error::PasswordTooShort)
        ));
        assert!(check_password_rules("ééééééé").is_err());
        assert!(check_password_rules("éééééééé").is_ok());
        assert!(matches!(
            check_password_rules(&"é".repeat(257)),
            Err(Error::PasswordTooLong)
        ));
    }

    #[tokio::test]
    async fn creates_and_finds_users() {
        let (_dir, identity) = start().await;
        assert!(!identity.has_owner().await.unwrap());

        let owner = identity
            .create_user(new_user("Alex", Role::Owner))
            .await
            .unwrap();
        assert_eq!(owner.username, "alex");
        assert!(identity.has_owner().await.unwrap());

        assert_eq!(identity.user(owner.id).await.unwrap(), Some(owner.clone()));
        assert_eq!(
            identity.user_by_username(" ALEX ").await.unwrap(),
            Some(owner)
        );
        assert_eq!(identity.user_by_username("sam").await.unwrap(), None);
        assert_eq!(identity.user(UserId::generate()).await.unwrap(), None);
    }

    #[tokio::test]
    async fn allows_only_one_owner() {
        let (_dir, identity) = start().await;
        identity
            .create_user(new_user("alex", Role::Owner))
            .await
            .unwrap();
        let err = identity
            .create_user(new_user("sam", Role::Owner))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::OwnerExists));
        identity
            .create_user(new_user("sam", Role::Admin))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn usernames_are_unique_ignoring_case() {
        let (_dir, identity) = start().await;
        identity
            .create_user(new_user("alex", Role::User))
            .await
            .unwrap();
        let err = identity
            .create_user(new_user("ALEX", Role::User))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::UsernameTaken));
    }

    #[tokio::test]
    async fn checks_passwords() {
        let (_dir, identity) = start().await;
        let alex = identity
            .create_user(new_user("alex", Role::User))
            .await
            .unwrap();

        assert_eq!(
            identity
                .check_password("Alex", "correct horse")
                .await
                .unwrap(),
            alex
        );
        for (username, password) in [
            ("alex", "wrong horse"),
            ("sam", "correct horse"),
            ("not a username", "correct horse"),
            ("alex", &"x".repeat(10_000)),
        ] {
            let err = identity
                .check_password(username, password)
                .await
                .unwrap_err();
            assert!(matches!(err, Error::WrongCredentials), "{username:?}");
        }
    }

    #[tokio::test]
    async fn changes_passwords() {
        let (_dir, identity) = start().await;
        let alex = identity
            .create_user(new_user("alex", Role::User))
            .await
            .unwrap();
        identity
            .set_password(alex.id, "battery staple".into())
            .await
            .unwrap();

        assert!(
            identity
                .check_password("alex", "correct horse")
                .await
                .is_err()
        );
        assert!(
            identity
                .check_password("alex", "battery staple")
                .await
                .is_ok()
        );

        let err = identity
            .set_password(UserId::generate(), "battery staple".into())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::UserNotFound));
    }

    #[tokio::test]
    async fn keeps_users_across_restarts() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("accordion.db");
        let identity = Identity::new(Database::open(&path).unwrap()).await.unwrap();
        let alex = identity
            .create_user(new_user("alex", Role::Owner))
            .await
            .unwrap();
        drop(identity);

        let identity = Identity::new(Database::open(&path).unwrap()).await.unwrap();
        assert_eq!(identity.user(alex.id).await.unwrap(), Some(alex));
    }
}

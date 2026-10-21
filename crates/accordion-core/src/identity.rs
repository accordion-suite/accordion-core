//! Identity: user accounts, roles, and sign-in checks.
//!
//! This is the one place that knows who users are. Modules ask it, through
//! this API, and never read the `identity_` tables directly.

mod app_password;
pub mod limits;
pub mod password;
mod session;

pub use app_password::{AppPassword, AppPasswordSecret, AppPasswordUse, UsingAppPassword};
pub use limits::SignInFrom;
pub use session::{AuthMethod, Client, Session, SessionToken, SignedIn};

use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use accordion_types::{Role, SessionId, UserId};
use rusqlite::{Connection, OptionalExtension, Row};
use tokio::sync::broadcast;

use crate::db::{self, Database, Migration};

const OWNER: &str = "identity";

static MIGRATIONS: &[Migration] = &[
    Migration {
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
    },
    Migration {
        version: 2,
        name: "Create sessions",
        sql: "
        CREATE TABLE identity_sessions (
            id BLOB PRIMARY KEY CHECK (length(id) = 16),
            user_id BLOB NOT NULL REFERENCES identity_users (id) ON DELETE CASCADE,
            -- SHA-256 of the token. The token itself is never stored.
            token_hash BLOB NOT NULL UNIQUE CHECK (length(token_hash) = 32),
            client TEXT NOT NULL CHECK (client IN ('browser', 'app')),
            device_name TEXT NOT NULL,
            auth_method TEXT NOT NULL
                CHECK (auth_method IN ('password', 'password_totp', 'passkey')),
            -- When the user last proved who they are, for sensitive actions.
            authenticated_at TEXT NOT NULL,
            created_at TEXT NOT NULL,
            -- Updated at most once an hour, so checks stay reads.
            last_used_at TEXT NOT NULL
        ) STRICT, WITHOUT ROWID;

        CREATE INDEX identity_sessions_user ON identity_sessions (user_id);
    ",
    },
    Migration {
        version: 3,
        name: "Create app passwords",
        sql: "
        CREATE TABLE identity_app_passwords (
            id BLOB PRIMARY KEY CHECK (length(id) = 16),
            user_id BLOB NOT NULL REFERENCES identity_users (id) ON DELETE CASCADE,
            -- SHA-256 of the password. The password itself is never stored.
            password_hash BLOB NOT NULL UNIQUE CHECK (length(password_hash) = 32),
            -- Mail (IMAP and SMTP), or files, calendars, and contacts (WebDAV,
            -- CalDAV, and CardDAV).
            use_for TEXT NOT NULL CHECK (use_for IN ('mail', 'dav')),
            device_name TEXT NOT NULL,
            created_at TEXT NOT NULL,
            -- NULL until first used. Updated at most once an hour.
            last_used_at TEXT
        ) STRICT, WITHOUT ROWID;

        CREATE INDEX identity_app_passwords_user ON identity_app_passwords (user_id);
    ",
    },
];

/// Names that can't be usernames. Storage's plain view keeps each user's
/// files in a folder named after them, next to a `shared` folder for shared
/// drives (decision 6).
const RESERVED_USERNAMES: &[&str] = &["shared"];

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

    #[error("That username is reserved for Accordion's own use. Try a different one.")]
    UsernameReserved,

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

    /// Sign-ins are paused after too many wrong passwords. See [`limits`].
    #[error(
        "There have been too many wrong passwords. Wait {}, then try again.",
        minutes_in_words(*wait)
    )]
    TooManyTries { wait: Duration },

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
    ended: broadcast::Sender<SessionId>,
    limits: Arc<Mutex<limits::Limits>>,
}

impl Identity {
    /// Starts the identity service, creating or updating its tables first.
    pub async fn new(db: Database) -> Result<Self, Error> {
        db.migrate(OWNER, MIGRATIONS).await?;
        // Sign-outs come in bursts of at most a few dozen, such as signing
        // out everywhere, so this is plenty.
        let (ended, _) = broadcast::channel(256);
        Ok(Self {
            db,
            ended,
            limits: Arc::default(),
        })
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
        let username = normalize_username(&new.username)?;
        if RESERVED_USERNAMES.contains(&username.as_str()) {
            return Err(Error::UsernameReserved);
        }
        let user = User {
            id: UserId::generate(),
            username,
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
                    |row| user_from_row(row, 0),
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
                    |row| user_from_row(row, 0),
                )
                .optional()
            })
            .await
            .map_err(Error::from)
    }

    /// Checks a username and password, and returns the account if they match.
    ///
    /// Every failure returns [`Error::WrongCredentials`] after the same amount
    /// of work, whether the username exists or not. After too many failures,
    /// it returns [`Error::TooManyTries`] without checking the password.
    pub async fn check_password(
        &self,
        username: &str,
        password: &str,
        from: SignInFrom,
    ) -> Result<User, Error> {
        self.limits()
            .start(username, from, Instant::now())
            .map_err(|wait| Error::TooManyTries { wait })?;
        let user = self.verify_password(username, password).await?;
        self.limits().succeeded(username, from);
        Ok(user)
    }

    fn limits(&self) -> MutexGuard<'_, limits::Limits> {
        self.limits
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    async fn verify_password(&self, username: &str, password: &str) -> Result<User, Error> {
        let found = match normalize_username(username) {
            Ok(username) => {
                self.db
                    .read(move |conn| {
                        conn.query_row(
                            "SELECT id, username, display_name, role, password_hash
                             FROM identity_users WHERE username = ?1",
                            [username],
                            |row| Ok((user_from_row(row, 0)?, row.get::<_, Option<String>>(4)?)),
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

    /// Sets a user's password and signs out their other sessions, in case
    /// someone else knew the old password.
    ///
    /// When users change their own password, pass the session they're using
    /// as `keep`. When an admin resets it, pass `None` to sign out everywhere.
    pub async fn set_password(
        &self,
        id: UserId,
        password: String,
        keep: Option<SessionId>,
    ) -> Result<(), Error> {
        check_password_rules(&password)?;
        let hash = password::hash(password).await?;
        let ended = self
            .db
            .write(move |tx| {
                let changed = tx.execute(
                    "UPDATE identity_users SET password_hash = ?2 WHERE id = ?1",
                    (id.as_uuid(), hash),
                )?;
                if changed == 0 {
                    return Err(Error::UserNotFound);
                }
                // A nil UUID matches no session, so `None` keeps none.
                let keep = keep.map_or(uuid::Uuid::nil(), |keep| keep.as_uuid());
                Ok(session::delete_sessions(
                    tx,
                    "user_id = ?1 AND id != ?2",
                    (id.as_uuid(), keep),
                )?)
            })
            .await?;
        self.announce_ended(&ended);
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

/// Reads a user from four columns, starting at `first`: id, username,
/// display name, and role.
fn user_from_row(row: &Row<'_>, first: usize) -> rusqlite::Result<User> {
    let role: String = row.get(first + 3)?;
    Ok(User {
        id: UserId::from_uuid(row.get(first)?),
        username: row.get(first + 1)?,
        display_name: row.get(first + 2)?,
        role: role.parse().map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                first + 3,
                rusqlite::types::Type::Text,
                Box::new(err),
            )
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

/// "a minute", "two minutes", or "15 minutes", rounding up.
fn minutes_in_words(wait: Duration) -> String {
    const WORDS: [&str; 10] = [
        "zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine",
    ];
    let minutes = wait.as_secs().div_ceil(60).max(1);
    match minutes {
        1 => "a minute".to_owned(),
        2..=9 => format!("{} minutes", WORDS[minutes as usize]),
        _ => format!("{minutes} minutes"),
    }
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

    const HOME: SignInFrom = SignInFrom {
        address: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        home_network: true,
    };

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
    async fn reserves_names_that_storage_uses() {
        let (_dir, identity) = start().await;
        let err = identity
            .create_user(new_user("Shared", Role::User))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::UsernameReserved));
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
                .check_password("Alex", "correct horse", HOME)
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
                .check_password(username, password, HOME)
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
            .set_password(alex.id, "battery staple".into(), None)
            .await
            .unwrap();

        assert!(
            identity
                .check_password("alex", "correct horse", HOME)
                .await
                .is_err()
        );
        assert!(
            identity
                .check_password("alex", "battery staple", HOME)
                .await
                .is_ok()
        );

        let err = identity
            .set_password(UserId::generate(), "battery staple".into(), None)
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

    #[tokio::test]
    async fn pauses_sign_ins_after_too_many_wrong_passwords() {
        let (_dir, identity) = start().await;
        identity
            .create_user(new_user("alex", Role::User))
            .await
            .unwrap();
        let stranger = SignInFrom {
            address: "203.0.113.5".parse().unwrap(),
            home_network: false,
        };
        for _ in 0..limits::ADDRESS_LIMIT {
            let err = identity
                .check_password("alex", "wrong horse", stranger)
                .await
                .unwrap_err();
            assert!(matches!(err, Error::WrongCredentials));
        }
        // Even the right password waits, or guessing could carry on.
        let err = identity
            .check_password("alex", "correct horse", stranger)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::TooManyTries { .. }));
        assert_eq!(
            err.to_string(),
            "There have been too many wrong passwords. Wait 15 minutes, then try again."
        );

        // The home network has its own count.
        assert!(
            identity
                .check_password("alex", "correct horse", HOME)
                .await
                .is_ok()
        );
    }

    #[test]
    fn says_how_long_to_wait() {
        for (secs, words) in [
            (1, "a minute"),
            (60, "a minute"),
            (61, "two minutes"),
            (9 * 60, "nine minutes"),
            (15 * 60, "15 minutes"),
        ] {
            assert_eq!(minutes_in_words(Duration::from_secs(secs)), words);
        }
    }
}

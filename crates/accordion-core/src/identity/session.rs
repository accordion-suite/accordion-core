//! Sign-in sessions (decision 3 in the `accordion` repo's `docs/decisions`).
//!
//! Signing in creates a session and gives the client a random token. The
//! database stores only a SHA-256 hash of it. Each request's token is looked
//! up, and deleting the session signs that device out at once.

use std::fmt;

use accordion_types::{SessionId, UserId};
use base64ct::{Base64UrlUnpadded, Encoding};
use rusqlite::{OptionalExtension, Row, Transaction};
use sha2::{Digest, Sha256};

use super::{Error, Identity, User, user_from_row};
use crate::sql_now;

/// Marks the string as an Accordion session token, so a leaked one is
/// recognizable in logs and to secret scanners.
const TOKEN_PREFIX: &str = "acs_";
const SECRET_LEN: usize = 32;

const MAX_DEVICE_NAME_LEN: usize = 100;

// Lifetimes from decision 3. SQLite date modifiers, so queries compare times
// in SQL and the server's clock and the database's always agree.
macro_rules! browser_idle_limit {
    () => {
        sql_now!("-30 days")
    };
}
macro_rules! app_idle_limit {
    () => {
        sql_now!("-180 days")
    };
}
/// Sensitive actions need a sign-in from within this window.
macro_rules! reauthentication_window {
    () => {
        sql_now!("-15 minutes")
    };
}
/// Recording each use would make every check a write. Once an hour is
/// precise enough for the device list and for expiry.
macro_rules! last_used_precision {
    () => {
        sql_now!("-1 hour")
    };
}

/// Whether the session is still live, for a query that names the sessions
/// table `s`.
macro_rules! not_expired {
    () => {
        concat!(
            "s.last_used_at >= CASE s.client WHEN 'browser' THEN ",
            browser_idle_limit!(),
            " ELSE ",
            app_idle_limit!(),
            " END"
        )
    };
}

macro_rules! session_columns {
    () => {
        "s.id, s.user_id, s.client, s.device_name, s.auth_method, \
         s.authenticated_at, s.created_at, s.last_used_at"
    };
}

/// What kind of client holds a session. It sets how long the session lasts
/// without use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Client {
    /// The web app. Ends after 30 days without use.
    Browser,
    /// The desktop or mobile app. Ends after 180 days without use, so apps
    /// can be offline for months.
    App,
}

impl Client {
    fn as_str(self) -> &'static str {
        match self {
            Self::Browser => "browser",
            Self::App => "app",
        }
    }
}

/// How the user proved who they are.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthMethod {
    Password,
    PasswordAndTotp,
    Passkey,
}

impl AuthMethod {
    fn as_str(self) -> &'static str {
        match self {
            Self::Password => "password",
            Self::PasswordAndTotp => "password_totp",
            Self::Passkey => "passkey",
        }
    }
}

/// A session token. Give it to the client once, at sign-in, and never log
/// it: whoever has it is signed in.
pub struct SessionToken(String);

impl SessionToken {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SessionToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SessionToken(..)")
    }
}

/// One signed-in browser or app, as the device list shows it.
///
/// Times are ISO 8601 in UTC.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Session {
    pub id: SessionId,
    pub user_id: UserId,
    pub client: Client,
    pub device_name: String,
    pub auth_method: AuthMethod,
    /// When the user last proved who they are in this session.
    pub authenticated_at: String,
    pub created_at: String,
    /// Updated at most once an hour.
    pub last_used_at: String,
}

/// The result of a successful session check.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignedIn {
    pub user: User,
    pub session: Session,
    /// Whether the user proved who they are in the last 15 minutes.
    /// Sensitive actions, such as changing a password or admin actions, need
    /// this. Otherwise ask the user to confirm with a passkey or password.
    pub recently_authenticated: bool,
}

impl Identity {
    /// Signs a user in on one device, after they've proved who they are.
    ///
    /// `device_name` describes the device in the device list, such as
    /// "Firefox on Windows" or the name an app reports.
    pub async fn create_session(
        &self,
        user_id: UserId,
        client: Client,
        device_name: &str,
        auth_method: AuthMethod,
    ) -> Result<(SessionToken, Session), Error> {
        let (token, hash) = new_token();
        let id = SessionId::generate();
        let device_name = clean_device_name(device_name);

        let session = self
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
                        "INSERT INTO identity_sessions (id, user_id, token_hash, client, \
                         device_name, auth_method, authenticated_at, created_at, last_used_at)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ",
                        sql_now!(),
                        ", ",
                        sql_now!(),
                        ", ",
                        sql_now!(),
                        ")"
                    ),
                    (
                        id.as_uuid(),
                        user_id.as_uuid(),
                        hash,
                        client.as_str(),
                        device_name,
                        auth_method.as_str(),
                    ),
                )?;
                let session = tx.query_row(
                    concat!(
                        "SELECT ",
                        session_columns!(),
                        " FROM identity_sessions s WHERE s.id = ?1"
                    ),
                    [id.as_uuid()],
                    session_from_row,
                )?;
                Ok(session)
            })
            .await?;
        Ok((token, session))
    }

    /// Checks a session token from a request. Returns `None` if the token is
    /// malformed, unknown, signed out, or expired.
    pub async fn check_session(&self, token: &str) -> Result<Option<SignedIn>, Error> {
        let Some(hash) = parse_token(token) else {
            return Ok(None);
        };
        let found = self
            .db
            .read(move |conn| {
                conn.query_row(
                    concat!(
                        "SELECT ",
                        session_columns!(),
                        ", u.id, u.username, u.display_name, u.role, s.authenticated_at >= ",
                        reauthentication_window!(),
                        ", s.last_used_at < ",
                        last_used_precision!(),
                        " FROM identity_sessions s JOIN identity_users u ON u.id = s.user_id
                         WHERE s.token_hash = ?1 AND ",
                        not_expired!()
                    ),
                    [hash],
                    |row| {
                        let signed_in = SignedIn {
                            session: session_from_row(row)?,
                            user: user_from_row(row, 8)?,
                            recently_authenticated: row.get(12)?,
                        };
                        let stale: bool = row.get(13)?;
                        Ok((signed_in, stale))
                    },
                )
                .optional()
            })
            .await?;

        let Some((signed_in, stale)) = found else {
            return Ok(None);
        };
        if stale {
            let id = signed_in.session.id;
            self.db
                .write(move |tx| {
                    tx.execute(
                        concat!(
                            "UPDATE identity_sessions SET last_used_at = ",
                            sql_now!(),
                            " WHERE id = ?1"
                        ),
                        [id.as_uuid()],
                    )
                })
                .await?;
        }
        Ok(Some(signed_in))
    }

    /// The user's live sessions, most recently used first.
    pub async fn sessions(&self, user_id: UserId) -> Result<Vec<Session>, Error> {
        self.db
            .read(move |conn| {
                conn.prepare(concat!(
                    "SELECT ",
                    session_columns!(),
                    " FROM identity_sessions s WHERE s.user_id = ?1 AND ",
                    not_expired!(),
                    " ORDER BY s.last_used_at DESC, s.id DESC"
                ))?
                .query_map([user_id.as_uuid()], session_from_row)?
                .collect()
            })
            .await
            .map_err(Error::from)
    }

    /// Signs out one of the user's sessions. Returns `false` if the user has
    /// no such session.
    ///
    /// Checking that the caller may do this is up to the caller: users can
    /// end their own sessions, and admins can end anyone's.
    pub async fn end_session(&self, user_id: UserId, session_id: SessionId) -> Result<bool, Error> {
        let ended = self
            .end_sessions_where(
                "user_id = ?1 AND id = ?2",
                (user_id.as_uuid(), session_id.as_uuid()),
            )
            .await?;
        Ok(!ended.is_empty())
    }

    /// Signs out every session of the user's except `keep`, the one making
    /// the request. Returns how many ended.
    pub async fn end_other_sessions(
        &self,
        user_id: UserId,
        keep: SessionId,
    ) -> Result<usize, Error> {
        let ended = self
            .end_sessions_where(
                "user_id = ?1 AND id != ?2",
                (user_id.as_uuid(), keep.as_uuid()),
            )
            .await?;
        Ok(ended.len())
    }

    /// Signs out every session of the user's. Returns how many ended.
    pub async fn end_all_sessions(&self, user_id: UserId) -> Result<usize, Error> {
        let ended = self
            .end_sessions_where("user_id = ?1", [user_id.as_uuid()])
            .await?;
        Ok(ended.len())
    }

    /// Deletes expired sessions. Expired sessions already fail every check,
    /// so this only keeps the table small. Run it from a daily job.
    pub async fn delete_expired_sessions(&self) -> Result<usize, Error> {
        let ended = self
            .end_sessions_where(concat!("NOT (", not_expired!(), ")"), ())
            .await?;
        Ok(ended.len())
    }

    /// Announces sessions that ended, so the server can close their open
    /// connections, such as Docs WebSockets.
    ///
    /// A receiver that falls behind misses announcements. It should then
    /// check each open connection's session again with
    /// [`check_session`](Self::check_session).
    pub fn ended_sessions(&self) -> tokio::sync::broadcast::Receiver<SessionId> {
        self.ended.subscribe()
    }

    async fn end_sessions_where(
        &self,
        condition: &'static str,
        params: impl rusqlite::Params + Send + 'static,
    ) -> Result<Vec<SessionId>, Error> {
        let ended = self
            .db
            .write(move |tx| delete_sessions(tx, condition, params))
            .await?;
        self.announce_ended(&ended);
        Ok(ended)
    }

    pub(super) fn announce_ended(&self, ended: &[SessionId]) {
        for &id in ended {
            // Sending fails only when nobody is listening, which is fine.
            let _ = self.ended.send(id);
        }
    }
}

/// Deletes sessions inside an existing transaction. Call
/// [`Identity::announce_ended`] with the result after the commit.
pub(super) fn delete_sessions(
    tx: &Transaction<'_>,
    condition: &str,
    params: impl rusqlite::Params,
) -> rusqlite::Result<Vec<SessionId>> {
    tx.prepare(&format!(
        "DELETE FROM identity_sessions AS s WHERE {condition} RETURNING id"
    ))?
    .query_map(params, |row| Ok(SessionId::from_uuid(row.get(0)?)))?
    .collect()
}

fn new_token() -> (SessionToken, Vec<u8>) {
    let mut secret = [0u8; SECRET_LEN];
    getrandom::fill(&mut secret).expect("the OS random number generator failed");
    let token = format!(
        "{TOKEN_PREFIX}{}",
        Base64UrlUnpadded::encode_string(&secret)
    );
    (SessionToken(token), Sha256::digest(secret).to_vec())
}

/// Returns the hash to look up, or `None` if `token` isn't a session token.
fn parse_token(token: &str) -> Option<Vec<u8>> {
    let encoded = token.strip_prefix(TOKEN_PREFIX)?;
    let mut secret = [0u8; SECRET_LEN];
    let decoded = Base64UrlUnpadded::decode(encoded, &mut secret).ok()?;
    (decoded.len() == SECRET_LEN).then(|| Sha256::digest(secret).to_vec())
}

/// Device names come from the client, so tidy them rather than reject them.
fn clean_device_name(input: &str) -> String {
    let name: String = input
        .chars()
        .filter(|c| !c.is_control())
        .take(MAX_DEVICE_NAME_LEN)
        .collect();
    match name.trim() {
        "" => "Unknown device".to_owned(),
        name => name.to_owned(),
    }
}

fn session_from_row(row: &Row<'_>) -> rusqlite::Result<Session> {
    Ok(Session {
        id: SessionId::from_uuid(row.get(0)?),
        user_id: UserId::from_uuid(row.get(1)?),
        client: match row.get_ref(2)?.as_str()? {
            "browser" => Client::Browser,
            "app" => Client::App,
            other => return Err(bad_value(2, other)),
        },
        device_name: row.get(3)?,
        auth_method: match row.get_ref(4)?.as_str()? {
            "password" => AuthMethod::Password,
            "password_totp" => AuthMethod::PasswordAndTotp,
            "passkey" => AuthMethod::Passkey,
            other => return Err(bad_value(4, other)),
        },
        authenticated_at: row.get(5)?,
        created_at: row.get(6)?,
        last_used_at: row.get(7)?,
    })
}

fn bad_value(column: usize, value: &str) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        column,
        rusqlite::types::Type::Text,
        format!("unexpected value {value:?}").into(),
    )
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

    async fn sign_in(identity: &Identity, user: &User, client: Client) -> (SessionToken, Session) {
        identity
            .create_session(user.id, client, "Firefox on Windows", AuthMethod::Password)
            .await
            .unwrap()
    }

    /// Moves a session's times into the past, as if it hadn't been used.
    async fn age(identity: &Identity, id: SessionId, modifier: &'static str) {
        identity
            .db
            .write(move |tx| {
                tx.execute(
                    "UPDATE identity_sessions
                     SET last_used_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?2),
                         authenticated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?2)
                     WHERE id = ?1",
                    (id.as_uuid(), modifier),
                )
            })
            .await
            .unwrap();
    }

    #[test]
    fn tokens_parse_only_in_their_own_format() {
        let (token, hash) = new_token();
        assert!(token.as_str().starts_with("acs_"));
        assert_eq!(token.as_str().len(), 4 + 43);
        assert_eq!(parse_token(token.as_str()), Some(hash));
        assert_eq!(format!("{token:?}"), "SessionToken(..)");

        let secret = &token.as_str()[4..];
        for bad in [
            secret,
            &format!("acs_{}", &secret[1..]),
            &format!("acs_{secret}A"),
            "acs_",
            "",
            "acs_!!!",
        ] {
            assert_eq!(parse_token(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn device_names_are_tidied() {
        assert_eq!(clean_device_name("  Alex's Pixel\n"), "Alex's Pixel");
        assert_eq!(clean_device_name("\u{7}"), "Unknown device");
        assert_eq!(clean_device_name(&"é".repeat(200)).chars().count(), 100);
    }

    #[tokio::test]
    async fn a_new_session_checks_out() {
        let (_dir, identity, alex) = start().await;
        let (token, session) = sign_in(&identity, &alex, Client::Browser).await;

        let signed_in = identity
            .check_session(token.as_str())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(signed_in.user, alex);
        assert_eq!(signed_in.session, session);
        assert_eq!(session.device_name, "Firefox on Windows");
        assert!(signed_in.recently_authenticated);
    }

    #[tokio::test]
    async fn unknown_tokens_are_rejected() {
        let (_dir, identity, _alex) = start().await;
        let (token, _) = new_token();
        assert_eq!(identity.check_session(token.as_str()).await.unwrap(), None);
        assert_eq!(identity.check_session("nonsense").await.unwrap(), None);
    }

    #[tokio::test]
    async fn sessions_need_a_real_user() {
        let (_dir, identity, _alex) = start().await;
        let result = identity
            .create_session(
                UserId::generate(),
                Client::App,
                "Phone",
                AuthMethod::Passkey,
            )
            .await;
        assert!(matches!(result, Err(Error::UserNotFound)));
    }

    #[tokio::test]
    async fn browser_sessions_expire_after_30_days_without_use() {
        let (_dir, identity, alex) = start().await;
        let (token, session) = sign_in(&identity, &alex, Client::Browser).await;

        age(&identity, session.id, "-29 days").await;
        let signed_in = identity
            .check_session(token.as_str())
            .await
            .unwrap()
            .unwrap();
        assert!(!signed_in.recently_authenticated);

        // That check counted as a use, so the clock restarted.
        let refreshed = identity
            .check_session(token.as_str())
            .await
            .unwrap()
            .unwrap();
        assert!(refreshed.session.last_used_at > signed_in.session.last_used_at);

        age(&identity, session.id, "-31 days").await;
        assert_eq!(identity.check_session(token.as_str()).await.unwrap(), None);
        assert_eq!(identity.sessions(alex.id).await.unwrap(), []);
    }

    #[tokio::test]
    async fn app_sessions_last_180_days_without_use() {
        let (_dir, identity, alex) = start().await;
        let (token, session) = sign_in(&identity, &alex, Client::App).await;

        age(&identity, session.id, "-179 days").await;
        assert!(
            identity
                .check_session(token.as_str())
                .await
                .unwrap()
                .is_some()
        );

        age(&identity, session.id, "-181 days").await;
        assert_eq!(identity.check_session(token.as_str()).await.unwrap(), None);
        assert_eq!(identity.delete_expired_sessions().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn signing_out_one_device_leaves_the_others() {
        let (_dir, identity, alex) = start().await;
        let (laptop, laptop_session) = sign_in(&identity, &alex, Client::Browser).await;
        let (phone, phone_session) = sign_in(&identity, &alex, Client::App).await;
        assert_eq!(identity.sessions(alex.id).await.unwrap().len(), 2);

        let mut ended = identity.ended_sessions();
        assert!(
            identity
                .end_session(alex.id, phone_session.id)
                .await
                .unwrap()
        );
        assert_eq!(ended.recv().await.unwrap(), phone_session.id);
        assert!(
            !identity
                .end_session(alex.id, phone_session.id)
                .await
                .unwrap()
        );

        assert_eq!(identity.check_session(phone.as_str()).await.unwrap(), None);
        assert!(
            identity
                .check_session(laptop.as_str())
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(identity.sessions(alex.id).await.unwrap(), [laptop_session]);
    }

    #[tokio::test]
    async fn users_cant_end_each_others_sessions_by_id() {
        let (_dir, identity, alex) = start().await;
        let sam = identity
            .create_user(NewUser {
                username: "sam".into(),
                display_name: "Sam".into(),
                role: Role::User,
                password: "battery staple".into(),
            })
            .await
            .unwrap();
        let (_, alex_session) = sign_in(&identity, &alex, Client::Browser).await;
        assert!(!identity.end_session(sam.id, alex_session.id).await.unwrap());
        assert_eq!(identity.sessions(alex.id).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn signing_out_everywhere_else_keeps_this_session() {
        let (_dir, identity, alex) = start().await;
        let (this, this_session) = sign_in(&identity, &alex, Client::Browser).await;
        for _ in 0..3 {
            sign_in(&identity, &alex, Client::App).await;
        }
        assert_eq!(
            identity
                .end_other_sessions(alex.id, this_session.id)
                .await
                .unwrap(),
            3
        );
        assert!(
            identity
                .check_session(this.as_str())
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(identity.end_all_sessions(alex.id).await.unwrap(), 1);
        assert_eq!(identity.check_session(this.as_str()).await.unwrap(), None);
    }

    #[tokio::test]
    async fn changing_the_password_signs_out_other_sessions() {
        let (_dir, identity, alex) = start().await;
        let (this, this_session) = sign_in(&identity, &alex, Client::Browser).await;
        let (other, _) = sign_in(&identity, &alex, Client::App).await;

        identity
            .set_password(alex.id, "battery staple".into(), Some(this_session.id))
            .await
            .unwrap();
        assert!(
            identity
                .check_session(this.as_str())
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(identity.check_session(other.as_str()).await.unwrap(), None);

        // An admin's reset keeps none.
        identity
            .set_password(alex.id, "new password".into(), None)
            .await
            .unwrap();
        assert_eq!(identity.check_session(this.as_str()).await.unwrap(), None);
    }
}

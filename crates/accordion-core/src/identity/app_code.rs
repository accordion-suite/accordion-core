//! Codes that add one of Accordion's apps (decision 23 in the `accordion`
//! repo's `docs/decisions`).
//!
//! Someone signed in elsewhere, in the web app or another app, makes a code
//! and types it into the new app, or scans it as a QR code. The new app
//! trades it for a session of its own. That works away from home, where the
//! app can't reach the sign-in page through the system browser.
//!
//! Each code works once, for 10 minutes, and only the newest one each
//! person made. The database stores only a SHA-256 hash of it. The new
//! session counts as signed in the way the session that made the code was,
//! and at the same time, so it isn't a fresh sign-in for sensitive actions.

use std::fmt;

use accordion_types::{SessionId, UserId};
use rusqlite::OptionalExtension;
use sha2::{Digest, Sha256};

use super::session::{clean_device_name, new_token, session_columns, session_from_row};
use super::{Error, Identity, Session, SessionToken, SignedIn, record};
use crate::events::{Action, Actor, NewEvent, Via};
use crate::{random, sql_now};

/// Letters in a code: 26^12 is about 2^56.
const LEN: usize = 12;

/// A code that adds an app. Show it once, and never log it.
pub struct AppCode(String);

impl AppCode {
    /// The code in groups of four, such as `abcd efgh ijkl`. Apps take it
    /// with or without the spaces.
    pub fn grouped(&self) -> String {
        random::grouped(&self.0, 4)
    }
}

impl fmt::Debug for AppCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AppCode(..)")
    }
}

impl Identity {
    /// Makes a code that adds an app for whoever is signed in, replacing
    /// any code they made before. Returns it with when it stops working.
    ///
    /// Adding a device is a sensitive action, so only allow it when the
    /// user has signed in recently, as for app passwords.
    pub async fn make_app_code(&self, signed_in: &SignedIn) -> Result<(AppCode, String), Error> {
        let code = random::letters(LEN);
        let hash = Sha256::digest(code.as_bytes()).to_vec();
        let user_id = signed_in.user.id;
        let session_id = signed_in.session.id;
        let expires_at = self
            .db
            .write(move |tx| {
                Ok::<_, Error>(tx.query_row(
                    concat!(
                        "INSERT INTO identity_app_codes (user_id, code_hash, session_id, \
                         created_at, expires_at)
                         VALUES (?1, ?2, ?3, ",
                        sql_now!(),
                        ", ",
                        sql_now!("+10 minutes"),
                        ")
                         ON CONFLICT (user_id) DO UPDATE SET code_hash = excluded.code_hash,
                             session_id = excluded.session_id, created_at = excluded.created_at,
                             expires_at = excluded.expires_at
                         RETURNING expires_at"
                    ),
                    (user_id.as_uuid(), hash, session_id.as_uuid()),
                    |row| row.get(0),
                )?)
            })
            .await?;
        Ok((AppCode(code), expires_at))
    }

    /// Trades a code for a session for the app that typed or scanned it,
    /// named `device_name` in the device list. `None` if the code is wrong,
    /// used, or expired, or if the session that made it has since ended or
    /// its account was turned off.
    pub async fn redeem_app_code(
        &self,
        code: &str,
        device_name: &str,
    ) -> Result<Option<(SessionToken, Session)>, Error> {
        let Some(code) = random::parse_letters(code, LEN) else {
            return Ok(None);
        };
        let hash = Sha256::digest(code.as_bytes()).to_vec();
        let (token, token_hash) = new_token();
        let id = SessionId::generate();
        let device_name = clean_device_name(device_name);
        let actor = self.actor;
        self.db
            .write(move |tx| {
                // Used up whether or not it works, so nobody can try it twice.
                let found: Option<(uuid::Uuid, uuid::Uuid, bool)> = tx
                    .query_row(
                        concat!(
                            "DELETE FROM identity_app_codes WHERE code_hash = ?1
                             RETURNING user_id, session_id, expires_at > ",
                            sql_now!()
                        ),
                        [&hash],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )
                    .optional()?;
                let Some((user_id, session_id, live)) = found else {
                    return Ok(None);
                };
                if !live {
                    return Ok(None);
                }
                // How and when the person last proved who they are, in the
                // session that made the code, if it's still signed in.
                let proved: Option<(String, String)> = tx
                    .query_row(
                        "SELECT s.auth_method, s.authenticated_at
                         FROM identity_sessions s JOIN identity_users u ON u.id = s.user_id
                         WHERE s.id = ?1 AND u.turned_off_at IS NULL",
                        [session_id],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .optional()?;
                let Some((auth_method, authenticated_at)) = proved else {
                    return Ok(None);
                };
                tx.execute(
                    concat!(
                        "INSERT INTO identity_sessions (id, user_id, token_hash, client, \
                         device_name, auth_method, authenticated_at, created_at, last_used_at)
                         VALUES (?1, ?2, ?3, 'app', ?4, ?5, ?6, ",
                        sql_now!(),
                        ", ",
                        sql_now!(),
                        ")"
                    ),
                    (
                        id.as_uuid(),
                        user_id,
                        token_hash,
                        &device_name,
                        auth_method,
                        authenticated_at,
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
                let actor = Actor {
                    user: Some(UserId::from_uuid(user_id)),
                    via: Via::App,
                    ..actor
                };
                let mut details =
                    serde_json::json!({ "device": session.device_name, "code": true });
                if let Some(address) = actor.address {
                    details["address"] = address.to_string().into();
                }
                record(tx, actor, NewEvent::new(Action::SignedIn).details(details))?;
                Ok(Some((token, session)))
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use accordion_types::Role;

    use super::*;
    use crate::db::Database;
    use crate::identity::{AuthMethod, Client, NewUser};

    async fn start() -> (tempfile::TempDir, Identity, SignedIn) {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir.path().join("accordion.db")).unwrap();
        let identity = Identity::new(db).await.unwrap();
        let user = identity
            .create_user(NewUser {
                username: "alex".into(),
                display_name: "Alex".into(),
                role: Role::User,
                password: "correct horse battery".into(),
            })
            .await
            .unwrap();
        let (token, _) = identity
            .create_session(
                user.id,
                Client::Browser,
                "Firefox on Linux",
                AuthMethod::Passkey,
            )
            .await
            .unwrap();
        let signed_in = identity
            .check_session(token.as_str())
            .await
            .unwrap()
            .unwrap();
        (dir, identity, signed_in)
    }

    #[tokio::test]
    async fn a_code_adds_one_app_once() {
        let (_dir, identity, browser) = start().await;
        let (code, expires_at) = identity.make_app_code(&browser).await.unwrap();
        assert!(expires_at > browser.session.created_at);
        let typed = code.grouped().to_uppercase();
        assert_eq!(typed.len(), 14);

        let (token, session) = identity
            .redeem_app_code(&typed, "Alex's laptop")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(session.client, Client::App);
        assert_eq!(session.user_id, browser.user.id);
        assert_eq!(session.device_name, "Alex's laptop");
        // Signed in the way the browser was, and not freshly.
        assert_eq!(session.auth_method, AuthMethod::Passkey);
        assert_eq!(session.authenticated_at, browser.session.authenticated_at);
        let app = identity
            .check_session(token.as_str())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(app.user.id, browser.user.id);

        assert!(
            identity
                .redeem_app_code(&typed, "Someone else")
                .await
                .unwrap()
                .is_none(),
            "a code works once"
        );
    }

    #[tokio::test]
    async fn only_the_newest_code_works() {
        let (_dir, identity, browser) = start().await;
        let (old, _) = identity.make_app_code(&browser).await.unwrap();
        let (new, _) = identity.make_app_code(&browser).await.unwrap();
        assert!(
            identity
                .redeem_app_code(&old.grouped(), "Laptop")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            identity
                .redeem_app_code(&new.grouped(), "Laptop")
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn expired_or_orphaned_codes_dont_work() {
        let (_dir, identity, browser) = start().await;
        let (code, _) = identity.make_app_code(&browser).await.unwrap();
        identity
            .db
            .write(|tx| {
                tx.execute(
                    "UPDATE identity_app_codes SET expires_at = '2000-01-01T00:00:00.000Z'",
                    [],
                )
            })
            .await
            .unwrap();
        assert!(
            identity
                .redeem_app_code(&code.grouped(), "Laptop")
                .await
                .unwrap()
                .is_none()
        );

        // Signing out the browser that made a code stops it working.
        let (code, _) = identity.make_app_code(&browser).await.unwrap();
        identity
            .end_session(browser.user.id, browser.session.id)
            .await
            .unwrap();
        assert!(
            identity
                .redeem_app_code(&code.grouped(), "Laptop")
                .await
                .unwrap()
                .is_none()
        );

        for wrong in ["", "abc", "abcd efgh ijk1", "abcd efgh ijkl"] {
            assert!(
                identity
                    .redeem_app_code(wrong, "Laptop")
                    .await
                    .unwrap()
                    .is_none()
            );
        }
    }
}

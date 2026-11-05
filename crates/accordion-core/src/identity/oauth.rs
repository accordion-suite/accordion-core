//! Signing in Accordion's apps through the system browser, with OAuth 2.0
//! and PKCE (decision 3 in the `accordion` repo's `docs/decisions`, and
//! RFCs 6749, 7636, and 8252).
//!
//! The app opens the server's sign-in page in the system browser, with a
//! challenge made from a secret only it knows. Once the person signs in and
//! allows the app, the browser hands the app a code, and the app trades the
//! code and the secret for a session of its own. A code alone is useless,
//! so another app on the device that sees it can't sign in with it.
//!
//! Each code works once, for 2 minutes, and only for the app and address it
//! was made for. The database stores only a SHA-256 hash of it. The new
//! session counts as signed in the way the browser was, and at the same
//! time, so it isn't a fresh sign-in for sensitive actions.

use std::fmt;

use base64ct::{Base64UrlUnpadded, Encoding};
use rusqlite::OptionalExtension;
use sha2::{Digest, Sha256};

use super::app_code::new_app_session;
use super::session::{clean_device_name, new_token};
use super::{Error, Identity, Session, SessionToken, SignedIn};
use crate::sql_now;

const CODE_PREFIX: &str = "aoc_";
const CODE_LEN: usize = 32;

/// A code from approving an app. Hand it to the app once, and never log it.
pub struct AuthorizationCode(String);

impl AuthorizationCode {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for AuthorizationCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AuthorizationCode(..)")
    }
}

/// What the app asked for, which the code is bound to.
pub struct AppRequest<'a> {
    pub client_id: &'a str,
    /// Where the browser hands the code back, such as
    /// `http://127.0.0.1:53412/callback`.
    pub redirect_uri: &'a str,
    /// The PKCE challenge: SHA-256 of the app's secret, in base64url.
    pub code_challenge: &'a str,
    /// What the app calls the device, such as "Sam's laptop".
    pub device_name: &'a str,
}

/// Whether `challenge` is a SHA-256 PKCE challenge: 32 bytes in base64url.
pub fn is_challenge(challenge: &str) -> bool {
    challenge.len() == 43
        && Base64UrlUnpadded::decode_vec(challenge).is_ok_and(|bytes| bytes.len() == 32)
}

/// The challenge for `verifier`, or `None` if it isn't a PKCE verifier:
/// 43 to 128 letters, digits, and `-._~`.
fn challenge_for(verifier: &str) -> Option<String> {
    let valid = (43..=128).contains(&verifier.len())
        && verifier
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-._~".contains(&byte));
    valid.then(|| Base64UrlUnpadded::encode_string(&Sha256::digest(verifier.as_bytes())))
}

impl Identity {
    /// Makes a code for the app in `request`, once the person signed in as
    /// `signed_in` allows it.
    ///
    /// Adding a device is a sensitive action, so only allow it when the
    /// user has signed in recently, as for app codes.
    pub async fn approve_app(
        &self,
        signed_in: &SignedIn,
        request: AppRequest<'_>,
    ) -> Result<AuthorizationCode, Error> {
        let mut secret = [0u8; CODE_LEN];
        getrandom::fill(&mut secret).expect("the OS random number generator failed");
        let code = format!("{CODE_PREFIX}{}", Base64UrlUnpadded::encode_string(&secret));
        let hash = Sha256::digest(code.as_bytes()).to_vec();
        let session_id = signed_in.session.id;
        let client_id = request.client_id.to_owned();
        let redirect_uri = request.redirect_uri.to_owned();
        let challenge = request.code_challenge.to_owned();
        let device_name = clean_device_name(request.device_name);
        self.db
            .write(move |tx| {
                tx.execute(
                    concat!(
                        "DELETE FROM identity_oauth_codes WHERE expires_at <= ",
                        sql_now!()
                    ),
                    [],
                )?;
                tx.execute(
                    concat!(
                        "INSERT INTO identity_oauth_codes (code_hash, session_id, client_id, \
                         redirect_uri, code_challenge, device_name, expires_at)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ",
                        sql_now!("+2 minutes"),
                        ")"
                    ),
                    (
                        hash,
                        session_id.as_uuid(),
                        client_id,
                        redirect_uri,
                        challenge,
                        device_name,
                    ),
                )?;
                Ok::<_, Error>(())
            })
            .await?;
        Ok(AuthorizationCode(code))
    }

    /// Trades a code for a session for the app it was made for. `None` if
    /// the code is wrong, used, or expired, if the app, address, or secret
    /// don't match, or if the browser session that allowed it has since
    /// ended or its account was turned off. The code is used up either way.
    pub async fn exchange_app_code(
        &self,
        code: &str,
        client_id: &str,
        redirect_uri: &str,
        code_verifier: &str,
    ) -> Result<Option<(SessionToken, Session)>, Error> {
        if !code.starts_with(CODE_PREFIX) {
            return Ok(None);
        }
        let Some(challenge) = challenge_for(code_verifier) else {
            return Ok(None);
        };
        let hash = Sha256::digest(code.as_bytes()).to_vec();
        let client_id = client_id.to_owned();
        let redirect_uri = redirect_uri.to_owned();
        let new = new_token();
        let actor = self.actor;
        self.db
            .write(move |tx| {
                // Used up whether or not it works, so nobody can try it twice.
                let found: Option<(uuid::Uuid, String, String, String, String, bool)> = tx
                    .query_row(
                        concat!(
                            "DELETE FROM identity_oauth_codes WHERE code_hash = ?1
                             RETURNING session_id, client_id, redirect_uri, code_challenge, \
                             device_name, expires_at > ",
                            sql_now!()
                        ),
                        [&hash],
                        |row| {
                            Ok((
                                row.get(0)?,
                                row.get(1)?,
                                row.get(2)?,
                                row.get(3)?,
                                row.get(4)?,
                                row.get(5)?,
                            ))
                        },
                    )
                    .optional()?;
                let Some((session_id, wanted_client, wanted_uri, wanted_challenge, device, live)) =
                    found
                else {
                    return Ok(None);
                };
                if !live
                    || wanted_client != client_id
                    || wanted_uri != redirect_uri
                    || wanted_challenge != challenge
                {
                    return Ok(None);
                }
                new_app_session(tx, session_id, &device, new, actor, "browser")
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

    const VERIFIER: &str = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
    /// RFC 7636's example challenge for `VERIFIER`.
    const CHALLENGE: &str = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM";
    const REDIRECT: &str = "http://127.0.0.1:53412/callback";

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

    fn request() -> AppRequest<'static> {
        AppRequest {
            client_id: "accordion-sync",
            redirect_uri: REDIRECT,
            code_challenge: CHALLENGE,
            device_name: "Alex's laptop",
        }
    }

    #[test]
    fn checks_challenges_and_verifiers() {
        assert_eq!(challenge_for(VERIFIER).as_deref(), Some(CHALLENGE));
        assert!(is_challenge(CHALLENGE));
        assert!(!is_challenge("plain"));
        assert!(!is_challenge(&format!("{CHALLENGE}A")));
        assert!(challenge_for("too short").is_none());
        assert!(challenge_for(&format!("{VERIFIER} ")).is_none());
        assert!(challenge_for(&"a".repeat(129)).is_none());
    }

    #[tokio::test]
    async fn a_code_signs_in_the_app_it_was_made_for_once() {
        let (_dir, identity, browser) = start().await;
        let code = identity.approve_app(&browser, request()).await.unwrap();
        assert!(code.as_str().starts_with("aoc_"));
        assert_eq!(format!("{code:?}"), "AuthorizationCode(..)");

        let (token, session) = identity
            .exchange_app_code(code.as_str(), "accordion-sync", REDIRECT, VERIFIER)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(session.client, Client::App);
        assert_eq!(session.user_id, browser.user.id);
        assert_eq!(session.device_name, "Alex's laptop");
        assert_eq!(session.auth_method, AuthMethod::Passkey);
        assert_eq!(session.authenticated_at, browser.session.authenticated_at);
        assert!(
            identity
                .check_session(token.as_str())
                .await
                .unwrap()
                .is_some()
        );

        assert!(
            identity
                .exchange_app_code(code.as_str(), "accordion-sync", REDIRECT, VERIFIER)
                .await
                .unwrap()
                .is_none(),
            "a code works once"
        );
    }

    #[tokio::test]
    async fn a_code_needs_its_secret_app_and_address() {
        let (_dir, identity, browser) = start().await;
        let other = "a".repeat(43);
        for (client, redirect, verifier) in [
            ("accordion-sync", REDIRECT, other.as_str()),
            ("accordion-desktop", REDIRECT, VERIFIER),
            ("accordion-sync", "http://127.0.0.1:1/callback", VERIFIER),
        ] {
            let code = identity.approve_app(&browser, request()).await.unwrap();
            assert!(
                identity
                    .exchange_app_code(code.as_str(), client, redirect, verifier)
                    .await
                    .unwrap()
                    .is_none(),
                "{client} {redirect} {verifier}"
            );
            // A wrong try uses the code up, so the secret can't be guessed.
            assert!(
                identity
                    .exchange_app_code(code.as_str(), "accordion-sync", REDIRECT, VERIFIER)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        for wrong in ["", "aoc_", "acs_whatever"] {
            assert!(
                identity
                    .exchange_app_code(wrong, "accordion-sync", REDIRECT, VERIFIER)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
    }

    #[tokio::test]
    async fn expired_or_orphaned_codes_dont_work() {
        let (_dir, identity, browser) = start().await;
        let code = identity.approve_app(&browser, request()).await.unwrap();
        identity
            .db
            .write(|tx| {
                tx.execute(
                    "UPDATE identity_oauth_codes SET expires_at = '2000-01-01T00:00:00.000Z'",
                    [],
                )
            })
            .await
            .unwrap();
        assert!(
            identity
                .exchange_app_code(code.as_str(), "accordion-sync", REDIRECT, VERIFIER)
                .await
                .unwrap()
                .is_none()
        );

        // Signing out the browser that allowed it stops the code working.
        let code = identity.approve_app(&browser, request()).await.unwrap();
        identity
            .end_session(browser.user.id, browser.session.id)
            .await
            .unwrap();
        assert!(
            identity
                .exchange_app_code(code.as_str(), "accordion-sync", REDIRECT, VERIFIER)
                .await
                .unwrap()
                .is_none()
        );
    }
}

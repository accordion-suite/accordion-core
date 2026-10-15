//! Password hashing with Argon2id.
//!
//! Hashing is slow on purpose, roughly 50–100 ms on a Raspberry Pi 5, so it
//! runs on Tokio's blocking pool. Never hash inside a database job, or every
//! other write waits.

use std::sync::LazyLock;

use argon2::{Argon2, PasswordHasher, PasswordVerifier};

/// NIST SP 800-63B's minimum. Length matters more than character rules, so
/// Accordion doesn't require digits or symbols.
pub const MIN_LEN: usize = 8;

/// Long enough for any passphrase or password manager, and short enough that
/// nobody can make the server hash megabytes at a time.
pub const MAX_LEN: usize = 256;

#[derive(Debug, thiserror::Error)]
#[error("Argon2 failed: {0}")]
pub struct HashError(argon2::password_hash::Error);

/// Hashes `password` with a new random salt. Returns a PHC string, which
/// records the algorithm and parameters next to the hash, so parameters can
/// change later without breaking existing hashes.
pub async fn hash(password: String) -> Result<String, HashError> {
    tokio::task::spawn_blocking(move || {
        Argon2::default()
            .hash_password(password.as_bytes())
            .map(|hash| hash.to_string())
            .map_err(HashError)
    })
    .await
    .expect("password hashing panicked")
}

/// Checks `password` against `hash`, a PHC string from [`hash`].
///
/// Pass `None` when the account doesn't exist or has no password. This still
/// spends the time a real check takes, so response times don't reveal which
/// usernames exist. It always returns `false`.
pub async fn verify(password: String, hash: Option<String>) -> bool {
    tokio::task::spawn_blocking(move || {
        let (hash, real) = match &hash {
            Some(hash) => (hash.as_str(), true),
            None => (DUMMY_HASH.as_str(), false),
        };
        let matches = Argon2::default()
            .verify_password(password.as_bytes(), hash)
            .is_ok();
        matches && real
    })
    .await
    .expect("password verification panicked")
}

static DUMMY_HASH: LazyLock<String> = LazyLock::new(|| {
    Argon2::default()
        .hash_password(b"accordion-dummy-password")
        .expect("hashing a fixed password can't fail")
        .to_string()
});

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn verifies_only_the_right_password() {
        let hash = hash("correct horse".into()).await.unwrap();
        assert!(hash.starts_with("$argon2id$"));
        assert!(verify("correct horse".into(), Some(hash.clone())).await);
        assert!(!verify("wrong horse".into(), Some(hash)).await);
    }

    #[tokio::test]
    async fn salts_every_hash() {
        let first = hash("correct horse".into()).await.unwrap();
        let second = hash("correct horse".into()).await.unwrap();
        assert_ne!(first, second);
    }

    #[tokio::test]
    async fn a_missing_hash_never_matches() {
        assert!(!verify("accordion-dummy-password".into(), None).await);
    }

    #[tokio::test]
    async fn a_corrupt_hash_never_matches() {
        assert!(!verify("correct horse".into(), Some("not a hash".into())).await);
    }
}

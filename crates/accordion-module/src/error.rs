//! API errors, returned as JSON: `{"error": "<message for the user>"}`.
//! Some also carry a `code` that apps check instead of the message, which
//! can change and will be translated: `{"error": "...", "code": "..."}`.

use std::time::Duration;

use accordion_core::{identity, storage};
use axum::Json;
use axum::http::StatusCode;
use axum::http::header::RETRY_AFTER;
use axum::response::{IntoResponse, Response};

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    message: String,
    code: Option<&'static str>,
    retry_after: Option<Duration>,
}

/// The code for [`ApiError::password_needed`].
pub const PASSWORD_NEEDED: &str = "password_needed";

impl ApiError {
    /// An error with a message for the user. Say what happened and what to
    /// do next.
    pub fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
            code: None,
            retry_after: None,
        }
    }

    /// A sensitive action needs a sign-in from the last 15 minutes. Apps ask
    /// for the password, confirm it with `POST /api/session/confirm`, and
    /// try again. The message says what the password is for, such as
    /// "For your security, enter your password again before you see the
    /// recovery key."
    pub fn password_needed(message: impl Into<String>) -> Self {
        Self {
            code: Some(PASSWORD_NEEDED),
            ..Self::new(StatusCode::FORBIDDEN, message)
        }
    }

    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn code(&self) -> Option<&'static str> {
        self.code
    }

    pub fn server_problem() -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Accordion ran into a problem on the server. Try again in a moment. \
             If it keeps happening, ask your admin to check the server log.",
        )
    }

    pub fn not_signed_in() -> Self {
        Self::new(
            StatusCode::UNAUTHORIZED,
            "You're not signed in, or your session has ended. Sign in, then try again.",
        )
    }

    pub fn not_found() -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            "That doesn't exist, or you don't have access to it.",
        )
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = match self.code {
            Some(code) => Json(serde_json::json!({ "error": self.message, "code": code })),
            None => Json(serde_json::json!({ "error": self.message })),
        };
        match self.retry_after {
            Some(wait) => {
                // Round up, so a client that waits exactly this long gets in.
                let secs = wait.as_secs() + u64::from(wait.subsec_nanos() > 0);
                (self.status, [(RETRY_AFTER, secs.max(1).to_string())], body).into_response()
            }
            None => (self.status, body).into_response(),
        }
    }
}

impl From<identity::Error> for ApiError {
    fn from(err: identity::Error) -> Self {
        use identity::Error::*;
        let status = match err {
            InvalidUsername | InvalidDisplayName | PasswordTooShort | PasswordTooLong
            | RoleNotAllowed => StatusCode::BAD_REQUEST,
            UsernameTaken | UsernameReserved | OwnerExists | OwnerAccount => StatusCode::CONFLICT,
            UserNotFound => StatusCode::NOT_FOUND,
            TurnedOff => StatusCode::FORBIDDEN,
            LinkNotValid => StatusCode::GONE,
            WrongCredentials => StatusCode::UNAUTHORIZED,
            TooManyTries { wait } => {
                return Self {
                    retry_after: Some(wait),
                    ..Self::new(StatusCode::TOO_MANY_REQUESTS, err.to_string())
                };
            }
            Database(_) | Hash(_) => {
                // The details are for the admin's log, not the user's screen.
                tracing::error!("identity error: {err}");
                return Self::server_problem();
            }
        };
        Self::new(status, err.to_string())
    }
}

impl From<storage::Error> for ApiError {
    fn from(err: storage::Error) -> Self {
        use storage::Error::*;
        let status = match err {
            InvalidName | GiveToSelf => StatusCode::BAD_REQUEST,
            NotFound | UserNotFound | UploadNotFound => StatusCode::NOT_FOUND,
            NameTaken { .. }
            | NotAFolder
            | NotAFile
            | TopFolder
            | MoveIntoItself
            | DifferentOwner
            | InTrash
            | NotInTrash
            | UploadOffset { .. }
            | UploadIncomplete { .. } => StatusCode::CONFLICT,
            QuotaFull => StatusCode::INSUFFICIENT_STORAGE,
            UploadTooLong { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            Identity(err) => return err.into(),
            NoHardLinks(_) | Io(_) | Database(_) => {
                tracing::error!("storage error: {err}");
                return Self::server_problem();
            }
        };
        Self::new(status, err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn body(error: ApiError) -> (StatusCode, serde_json::Value) {
        let response = error.into_response();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[tokio::test]
    async fn sends_a_code_only_with_errors_that_have_one() {
        let (status, json) = body(ApiError::not_found()).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(json.get("code"), None);

        let (status, json) = body(ApiError::password_needed("Enter it again.")).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(json["error"], "Enter it again.");
        assert_eq!(json["code"], PASSWORD_NEEDED);
    }
}

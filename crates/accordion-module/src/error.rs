//! API errors, returned as JSON: `{"error": "<message for the user>"}`.

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
    retry_after: Option<Duration>,
}

impl ApiError {
    /// An error with a message for the user. Say what happened and what to
    /// do next.
    pub fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
            retry_after: None,
        }
    }

    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn message(&self) -> &str {
        &self.message
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
        let body = Json(serde_json::json!({ "error": self.message }));
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
            NameTaken
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

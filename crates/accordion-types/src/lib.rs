//! Shared types and protocol definitions.
//!
//! The server, every module, and the desktop and mobile apps all depend on
//! this crate, so they agree on the shape of the data they exchange.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Defines a UUIDv7 ID type. IDs sort by creation time and index well in
/// SQLite.
macro_rules! id_type {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            /// Creates a new, unique ID.
            pub fn generate() -> Self {
                Self(Uuid::now_v7())
            }

            pub fn from_uuid(uuid: Uuid) -> Self {
                Self(uuid)
            }

            pub fn as_uuid(&self) -> Uuid {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

id_type!(
    /// The ID of a user account. It never changes, unlike the username, so
    /// other data refers to users by ID.
    UserId
);

id_type!(
    /// The ID of a sign-in session: one browser or app that's signed in.
    /// Unlike the session's token, it's safe to show and to log.
    SessionId
);

id_type!(
    /// The ID of an app password: the password that one standard client,
    /// such as a WebDAV mount or a mail app, signs in with.
    AppPasswordId
);

id_type!(
    /// The ID of a file or folder in storage. It stays the same when the
    /// item is renamed, moved, or changed, so sync clients can follow it.
    ItemId
);

id_type!(
    /// The ID of one saved version of a file.
    VersionId
);

id_type!(
    /// The ID of an upload that can resume after a dropped connection.
    UploadId
);

/// What a user account is allowed to do.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    /// The person who set up the server. There's exactly one.
    Owner,
    /// Manages users, modules, and server settings.
    Admin,
    /// A standard account.
    User,
    /// Can open only what's shared with them by link.
    Guest,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Owner => "owner",
            Self::Admin => "admin",
            Self::User => "user",
            Self::Guest => "guest",
        }
    }

    /// Whether this role can use the admin panel.
    pub fn is_admin(self) -> bool {
        matches!(self, Self::Owner | Self::Admin)
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct UnknownRole;

impl fmt::Display for UnknownRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("unknown role")
    }
}

impl std::error::Error for UnknownRole {}

impl FromStr for Role {
    type Err = UnknownRole;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "owner" => Ok(Self::Owner),
            "admin" => Ok(Self::Admin),
            "user" => Ok(Self::User),
            "guest" => Ok(Self::Guest),
            _ => Err(UnknownRole),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_round_trips_through_its_name() {
        for role in [Role::Owner, Role::Admin, Role::User, Role::Guest] {
            assert_eq!(role.as_str().parse(), Ok(role));
        }
        assert_eq!("Owner".parse::<Role>(), Err(UnknownRole));
    }

    #[test]
    fn user_ids_sort_by_creation() {
        let first = UserId::generate();
        let second = UserId::generate();
        assert!(first < second);
    }
}

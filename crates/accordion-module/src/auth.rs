//! Finds the signed-in user for a request (decision 3 in the `accordion`
//! repo's `docs/decisions`).
//!
//! The web app sends its session token in a cookie, and the desktop and
//! mobile apps send it as a bearer token. Standard clients, such as WebDAV
//! mounts, send a username and app password instead.
//!
//! Both checks find identity in the router's state, so a module's state only
//! has to implement `FromRef` for [`Identity`].

use accordion_core::identity::{
    AppPasswordUse, Identity, SessionToken, SignedIn, UsingAppPassword,
};
use axum::extract::{FromRef, FromRequestParts};
use axum::http::header::{AUTHORIZATION, COOKIE, WWW_AUTHENTICATE};
use axum::http::request::Parts;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use base64ct::{Base64, Encoding};

use crate::ApiError;

/// The `__Host-` prefix makes browsers refuse the cookie unless it's
/// `Secure`, has `Path=/`, and has no `Domain`, so no other site or
/// subdomain can set or overwrite it.
pub const COOKIE_NAME: &str = "__Host-accordion-session";

/// The server ends idle sessions itself. The cookie only has to outlast them,
/// and 400 days is the longest that browsers keep a cookie.
const COOKIE_MAX_AGE_SECS: u32 = 400 * 24 * 60 * 60;

/// A request from a signed-in user. Add it as a handler argument to require
/// a sign-in. Requests without a live session get a 401 response.
pub struct Authenticated(pub SignedIn);

impl<S> FromRequestParts<S> for Authenticated
where
    Identity: FromRef<S>,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, ApiError> {
        let token = token_from(&parts.headers).ok_or_else(ApiError::not_signed_in)?;
        let signed_in = Identity::from_ref(state)
            .check_session(token)
            .await?
            .ok_or_else(ApiError::not_signed_in)?;
        Ok(Self(signed_in))
    }
}

/// A request from a standard client that signed in with an app password for
/// files, calendars, and contacts. Requests without one get a 401 response
/// that asks the client for a username and password.
///
/// The account password never works here, because these protocols can't ask
/// for a second sign-in step.
pub struct DavUser(pub UsingAppPassword);

impl<S> FromRequestParts<S> for DavUser
where
    Identity: FromRef<S>,
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Response> {
        let Some((username, password)) = basic_credentials(&parts.headers) else {
            return Err(ask_for_app_password());
        };
        match Identity::from_ref(state)
            .check_app_password(&username, &password, AppPasswordUse::Dav)
            .await
        {
            Ok(Some(using)) => Ok(Self(using)),
            Ok(None) => Err(ask_for_app_password()),
            Err(err) => Err(ApiError::from(err).into_response()),
        }
    }
}

fn ask_for_app_password() -> Response {
    let mut response = ApiError::new(
        StatusCode::UNAUTHORIZED,
        "Sign in with your username and an app password. Your account password doesn't \
         work here. Create an app password in your account settings, then try again.",
    )
    .into_response();
    response.headers_mut().insert(
        WWW_AUTHENTICATE,
        HeaderValue::from_static(r#"Basic realm="Accordion", charset="UTF-8""#),
    );
    response
}

fn token_from(headers: &HeaderMap) -> Option<&str> {
    if let Some(value) = headers.get(AUTHORIZATION) {
        return value.to_str().ok()?.strip_prefix("Bearer ");
    }
    headers
        .get_all(COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(';'))
        .find_map(|pair| {
            let (name, value) = pair.trim().split_once('=')?;
            (name == COOKIE_NAME).then_some(value)
        })
}

/// The username and password from a `Basic` authorization header.
fn basic_credentials(headers: &HeaderMap) -> Option<(String, String)> {
    let value = headers.get(AUTHORIZATION)?.to_str().ok()?;
    let (scheme, encoded) = value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("basic") {
        return None;
    }
    let decoded = Base64::decode_vec(encoded.trim()).ok()?;
    let decoded = String::from_utf8(decoded).ok()?;
    let (username, password) = decoded.split_once(':')?;
    Some((username.to_owned(), password.to_owned()))
}

pub fn session_cookie(token: &SessionToken) -> HeaderValue {
    HeaderValue::try_from(format!(
        "{COOKIE_NAME}={}; Path=/; Max-Age={COOKIE_MAX_AGE_SECS}; Secure; HttpOnly; SameSite=Lax",
        token.as_str()
    ))
    .expect("session tokens are always valid header text")
}

pub fn removal_cookie() -> HeaderValue {
    HeaderValue::from_static(
        "__Host-accordion-session=; Path=/; Max-Age=0; Secure; HttpOnly; SameSite=Lax",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&'static str, &'static str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for &(name, value) in pairs {
            map.append(name, HeaderValue::from_static(value));
        }
        map
    }

    #[test]
    fn finds_the_token_in_a_cookie_or_bearer_header() {
        let cookie = headers(&[(
            "cookie",
            "theme=dark; __Host-accordion-session=acs_abc; x=1",
        )]);
        assert_eq!(token_from(&cookie), Some("acs_abc"));

        let bearer = headers(&[("authorization", "Bearer acs_xyz")]);
        assert_eq!(token_from(&bearer), Some("acs_xyz"));

        let neither = headers(&[("cookie", "accordion-session=acs_abc")]);
        assert_eq!(token_from(&neither), None);

        let basic = headers(&[("authorization", "Basic YWxleDpwYXNz")]);
        assert_eq!(token_from(&basic), None);
    }

    #[test]
    fn reads_basic_credentials() {
        // alex:abcd efgh:ijkl, with a colon in the password.
        let basic = headers(&[("authorization", "Basic YWxleDphYmNkIGVmZ2g6aWprbA==")]);
        assert_eq!(
            basic_credentials(&basic),
            Some(("alex".into(), "abcd efgh:ijkl".into()))
        );
        let lowercase = headers(&[("authorization", "basic YWxleDpwYXNz")]);
        assert_eq!(
            basic_credentials(&lowercase),
            Some(("alex".into(), "pass".into()))
        );
        for bad in [
            "Bearer acs_xyz",
            "Basic not base64!",
            // "alexpass", with no colon.
            "Basic YWxleHBhc3M=",
            "Basic",
        ] {
            let map = headers(&[("authorization", bad)]);
            assert_eq!(basic_credentials(&map), None, "{bad}");
        }
    }

    #[test]
    fn the_removal_cookie_matches_the_session_cookie() {
        let removal = removal_cookie();
        assert!(
            removal
                .to_str()
                .unwrap()
                .starts_with(&format!("{COOKIE_NAME}=;"))
        );
    }
}

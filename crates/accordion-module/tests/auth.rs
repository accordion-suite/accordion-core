//! Signs in to a module's routes with a session or an app password.

use accordion_core::db::Database;
use accordion_core::identity::{AppPasswordUse, AuthMethod, Client, Identity, NewUser};
use accordion_module::{Authenticated, DavUser};
use accordion_types::Role;
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::get;
use base64ct::{Base64, Encoding};
use tower::ServiceExt;

async fn start() -> (tempfile::TempDir, Identity, Router) {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(&dir.path().join("accordion.db")).unwrap();
    let identity = Identity::new(db).await.unwrap();
    identity
        .create_user(NewUser {
            username: "alex".into(),
            display_name: "Alex Kim".into(),
            role: Role::Owner,
            password: "correct horse".into(),
        })
        .await
        .unwrap();
    let router = Router::new()
        .route(
            "/api",
            get(|Authenticated(signed_in): Authenticated| async move { signed_in.user.username }),
        )
        .route(
            "/dav",
            get(|DavUser(using): DavUser| async move { using.user.username }),
        )
        .with_state(identity.clone());
    (dir, identity, router)
}

async fn get_with(router: &Router, path: &str, authorization: Option<String>) -> StatusCode {
    let mut request = Request::get(path);
    if let Some(value) = authorization {
        request = request.header("authorization", value);
    }
    let response = router
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    if response.status() == StatusCode::UNAUTHORIZED && path == "/dav" {
        assert_eq!(
            response.headers()["www-authenticate"],
            r#"Basic realm="Accordion", charset="UTF-8""#
        );
    }
    response.status()
}

fn basic(username: &str, password: &str) -> Option<String> {
    Some(format!(
        "Basic {}",
        Base64::encode_string(format!("{username}:{password}").as_bytes())
    ))
}

#[tokio::test]
async fn sessions_sign_in_to_the_api() {
    let (_dir, identity, router) = start().await;
    let alex = identity.user_by_username("alex").await.unwrap().unwrap();
    let (token, _) = identity
        .create_session(alex.id, Client::App, "Phone", AuthMethod::Password)
        .await
        .unwrap();

    let bearer = Some(format!("Bearer {}", token.as_str()));
    assert_eq!(
        get_with(&router, "/api", bearer.clone()).await,
        StatusCode::OK
    );
    assert_eq!(
        get_with(&router, "/api", None).await,
        StatusCode::UNAUTHORIZED
    );
    // A session token isn't a WebDAV password.
    assert_eq!(
        get_with(&router, "/dav", bearer).await,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn only_app_passwords_for_files_sign_in_to_webdav() {
    let (_dir, identity, router) = start().await;
    let alex = identity.user_by_username("alex").await.unwrap().unwrap();
    let (files, _) = identity
        .create_app_password(alex.id, AppPasswordUse::Dav, "Laptop")
        .await
        .unwrap();
    let (mail, _) = identity
        .create_app_password(alex.id, AppPasswordUse::Mail, "Phone")
        .await
        .unwrap();

    assert_eq!(
        get_with(&router, "/dav", basic("alex", &files.grouped())).await,
        StatusCode::OK
    );
    for refused in [
        None,
        basic("alex", "correct horse"),
        basic("alex", mail.as_str()),
        basic("sam", files.as_str()),
    ] {
        assert_eq!(
            get_with(&router, "/dav", refused).await,
            StatusCode::UNAUTHORIZED
        );
    }
    // An app password isn't a session.
    assert_eq!(
        get_with(&router, "/api", basic("alex", files.as_str())).await,
        StatusCode::UNAUTHORIZED
    );
}

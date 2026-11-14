//! Co-editing over a real WebSocket: sign-in before the upgrade, edits
//! passing between browsers, and close codes.

use std::sync::Arc;
use std::time::Duration;

use accordion_core::coedit::{self, CoEditing, Format, FormatError, Timing};
use accordion_core::db::Database;
use accordion_core::identity::{AuthMethod, Client, Identity, NewUser, SessionToken};
use accordion_core::storage::Storage;
use accordion_module::auth::COOKIE_NAME;
use accordion_module::{ApiError, Authenticated};
use accordion_types::{ItemId, Role, SessionId, UserId};
use axum::Router;
use axum::extract::ws::WebSocketUpgrade;
use axum::extract::{FromRef, Path, State};
use axum::response::Response;
use axum::routing::get;
use futures_util::{SinkExt, StreamExt};
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::{self, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use yrs::sync::{Message as YMessage, SyncMessage};
use yrs::updates::encoder::Encode;
use yrs::{Doc, GetString, OffsetKind, Options, ReadTxn, Text, Transact};

struct Plain;

impl Format for Plain {
    fn name(&self) -> &'static str {
        "plain"
    }

    fn absorb(&self, doc: &Doc, contents: &[u8]) -> Result<(), FormatError> {
        let new = std::str::from_utf8(contents).map_err(|err| FormatError(err.to_string()))?;
        let body = doc.get_or_insert_text("body");
        coedit::set_text(&mut doc.transact_mut(), &body, new);
        Ok(())
    }

    fn write(&self, doc: &Doc) -> Result<Vec<u8>, FormatError> {
        let body = doc.get_or_insert_text("body");
        Ok(body.get_string(&doc.transact()).into_bytes())
    }
}

#[derive(Clone)]
struct AppState {
    identity: Identity,
    coediting: CoEditing,
}

impl FromRef<AppState> for Identity {
    fn from_ref(state: &AppState) -> Self {
        state.identity.clone()
    }
}

async fn live(
    Authenticated(signed_in): Authenticated,
    Path(item): Path<String>,
    State(state): State<AppState>,
    socket: WebSocketUpgrade,
) -> Result<Response, ApiError> {
    let item: ItemId = item.parse().map_err(|_| ApiError::not_found())?;
    let session = Some(signed_in.session.id);
    let connection = state
        .coediting
        .join(item, signed_in.user.id, session, Arc::new(Plain))
        .await?;
    Ok(accordion_module::coedit::upgrade(socket, connection))
}

struct Server {
    _dir: tempfile::TempDir,
    address: std::net::SocketAddr,
    identity: Identity,
    storage: Storage,
    coediting: CoEditing,
    alex: UserId,
}

async fn start() -> Server {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(&dir.path().join("accordion.db")).unwrap();
    let identity = Identity::new(db.clone()).await.unwrap();
    let storage = Storage::open(db.clone(), identity.clone(), &dir.path().join("storage"))
        .await
        .unwrap();
    let timing = Timing {
        save_after: Duration::from_secs(3600),
        ..Timing::default()
    };
    let coediting = CoEditing::open(db, storage.clone(), identity.clone(), timing)
        .await
        .unwrap();
    let alex = identity
        .create_user(NewUser {
            username: "alex".into(),
            display_name: "Alex Kim".into(),
            role: Role::Owner,
            password: "correct horse".into(),
        })
        .await
        .unwrap()
        .id;
    let router = Router::new()
        .route("/live/{item}", get(live))
        .with_state(AppState {
            identity: identity.clone(),
            coediting: coediting.clone(),
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    Server {
        _dir: dir,
        address,
        identity,
        storage,
        coediting,
        alex,
    }
}

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

impl Server {
    async fn sign_in(&self) -> (SessionToken, SessionId) {
        let (token, session) = self
            .identity
            .create_session(self.alex, Client::Browser, "Laptop", AuthMethod::Password)
            .await
            .unwrap();
        (token, session.id)
    }

    async fn file(&self, contents: &str) -> ItemId {
        let top = self.storage.top_folder(self.alex).await.unwrap();
        self.storage
            .write_file(top, "plan.txt", contents.as_bytes())
            .await
            .unwrap()
            .id
    }

    async fn connect(
        &self,
        item: &str,
        token: Option<&SessionToken>,
    ) -> Result<Socket, tungstenite::Error> {
        let mut request = format!("ws://{}/live/{item}", self.address)
            .into_client_request()
            .unwrap();
        if let Some(token) = token {
            let cookie = format!("{COOKIE_NAME}={}", token.as_str());
            request
                .headers_mut()
                .insert("cookie", cookie.parse().unwrap());
        }
        tokio_tungstenite::connect_async(request)
            .await
            .map(|(socket, _)| socket)
    }
}

/// The next binary message, skipping pings.
async fn next_binary(socket: &mut Socket) -> Option<Vec<u8>> {
    loop {
        let next = tokio::time::timeout(Duration::from_secs(5), socket.next())
            .await
            .expect("a message within 5 seconds")?;
        match next.ok()? {
            Message::Binary(bytes) => return Some(bytes.to_vec()),
            Message::Close(_) => return None,
            _ => {}
        }
    }
}

/// The close code the server sends.
async fn close_code(socket: &mut Socket) -> CloseCode {
    loop {
        let next = tokio::time::timeout(Duration::from_secs(5), socket.next())
            .await
            .expect("closed within 5 seconds");
        match next {
            Some(Ok(Message::Close(Some(frame)))) => return frame.code,
            Some(Ok(_)) => {}
            other => panic!("closed without a code: {other:?}"),
        }
    }
}

#[tokio::test]
async fn edits_travel_between_browsers_and_reach_the_file() {
    let s = start().await;
    let item = s.file("Plan").await;
    let (token, _) = s.sign_in().await;
    let mut a = s.connect(&item.to_string(), Some(&token)).await.unwrap();
    let mut b = s.connect(&item.to_string(), Some(&token)).await.unwrap();

    // Each is first asked for what it has, as y-websocket expects.
    for socket in [&mut a, &mut b] {
        let first = next_binary(socket).await.unwrap();
        assert_eq!(first[..2], [0, 0], "sync step 1");
    }

    // A catches up, then types.
    let doc = Doc::with_options(Options {
        offset_kind: OffsetKind::Bytes,
        ..Options::default()
    });
    let step1 = YMessage::Sync(SyncMessage::SyncStep1(doc.transact().state_vector()));
    a.send(Message::Binary(step1.encode_v1().into()))
        .await
        .unwrap();
    let step2 = next_binary(&mut a).await.unwrap();
    assert_eq!(step2[..2], [0, 1], "sync step 2");
    let update = match yrs::updates::decoder::Decode::decode_v1(&step2[..]).unwrap() {
        YMessage::Sync(SyncMessage::SyncStep2(update)) => update,
        other => panic!("{other:?}"),
    };
    doc.transact_mut()
        .apply_update(yrs::updates::decoder::Decode::decode_v1(&update).unwrap())
        .unwrap();
    let body = doc.get_or_insert_text("body");
    assert_eq!(body.get_string(&doc.transact()), "Plan");
    let before = doc.transact().state_vector();
    body.insert(&mut doc.transact_mut(), 4, " B");
    let edit = doc.transact().encode_diff_v1(&before);
    let message = YMessage::Sync(SyncMessage::Update(edit)).encode_v1();
    a.send(Message::Binary(message.clone().into()))
        .await
        .unwrap();

    // B hears the edit as it was sent.
    assert_eq!(next_binary(&mut b).await.unwrap(), message);

    a.close(None).await.unwrap();
    b.close(None).await.unwrap();
    for _ in 0..100 {
        if s.coediting.open_documents() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let mut contents = String::new();
    s.storage
        .open_file(item)
        .await
        .unwrap()
        .read_to_string(&mut contents)
        .await
        .unwrap();
    assert_eq!(contents, "Plan B");
}

#[tokio::test]
async fn refusals_are_http_errors_before_the_upgrade() {
    let s = start().await;
    let item = s.file("Plan").await;
    let (token, _) = s.sign_in().await;
    let status = |result: Result<Socket, tungstenite::Error>| match result {
        Err(tungstenite::Error::Http(response)) => response.status().as_u16(),
        other => panic!("{:?}", other.map(|_| "upgraded")),
    };
    assert_eq!(status(s.connect(&item.to_string(), None).await), 401);
    let missing = ItemId::generate().to_string();
    assert_eq!(status(s.connect(&missing, Some(&token)).await), 404);
}

#[tokio::test]
async fn the_close_code_says_why() {
    let s = start().await;
    let item = s.file("Plan").await;

    let (token, session) = s.sign_in().await;
    let mut socket = s.connect(&item.to_string(), Some(&token)).await.unwrap();
    s.identity.sign_out(s.alex, session).await.unwrap();
    assert_eq!(
        close_code(&mut socket).await,
        CloseCode::from(accordion_module::coedit::SIGNED_OUT)
    );

    let (token, _) = s.sign_in().await;
    let mut socket = s.connect(&item.to_string(), Some(&token)).await.unwrap();
    socket.send(Message::text("hello")).await.unwrap();
    assert_eq!(close_code(&mut socket).await, CloseCode::Unsupported);

    let mut socket = s.connect(&item.to_string(), Some(&token)).await.unwrap();
    socket
        .send(Message::Binary(vec![0, 7, 255, 255, 255, 255, 255].into()))
        .await
        .unwrap();
    assert_eq!(close_code(&mut socket).await, CloseCode::Invalid);

    let mut socket = s.connect(&item.to_string(), Some(&token)).await.unwrap();
    s.coediting.stop().await;
    assert_eq!(close_code(&mut socket).await, CloseCode::Restart);
}

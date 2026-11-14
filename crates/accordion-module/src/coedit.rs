//! Carries co-editing over a WebSocket (decision 30 in the `accordion`
//! repo's `docs/decisions`).
//!
//! A module's route joins the document first, so a refusal is an ordinary
//! HTTP error, then hands the connection to [`upgrade`]:
//!
//! ```ignore
//! async fn live(
//!     Authenticated(signed_in): Authenticated,
//!     Path(item): Path<ItemId>,
//!     State(services): State<Services>,
//!     socket: WebSocketUpgrade,
//! ) -> Result<Response, ApiError> {
//!     let format = Arc::new(NoteFormat);
//!     let user = signed_in.user.id;
//!     let session = Some(signed_in.session.id);
//!     let connection = services.coediting.join(item, user, session, format).await?;
//!     Ok(coedit::upgrade(socket, connection))
//! }
//! ```
//!
//! Each WebSocket message carries one of Yjs's binary sync or awareness
//! messages, as `y-websocket` sends them. When the server closes the
//! connection, the close code says why (see [`close_code`]).

use std::time::Duration;

use accordion_core::coedit::{Closed, Connection, MAX_MESSAGE};
use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::response::Response;

/// How often an idle connection is pinged, so proxies and routers between
/// the browser and the server don't drop it.
const PING_EVERY: Duration = Duration::from_secs(30);

/// Close codes in the range for applications, for reasons the standard
/// codes don't cover. The web app reconnects after [`TOO_SLOW`], and checks
/// its access or session before reconnecting after the others.
pub const SIGNED_OUT: u16 = 4001;
pub const ACCESS_CHANGED: u16 = 4003;
pub const GONE: u16 = 4004;
pub const TOO_SLOW: u16 = 4008;

/// Finishes the WebSocket handshake and carries messages between the
/// client and `connection` until either side closes.
pub fn upgrade(socket: WebSocketUpgrade, connection: Connection) -> Response {
    // A little room over the largest message, for the frame around it, so
    // the room is what turns a big message away, with its own close code.
    socket
        .max_message_size(MAX_MESSAGE + 1024)
        .max_frame_size(MAX_MESSAGE + 1024)
        .on_upgrade(move |socket| carry(socket, connection))
}

/// The close code for each reason the server closes a connection.
pub fn close_code(why: Option<Closed>) -> u16 {
    match why {
        Some(Closed::SignedOut) => SIGNED_OUT,
        Some(Closed::AccessChanged) => ACCESS_CHANGED,
        Some(Closed::Gone) => GONE,
        // Unknown counts as too slow: the client reconnects and catches up.
        Some(Closed::TooSlow) | None => TOO_SLOW,
        Some(Closed::TooBig) => 1009,
        Some(Closed::Invalid) => 1007,
        Some(Closed::Failed) => 1011,
        Some(Closed::Stopping) => 1012,
    }
}

async fn carry(mut socket: WebSocket, mut connection: Connection) {
    let mut ping = tokio::time::interval(PING_EVERY);
    ping.tick().await;
    loop {
        tokio::select! {
            incoming = socket.recv() => match incoming {
                Some(Ok(Message::Binary(bytes))) => {
                    if !connection.send(bytes.into()).await {
                        break;
                    }
                }
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                // Yjs sends only binary messages.
                Some(Ok(Message::Text(_))) => {
                    close(&mut socket, 1003).await;
                    return;
                }
                Some(Ok(Message::Close(_)) | Err(_)) | None => return,
            },
            outgoing = connection.recv() => match outgoing {
                Some(bytes) => {
                    if socket.send(Message::Binary(bytes.to_vec().into())).await.is_err() {
                        return;
                    }
                }
                None => break,
            },
            _ = ping.tick() => {
                if socket.send(Message::Ping(Vec::new().into())).await.is_err() {
                    return;
                }
            }
        }
    }
    close(&mut socket, close_code(connection.closed_because())).await;
}

async fn close(socket: &mut WebSocket, code: u16) {
    let frame = CloseFrame {
        code,
        reason: "".into(),
    };
    let _ = socket.send(Message::Close(Some(frame))).await;
}

//! Talks to one Accordion server: Drive's sync addresses for what changed,
//! and its usual addresses for downloading and saving files. Requests go
//! straight to the server or through the account's tunnel, whichever
//! answers ([`crate::route`]).

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::header::{
    AUTHORIZATION, CONTENT_TYPE, ETAG, IF_MATCH, IF_NONE_MATCH, IF_RANGE, RANGE, USER_AGENT,
};
use hyper::{Method, Request, StatusCode, Uri};
use hyper_rustls::ConfigBuilderExt;
use hyper_util::client::legacy::Client as HttpClient;
use hyper_util::rt::TokioExecutor;
use rustls::ClientConfig;
use rustls::pki_types::CertificateDer;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::route::{Connector, Route, Via};
use crate::tunnel::TunnelSetup;

/// A request that takes longer than this, between pieces of a download,
/// has stalled.
const TIMEOUT: Duration = Duration::from_secs(60);
/// Files up to this size are saved in one request. Bigger ones go up as a
/// resumable upload, in pieces this size.
pub const PIECE: u64 = 8 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The server can't follow the cursor, so the device starts over.
    #[error("Accordion needs to compare everything again.")]
    StartOver,
    /// The file changed on the server since the device last synced it.
    #[error("Someone changed this file in Accordion since this device last synced it.")]
    Changed,
    /// It's gone, or out of reach.
    #[error("That isn't in Accordion any more, or you can't open it.")]
    NotFound,
    /// The session ended, so the app has to be added again.
    #[error(
        "This device was signed out of Accordion. Make a new code in Settings, under Apps and \
         devices, then add the account again."
    )]
    SignedOut,
    /// The server said no, with its reason, such as a name that's taken.
    #[error("{0}")]
    Refused(String),
    #[error("Accordion couldn't reach {0}. Check your connection, then try again.")]
    Unreachable(String),
    #[error("Accordion ran into a problem: {0}")]
    Server(String),
    #[error("Accordion couldn't use a file on this computer: {0}")]
    Local(#[from] std::io::Error),
}

impl Error {
    /// Whether trying again later could work, as opposed to a refusal.
    pub fn is_temporary(&self) -> bool {
        matches!(self, Self::Unreachable(_) | Self::Server(_))
    }
}

/// An item as the server's sync addresses and file addresses describe it.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct RemoteItem {
    pub id: String,
    pub parent: Option<String>,
    pub name: String,
    pub kind: String,
    pub size: Option<u64>,
    pub sha256: Option<String>,
    pub modified_at: String,
    /// "owner", "editor", or "viewer".
    pub access: String,
    /// Everything inside it changed too, so list it again.
    #[serde(default)]
    pub whole: bool,
    /// Which place a top folder is: "drive" for My Drive, or an app's,
    /// such as "notes". Servers before places don't send it.
    #[serde(default)]
    pub place: Option<String>,
}

impl RemoteItem {
    pub fn is_folder(&self) -> bool {
        self.kind == "folder"
    }
}

/// One entry in the list of changes.
#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
pub enum RemoteChange {
    Gone {
        id: String,
        #[allow(dead_code)]
        gone: bool,
    },
    Item(RemoteItem),
}

#[derive(Deserialize)]
pub struct Start {
    pub cursor: String,
    pub roots: Vec<RemoteItem>,
}

#[derive(Deserialize)]
pub struct Page {
    pub items: Vec<RemoteItem>,
    pub next: Option<String>,
}

#[derive(Deserialize)]
pub struct Changes {
    pub changes: Vec<RemoteChange>,
    pub cursor: String,
    pub more: bool,
}

/// What redeeming a code gives the app.
#[derive(Deserialize)]
pub struct SignedIn {
    pub token: String,
    pub username: String,
    pub display_name: String,
    /// How the app reaches the server away from home, if it sent a key and
    /// remote access is on.
    #[serde(default)]
    pub tunnel: Option<TunnelReply>,
    /// Why it can't, if it sent a key.
    #[serde(default)]
    pub tunnel_problem: Option<String>,
}

/// The server's half of a tunnel's setup, from registering the app's key.
#[derive(Clone, Debug, Deserialize)]
pub struct TunnelReply {
    pub server_key: String,
    pub endpoint: String,
    pub address: std::net::Ipv4Addr,
    pub gateway: std::net::Ipv4Addr,
}

impl TunnelReply {
    /// The whole setup, with the app's own private key.
    pub fn with_key(self, private_key: String) -> TunnelSetup {
        TunnelSetup {
            private_key,
            server_key: self.server_key,
            endpoint: self.endpoint,
            address: self.address,
            gateway: self.gateway,
        }
    }
}

/// The token endpoint's answer.
#[derive(Deserialize)]
struct TokenReply {
    access_token: String,
    username: String,
    display_name: String,
    #[serde(default)]
    tunnel: Option<TunnelReply>,
    #[serde(default)]
    tunnel_problem: Option<String>,
}

/// Who the session belongs to.
#[derive(Deserialize)]
pub struct Me {
    pub username: String,
}

/// The ways a client may reach its server.
#[derive(Clone, Default)]
pub struct Reach {
    /// The account's tunnel, for away from home.
    pub tunnel: Option<TunnelSetup>,
    /// Only this way, for tests and for trying the tunnel at home.
    pub only: Option<Via>,
    /// Certificates to trust instead of the system's, for tests.
    pub trust: Vec<CertificateDer<'static>>,
}

#[derive(Clone)]
pub struct Client {
    http: HttpClient<Connector, Full<Bytes>>,
    route: Arc<Route>,
    /// Such as `https://ourhome.dedyn.io`, with no slash at the end.
    origin: String,
    token: Option<String>,
}

impl Client {
    /// A client for the server at `origin`, reached directly. Plain HTTP is
    /// only allowed for a development server on this computer, since
    /// anything else would send the session token unencrypted.
    pub fn new(origin: &str, token: Option<String>) -> Result<Self, Error> {
        Self::with_reach(origin, token, Reach::default())
    }

    /// A client for the server at `origin`, reached the ways `reach` says.
    pub fn with_reach(origin: &str, token: Option<String>, reach: Reach) -> Result<Self, Error> {
        let origin = origin.trim().trim_end_matches('/').to_owned();
        let uri: Uri = origin
            .parse()
            .map_err(|_| Error::Refused(format!("\"{origin}\" isn't a server address.")))?;
        let local = matches!(uri.host(), Some("127.0.0.1" | "localhost" | "[::1]"));
        match uri.scheme_str() {
            Some("https") => {}
            Some("http") if local => {}
            _ => {
                return Err(Error::Refused(format!(
                    "Use the server's https:// address, such as https://ourhome.dedyn.io, not \
                     \"{origin}\"."
                )));
            }
        }
        let builder =
            ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .map_err(|err| Error::Server(format!("couldn't set up TLS ({err})")))?;
        let mut tls = if reach.trust.is_empty() {
            builder
                .try_with_platform_verifier()
                .map_err(|err| {
                    Error::Server(format!("couldn't load the system's certificates ({err})"))
                })?
                .with_no_client_auth()
        } else {
            let mut roots = rustls::RootCertStore::empty();
            for cert in reach.trust {
                roots.add(cert).map_err(|err| {
                    Error::Server(format!("couldn't trust a certificate ({err})"))
                })?;
            }
            builder.with_root_certificates(roots).with_no_client_auth()
        };
        tls.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        let route = Arc::new(Route::new(
            &uri,
            Some(Arc::new(tls)),
            reach.tunnel,
            reach.only,
        ));
        Ok(Self {
            http: HttpClient::builder(TokioExecutor::new()).build(Connector(Arc::clone(&route))),
            route,
            origin,
            token,
        })
    }

    pub fn origin(&self) -> &str {
        &self.origin
    }

    /// The way the last connection reached the server, if one has.
    pub fn via(&self) -> Option<Via> {
        self.route.last()
    }

    /// Whether the client has a tunnel for away from home.
    pub fn has_tunnel(&self) -> bool {
        self.route.has_tunnel()
    }

    /// Who the session belongs to. It also checks the server answers.
    pub async fn me(&self) -> Result<Me, Error> {
        let reply: Value = parse(&self.get("/api/session").await?)?;
        serde_json::from_value(reply["user"].clone())
            .map_err(|err| Error::Server(format!("the server's answer didn't make sense ({err})")))
    }

    /// Registers the app's tunnel key, made on this device, for its
    /// session, and returns the server's half of the setup.
    pub async fn register_tunnel(&self, public_key: &str) -> Result<TunnelReply, Error> {
        parse(
            &self
                .send(
                    Method::POST,
                    "/api/app-tunnel",
                    &[],
                    Some(json!({ "public_key": public_key })),
                )
                .await?,
        )
    }

    /// Trades a code from **Apps and devices** for a session token. With
    /// `tunnel_key`, the server registers that public key for the new
    /// session too.
    pub async fn redeem(
        &self,
        code: &str,
        device_name: &str,
        tunnel_key: Option<&str>,
    ) -> Result<SignedIn, Error> {
        let body = json!({ "code": code, "device_name": device_name, "tunnel_key": tunnel_key });
        match self
            .send(Method::POST, "/api/app-codes/redeem", &[], Some(body))
            .await
        {
            Ok(reply) => parse(&reply),
            // Not a session that ended: the code itself didn't work.
            Err(Error::SignedOut) => Err(Error::Refused(
                "That code doesn't work. It may have expired or been used already. Make a new \
                 one in Settings, under Apps and devices, then try again."
                    .into(),
            )),
            Err(err) => Err(err),
        }
    }

    /// Trades a code from signing in through the browser, and the secret
    /// its challenge came from, for a session token (RFC 6749 and 7636).
    /// With `tunnel_key`, the server registers that public key for the new
    /// session too.
    pub async fn exchange_code(
        &self,
        client_id: &str,
        code: &str,
        redirect_uri: &str,
        verifier: &str,
        tunnel_key: Option<&str>,
    ) -> Result<SignedIn, Error> {
        let mut form = format!(
            "grant_type=authorization_code&client_id={}&code={}&redirect_uri={}&code_verifier={}",
            form_encode(client_id),
            form_encode(code),
            form_encode(redirect_uri),
            form_encode(verifier),
        );
        if let Some(key) = tunnel_key {
            form.push_str(&format!("&tunnel_key={}", form_encode(key)));
        }
        let reply = self
            .send_bytes(
                Method::POST,
                "/api/oauth/token",
                &[(CONTENT_TYPE.as_str(), "application/x-www-form-urlencoded")],
                Bytes::from(form),
            )
            .await
            .map_err(|err| match err {
                // Not a session that ended: the sign-in itself didn't work.
                Error::SignedOut => {
                    Error::Refused("That sign-in didn't work. Sign in from the app again.".into())
                }
                err => err,
            })?;
        let token: TokenReply = parse(&reply)?;
        Ok(SignedIn {
            token: token.access_token,
            username: token.username,
            display_name: token.display_name,
            tunnel: token.tunnel,
            tunnel_problem: token.tunnel_problem,
        })
    }

    pub async fn start(&self) -> Result<Start, Error> {
        parse(&self.get("/api/drive/sync/start").await?)
    }

    pub async fn folder(&self, id: &str, after: Option<&str>) -> Result<Page, Error> {
        let mut path = format!("/api/drive/sync/folders/{id}");
        if let Some(after) = after {
            path.push_str(&format!("?after={after}"));
        }
        parse(&self.get(&path).await?)
    }

    pub async fn changes(&self, cursor: &str) -> Result<Changes, Error> {
        parse(
            &self
                .get(&format!("/api/drive/sync/changes?after={}", encode(cursor)))
                .await?,
        )
    }

    pub async fn create_folder(&self, parent: &str, name: &str) -> Result<RemoteItem, Error> {
        let reply = self
            .send(
                Method::POST,
                &format!("/api/drive/items/{parent}/children"),
                &[],
                Some(json!({ "name": name })),
            )
            .await?;
        parse(&reply)
    }

    /// Renames or moves an item.
    pub async fn move_item(&self, id: &str, parent: &str, name: &str) -> Result<RemoteItem, Error> {
        let reply = self
            .send(
                Method::PATCH,
                &format!("/api/drive/items/{id}"),
                &[],
                Some(json!({ "parent": parent, "name": name })),
            )
            .await?;
        parse(&reply)
    }

    /// Moves an item to the trash, where it stays as long as the admin chose
    /// (30 days unless they changed it).
    pub async fn trash(&self, id: &str) -> Result<(), Error> {
        self.send(Method::DELETE, &format!("/api/drive/items/{id}"), &[], None)
            .await?;
        Ok(())
    }

    /// Signs this device out, such as when its account is removed, so its
    /// session token stops working.
    pub async fn sign_out(&self) -> Result<(), Error> {
        self.send(Method::DELETE, "/api/session", &[], None).await?;
        Ok(())
    }

    /// Uploads `file` as a new file `name` in `parent`, refusing the name if
    /// a file already has it.
    pub async fn upload_new(
        &self,
        parent: &str,
        name: &str,
        file: &Path,
    ) -> Result<RemoteItem, Error> {
        let size = tokio::fs::metadata(file).await?.len();
        if size <= PIECE {
            let body = tokio::fs::read(file).await?;
            let reply = self
                .send_bytes(
                    Method::PUT,
                    &format!("/api/drive/items/{parent}/children/{}", encode(name)),
                    &[(IF_NONE_MATCH.as_str(), "*")],
                    body.into(),
                )
                .await?;
            return parse(&reply);
        }
        let upload = self.resumable(Some(parent), file, size).await?;
        let reply = self
            .send(
                Method::POST,
                &format!("/api/drive/uploads/{upload}/finish"),
                &[(IF_NONE_MATCH.as_str(), "*")],
                Some(json!({ "parent": parent, "name": name })),
            )
            .await?;
        parse(&reply)
    }

    /// Saves `file` as the new version of `id`, only if the server still
    /// holds `base`, the contents this device last synced.
    pub async fn upload_over(
        &self,
        id: &str,
        parent: Option<&str>,
        base: &str,
        file: &Path,
    ) -> Result<RemoteItem, Error> {
        let size = tokio::fs::metadata(file).await?.len();
        let if_match = format!("\"{base}\"");
        if size <= PIECE {
            let body = tokio::fs::read(file).await?;
            let reply = self
                .send_bytes(
                    Method::PUT,
                    &format!("/api/drive/items/{id}/content"),
                    &[(IF_MATCH.as_str(), &if_match)],
                    body.into(),
                )
                .await?;
            return parse(&reply);
        }
        let upload = self.resumable(parent, file, size).await?;
        let reply = self
            .send(
                Method::POST,
                &format!("/api/drive/uploads/{upload}/finish"),
                &[(IF_MATCH.as_str(), &if_match)],
                Some(json!({ "file": id })),
            )
            .await?;
        parse(&reply)
    }

    /// Sends a big file in pieces, and returns the upload's ID for
    /// finishing it.
    async fn resumable(
        &self,
        parent: Option<&str>,
        file: &Path,
        size: u64,
    ) -> Result<String, Error> {
        let reply = self
            .send(
                Method::POST,
                "/api/drive/uploads",
                &[],
                Some(json!({ "size": size, "parent": parent })),
            )
            .await?;
        let upload: Value = parse(&reply)?;
        let id = upload["id"]
            .as_str()
            .ok_or_else(|| Error::Server("the upload had no ID".into()))?
            .to_owned();
        let mut reader = tokio::fs::File::open(file).await?;
        let mut offset = 0u64;
        while offset < size {
            let mut piece = vec![0; usize::try_from(PIECE.min(size - offset)).unwrap_or(0)];
            reader.read_exact(&mut piece).await?;
            let len = piece.len() as u64;
            self.send_bytes(
                Method::PATCH,
                &format!("/api/drive/uploads/{id}"),
                &[("upload-offset", &offset.to_string())],
                piece.into(),
            )
            .await?;
            offset += len;
        }
        Ok(id)
    }

    /// Downloads a file's contents into `to`, and returns their SHA-256 as
    /// lowercase hex.
    pub async fn download(&self, id: &str, to: &Path) -> Result<String, Error> {
        let request = self
            .request(Method::GET, &format!("/api/drive/items/{id}/content"), &[])?
            .body(Full::new(Bytes::new()))
            .expect("the request is well formed");
        let response = tokio::time::timeout(TIMEOUT, self.http.request(request))
            .await
            .map_err(|_| self.unreachable())?
            .map_err(|_| self.unreachable())?;
        let status = response.status();
        if !status.is_success() {
            let body = response.into_body().collect().await.map(|b| b.to_bytes());
            return Err(status_error(status, &body.unwrap_or_default()));
        }
        let mut file = tokio::fs::File::create(to).await?;
        let mut hash = Sha256::new();
        let mut body = response.into_body();
        loop {
            let frame = tokio::time::timeout(TIMEOUT, body.frame())
                .await
                .map_err(|_| self.unreachable())?;
            let Some(frame) = frame else { break };
            let frame = frame.map_err(|_| self.unreachable())?;
            if let Ok(data) = frame.into_data() {
                hash.update(&data);
                file.write_all(&data).await?;
            }
        }
        file.sync_all().await?;
        Ok(hex(&hash.finalize()))
    }

    /// Opens a file's contents from byte `from`, for a placeholder made for
    /// the version with `sha256`, when something opens it on a device with
    /// files on demand. If the file changed since, it's
    /// [`Error::Changed`], and the next pull brings the placeholder up to
    /// date.
    pub async fn contents(&self, id: &str, from: u64, sha256: &str) -> Result<Contents, Error> {
        let range = format!("bytes={from}-");
        let version = format!("\"{sha256}\"");
        let request = self
            .request(
                Method::GET,
                &format!("/api/drive/items/{id}/content"),
                &[(RANGE.as_str(), &range), (IF_RANGE.as_str(), &version)],
            )?
            .body(Full::new(Bytes::new()))
            .expect("the request is well formed");
        let response = tokio::time::timeout(TIMEOUT, self.http.request(request))
            .await
            .map_err(|_| self.unreachable())?
            .map_err(|_| self.unreachable())?;
        let status = response.status();
        let same = response
            .headers()
            .get(ETAG)
            .is_some_and(|etag| etag.as_bytes() == version.as_bytes());
        match status {
            StatusCode::PARTIAL_CONTENT if same => {}
            // The whole file, which is what was asked for from the start.
            StatusCode::OK if same && from == 0 => {}
            StatusCode::OK | StatusCode::PARTIAL_CONTENT | StatusCode::RANGE_NOT_SATISFIABLE => {
                return Err(Error::Changed);
            }
            _ => {
                let body = response.into_body().collect().await.map(|b| b.to_bytes());
                return Err(status_error(status, &body.unwrap_or_default()));
            }
        }
        Ok(Contents {
            body: response.into_body(),
            origin: self.origin.clone(),
        })
    }

    /// Opens the live updates stream (decision 19), for the engine to sync
    /// as soon as something changes.
    pub async fn live(&self) -> Result<hyper::body::Incoming, Error> {
        let request = self
            .request(Method::GET, "/api/live", &[])?
            .body(Full::new(Bytes::new()))
            .expect("the request is well formed");
        let response = tokio::time::timeout(TIMEOUT, self.http.request(request))
            .await
            .map_err(|_| self.unreachable())?
            .map_err(|_| self.unreachable())?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.into_body().collect().await.map(|b| b.to_bytes());
            return Err(status_error(status, &body.unwrap_or_default()));
        }
        Ok(response.into_body())
    }

    async fn get(&self, path: &str) -> Result<Bytes, Error> {
        self.send(Method::GET, path, &[], None).await
    }

    async fn send(
        &self,
        method: Method,
        path: &str,
        headers: &[(&str, &str)],
        json: Option<Value>,
    ) -> Result<Bytes, Error> {
        let mut all = headers.to_vec();
        let body = match json {
            Some(json) => {
                all.push((CONTENT_TYPE.as_str(), "application/json"));
                Bytes::from(json.to_string())
            }
            None => Bytes::new(),
        };
        self.send_bytes(method, path, &all, body).await
    }

    async fn send_bytes(
        &self,
        method: Method,
        path: &str,
        headers: &[(&str, &str)],
        body: Bytes,
    ) -> Result<Bytes, Error> {
        let request = self
            .request(method, path, headers)?
            .body(Full::new(body))
            .expect("the request is well formed");
        let exchange = async {
            let response = self
                .http
                .request(request)
                .await
                .map_err(|_| self.unreachable())?;
            let status = response.status();
            let body = response
                .into_body()
                .collect()
                .await
                .map_err(|_| self.unreachable())?
                .to_bytes();
            if status.is_success() {
                Ok(body)
            } else {
                Err(status_error(status, &body))
            }
        };
        tokio::time::timeout(TIMEOUT, exchange)
            .await
            .map_err(|_| self.unreachable())?
    }

    fn request(
        &self,
        method: Method,
        path: &str,
        headers: &[(&str, &str)],
    ) -> Result<hyper::http::request::Builder, Error> {
        let mut request = Request::builder()
            .method(method)
            .uri(format!("{}{path}", self.origin))
            .header(USER_AGENT, "accordion-sync");
        if let Some(token) = &self.token {
            request = request.header(AUTHORIZATION, format!("Bearer {token}"));
        }
        for &(name, value) in headers {
            request = request.header(name, value);
        }
        Ok(request)
    }

    fn unreachable(&self) -> Error {
        Error::Unreachable(self.origin.clone())
    }
}

/// A file's contents as they arrive, from [`Client::contents`].
pub struct Contents {
    body: hyper::body::Incoming,
    origin: String,
}

impl Contents {
    /// The next part, or `None` at the end.
    pub async fn next(&mut self) -> Result<Option<Bytes>, Error> {
        loop {
            let frame = tokio::time::timeout(TIMEOUT, self.body.frame())
                .await
                .map_err(|_| Error::Unreachable(self.origin.clone()))?;
            let Some(frame) = frame else {
                return Ok(None);
            };
            let frame = frame.map_err(|_| Error::Unreachable(self.origin.clone()))?;
            if let Ok(data) = frame.into_data() {
                return Ok(Some(data));
            }
        }
    }
}

fn parse<T: serde::de::DeserializeOwned>(body: &Bytes) -> Result<T, Error> {
    serde_json::from_slice(body)
        .map_err(|err| Error::Server(format!("the server's answer didn't make sense ({err})")))
}

fn status_error(status: StatusCode, body: &Bytes) -> Error {
    // OAuth's errors put the message in `error_description`, and a code in
    // `error`.
    let message = serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|json| {
            json["error_description"]
                .as_str()
                .or_else(|| json["error"].as_str())
                .map(str::to_owned)
        })
        .unwrap_or_else(|| format!("the server answered {status}"));
    match status {
        StatusCode::GONE => Error::StartOver,
        StatusCode::PRECONDITION_FAILED => Error::Changed,
        StatusCode::NOT_FOUND => Error::NotFound,
        StatusCode::UNAUTHORIZED => Error::SignedOut,
        status if status.is_server_error() && status != StatusCode::INSUFFICIENT_STORAGE => {
            Error::Server(message)
        }
        _ => Error::Refused(message),
    }
}

/// Percent-encodes everything but unreserved characters, for a name or a
/// cursor in a URL.
/// Percent-encodes everything but letters, digits, and `-._~`, for a form.
fn form_encode(text: &str) -> String {
    let mut out = String::new();
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || b"-._~".contains(&byte) {
            out.push(char::from(byte));
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

fn encode(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(byte));
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_names_for_addresses() {
        assert_eq!(encode("soup recipe (2).md"), "soup%20recipe%20%282%29.md");
        assert_eq!(encode("café"), "caf%C3%A9");
    }

    #[test]
    fn sends_tokens_only_over_https_except_to_this_computer() {
        assert!(Client::new("https://ourhome.dedyn.io/", None).is_ok());
        assert!(Client::new("http://127.0.0.1:8080", None).is_ok());
        assert!(Client::new("http://ourhome.dedyn.io", None).is_err());
        assert!(Client::new("ourhome.dedyn.io", None).is_err());
    }

    #[test]
    fn reads_gone_and_changed_entries() {
        let changes: Changes = serde_json::from_str(
            r#"{"changes":[{"id":"a","gone":true},
                {"id":"b","parent":"a","name":"x.txt","kind":"file","size":1,
                 "sha256":"00","created_at":"t","modified_at":"t","access":"owner"}],
                "cursor":"c","more":false}"#,
        )
        .unwrap();
        assert!(matches!(&changes.changes[0], RemoteChange::Gone { id, .. } if id == "a"));
        assert!(matches!(&changes.changes[1], RemoteChange::Item(item) if !item.whole));
    }
}

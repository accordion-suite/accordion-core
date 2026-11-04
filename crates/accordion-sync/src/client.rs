//! Talks to one Accordion server: Drive's sync addresses for what changed,
//! and its usual addresses for downloading and saving files.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::header::{AUTHORIZATION, CONTENT_TYPE, IF_MATCH, IF_NONE_MATCH, USER_AGENT};
use hyper::{Method, Request, StatusCode, Uri};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::client::legacy::Client as HttpClient;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
}

#[derive(Clone)]
pub struct Client {
    http: HttpClient<HttpsConnector<HttpConnector>, Full<Bytes>>,
    /// Such as `https://ourhome.dedyn.io`, with no slash at the end.
    origin: String,
    token: Option<String>,
}

impl Client {
    /// A client for the server at `origin`. Plain HTTP is only allowed for
    /// a development server on this computer, since anything else would
    /// send the session token unencrypted.
    pub fn new(origin: &str, token: Option<String>) -> Result<Self, Error> {
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
        let connector = HttpsConnectorBuilder::new()
            .with_provider_and_platform_verifier(Arc::new(rustls::crypto::ring::default_provider()))
            .map_err(|err| {
                Error::Server(format!("couldn't load the system's certificates ({err})"))
            })?
            .https_or_http()
            .enable_http1()
            .enable_http2()
            .build();
        Ok(Self {
            http: HttpClient::builder(TokioExecutor::new()).build(connector),
            origin,
            token,
        })
    }

    pub fn origin(&self) -> &str {
        &self.origin
    }

    /// Trades a code from **Apps and devices** for a session token.
    pub async fn redeem(&self, code: &str, device_name: &str) -> Result<SignedIn, Error> {
        let body = json!({ "code": code, "device_name": device_name });
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

    /// Moves an item to the trash, where it stays for 30 days.
    pub async fn trash(&self, id: &str) -> Result<(), Error> {
        self.send(Method::DELETE, &format!("/api/drive/items/{id}"), &[], None)
            .await?;
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

fn parse<T: serde::de::DeserializeOwned>(body: &Bytes) -> Result<T, Error> {
    serde_json::from_slice(body)
        .map_err(|err| Error::Server(format!("the server's answer didn't make sense ({err})")))
}

fn status_error(status: StatusCode, body: &Bytes) -> Error {
    let message = serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|json| json["error"].as_str().map(str::to_owned))
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

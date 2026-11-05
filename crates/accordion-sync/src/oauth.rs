//! Signing in through the system browser (decision 3 in the `accordion`
//! repo's `docs/decisions`): OAuth 2.0 with PKCE, as RFC 8252 describes for
//! native apps.
//!
//! The app makes a secret, and opens the server's sign-in page with a
//! challenge made from it and the address of a small listener on this
//! computer. The person signs in there, with a passkey or a password, and
//! allows the app. The browser then brings a code to the listener, and the
//! app trades the code and the secret for a session. The app never sees the
//! person's password.

use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::add::Added;
use crate::client::{Client, Error, Reach};
use crate::tunnel;

/// The command-line client's ID, as the server knows it.
pub const CLIENT_ID: &str = "accordion-sync";

/// The desktop app's ID, as the server knows it.
pub const DESKTOP_CLIENT_ID: &str = "accordion-desktop";

/// How long to wait for the person to sign in.
const WAIT: Duration = Duration::from_secs(10 * 60);

/// A sign-in that's waiting for the browser.
pub struct Pending {
    client: Client,
    client_id: &'static str,
    listener: TcpListener,
    redirect_uri: String,
    verifier: String,
    state: String,
    /// The address to open in the browser.
    pub url: String,
}

fn random() -> String {
    use ring::rand::{SecureRandom, SystemRandom};
    let mut bytes = [0; 32];
    SystemRandom::new()
        .fill(&mut bytes)
        .expect("the operating system's random number generator works");
    URL_SAFE_NO_PAD.encode(bytes)
}

/// Percent-encodes everything but letters, digits, and `-._~`.
fn encode(text: &str) -> String {
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

/// Undoes percent-encoding, and `+` for a space, as in a query.
fn decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let escaped = (bytes[i] == b'%')
            .then(|| bytes.get(i + 1..i + 3))
            .flatten()
            .and_then(|hex| std::str::from_utf8(hex).ok())
            .and_then(|hex| u8::from_str_radix(hex, 16).ok());
        match (escaped, bytes[i]) {
            (Some(byte), _) => {
                out.push(byte);
                i += 3;
                continue;
            }
            (None, b'+') => out.push(b' '),
            (None, byte) => out.push(byte),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

impl Pending {
    /// Starts signing in to the server at `server`, as `device_name`, with
    /// a listener on this computer for the browser to come back to.
    pub async fn start(server: &str, device_name: &str, reach: Reach) -> Result<Self, Error> {
        Self::start_as(CLIENT_ID, server, device_name, reach).await
    }

    /// The same, for the app with `client_id`, such as
    /// [`DESKTOP_CLIENT_ID`].
    pub async fn start_as(
        client_id: &'static str,
        server: &str,
        device_name: &str,
        reach: Reach,
    ) -> Result<Self, Error> {
        let client = Client::with_reach(server, None, reach)?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let redirect_uri = format!(
            "http://127.0.0.1:{}/callback",
            listener.local_addr()?.port()
        );
        let verifier = random();
        let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
        let state = random();
        let url = format!(
            "{}/oauth/authorize?response_type=code&client_id={client_id}&redirect_uri={}\
             &code_challenge={challenge}&code_challenge_method=S256&state={state}\
             &device_name={}",
            client.origin(),
            encode(&redirect_uri),
            encode(device_name),
        );
        Ok(Self {
            client,
            client_id,
            listener,
            redirect_uri,
            verifier,
            state,
            url,
        })
    }

    /// Waits for the browser to come back, then trades its code for a
    /// session, and registers a new tunnel key for it.
    pub async fn finish(self) -> Result<Added, Error> {
        let (code, mut browser) =
            tokio::time::timeout(WAIT, self.callback())
                .await
                .map_err(|_| {
                    Error::Refused(
                        "Signing in took too long. Run the command again to start over.".into(),
                    )
                })??;
        let (private_key, public_key) = tunnel::new_key();
        let exchanged = self
            .client
            .exchange_code(
                self.client_id,
                &code,
                &self.redirect_uri,
                &self.verifier,
                Some(&public_key),
            )
            .await;
        self.done(&mut browser, exchanged.as_ref().err().map(|_| "failed"))
            .await;
        let mut signed_in = exchanged?;
        let tunnel = signed_in
            .tunnel
            .take()
            .map(|reply| reply.with_key(private_key));
        Ok(Added {
            server: self.client.origin().to_owned(),
            signed_in,
            tunnel,
        })
    }

    /// Answers the browser until it brings this sign-in's code, or says the
    /// person canceled.
    async fn callback(&self) -> Result<(String, TcpStream), Error> {
        loop {
            let (mut stream, _) = self.listener.accept().await?;
            let Ok(Some(target)) =
                tokio::time::timeout(Duration::from_secs(10), read_target(&mut stream))
                    .await
                    .unwrap_or(Ok(None))
            else {
                continue;
            };
            let Some(query) = target.strip_prefix("/callback?") else {
                respond(&mut stream, "404 Not Found", "There's nothing here.").await;
                continue;
            };
            let mut code = None;
            let mut state = None;
            let mut error = None;
            for pair in query.split('&') {
                let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
                match name {
                    "code" => code = Some(decode(value)),
                    "state" => state = Some(decode(value)),
                    "error" => error = Some(decode(value)),
                    _ => {}
                }
            }
            // Another page on this computer could send the browser here, but
            // only this sign-in's page knows its state.
            if state.as_deref() != Some(self.state.as_str()) {
                respond(
                    &mut stream,
                    "400 Bad Request",
                    "This isn't the sign-in the app started. Go back to the app, and start \
                     signing in again.",
                )
                .await;
                continue;
            }
            if error.is_some() {
                self.done(&mut stream, Some("canceled")).await;
                return Err(Error::Refused(
                    "You didn't allow the app in the browser, so it isn't signed in.".into(),
                ));
            }
            let Some(code) = code else {
                respond(
                    &mut stream,
                    "400 Bad Request",
                    "The sign-in didn't include a code.",
                )
                .await;
                continue;
            };
            // The browser waits until the app has its session, so the page
            // it ends on is true.
            return Ok((code, stream));
        }
    }

    /// Sends the browser to the server's page that says how signing in
    /// went, so it looks like the rest of Accordion. `result` is `None`
    /// when the app is signed in.
    async fn done(&self, stream: &mut TcpStream, result: Option<&str>) {
        let mut page = format!("{}/oauth/done", self.client.origin());
        if let Some(result) = result {
            page.push_str("?result=");
            page.push_str(result);
        }
        let response = format!(
            "HTTP/1.1 303 See Other\r\nLocation: {page}\r\nContent-Length: 0\r\n\
             Cache-Control: no-store\r\nConnection: close\r\n\r\n"
        );
        let _ = stream.write_all(response.as_bytes()).await;
        let _ = stream.shutdown().await;
    }
}

/// The path and query of an HTTP request's first line, or `None` if it
/// isn't a GET.
async fn read_target(stream: &mut TcpStream) -> std::io::Result<Option<String>> {
    let mut head = Vec::new();
    let mut buffer = [0; 1024];
    while !head.windows(4).any(|window| window == b"\r\n\r\n") && head.len() < 16 * 1024 {
        let read = stream.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        head.extend_from_slice(&buffer[..read]);
    }
    let head = String::from_utf8_lossy(&head);
    let mut first = head.lines().next().unwrap_or_default().split(' ');
    Ok(match (first.next(), first.next()) {
        (Some("GET"), Some(target)) => Some(target.to_owned()),
        _ => None,
    })
}

async fn respond(stream: &mut TcpStream, status: &str, message: &str) {
    let body = format!(
        "<!doctype html><html lang=\"en\"><meta charset=\"utf-8\">\
         <meta name=\"viewport\" content=\"width=device-width\">\
         <meta name=\"color-scheme\" content=\"light dark\">\
         <title>Accordion</title>\
         <body style=\"font-family: system-ui, sans-serif; margin: 3rem auto; max-width: 32rem; \
         padding: 0 1rem\"><h1>Accordion</h1><p>{message}</p></body></html>"
    );
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/html; charset=utf-8\r\n\
         Content-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.shutdown().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_and_decodes_query_values() {
        assert_eq!(
            encode("http://127.0.0.1:5/callback"),
            "http%3A%2F%2F127.0.0.1%3A5%2Fcallback"
        );
        assert_eq!(decode("Sam%27s+laptop%2"), "Sam's laptop%2");
        assert_eq!(decode(&encode("Sam's laptop ü")), "Sam's laptop ü");
    }

    #[tokio::test]
    async fn waits_for_its_own_state_and_says_when_its_done() {
        let pending = Pending::start("http://127.0.0.1:9", "Laptop", Reach::default())
            .await
            .unwrap();
        assert!(
            pending
                .url
                .starts_with("http://127.0.0.1:9/oauth/authorize?response_type=code")
        );
        assert!(pending.url.contains("&device_name=Laptop"));
        let port = pending
            .redirect_uri
            .rsplit(':')
            .next()
            .unwrap()
            .trim_end_matches("/callback")
            .to_owned();
        let state = pending.state.clone();
        let browser = async move {
            let get = |path: String| {
                let port = port.clone();
                async move {
                    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}"))
                        .await
                        .unwrap();
                    stream
                        .write_all(format!("GET {path} HTTP/1.1\r\nHost: x\r\n\r\n").as_bytes())
                        .await
                        .unwrap();
                    let mut reply = String::new();
                    stream.read_to_string(&mut reply).await.unwrap();
                    reply
                }
            };
            assert!(get("/favicon.ico".into()).await.starts_with("HTTP/1.1 404"));
            assert!(
                get("/callback?code=x&state=wrong".into())
                    .await
                    .starts_with("HTTP/1.1 400")
            );
            get(format!("/callback?code=aoc_abc&state={state}")).await
        };
        let callback = async {
            let (code, mut stream) = pending.callback().await.unwrap();
            pending.done(&mut stream, None).await;
            code
        };
        let (code, reply) = tokio::join!(callback, browser);
        assert_eq!(code, "aoc_abc");
        assert!(reply.starts_with("HTTP/1.1 303"), "{reply}");
        assert!(
            reply.contains("Location: http://127.0.0.1:9/oauth/done\r\n"),
            "{reply}"
        );
    }
}

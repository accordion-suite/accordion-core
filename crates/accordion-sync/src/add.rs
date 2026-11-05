//! Adding an account with a code from **Apps and devices** (decision 23 in
//! the `accordion` repo's `docs/decisions`).
//!
//! The code is either the 12 letters to type, which work where the app
//! reaches the server directly, such as at home, or the whole setup from
//! the QR code or the text to copy. The setup also carries a temporary
//! tunnel key, so an app away from home can reach the server to redeem the
//! code. Either way, the app makes its own tunnel key and registers its
//! public half with its new session.

use std::net::Ipv4Addr;

use crate::client::{Client, Error, Reach, SignedIn};
use crate::tunnel::{self, TunnelSetup};

/// What a code's setup says.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CodeSetup {
    /// The server's address, such as `https://ourhome.dedyn.io`.
    pub server: String,
    /// The code's letters.
    pub code: String,
    /// The code's temporary tunnel.
    pub tunnel: TunnelSetup,
}

impl CodeSetup {
    /// Reads a setup such as `accordion:add?server=…&code=…`, or `None` if
    /// `text` isn't one.
    pub fn parse(text: &str) -> Option<Self> {
        let query = text.trim().strip_prefix("accordion:add?")?;
        let mut fields = std::collections::HashMap::new();
        for pair in query.split('&') {
            let (name, value) = pair.split_once('=')?;
            fields.insert(name, decode(value)?);
        }
        let mut take = |name: &str| fields.remove(name).filter(|value| !value.is_empty());
        let address: Ipv4Addr = take("address")?.parse().ok()?;
        let gateway: Ipv4Addr = take("gateway")?.parse().ok()?;
        let setup = Self {
            server: take("server")?,
            code: take("code")?,
            tunnel: TunnelSetup {
                private_key: take("key")?,
                server_key: take("peer")?,
                endpoint: take("endpoint")?,
                address,
                gateway,
            },
        };
        tunnel::public_key(&setup.tunnel.private_key)?;
        Some(setup)
    }
}

/// Undoes percent-encoding.
fn decode(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = std::str::from_utf8(bytes.get(i + 1..i + 3)?).ok()?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// A new account.
pub struct Added {
    /// The server's address.
    pub server: String,
    pub signed_in: SignedIn,
    /// The account's own tunnel, if remote access is on.
    pub tunnel: Option<TunnelSetup>,
}

/// Trades `typed`, the code or its whole setup, for a session, and
/// registers a new tunnel key for it. `server` is needed only for a code
/// without its setup. With a setup, the app tries the server directly and
/// through the code's temporary tunnel at once.
pub async fn add(
    typed: &str,
    server: Option<&str>,
    device_name: &str,
    reach: Reach,
) -> Result<Added, Error> {
    let (server, code, temporary) = match CodeSetup::parse(typed) {
        Some(setup) => {
            if let Some(server) = server
                && server.trim().trim_end_matches('/') != setup.server
            {
                return Err(Error::Refused(format!(
                    "That code is for {}, not {server}.",
                    setup.server
                )));
            }
            (setup.server, setup.code, Some(setup.tunnel))
        }
        None if typed.trim().starts_with("accordion:") => {
            return Err(Error::Refused(
                "That setup isn't complete. Copy all of it, then try again.".into(),
            ));
        }
        None => {
            let Some(server) = server else {
                return Err(Error::Refused(
                    "Enter the server's address with the code, or use the whole setup from the \
                     QR code."
                        .into(),
                ));
            };
            (server.to_owned(), typed.trim().to_owned(), None)
        }
    };
    let client = Client::with_reach(
        &server,
        None,
        Reach {
            tunnel: temporary,
            ..reach
        },
    )?;
    let (private_key, public_key) = tunnel::new_key();
    let mut signed_in = client.redeem(&code, device_name, Some(&public_key)).await?;
    let tunnel = signed_in
        .tunnel
        .take()
        .map(|reply| reply.with_key(private_key));
    Ok(Added {
        server: client.origin().to_owned(),
        signed_in,
        tunnel,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_a_codes_setup() {
        let (key, _) = tunnel::new_key();
        let text = format!(
            "accordion:add?server=https://ourhome.dedyn.io&code=abcdefghijkl&key={}\
             &peer=xTIBA5rboUvnH4htodjb6e697QjLERt1NAB4mZqp8Dg%3D\
             &endpoint=k3x9q2wm7f.ourhome.dedyn.io:51820&address=10.42.7.254&gateway=10.42.7.1",
            key.replace('+', "%2B").replace('=', "%3D")
        );
        let setup = CodeSetup::parse(&text).unwrap();
        assert_eq!(setup.server, "https://ourhome.dedyn.io");
        assert_eq!(setup.code, "abcdefghijkl");
        assert_eq!(setup.tunnel.private_key, key);
        assert_eq!(
            setup.tunnel.server_key,
            "xTIBA5rboUvnH4htodjb6e697QjLERt1NAB4mZqp8Dg="
        );
        assert_eq!(setup.tunnel.endpoint, "k3x9q2wm7f.ourhome.dedyn.io:51820");
        assert_eq!(setup.tunnel.address, Ipv4Addr::new(10, 42, 7, 254));
        assert_eq!(setup.tunnel.gateway, Ipv4Addr::new(10, 42, 7, 1));

        assert!(CodeSetup::parse("abcd efgh ijkl").is_none());
        assert!(CodeSetup::parse(&text[..text.len() - 20]).is_none());
        assert!(CodeSetup::parse(&text.replace("&code=abcdefghijkl", "")).is_none());
    }
}

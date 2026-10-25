//! REST server targets: restic's own protocol, served by rest-server or
//! `rclone serve restic`, usually on another machine at home. In
//! append-only mode, the server refuses every delete except restic's lock
//! files, so a server taken over by an attacker can't delete its own
//! backups (decision 10 in the `accordion` repo's `docs/decisions`).
//!
//! A REST server on a home network usually has a self-signed certificate.
//! Accordion trusts the one it finds the first time it connects, the way
//! ssh trusts an SFTP server's host key, and from then on restic accepts
//! only that one. A certificate that the system already trusts, such as one
//! from Let's Encrypt, isn't pinned, because it's replaced every few months.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use base64ct::{Base64, Encoding};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use sha2::{Digest, Sha256};

use super::restic::Failure;

/// How long to wait for a server to answer when reading its certificate.
const CONNECT_TIME: Duration = Duration::from_secs(30);

/// A REST server, as the owner types it in.
#[derive(Clone, PartialEq, Eq)]
pub struct RestServer {
    /// The repository's address, such as `https://nas.local:8000/accordion`.
    pub address: String,
    /// From the server's password file. Both are empty for a server that
    /// doesn't ask for them.
    pub username: String,
    pub password: String,
}

impl fmt::Debug for RestServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RestServer")
            .field("address", &self.address)
            .field("username", &self.username)
            .finish_non_exhaustive()
    }
}

/// A checked address, as `https://host:port/path` with no slash at the end.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Address {
    pub url: String,
    pub host: String,
    pub port: u16,
}

/// Checks a typed address. It has to be HTTPS, because the server's
/// password goes with every request, and can't hold a username or password,
/// which belong in their own fields so they stay out of logs.
pub(super) fn address(typed: &str) -> Option<Address> {
    let typed = typed.trim().trim_end_matches('/');
    let (scheme, rest) = typed.split_once("://")?;
    if !scheme.eq_ignore_ascii_case("https") {
        return None;
    }
    let (authority, path) = match rest.find('/') {
        Some(slash) => rest.split_at(slash),
        None => (rest, ""),
    };
    let (host, port) = match authority.rsplit_once(':') {
        // A bracketed IPv6 address has colons of its own.
        Some((host, port)) if !port.contains(']') => (host, Some(port)),
        _ => (authority, None),
    };
    let host = host.to_ascii_lowercase();
    let port = match port {
        Some(port) => port.parse::<u16>().ok().filter(|&port| port > 0)?,
        None => 443,
    };
    let host_ok = if let Some(ip) = host.strip_prefix('[') {
        ip.strip_suffix(']')
            .is_some_and(|ip| ip.parse::<std::net::Ipv6Addr>().is_ok())
    } else {
        !host.is_empty()
            && host.len() <= 253
            && host.split('.').all(|label| {
                !label.is_empty()
                    && !label.starts_with('-')
                    && label
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            })
    };
    let path_ok = path.split('/').skip(1).all(|part| {
        !part.is_empty()
            && part != "."
            && part != ".."
            && part
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-' | b'~'))
    });
    (host_ok && path_ok).then(|| Address {
        url: format!("https://{host}:{port}{path}"),
        host,
        port,
    })
}

/// The address in a `rest:` location, parsed again.
pub(super) fn location_address(location: &str) -> Option<Address> {
    address(location.strip_prefix("rest:")?)
}

/// Reads the certificate a server shows, to pin it. It doesn't check the
/// certificate, which is the point, but it does check that the server holds
/// the certificate's key. Nothing is sent over the connection.
pub(super) async fn certificate(host: &str, port: u16) -> Result<CertificateDer<'static>, String> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .map_err(|err| err.to_string())?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AnyCertificate(provider)))
        .with_no_client_auth();
    let name = host.trim_start_matches('[').trim_end_matches(']');
    let name = ServerName::try_from(name.to_owned()).map_err(|err| err.to_string())?;
    let connect = async {
        let stream = tokio::net::TcpStream::connect((name.to_str().as_ref(), port)).await?;
        tokio_rustls::TlsConnector::from(Arc::new(config))
            .connect(name, stream)
            .await
    };
    let tls = tokio::time::timeout(CONNECT_TIME, connect)
        .await
        .map_err(|_| "it didn't answer".to_owned())?
        .map_err(|err| err.to_string())?;
    let (_, session) = tls.get_ref();
    session
        .peer_certificates()
        .and_then(|chain| chain.first())
        .map(|certificate| certificate.clone().into_owned())
        .ok_or_else(|| "it didn't show a certificate".to_owned())
}

/// Accepts whatever certificate the server shows, while still checking the
/// handshake's signatures with it.
#[derive(Debug)]
struct AnyCertificate(Arc<CryptoProvider>);

impl ServerCertVerifier for AnyCertificate {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// A certificate in PEM form, as restic's `--cacert` reads it.
pub(super) fn pem(certificate: &CertificateDer<'_>) -> String {
    let encoded = Base64::encode_string(certificate);
    let mut pem = String::from("-----BEGIN CERTIFICATE-----\n");
    for line in encoded.as_bytes().chunks(64) {
        pem.push_str(&String::from_utf8_lossy(line));
        pem.push('\n');
    }
    pem.push_str("-----END CERTIFICATE-----\n");
    pem
}

/// The certificate's SHA-256 fingerprint, for the log.
pub(super) fn fingerprint(certificate: &CertificateDer<'_>) -> String {
    Sha256::digest(certificate)
        .iter()
        .map(|byte| format!("{byte:02X}"))
        .collect::<Vec<_>>()
        .join(":")
}

/// What went wrong talking to a REST server, as restic describes it.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Problem {
    /// The certificate isn't one the system or Accordion trusts.
    Untrusted,
    /// The certificate doesn't name the address Accordion connected to.
    WrongName,
    Expired,
    /// The server answered without TLS.
    NotHttps,
    /// The username or password is wrong.
    LoginRefused,
    /// The server refused to delete something, as an append-only server
    /// does.
    Forbidden,
}

/// Finds what went wrong in a failure on a REST repository.
pub(super) fn problem(failure: &Failure) -> Option<Problem> {
    let text = match failure {
        Failure::Retrying(problem) => problem,
        Failure::Failed { output, .. } => output,
        _ => return None,
    };
    let found = [
        (
            "certificate signed by unknown authority",
            Problem::Untrusted,
        ),
        ("cannot validate certificate for", Problem::WrongName),
        ("certificate is valid for", Problem::WrongName),
        ("certificate is not valid for", Problem::WrongName),
        ("has expired or is not yet valid", Problem::Expired),
        (
            "server gave HTTP response to HTTPS client",
            Problem::NotHttps,
        ),
        (
            "first record does not look like a TLS handshake",
            Problem::NotHttps,
        ),
        ("(401)", Problem::LoginRefused),
        ("(403)", Problem::Forbidden),
    ];
    found
        .into_iter()
        .find(|(phrase, _)| text.contains(phrase))
        .map(|(_, problem)| problem)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checks_addresses() {
        let checked = |typed: &str| address(typed).map(|address| address.url);
        assert_eq!(
            checked(" HTTPS://NAS.local:8000/accordion/ ").as_deref(),
            Some("https://nas.local:8000/accordion")
        );
        assert_eq!(
            checked("https://192.168.1.50").as_deref(),
            Some("https://192.168.1.50:443")
        );
        assert_eq!(
            checked("https://[fd00::50]:8000/backups/home").as_deref(),
            Some("https://[fd00::50]:8000/backups/home")
        );
        for bad in [
            "",
            "nas.local:8000",
            "http://nas.local:8000",
            "https://user:password@nas.local",
            "https://nas.local:0",
            "https://nas.local:99999",
            "https://-nas.local",
            "https://nas..local",
            "https://nas.local/../etc",
            "https://nas.local//double",
            "https://nas.local/a path",
            "https://[not-ipv6]:8000",
        ] {
            assert_eq!(checked(bad), None, "{bad}");
        }
        let parsed = location_address("rest:https://nas.local:8000/accordion").unwrap();
        assert_eq!((parsed.host.as_str(), parsed.port), ("nas.local", 8000));
    }

    #[test]
    fn reads_what_went_wrong() {
        let retrying = |text: &str| problem(&Failure::Retrying(text.into()));
        assert_eq!(
            retrying(
                "Head \"https://u:***@127.0.0.1:8000/t/config\": tls: failed to verify \
                 certificate: x509: certificate signed by unknown authority"
            ),
            Some(Problem::Untrusted)
        );
        assert_eq!(
            retrying(
                "x509: cannot validate certificate for 127.0.0.1 because it doesn't contain \
                 any IP SANs"
            ),
            Some(Problem::WrongName)
        );
        assert_eq!(
            retrying("unexpected HTTP response (401): 401 Unauthorized"),
            Some(Problem::LoginRefused)
        );
        let failed = Failure::Failed {
            code: Some(3),
            message: "failed to remove one or more snapshots".into(),
            output: "Remove(<snapshot/97895728d8>) failed: unexpected HTTP response (403): \
                     403 Forbidden\nfailed to remove one or more snapshots"
                .into(),
        };
        assert_eq!(problem(&failed), Some(Problem::Forbidden));
        assert_eq!(retrying("dial tcp: connection refused"), None);
        assert_eq!(problem(&Failure::Locked), None);
    }

    #[test]
    fn writes_pem() {
        let pem = pem(&CertificateDer::from(vec![7u8; 100]));
        let lines: Vec<_> = pem.lines().collect();
        assert_eq!(lines[0], "-----BEGIN CERTIFICATE-----");
        assert_eq!(lines[1].len(), 64);
        assert_eq!(lines.last(), Some(&"-----END CERTIFICATE-----"));
    }
}

//! TLS for the stream carrier (`tls://`): the Python transport wraps the
//! listener and the outbound connection in an `ssl.SSLContext` built from
//! `certfile` / `keyfile` / `cafile`; here the same material drives a
//! `rustls` session over the plain `TcpStream`.
//!
//! The session is shared by the link's reader and writer threads (the
//! Python `StreamReader` / `StreamWriter` pair over one SSL object):
//! the blocking socket I/O happens *outside* the session lock, so a
//! reader parked on the socket never stalls a writer.

use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{
    ClientConfig, ClientConnection, Connection, RootCertStore, ServerConfig, ServerConnection,
};

use crate::python::sync::Mutex;
use crate::python::{LailaError, Result};

use super::super::base::Protocol;

/// A TLS-wrapped TCP connection.
pub struct TlsSock {
    /// The socket for reads / shutdown / timeouts.
    tcp: TcpStream,
    /// A second handle for writes (the session lock is held only while
    /// (de)framing, never across a blocking socket call).
    tcp_w: Mutex<TcpStream>,
    conn: Mutex<Connection>,
}

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

fn tls_err(prefix: &str, e: impl core::fmt::Display) -> LailaError {
    LailaError::connection_error(alloc::format!("{prefix}: {e}"))
}

/// `cafile` unset: `check_hostname=False`, `verify_mode=CERT_NONE` (a
/// self-signed test certificate is accepted).
#[derive(Debug)]
struct NoVerify(Arc<rustls::crypto::CryptoProvider>);

impl rustls::client::danger::ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> core::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> core::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
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
        dss: &rustls::DigitallySignedStruct,
    ) -> core::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// `_server_ssl_context()`: `PROTOCOL_TLS_SERVER` + `load_cert_chain`.
pub fn server_config(proto: &Protocol) -> Result<Arc<ServerConfig>> {
    let certfile = proto.str_field("certfile");
    if certfile.is_empty() {
        return Err(LailaError::runtime_error(
            "TLS transport requires `certfile` (and `keyfile`) to serve. Set them on the protocol or via laila.args.",
        ));
    }
    let keyfile = proto.str_field("keyfile");
    let keyfile = if keyfile.is_empty() {
        certfile.clone()
    } else {
        keyfile
    };
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(&certfile)
        .map_err(|e| tls_err("TLS certfile", e))?
        .collect::<core::result::Result<_, _>>()
        .map_err(|e| tls_err("TLS certfile", e))?;
    let key = PrivateKeyDer::from_pem_file(&keyfile).map_err(|e| tls_err("TLS keyfile", e))?;
    let cfg = ServerConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| tls_err("TLS server context", e))?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| tls_err("TLS server context", e))?;
    Ok(Arc::new(cfg))
}

/// `_client_ssl_context()`: `cafile` verifies the peer, otherwise
/// verification is off.
pub fn client_config(proto: &Protocol) -> Result<Arc<ClientConfig>> {
    let cafile = proto.str_field("cafile");
    let builder = ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| tls_err("TLS client context", e))?;
    let cfg = if cafile.is_empty() {
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerify(provider())))
            .with_no_client_auth()
    } else {
        let mut roots = RootCertStore::empty();
        for c in CertificateDer::pem_file_iter(&cafile).map_err(|e| tls_err("TLS cafile", e))? {
            roots
                .add(c.map_err(|e| tls_err("TLS cafile", e))?)
                .map_err(|e| tls_err("TLS cafile", e))?;
        }
        builder.with_root_certificates(roots).with_no_client_auth()
    };
    Ok(Arc::new(cfg))
}

fn finish_handshake(
    mut conn: Connection,
    mut tcp: TcpStream,
    tcp_w: TcpStream,
) -> Result<Arc<TlsSock>> {
    while conn.is_handshaking() {
        conn.complete_io(&mut tcp)
            .map_err(|e| tls_err("TLS handshake", e))?;
    }
    Ok(Arc::new(TlsSock {
        tcp,
        tcp_w: Mutex::new(tcp_w),
        conn: Mutex::new(conn),
    }))
}

/// The client side: handshake over *tcp* presenting *server_hostname*
/// (`server_hostname=self.server_hostname or host`).
pub fn connect(proto: &Protocol, tcp: TcpStream, host: &str) -> Result<Arc<TlsSock>> {
    let cfg = client_config(proto)?;
    let name = proto.str_field("server_hostname");
    let name = if name.is_empty() {
        String::from(host)
    } else {
        name
    };
    let server_name = ServerName::try_from(name.clone())
        .map_err(|e| tls_err(&alloc::format!("TLS server name {name:?}"), e))?;
    let conn = ClientConnection::new(cfg, server_name).map_err(|e| tls_err("TLS client", e))?;
    let tcp_w = tcp.try_clone().map_err(|e| tls_err("socket clone", e))?;
    finish_handshake(Connection::Client(conn), tcp, tcp_w)
}

/// The server side: handshake over an accepted *tcp* connection.
pub fn accept(cfg: Arc<ServerConfig>, tcp: TcpStream) -> Result<Arc<TlsSock>> {
    let conn = ServerConnection::new(cfg).map_err(|e| tls_err("TLS server", e))?;
    let tcp_w = tcp.try_clone().map_err(|e| tls_err("socket clone", e))?;
    finish_handshake(Connection::Server(conn), tcp, tcp_w)
}

impl TlsSock {
    pub fn shutdown(&self) {
        let _ = self.tcp.shutdown(Shutdown::Both);
    }
    pub fn set_read_timeout(&self, t: Option<core::time::Duration>) {
        let _ = self.tcp.set_read_timeout(t);
    }
    pub fn set_nodelay(&self) {
        let _ = self.tcp.set_nodelay(true);
    }
    /// Send whatever the session has queued (records, alerts).
    fn flush_tls(&self, conn: &mut Connection) -> std::io::Result<()> {
        let mut out = Vec::new();
        while conn.wants_write() {
            conn.write_tls(&mut out)?;
        }
        if !out.is_empty() {
            let mut w = self.tcp_w.lock();
            w.write_all(&out)?;
            w.flush()?;
        }
        Ok(())
    }
    pub fn read(&self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut chunk = [0u8; 16 * 1024];
        loop {
            {
                let mut conn = self.conn.lock();
                match conn.reader().read(buf) {
                    Ok(n) => return Ok(n),
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                    Err(e) => return Err(e),
                }
            }
            // No plaintext ready: pull more records off the socket
            // (blocking, outside the session lock).
            let n = (&self.tcp).read(&mut chunk)?;
            if n == 0 {
                return Ok(0);
            }
            let mut conn = self.conn.lock();
            let mut slice = &chunk[..n];
            while !slice.is_empty() {
                conn.read_tls(&mut slice)?;
                conn.process_new_packets()
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            }
            // A session-level reply (key update, close_notify ack) goes
            // straight back out.
            let _ = self.flush_tls(&mut conn);
        }
    }
    pub fn write(&self, buf: &[u8]) -> std::io::Result<usize> {
        let mut conn = self.conn.lock();
        conn.writer().write_all(buf)?;
        self.flush_tls(&mut conn)?;
        Ok(buf.len())
    }
    pub fn flush(&self) -> std::io::Result<()> {
        let mut conn = self.conn.lock();
        self.flush_tls(&mut conn)
    }
}

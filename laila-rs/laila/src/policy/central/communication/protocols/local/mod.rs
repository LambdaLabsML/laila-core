//! Same-host transports: in-process loopback and unix domain sockets.
//!
//! Python: `laila.policy.central.communication.protocols.local`.

pub mod loopback;
pub mod unixsocket;

pub use loopback::_LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL;
pub use unixsocket::_LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL;

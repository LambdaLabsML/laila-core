//! `unix` -- unix domain socket communication transport.
//!
//! Same-host inter-policy RPC over a unix-socket stream -- the fast,
//! file-permission-secured local channel (no TCP port exposed). Built on
//! the reliable `_StreamRPCProtocol` carrier.
//!
//! - `protocol_name` `"unix"` (aliases `uds` / `unixsocket`)
//! - URI scheme `unix:///path/to/socket`
//!
//! Python: `laila.policy.central.communication.protocols.local.unixsocket`.

use alloc::string::String;

use super::super::base::{CarrierKind, FieldDefault, Protocol, ProtocolSpec, Wire, SPEC_DEFAULTS};

/// `_LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL`'s static spec.
pub static SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL",
    module: "laila.policy.central.communication.protocols.local.unixsocket",
    protocol_name: "unix",
    tokens: &["uds", "unix", "unixsocket"],
    schemes: &["unix://"],
    carrier: CarrierKind::Stream,
    wire: Wire::Unix,
    supports_channels: true,
    fields: &[("path", FieldDefault::Str(""))],
    bases: &[
        "_StreamRPCProtocol",
        "_CarrierRPCProtocol",
        "_LAILA_IDENTIFIABLE_COMM_PROTOCOL",
    ],
    tier: "works",
    ..SPEC_DEFAULTS
};

crate::protocol_class!(_LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL, SPEC);

/// `_effective_path()`: the configured `path`, or a stable path under the
/// system temp dir derived from this protocol's uuid
/// (`laila_unix_<sha1(uuid)[:16]>.sock`).
pub fn effective_path(proto: &Protocol) -> String {
    let path = proto.str_field("path");
    if !path.is_empty() {
        return path;
    }
    let digest = {
        use sha1::{Digest, Sha1};
        let mut h = Sha1::new();
        h.update(proto.uuid().as_bytes());
        h.finalize()
            .iter()
            .map(|b| alloc::format!("{b:02x}"))
            .collect::<String>()
    };
    alloc::format!(
        "{}/laila_unix_{}.sock",
        crate::python::stdlib::tempfile.gettempdir(),
        &digest[..16]
    )
}

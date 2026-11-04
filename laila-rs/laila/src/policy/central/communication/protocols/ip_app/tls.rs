//! `tls` -- TLS communication transport (TCP + SSL).
//!
//! Python: `laila.policy.central.communication.protocols.ip_app.tls`;
//! rides on the `_StreamRPCProtocol` carrier (`Stream` / `Wire::Tls`).

use super::super::base::{CarrierKind, FieldDefault, ProtocolSpec, Wire, SPEC_DEFAULTS};

/// `_LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL`'s static spec.
pub static SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL",
    module: "laila.policy.central.communication.protocols.ip_app.tls",
    protocol_name: "tls",
    tokens: &["ssl", "tcps", "tls", "tls/tcp"],
    schemes: &["tls://", "tcps://"],
    carrier: CarrierKind::Stream,
    wire: Wire::Tls,
    supports_channels: true,
    fields: &[
        ("host", FieldDefault::Str("0.0.0.0")),
        ("port", FieldDefault::Int(0)),
        ("certfile", FieldDefault::None),
        ("keyfile", FieldDefault::None),
        ("cafile", FieldDefault::None),
        ("server_hostname", FieldDefault::None),
    ],
    driver_modules: &[],
    driver_extra: "",
    bases: &[
        "_LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL",
        "_StreamRPCProtocol",
        "_CarrierRPCProtocol",
        "_LAILA_IDENTIFIABLE_COMM_PROTOCOL",
    ],
    tier: "config-required",
    note: "",
    ..SPEC_DEFAULTS
};

crate::protocol_class!(_LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL, SPEC);

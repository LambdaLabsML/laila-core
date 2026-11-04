//! `ethernet` -- Ethernet (incl. PoE) communication transport.
//!
//! Python: `laila.policy.central.communication.protocols.ip_app.ethernet`;
//! rides on the `_StreamRPCProtocol` carrier (`Stream` / `Wire::Tcp`).

use super::super::base::{CarrierKind, FieldDefault, ProtocolSpec, Wire, SPEC_DEFAULTS};

/// `_LAILA_IDENTIFIABLE_ETHERNET_COMM_PROTOCOL`'s static spec.
pub static SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_LAILA_IDENTIFIABLE_ETHERNET_COMM_PROTOCOL",
    module: "laila.policy.central.communication.protocols.ip_app.ethernet",
    protocol_name: "ethernet",
    tokens: &["eth", "ethernet", "poe"],
    schemes: &["ethernet://"],
    carrier: CarrierKind::Stream,
    wire: Wire::Tcp,
    supports_channels: true,
    fields: &[
        ("host", FieldDefault::Str("0.0.0.0")),
        ("port", FieldDefault::Int(0)),
        ("interface", FieldDefault::None),
    ],
    driver_modules: &[],
    driver_extra: "",
    bases: &[
        "_LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL",
        "_StreamRPCProtocol",
        "_CarrierRPCProtocol",
        "_LAILA_IDENTIFIABLE_COMM_PROTOCOL",
    ],
    tier: "works",
    note: "",
    ..SPEC_DEFAULTS
};

crate::protocol_class!(_LAILA_IDENTIFIABLE_ETHERNET_COMM_PROTOCOL, SPEC);

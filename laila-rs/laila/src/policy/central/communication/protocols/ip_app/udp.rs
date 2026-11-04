//! `udp` -- UDP communication transport.
//!
//! Python: `laila.policy.central.communication.protocols.ip_app.udp`;
//! rides on the `_DatagramRPCProtocol` carrier (`Datagram` / `Wire::Udp`).

use super::super::base::{CarrierKind, FieldDefault, ProtocolSpec, Wire, SPEC_DEFAULTS};

/// `_LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL`'s static spec.
pub static SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL",
    module: "laila.policy.central.communication.protocols.ip_app.udp",
    protocol_name: "udp",
    tokens: &["udp", "udp4", "udp6"],
    schemes: &["udp://"],
    carrier: CarrierKind::Datagram,
    wire: Wire::Udp,
    supports_channels: true,
    fields: &[
        ("host", FieldDefault::Str("0.0.0.0")),
        ("port", FieldDefault::Int(0)),
    ],
    driver_modules: &[],
    driver_extra: "",
    bases: &[
        "_DatagramRPCProtocol",
        "_CarrierRPCProtocol",
        "_LAILA_IDENTIFIABLE_COMM_PROTOCOL",
    ],
    tier: "works",
    note: "",
    ..SPEC_DEFAULTS
};

crate::protocol_class!(_LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL, SPEC);

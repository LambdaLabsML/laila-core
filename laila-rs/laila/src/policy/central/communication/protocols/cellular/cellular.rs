//! `cellular` -- Cellular (generic WWAN) communication transport.
//!
//! Python: `laila.policy.central.communication.protocols.cellular.cellular`;
//! rides on the `_StreamRPCProtocol` carrier (`Stream` / `Wire::Tcp`).

use super::super::base::{CarrierKind, FieldDefault, ProtocolSpec, Wire, SPEC_DEFAULTS};

/// `_LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL`'s static spec.
pub static SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL",
    module: "laila.policy.central.communication.protocols.cellular.cellular",
    protocol_name: "cellular",
    tokens: &["cellular", "modem", "wwan"],
    schemes: &["cellular://"],
    carrier: CarrierKind::Stream,
    wire: Wire::Tcp,
    supports_channels: true,
    fields: &[
        ("host", FieldDefault::Str("0.0.0.0")),
        ("port", FieldDefault::Int(0)),
        ("apn", FieldDefault::None),
        ("modem_device", FieldDefault::None),
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

crate::protocol_class!(_LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL, SPEC);

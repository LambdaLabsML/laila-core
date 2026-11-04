//! `wifi-direct` -- Wi-Fi Direct (P2P) communication transport.
//!
//! Python: `laila.policy.central.communication.protocols.short_range.wifi_direct`;
//! rides on the `_StreamRPCProtocol` carrier (`Stream` / `Wire::Tcp`).

use super::super::base::{CarrierKind, FieldDefault, ProtocolSpec, Wire, SPEC_DEFAULTS};

/// `_LAILA_IDENTIFIABLE_WIFIDIRECT_COMM_PROTOCOL`'s static spec.
pub static SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_LAILA_IDENTIFIABLE_WIFIDIRECT_COMM_PROTOCOL",
    module: "laila.policy.central.communication.protocols.short_range.wifi_direct",
    protocol_name: "wifi-direct",
    tokens: &["p2p", "wfd", "wifi-direct", "wifidirect"],
    schemes: &["wifidirect://"],
    carrier: CarrierKind::Stream,
    wire: Wire::Tcp,
    supports_channels: true,
    fields: &[
        ("host", FieldDefault::Str("0.0.0.0")),
        ("port", FieldDefault::Int(0)),
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

crate::protocol_class!(_LAILA_IDENTIFIABLE_WIFIDIRECT_COMM_PROTOCOL, SPEC);

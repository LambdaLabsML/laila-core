//! `wifi` -- Wi-Fi (station / AP) communication transport.
//!
//! Python: `laila.policy.central.communication.protocols.short_range.wifi`;
//! rides on the `_StreamRPCProtocol` carrier (`Stream` / `Wire::Tcp`).

use super::super::base::{CarrierKind, FieldDefault, ProtocolSpec, Wire, SPEC_DEFAULTS};

/// `_LAILA_IDENTIFIABLE_WIFI_COMM_PROTOCOL`'s static spec.
pub static SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_LAILA_IDENTIFIABLE_WIFI_COMM_PROTOCOL",
    module: "laila.policy.central.communication.protocols.short_range.wifi",
    protocol_name: "wifi",
    tokens: &["ap", "sta", "wifi", "wlan"],
    schemes: &["wifi://"],
    carrier: CarrierKind::Stream,
    wire: Wire::Tcp,
    supports_channels: true,
    fields: &[
        ("host", FieldDefault::Str("0.0.0.0")),
        ("port", FieldDefault::Int(0)),
        ("ssid", FieldDefault::None),
        ("mode", FieldDefault::Str("station")),
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

crate::protocol_class!(_LAILA_IDENTIFIABLE_WIFI_COMM_PROTOCOL, SPEC);

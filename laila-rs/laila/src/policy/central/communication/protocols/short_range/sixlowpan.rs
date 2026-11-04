//! `6lowpan` -- 6LoWPAN communication transport.
//!
//! Python: `laila.policy.central.communication.protocols.short_range.sixlowpan`;
//! rides on the `_DatagramRPCProtocol` carrier (`Datagram` / `Wire::Udp`).

use super::super::base::{CarrierKind, FieldDefault, ProtocolSpec, Wire, SPEC_DEFAULTS};

/// `_LAILA_IDENTIFIABLE_SIXLOWPAN_COMM_PROTOCOL`'s static spec.
pub static SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_LAILA_IDENTIFIABLE_SIXLOWPAN_COMM_PROTOCOL",
    module: "laila.policy.central.communication.protocols.short_range.sixlowpan",
    protocol_name: "6lowpan",
    tokens: &["6lowpan", "sixlowpan"],
    schemes: &["sixlowpan://"],
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
        "_LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL",
        "_DatagramRPCProtocol",
        "_CarrierRPCProtocol",
        "_LAILA_IDENTIFIABLE_COMM_PROTOCOL",
    ],
    tier: "works",
    note: "",
    ..SPEC_DEFAULTS
};

crate::protocol_class!(_LAILA_IDENTIFIABLE_SIXLOWPAN_COMM_PROTOCOL, SPEC);

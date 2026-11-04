//! `ethernet-ip` -- EtherNet/IP (CIP) communication transport.
//!
//! Python: `laila.policy.central.communication.protocols.wired.enip`;
//! rides on the `_RegisterRPCProtocol` carrier (`Register` / `Wire::Driver`).

use super::super::base::{CarrierKind, FieldDefault, ProtocolSpec, Wire, SPEC_DEFAULTS};

/// `_LAILA_IDENTIFIABLE_ENIP_COMM_PROTOCOL`'s static spec.
pub static SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_LAILA_IDENTIFIABLE_ENIP_COMM_PROTOCOL",
    module: "laila.policy.central.communication.protocols.wired.enip",
    protocol_name: "ethernet-ip",
    tokens: &["enip", "ethernet-ip", "ethernetip"],
    schemes: &["enip://"],
    carrier: CarrierKind::Register,
    wire: Wire::Driver,
    supports_channels: false,
    fields: &[
        ("host", FieldDefault::Str("127.0.0.1")),
        ("inbox_tag", FieldDefault::Str("LAILA_IN")),
        ("outbox_tag", FieldDefault::Str("LAILA_OUT")),
    ],
    driver_modules: &["pycomm3"],
    driver_extra: "enip",
    bases: &[
        "_RegisterRPCProtocol",
        "_CarrierRPCProtocol",
        "_LAILA_IDENTIFIABLE_COMM_PROTOCOL",
    ],
    tier: "driver/hardware",
    note: "",
    ..SPEC_DEFAULTS
};

crate::protocol_class!(_LAILA_IDENTIFIABLE_ENIP_COMM_PROTOCOL, SPEC);

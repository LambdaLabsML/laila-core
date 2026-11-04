//! `uart` -- UART / serial communication transport.
//!
//! Python: `laila.policy.central.communication.protocols.wired.uart`;
//! rides on the `_P2PStreamRPCProtocol` carrier (`P2P` / `Wire::Serial`).

use super::super::base::{CarrierKind, FieldDefault, ProtocolSpec, Wire, SPEC_DEFAULTS};

/// `_LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL`'s static spec.
pub static SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL",
    module: "laila.policy.central.communication.protocols.wired.uart",
    protocol_name: "uart",
    tokens: &["serial", "uart"],
    schemes: &["serial://", "uart://"],
    carrier: CarrierKind::P2P,
    wire: Wire::Serial,
    supports_channels: true,
    fields: &[
        ("stream_chunk_bytes", FieldDefault::Int(4096)),
        ("port", FieldDefault::Str("")),
        ("baudrate", FieldDefault::Int(115200)),
    ],
    driver_modules: &[],
    driver_extra: "",
    bases: &[
        "_P2PStreamRPCProtocol",
        "_CarrierRPCProtocol",
        "_LAILA_IDENTIFIABLE_COMM_PROTOCOL",
    ],
    tier: "config-required",
    note: "",
    ..SPEC_DEFAULTS
};

crate::protocol_class!(_LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL, SPEC);

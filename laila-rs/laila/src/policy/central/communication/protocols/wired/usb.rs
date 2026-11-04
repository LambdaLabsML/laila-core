//! `usb` -- USB communication transport (CDC-ACM serial).
//!
//! Python: `laila.policy.central.communication.protocols.wired.usb`;
//! rides on the `_P2PStreamRPCProtocol` carrier (`P2P` / `Wire::Serial`).

use super::super::base::{CarrierKind, FieldDefault, ProtocolSpec, Wire, SPEC_DEFAULTS};

/// `_LAILA_IDENTIFIABLE_USB_COMM_PROTOCOL`'s static spec.
pub static SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_LAILA_IDENTIFIABLE_USB_COMM_PROTOCOL",
    module: "laila.policy.central.communication.protocols.wired.usb",
    protocol_name: "usb",
    tokens: &["cdc-acm", "cdcacm", "usb"],
    schemes: &["usb://"],
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
        "_LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL",
        "_P2PStreamRPCProtocol",
        "_CarrierRPCProtocol",
        "_LAILA_IDENTIFIABLE_COMM_PROTOCOL",
    ],
    tier: "config-required",
    note: "",
    ..SPEC_DEFAULTS
};

crate::protocol_class!(_LAILA_IDENTIFIABLE_USB_COMM_PROTOCOL, SPEC);

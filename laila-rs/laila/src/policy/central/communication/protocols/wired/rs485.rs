//! `rs485` -- RS-485 communication transport.
//!
//! Python: `laila.policy.central.communication.protocols.wired.rs485`;
//! rides on the `_P2PStreamRPCProtocol` carrier (`P2P` / `Wire::Serial`).

use super::super::base::{CarrierKind, FieldDefault, ProtocolSpec, Wire, SPEC_DEFAULTS};

/// `log = logging.getLogger(__name__)` -- the module logger.
pub fn log() -> crate::python::stdlib::logging::Logger {
    crate::python::stdlib::logging::Logger::get(
        "laila.policy.central.communication.protocols.wired.rs485",
    )
}

/// `_LAILA_IDENTIFIABLE_RS485_COMM_PROTOCOL`'s static spec.
pub static SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_LAILA_IDENTIFIABLE_RS485_COMM_PROTOCOL",
    module: "laila.policy.central.communication.protocols.wired.rs485",
    protocol_name: "rs485",
    tokens: &["rs-485", "rs485"],
    schemes: &["rs485://"],
    carrier: CarrierKind::P2P,
    wire: Wire::Serial,
    supports_channels: true,
    fields: &[
        ("stream_chunk_bytes", FieldDefault::Int(4096)),
        ("port", FieldDefault::Str("")),
        ("baudrate", FieldDefault::Int(115200)),
        ("rs485_mode", FieldDefault::Bool(true)),
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

crate::protocol_class!(_LAILA_IDENTIFIABLE_RS485_COMM_PROTOCOL, SPEC);

//! `modbus-rtu` -- Modbus-RTU (over RS-485) communication transport.
//!
//! Python: `laila.policy.central.communication.protocols.wired.modbus_rtu`;
//! rides on the `_RegisterRPCProtocol` carrier (`Register` / `Wire::Driver`).
//!
//! Modbus RTU transport requires a serial `port` (e.g. '/dev/ttyUSB0').

use super::super::base::{CarrierKind, FieldDefault, ProtocolSpec, Wire, SPEC_DEFAULTS};

/// `_LAILA_IDENTIFIABLE_MODBUS_RTU_COMM_PROTOCOL`'s static spec.
pub static SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_LAILA_IDENTIFIABLE_MODBUS_RTU_COMM_PROTOCOL",
    module: "laila.policy.central.communication.protocols.wired.modbus_rtu",
    protocol_name: "modbus-rtu",
    tokens: &["modbus-rtu", "modbusrtu"],
    schemes: &["modbusrtu://"],
    carrier: CarrierKind::Register,
    wire: Wire::Driver,
    supports_channels: false,
    fields: &[
        ("host", FieldDefault::Str("127.0.0.1")),
        ("port", FieldDefault::Str("")),
        ("unit_id", FieldDefault::Int(1)),
        ("inbox_base", FieldDefault::Int(0)),
        ("outbox_base", FieldDefault::Int(2000)),
        ("baudrate", FieldDefault::Int(9600)),
    ],
    driver_modules: &["pymodbus"],
    driver_extra: "modbus",
    bases: &[
        "_LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL",
        "_RegisterRPCProtocol",
        "_CarrierRPCProtocol",
        "_LAILA_IDENTIFIABLE_COMM_PROTOCOL",
    ],
    tier: "driver/hardware",
    note: "Modbus RTU transport requires a serial `port` (e.g. '/dev/ttyUSB0').",
    ..SPEC_DEFAULTS
};

crate::protocol_class!(_LAILA_IDENTIFIABLE_MODBUS_RTU_COMM_PROTOCOL, SPEC);

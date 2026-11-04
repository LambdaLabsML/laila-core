//! `modbus-tcp` -- Modbus-TCP communication transport.
//!
//! Python: `laila.policy.central.communication.protocols.ip_app.modbus_tcp`;
//! rides on the `_RegisterRPCProtocol` carrier (`Register` / `Wire::Driver`).

use super::super::base::{CarrierKind, FieldDefault, ProtocolSpec, Wire, SPEC_DEFAULTS};

/// `_LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL`'s static spec.
pub static SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL",
    module: "laila.policy.central.communication.protocols.ip_app.modbus_tcp",
    protocol_name: "modbus-tcp",
    tokens: &["modbus", "modbus-tcp", "modbustcp"],
    schemes: &["modbustcp://"],
    carrier: CarrierKind::Register,
    wire: Wire::Driver,
    supports_channels: false,
    fields: &[
        ("host", FieldDefault::Str("127.0.0.1")),
        ("port", FieldDefault::Int(502)),
        ("unit_id", FieldDefault::Int(1)),
        ("inbox_base", FieldDefault::Int(0)),
        ("outbox_base", FieldDefault::Int(2000)),
    ],
    driver_modules: &["pymodbus"],
    driver_extra: "modbus",
    bases: &[
        "_RegisterRPCProtocol",
        "_CarrierRPCProtocol",
        "_LAILA_IDENTIFIABLE_COMM_PROTOCOL",
    ],
    tier: "driver/hardware",
    note: "",
    ..SPEC_DEFAULTS
};

crate::protocol_class!(_LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL, SPEC);

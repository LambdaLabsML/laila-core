//! `i2c` -- I2C communication transport.
//!
//! Python: `laila.policy.central.communication.protocols.wired.i2c`;
//! rides on the `_RegisterRPCProtocol` carrier (`Register` / `Wire::Driver`).

use super::super::base::{CarrierKind, FieldDefault, ProtocolSpec, Wire, SPEC_DEFAULTS};

/// `_LAILA_IDENTIFIABLE_I2C_COMM_PROTOCOL`'s static spec.
pub static SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_LAILA_IDENTIFIABLE_I2C_COMM_PROTOCOL",
    module: "laila.policy.central.communication.protocols.wired.i2c",
    protocol_name: "i2c",
    tokens: &["i2c"],
    schemes: &["i2c://"],
    carrier: CarrierKind::Register,
    wire: Wire::Driver,
    supports_channels: false,
    fields: &[
        ("bus_number", FieldDefault::Int(1)),
        ("address", FieldDefault::Int(32)),
        ("inbox_reg", FieldDefault::Int(0)),
        ("outbox_reg", FieldDefault::Int(64)),
    ],
    driver_modules: &["smbus2"],
    driver_extra: "i2c",
    bases: &[
        "_RegisterRPCProtocol",
        "_CarrierRPCProtocol",
        "_LAILA_IDENTIFIABLE_COMM_PROTOCOL",
    ],
    tier: "driver/hardware",
    note: "",
    ..SPEC_DEFAULTS
};

crate::protocol_class!(_LAILA_IDENTIFIABLE_I2C_COMM_PROTOCOL, SPEC);

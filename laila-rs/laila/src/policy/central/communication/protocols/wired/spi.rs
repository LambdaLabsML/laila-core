//! `spi` -- SPI communication transport.
//!
//! Python: `laila.policy.central.communication.protocols.wired.spi`;
//! rides on the `_RegisterRPCProtocol` carrier (`Register` / `Wire::Driver`).

use super::super::base::{CarrierKind, FieldDefault, ProtocolSpec, Wire, SPEC_DEFAULTS};

/// `_LAILA_IDENTIFIABLE_SPI_COMM_PROTOCOL`'s static spec.
pub static SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_LAILA_IDENTIFIABLE_SPI_COMM_PROTOCOL",
    module: "laila.policy.central.communication.protocols.wired.spi",
    protocol_name: "spi",
    tokens: &["spi"],
    schemes: &["spi://"],
    carrier: CarrierKind::Register,
    wire: Wire::Driver,
    supports_channels: false,
    fields: &[
        ("bus", FieldDefault::Int(0)),
        ("device", FieldDefault::Int(0)),
        ("max_speed_hz", FieldDefault::Int(1000000)),
    ],
    driver_modules: &["spidev"],
    driver_extra: "spi",
    bases: &[
        "_RegisterRPCProtocol",
        "_CarrierRPCProtocol",
        "_LAILA_IDENTIFIABLE_COMM_PROTOCOL",
    ],
    tier: "driver/hardware",
    note: "",
    ..SPEC_DEFAULTS
};

crate::protocol_class!(_LAILA_IDENTIFIABLE_SPI_COMM_PROTOCOL, SPEC);

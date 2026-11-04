//! `i2s` -- I2S (audio) communication transport (datagram, experimental).
//!
//! Python: `laila.policy.central.communication.protocols.wired.i2s`;
//! rides on the `_LazyDatagramRPCProtocol` carrier (`LazyDatagram` / `Wire::Driver`).

use super::super::base::{CarrierKind, ProtocolSpec, Wire, SPEC_DEFAULTS};

/// `_LAILA_IDENTIFIABLE_I2S_COMM_PROTOCOL`'s static spec.
pub static SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_LAILA_IDENTIFIABLE_I2S_COMM_PROTOCOL",
    module: "laila.policy.central.communication.protocols.wired.i2s",
    protocol_name: "i2s",
    tokens: &["i2s"],
    schemes: &["i2s://"],
    carrier: CarrierKind::LazyDatagram,
    wire: Wire::Driver,
    supports_channels: true,
    fields: &[],
    driver_modules: &[],
    driver_extra: "",
    bases: &[
        "_LazyDatagramRPCProtocol",
        "_DatagramRPCProtocol",
        "_CarrierRPCProtocol",
        "_LAILA_IDENTIFIABLE_COMM_PROTOCOL",
    ],
    tier: "driver/hardware",
    note: "",
    ..SPEC_DEFAULTS
};

crate::protocol_class!(_LAILA_IDENTIFIABLE_I2S_COMM_PROTOCOL, SPEC);

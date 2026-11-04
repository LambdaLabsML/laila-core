//! `1-wire` -- 1-Wire (Dallas) communication transport (datagram).
//!
//! Python: `laila.policy.central.communication.protocols.wired.one_wire`;
//! rides on the `_LazyDatagramRPCProtocol` carrier (`LazyDatagram` / `Wire::Driver`).

use super::super::base::{CarrierKind, ProtocolSpec, Wire, SPEC_DEFAULTS};

/// `_LAILA_IDENTIFIABLE_ONEWIRE_COMM_PROTOCOL`'s static spec.
pub static SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_LAILA_IDENTIFIABLE_ONEWIRE_COMM_PROTOCOL",
    module: "laila.policy.central.communication.protocols.wired.one_wire",
    protocol_name: "1-wire",
    tokens: &["1-wire", "onewire", "w1"],
    schemes: &["onewire://"],
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

crate::protocol_class!(_LAILA_IDENTIFIABLE_ONEWIRE_COMM_PROTOCOL, SPEC);

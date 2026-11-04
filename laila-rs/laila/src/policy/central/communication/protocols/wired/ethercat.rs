//! `ethercat` -- EtherCAT (industrial fieldbus) communication transport (datagram).
//!
//! Python: `laila.policy.central.communication.protocols.wired.ethercat`;
//! rides on the `_LazyDatagramRPCProtocol` carrier (`LazyDatagram` / `Wire::Driver`).

use super::super::base::{CarrierKind, ProtocolSpec, Wire, SPEC_DEFAULTS};

/// `_LAILA_IDENTIFIABLE_ETHERCAT_COMM_PROTOCOL`'s static spec.
pub static SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_LAILA_IDENTIFIABLE_ETHERCAT_COMM_PROTOCOL",
    module: "laila.policy.central.communication.protocols.wired.ethercat",
    protocol_name: "ethercat",
    tokens: &["ethercat"],
    schemes: &["ethercat://"],
    carrier: CarrierKind::LazyDatagram,
    wire: Wire::Driver,
    supports_channels: true,
    fields: &[],
    driver_modules: &["pysoem"],
    driver_extra: "ethercat",
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

crate::protocol_class!(_LAILA_IDENTIFIABLE_ETHERCAT_COMM_PROTOCOL, SPEC);

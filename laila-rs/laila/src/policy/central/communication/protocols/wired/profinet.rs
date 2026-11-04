//! `profinet` -- PROFINET (industrial fieldbus) communication transport (datagram).
//!
//! Python: `laila.policy.central.communication.protocols.wired.profinet`;
//! rides on the `_LazyDatagramRPCProtocol` carrier (`LazyDatagram` / `Wire::Driver`).

use super::super::base::{CarrierKind, ProtocolSpec, Wire, SPEC_DEFAULTS};

/// `_LAILA_IDENTIFIABLE_PROFINET_COMM_PROTOCOL`'s static spec.
pub static SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_LAILA_IDENTIFIABLE_PROFINET_COMM_PROTOCOL",
    module: "laila.policy.central.communication.protocols.wired.profinet",
    protocol_name: "profinet",
    tokens: &["profinet"],
    schemes: &["profinet://"],
    carrier: CarrierKind::LazyDatagram,
    wire: Wire::Driver,
    supports_channels: true,
    fields: &[],
    driver_modules: &["pnet"],
    driver_extra: "profinet",
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

crate::protocol_class!(_LAILA_IDENTIFIABLE_PROFINET_COMM_PROTOCOL, SPEC);

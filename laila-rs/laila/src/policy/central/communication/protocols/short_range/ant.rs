//! `ant` -- ANT / ANT+ communication transport (datagram radio).
//!
//! Python: `laila.policy.central.communication.protocols.short_range.ant`;
//! rides on the `_LazyDatagramRPCProtocol` carrier (`LazyDatagram` / `Wire::Driver`).

use super::super::base::{CarrierKind, ProtocolSpec, Wire, SPEC_DEFAULTS};

/// `_LAILA_IDENTIFIABLE_ANT_COMM_PROTOCOL`'s static spec.
pub static SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_LAILA_IDENTIFIABLE_ANT_COMM_PROTOCOL",
    module: "laila.policy.central.communication.protocols.short_range.ant",
    protocol_name: "ant",
    tokens: &["ant", "ant+"],
    schemes: &["ant://"],
    carrier: CarrierKind::LazyDatagram,
    wire: Wire::Driver,
    supports_channels: true,
    fields: &[],
    driver_modules: &["openant"],
    driver_extra: "ant",
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

crate::protocol_class!(_LAILA_IDENTIFIABLE_ANT_COMM_PROTOCOL, SPEC);

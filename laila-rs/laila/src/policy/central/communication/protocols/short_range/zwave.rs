//! `zwave` -- Z-Wave communication transport (datagram radio).
//!
//! Python: `laila.policy.central.communication.protocols.short_range.zwave`;
//! rides on the `_LazyDatagramRPCProtocol` carrier (`LazyDatagram` / `Wire::Driver`).

use super::super::base::{CarrierKind, ProtocolSpec, Wire, SPEC_DEFAULTS};

/// `_LAILA_IDENTIFIABLE_ZWAVE_COMM_PROTOCOL`'s static spec.
pub static SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_LAILA_IDENTIFIABLE_ZWAVE_COMM_PROTOCOL",
    module: "laila.policy.central.communication.protocols.short_range.zwave",
    protocol_name: "zwave",
    tokens: &["z-wave", "zwave"],
    schemes: &["zwave://"],
    carrier: CarrierKind::LazyDatagram,
    wire: Wire::Driver,
    supports_channels: true,
    fields: &[],
    driver_modules: &["zwave_js_server"],
    driver_extra: "zwave",
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

crate::protocol_class!(_LAILA_IDENTIFIABLE_ZWAVE_COMM_PROTOCOL, SPEC);

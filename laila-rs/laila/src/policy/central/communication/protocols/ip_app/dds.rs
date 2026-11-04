//! `dds` -- DDS / RTPS communication transport (broker-mediated / pub-sub).
//!
//! Python: `laila.policy.central.communication.protocols.ip_app.dds`;
//! rides on the `_BrokerRPCProtocol` carrier (`Broker` / `Wire::Driver`).

use super::super::base::{CarrierKind, ProtocolSpec, Wire, SPEC_DEFAULTS};

/// `_LAILA_IDENTIFIABLE_DDS_COMM_PROTOCOL`'s static spec.
pub static SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_LAILA_IDENTIFIABLE_DDS_COMM_PROTOCOL",
    module: "laila.policy.central.communication.protocols.ip_app.dds",
    protocol_name: "dds",
    tokens: &["dds", "rtps"],
    schemes: &["dds://"],
    carrier: CarrierKind::Broker,
    wire: Wire::Driver,
    supports_channels: false,
    fields: &[],
    driver_modules: &["cyclonedds"],
    driver_extra: "dds",
    bases: &[
        "_BrokerRPCProtocol",
        "_CarrierRPCProtocol",
        "_LAILA_IDENTIFIABLE_COMM_PROTOCOL",
    ],
    tier: "driver/hardware",
    note: "",
    ..SPEC_DEFAULTS
};

crate::protocol_class!(_LAILA_IDENTIFIABLE_DDS_COMM_PROTOCOL, SPEC);

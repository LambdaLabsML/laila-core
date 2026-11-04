//! `coap` -- CoAP communication transport (datagram).
//!
//! Python: `laila.policy.central.communication.protocols.ip_app.coap`;
//! rides on the `_LazyDatagramRPCProtocol` carrier (`LazyDatagram` / `Wire::Driver`).

use super::super::base::{CarrierKind, ProtocolSpec, Wire, SPEC_DEFAULTS};

/// `_LAILA_IDENTIFIABLE_COAP_COMM_PROTOCOL`'s static spec.
pub static SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_LAILA_IDENTIFIABLE_COAP_COMM_PROTOCOL",
    module: "laila.policy.central.communication.protocols.ip_app.coap",
    protocol_name: "coap",
    tokens: &["coap", "coaps"],
    schemes: &["coap://", "coaps://"],
    carrier: CarrierKind::LazyDatagram,
    wire: Wire::Driver,
    supports_channels: true,
    fields: &[],
    driver_modules: &["aiocoap"],
    driver_extra: "coap",
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

crate::protocol_class!(_LAILA_IDENTIFIABLE_COAP_COMM_PROTOCOL, SPEC);

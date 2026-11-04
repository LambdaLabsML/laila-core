//! `dtls` -- DTLS communication transport (datagram, secured).
//!
//! Python: `laila.policy.central.communication.protocols.ip_app.dtls`;
//! rides on the `_LazyDatagramRPCProtocol` carrier (`LazyDatagram` / `Wire::Driver`).

use super::super::base::{CarrierKind, ProtocolSpec, Wire, SPEC_DEFAULTS};

/// `_LAILA_IDENTIFIABLE_DTLS_COMM_PROTOCOL`'s static spec.
pub static SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_LAILA_IDENTIFIABLE_DTLS_COMM_PROTOCOL",
    module: "laila.policy.central.communication.protocols.ip_app.dtls",
    protocol_name: "dtls",
    tokens: &["dtls"],
    schemes: &["dtls://"],
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

crate::protocol_class!(_LAILA_IDENTIFIABLE_DTLS_COMM_PROTOCOL, SPEC);

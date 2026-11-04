//! `http3` -- HTTP/3 (QUIC) communication transport (datagram).
//!
//! Python: `laila.policy.central.communication.protocols.ip_app.http3`;
//! rides on the `_LazyDatagramRPCProtocol` carrier (`LazyDatagram` / `Wire::Driver`).

use super::super::base::{CarrierKind, ProtocolSpec, Wire, SPEC_DEFAULTS};

/// `_LAILA_IDENTIFIABLE_HTTP3_COMM_PROTOCOL`'s static spec.
pub static SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_LAILA_IDENTIFIABLE_HTTP3_COMM_PROTOCOL",
    module: "laila.policy.central.communication.protocols.ip_app.http3",
    protocol_name: "http3",
    tokens: &["h3", "http3", "quic"],
    schemes: &["http3://", "quic://"],
    carrier: CarrierKind::LazyDatagram,
    wire: Wire::Driver,
    supports_channels: true,
    fields: &[],
    driver_modules: &["aioquic"],
    driver_extra: "quic",
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

crate::protocol_class!(_LAILA_IDENTIFIABLE_HTTP3_COMM_PROTOCOL, SPEC);

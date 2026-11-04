//! `http2` -- HTTP/2 communication transport.
//!
//! Python: `laila.policy.central.communication.protocols.ip_app.http2`;
//! rides on the `_P2PStreamRPCProtocol` carrier (`P2P` / `Wire::Driver`).
//!
//! HTTP/2 transport requires an established h2 connection/stream; configure the HTTP/2 endpoint before peering.

use super::super::base::{CarrierKind, FieldDefault, ProtocolSpec, Wire, SPEC_DEFAULTS};

/// `_LAILA_IDENTIFIABLE_HTTP2_COMM_PROTOCOL`'s static spec.
pub static SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_LAILA_IDENTIFIABLE_HTTP2_COMM_PROTOCOL",
    module: "laila.policy.central.communication.protocols.ip_app.http2",
    protocol_name: "http2",
    tokens: &["h2", "http2"],
    schemes: &["http2://"],
    carrier: CarrierKind::P2P,
    wire: Wire::Driver,
    supports_channels: true,
    fields: &[("stream_chunk_bytes", FieldDefault::Int(4096))],
    driver_modules: &["h2"],
    driver_extra: "http",
    bases: &["_P2PStreamRPCProtocol", "_CarrierRPCProtocol", "_LAILA_IDENTIFIABLE_COMM_PROTOCOL"],
    tier: "driver/hardware",
    note: "HTTP/2 transport requires an established h2 connection/stream; configure the HTTP/2 endpoint before peering.",
    ..SPEC_DEFAULTS
};

crate::protocol_class!(_LAILA_IDENTIFIABLE_HTTP2_COMM_PROTOCOL, SPEC);

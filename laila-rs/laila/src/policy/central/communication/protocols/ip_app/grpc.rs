//! `grpc` -- gRPC communication transport.
//!
//! Python: `laila.policy.central.communication.protocols.ip_app.grpc`;
//! rides on the `_P2PStreamRPCProtocol` carrier (`P2P` / `Wire::Driver`).
//!
//! gRPC transport requires an established grpc channel/stream; configure the gRPC endpoint before peering.

use super::super::base::{CarrierKind, FieldDefault, ProtocolSpec, Wire, SPEC_DEFAULTS};

/// `_LAILA_IDENTIFIABLE_GRPC_COMM_PROTOCOL`'s static spec.
pub static SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_LAILA_IDENTIFIABLE_GRPC_COMM_PROTOCOL",
    module: "laila.policy.central.communication.protocols.ip_app.grpc",
    protocol_name: "grpc",
    tokens: &["grpc"],
    schemes: &["grpc://"],
    carrier: CarrierKind::P2P,
    wire: Wire::Driver,
    supports_channels: true,
    fields: &[("stream_chunk_bytes", FieldDefault::Int(4096)), ("target", FieldDefault::Str("127.0.0.1:50051"))],
    driver_modules: &["grpc"],
    driver_extra: "grpc",
    bases: &["_P2PStreamRPCProtocol", "_CarrierRPCProtocol", "_LAILA_IDENTIFIABLE_COMM_PROTOCOL"],
    tier: "driver/hardware",
    note: "gRPC transport requires an established grpc channel/stream; configure the gRPC endpoint before peering.",
    ..SPEC_DEFAULTS
};

crate::protocol_class!(_LAILA_IDENTIFIABLE_GRPC_COMM_PROTOCOL, SPEC);

//! `opcua` -- OPC-UA (industrial) communication transport.
//!
//! Python: `laila.policy.central.communication.protocols.ip_app.opcua`;
//! rides on the `_P2PStreamRPCProtocol` carrier (`P2P` / `Wire::Driver`).
//!
//! OPC UA transport requires an established asyncua client session; configure the OPC UA endpoint before peering.

use super::super::base::{CarrierKind, FieldDefault, ProtocolSpec, Wire, SPEC_DEFAULTS};

/// `_LAILA_IDENTIFIABLE_OPCUA_COMM_PROTOCOL`'s static spec.
pub static SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_LAILA_IDENTIFIABLE_OPCUA_COMM_PROTOCOL",
    module: "laila.policy.central.communication.protocols.ip_app.opcua",
    protocol_name: "opcua",
    tokens: &["opc-ua", "opc.tcp", "opcua"],
    schemes: &["opc.tcp://"],
    carrier: CarrierKind::P2P,
    wire: Wire::Driver,
    supports_channels: true,
    fields: &[("stream_chunk_bytes", FieldDefault::Int(4096)), ("endpoint", FieldDefault::Str("opc.tcp://127.0.0.1:4840"))],
    driver_modules: &["asyncua"],
    driver_extra: "opcua",
    bases: &["_P2PStreamRPCProtocol", "_CarrierRPCProtocol", "_LAILA_IDENTIFIABLE_COMM_PROTOCOL"],
    tier: "driver/hardware",
    note: "OPC UA transport requires an established asyncua client session; configure the OPC UA endpoint before peering.",
    ..SPEC_DEFAULTS
};

crate::protocol_class!(_LAILA_IDENTIFIABLE_OPCUA_COMM_PROTOCOL, SPEC);

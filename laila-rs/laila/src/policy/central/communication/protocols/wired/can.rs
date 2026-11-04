//! `can` -- CAN / CAN-FD communication transport.
//!
//! Python: `laila.policy.central.communication.protocols.wired.can`;
//! rides on the `_P2PStreamRPCProtocol` carrier (`P2P` / `Wire::Driver`).
//!
//! The CAN transport requires python-can and can-isotp. Install them with `pip install laila-core[can]` (or `pip install python-can can-isotp`), and bring up a CAN interface (e.g. `sudo modprobe vcan && sudo ip link add dev vcan0 type vcan && sudo ip link set up vcan0`).

use super::super::base::{CarrierKind, FieldDefault, ProtocolSpec, Wire, SPEC_DEFAULTS};

/// `_LAILA_IDENTIFIABLE_CAN_COMM_PROTOCOL`'s static spec.
pub static SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_LAILA_IDENTIFIABLE_CAN_COMM_PROTOCOL",
    module: "laila.policy.central.communication.protocols.wired.can",
    protocol_name: "can",
    tokens: &["can", "can-fd", "canfd"],
    schemes: &["can://"],
    carrier: CarrierKind::P2P,
    wire: Wire::Driver,
    supports_channels: true,
    fields: &[("stream_chunk_bytes", FieldDefault::Int(4096)), ("channel", FieldDefault::Str("vcan0")), ("bustype", FieldDefault::Str("socketcan")), ("tx_id", FieldDefault::Int(291)), ("rx_id", FieldDefault::Int(1110)), ("fd", FieldDefault::Bool(false))],
    driver_modules: &[],
    driver_extra: "",
    bases: &["_P2PStreamRPCProtocol", "_CarrierRPCProtocol", "_LAILA_IDENTIFIABLE_COMM_PROTOCOL"],
    tier: "driver/hardware",
    note: "The CAN transport requires python-can and can-isotp. Install them with `pip install laila-core[can]` (or `pip install python-can can-isotp`), and bring up a CAN interface (e.g. `sudo modprobe vcan && sudo ip link add dev vcan0 type vcan && sudo ip link set up vcan0`).",
    ..SPEC_DEFAULTS
};

crate::protocol_class!(_LAILA_IDENTIFIABLE_CAN_COMM_PROTOCOL, SPEC);

//! `lora` -- LoRa communication protocol (scaffold / planned transport).
//!
//! Python: `laila.policy.central.communication.protocols.lpwan.lora`;
//! rides on the `_LAILA_IDENTIFIABLE_COMM_PROTOCOL` carrier (`Base` / `Wire::None`).

use super::super::base::{CarrierKind, ProtocolSpec, Wire, SPEC_DEFAULTS};

/// `_LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL`'s static spec.
pub static SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL",
    module: "laila.policy.central.communication.protocols.lpwan.lora",
    protocol_name: "lora",
    tokens: &[],
    schemes: &["lora://"],
    carrier: CarrierKind::Base,
    wire: Wire::None,
    supports_channels: false,
    fields: &[],
    driver_modules: &[],
    driver_extra: "",
    bases: &["_LAILA_IDENTIFIABLE_COMM_PROTOCOL"],
    tier: "scaffold",
    note: "The LoRa transport is a planned protocol and is not implemented yet. Use the TCP/IP protocol (comm_protocol='tcpip') for now.",
    ..SPEC_DEFAULTS
};

crate::protocol_class!(_LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL, SPEC);

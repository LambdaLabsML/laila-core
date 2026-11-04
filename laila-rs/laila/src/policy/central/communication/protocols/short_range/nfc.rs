//! `nfc` -- NFC communication transport (datagram radio).
//!
//! Python: `laila.policy.central.communication.protocols.short_range.nfc`;
//! rides on the `_LazyDatagramRPCProtocol` carrier (`LazyDatagram` / `Wire::Driver`).

use super::super::base::{CarrierKind, ProtocolSpec, Wire, SPEC_DEFAULTS};

/// `_LAILA_IDENTIFIABLE_NFC_COMM_PROTOCOL`'s static spec.
pub static SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_LAILA_IDENTIFIABLE_NFC_COMM_PROTOCOL",
    module: "laila.policy.central.communication.protocols.short_range.nfc",
    protocol_name: "nfc",
    tokens: &["nfc"],
    schemes: &["nfc://"],
    carrier: CarrierKind::LazyDatagram,
    wire: Wire::Driver,
    supports_channels: true,
    fields: &[],
    driver_modules: &["nfc"],
    driver_extra: "nfc",
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

crate::protocol_class!(_LAILA_IDENTIFIABLE_NFC_COMM_PROTOCOL, SPEC);

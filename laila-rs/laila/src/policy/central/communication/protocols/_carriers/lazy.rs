//! `_LazyDatagramRPCProtocol` -- the datagram carrier for driver /
//! hardware-backed packet radios (Zigbee, Thread, Z-Wave, ANT, ESP-NOW,
//! CoAP, DTLS, QUIC, ...).
//!
//! `start()` first imports the declared driver(s) (`_DRIVER_MODULES`,
//! raising the `laila-core[<extra>]` install hint when missing) and then
//! brings the radio up (`_setup_endpoint`). Transports with no host-side
//! driver (the device is firmware, e.g. ESP-NOW) raise a clear runtime
//! error describing the hardware requirement -- never a silent stub.
//!
//! The Rust build has no host-side Python drivers, so every lazy datagram
//! transport stops at the driver check (see `datagram::create_endpoint`);
//! the peer address of a `<scheme>://<node>` URI is the URI authority.

use crate::python::Arg;

use super::super::base::{CarrierKind, Protocol};

/// `_LazyDatagramRPCProtocol` as a class marker.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct _LazyDatagramRPCProtocol;
impl crate::python::builtins::IsInstance for _LazyDatagramRPCProtocol {
    fn isinstance(x: &Arg) -> bool {
        Protocol::of(x).is_some_and(|p| p.spec().carrier == CarrierKind::LazyDatagram)
    }
}

/// `_DRIVER_MODULES` of a lazy transport (`()` for firmware devices).
pub fn driver_modules(proto: &Protocol) -> &'static [&'static str] {
    proto.spec().driver_modules
}

/// `_resolve_peer_addr(uri)`: the URI authority is the radio node address.
pub fn _resolve_peer_addr(uri: &str) -> alloc::string::String {
    super::uri::uri_authority(uri)
}

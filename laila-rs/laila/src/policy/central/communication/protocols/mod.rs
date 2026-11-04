//! Communication protocol implementations.
//!
//! The base contract is [`base::_LAILA_IDENTIFIABLE_COMM_PROTOCOL`]; the
//! reusable RPC carriers live under [`_carriers`]. Concrete transports are
//! grouped into category sub-modules (`ip_app`, `local`, `lpwan`,
//! `short_range`, `cellular`, `wired`) and re-exported here.
//!
//! Python discovers transports by walking the package
//! (`_autoload_transports`) and the class tree (`__subclasses__`); the
//! port keeps the same inventory as a static table, [`ALL_SPECS`], which
//! [`iter_comm_protocols`] / [`comm_protocol_for_token`] walk.

pub mod _carriers;
pub mod base;
pub mod cellular;
pub mod ip_app;
pub mod local;
pub mod lpwan;
pub mod short_range;
pub mod tcpip;
pub mod wired;

pub use base::{Protocol, ProtocolSpec, _LAILA_IDENTIFIABLE_COMM_PROTOCOL};
pub use ip_app::tcp::_LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL;
pub use ip_app::tls::_LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL;
pub use ip_app::udp::_LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL;
pub use local::loopback::_LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL;
pub use local::unixsocket::_LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL;
pub use tcpip::_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL;

use alloc::vec::Vec;

use crate::python::{Arg, IntoArg, PyClass};

/// Every concrete transport class (the `_autoload_transports` inventory),
/// in package walk order.
pub static ALL_SPECS: &[&ProtocolSpec] = &[
    &tcpip::SPEC,
    &cellular::cellular::SPEC,
    &cellular::gsm_gprs::SPEC,
    &cellular::lte::SPEC,
    &cellular::nr5g::SPEC,
    &ip_app::amqp::SPEC,
    &ip_app::coap::SPEC,
    &ip_app::dds::SPEC,
    &ip_app::dtls::SPEC,
    &ip_app::ethernet::SPEC,
    &ip_app::grpc::SPEC,
    &ip_app::http2::SPEC,
    &ip_app::http3::SPEC,
    &ip_app::modbus_tcp::SPEC,
    &ip_app::mqtt::SPEC,
    &ip_app::opcua::SPEC,
    &ip_app::tcp::SPEC,
    &ip_app::tls::SPEC,
    &ip_app::udp::SPEC,
    &ip_app::xmpp::SPEC,
    &ip_app::zeromq::SPEC,
    &local::loopback::SPEC,
    &local::unixsocket::SPEC,
    &lpwan::lora::SPEC,
    &lpwan::lorawan::SPEC,
    &lpwan::ltem::SPEC,
    &lpwan::nbiot::SPEC,
    &lpwan::satellite::SPEC,
    &lpwan::sigfox::SPEC,
    &short_range::ant::SPEC,
    &short_range::bluetooth::SPEC,
    &short_range::espnow::SPEC,
    &short_range::irda::SPEC,
    &short_range::matter::SPEC,
    &short_range::nfc::SPEC,
    &short_range::rfid::SPEC,
    &short_range::sixlowpan::SPEC,
    &short_range::thread::SPEC,
    &short_range::uwb::SPEC,
    &short_range::wifi::SPEC,
    &short_range::wifi_direct::SPEC,
    &short_range::zigbee::SPEC,
    &short_range::zwave::SPEC,
    &wired::can::SPEC,
    &wired::enip::SPEC,
    &wired::ethercat::SPEC,
    &wired::i2c::SPEC,
    &wired::i2s::SPEC,
    &wired::lin::SPEC,
    &wired::modbus_rtu::SPEC,
    &wired::one_wire::SPEC,
    &wired::profinet::SPEC,
    &wired::rs232::SPEC,
    &wired::rs485::SPEC,
    &wired::sdio::SPEC,
    &wired::spi::SPEC,
    &wired::uart::SPEC,
    &wired::usb::SPEC,
];

/// `iter_comm_protocols()`: every concrete transport class currently
/// loaded (the specs, `base` / `carrier` excluded).
pub fn iter_comm_protocol_specs() -> Vec<&'static ProtocolSpec> {
    ALL_SPECS
        .iter()
        .copied()
        .filter(|s| !matches!(s.protocol_name, "base" | "carrier"))
        .collect()
}

/// `iter_comm_protocols()` as class objects.
pub fn iter_comm_protocols() -> Vec<PyClass> {
    iter_comm_protocol_specs()
        .into_iter()
        .map(|s| s.class_object())
        .collect()
}

/// `comm_protocol_for_token(token)`: the spec answering to *token*.
pub fn comm_protocol_spec_for_token(token: &str) -> Option<&'static ProtocolSpec> {
    iter_comm_protocol_specs()
        .into_iter()
        .find(|s| s.matches_token(token))
}

/// `comm_protocol_for_token(token)`: the class object, or `None`.
pub fn comm_protocol_for_token(token: &str) -> Arg {
    match comm_protocol_spec_for_token(token) {
        Some(s) => s.class_object().into_arg(),
        None => Arg::None,
    }
}

/// The spec claiming *uri* (`can_handle_uri`), if any.
pub fn comm_protocol_spec_for_uri(uri: &str) -> Option<&'static ProtocolSpec> {
    iter_comm_protocol_specs()
        .into_iter()
        .find(|s| s.can_handle_uri(uri))
}

/// The spec named *class_name*, if any (the `isinstance` helper).
pub fn spec_by_class_name(class_name: &str) -> Option<&'static ProtocolSpec> {
    if class_name == base::BASE_SPEC.class_name {
        return Some(&base::BASE_SPEC);
    }
    ALL_SPECS
        .iter()
        .copied()
        .find(|s| s.class_name == class_name)
}

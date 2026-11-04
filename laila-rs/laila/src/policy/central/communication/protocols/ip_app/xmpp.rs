//! `xmpp` -- XMPP communication transport (broker-mediated).
//!
//! Python: `laila.policy.central.communication.protocols.ip_app.xmpp`;
//! rides on the `_BrokerRPCProtocol` carrier (`Broker` / `Wire::Driver`).

use super::super::base::{CarrierKind, FieldDefault, ProtocolSpec, Wire, SPEC_DEFAULTS};

/// `_LAILA_IDENTIFIABLE_XMPP_COMM_PROTOCOL`'s static spec.
pub static SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_LAILA_IDENTIFIABLE_XMPP_COMM_PROTOCOL",
    module: "laila.policy.central.communication.protocols.ip_app.xmpp",
    protocol_name: "xmpp",
    tokens: &["jabber", "xmpp"],
    schemes: &["xmpp://"],
    carrier: CarrierKind::Broker,
    wire: Wire::Driver,
    supports_channels: false,
    fields: &[
        ("jid", FieldDefault::Str("")),
        ("password", FieldDefault::Str("")),
        ("server_host", FieldDefault::Str("127.0.0.1")),
        ("server_port", FieldDefault::Int(5222)),
    ],
    driver_modules: &["slixmpp"],
    driver_extra: "xmpp",
    bases: &[
        "_BrokerRPCProtocol",
        "_CarrierRPCProtocol",
        "_LAILA_IDENTIFIABLE_COMM_PROTOCOL",
    ],
    tier: "driver/hardware",
    note: "",
    ..SPEC_DEFAULTS
};

crate::protocol_class!(_LAILA_IDENTIFIABLE_XMPP_COMM_PROTOCOL, SPEC);

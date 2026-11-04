//! `mqtt` -- MQTT (and MQTT-SN) communication transport.
//!
//! Python: `laila.policy.central.communication.protocols.ip_app.mqtt`;
//! rides on the `_BrokerRPCProtocol` carrier (`Broker` / `Wire::Driver`).
//!
//! The MQTT transport requires paho-mqtt. Install it with `pip install laila-core[mqtt]` (or `pip install paho-mqtt`).

use super::super::base::{CarrierKind, FieldDefault, ProtocolSpec, Wire, SPEC_DEFAULTS};

/// `log = logging.getLogger(__name__)` -- the module logger.
pub fn log() -> crate::python::stdlib::logging::Logger {
    crate::python::stdlib::logging::Logger::get(
        "laila.policy.central.communication.protocols.ip_app.mqtt",
    )
}

/// `_LAILA_IDENTIFIABLE_MQTT_COMM_PROTOCOL`'s static spec.
pub static SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_LAILA_IDENTIFIABLE_MQTT_COMM_PROTOCOL",
    module: "laila.policy.central.communication.protocols.ip_app.mqtt",
    protocol_name: "mqtt",
    tokens: &["mqtt", "mqtt-sn", "mqtts"],
    schemes: &["mqtt://", "mqtts://"],
    carrier: CarrierKind::Broker,
    wire: Wire::Driver,
    supports_channels: false,
    fields: &[("broker_host", FieldDefault::Str("127.0.0.1")), ("broker_port", FieldDefault::Int(1883))],
    driver_modules: &[],
    driver_extra: "",
    bases: &["_BrokerRPCProtocol", "_CarrierRPCProtocol", "_LAILA_IDENTIFIABLE_COMM_PROTOCOL"],
    tier: "driver/hardware",
    note: "The MQTT transport requires paho-mqtt. Install it with `pip install laila-core[mqtt]` (or `pip install paho-mqtt`).",
    ..SPEC_DEFAULTS
};

crate::protocol_class!(_LAILA_IDENTIFIABLE_MQTT_COMM_PROTOCOL, SPEC);

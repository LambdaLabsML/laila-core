//! `amqp` -- AMQP (RabbitMQ) communication transport.
//!
//! Python: `laila.policy.central.communication.protocols.ip_app.amqp`;
//! rides on the `_BrokerRPCProtocol` carrier (`Broker` / `Wire::Driver`).
//!
//! The AMQP transport requires aio-pika. Install it with `pip install laila-core[amqp]` (or `pip install aio-pika`).

use super::super::base::{CarrierKind, FieldDefault, ProtocolSpec, Wire, SPEC_DEFAULTS};

/// `log = logging.getLogger(__name__)` -- the module logger.
pub fn log() -> crate::python::stdlib::logging::Logger {
    crate::python::stdlib::logging::Logger::get(
        "laila.policy.central.communication.protocols.ip_app.amqp",
    )
}

/// `_LAILA_IDENTIFIABLE_AMQP_COMM_PROTOCOL`'s static spec.
pub static SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_LAILA_IDENTIFIABLE_AMQP_COMM_PROTOCOL",
    module: "laila.policy.central.communication.protocols.ip_app.amqp",
    protocol_name: "amqp",
    tokens: &["amqp", "amqps", "rabbitmq"],
    schemes: &["amqp://", "amqps://"],
    carrier: CarrierKind::Broker,
    wire: Wire::Driver,
    supports_channels: false,
    fields: &[("broker_url", FieldDefault::Str("amqp://guest:guest@127.0.0.1/"))],
    driver_modules: &[],
    driver_extra: "",
    bases: &["_BrokerRPCProtocol", "_CarrierRPCProtocol", "_LAILA_IDENTIFIABLE_COMM_PROTOCOL"],
    tier: "driver/hardware",
    note: "The AMQP transport requires aio-pika. Install it with `pip install laila-core[amqp]` (or `pip install aio-pika`).",
    ..SPEC_DEFAULTS
};

crate::protocol_class!(_LAILA_IDENTIFIABLE_AMQP_COMM_PROTOCOL, SPEC);

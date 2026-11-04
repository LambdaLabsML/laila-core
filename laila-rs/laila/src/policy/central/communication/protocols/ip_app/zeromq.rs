//! `zeromq` -- ZeroMQ (and nanomsg/nng) communication transport.
//!
//! Python: `laila.policy.central.communication.protocols.ip_app.zeromq`;
//! rides on the `_BrokerRPCProtocol` carrier (`Broker` / `Wire::Driver`).
//!
//! The ZeroMQ transport requires pyzmq. Install it with `pip install laila-core[zmq]` (or `pip install pyzmq`).

use super::super::base::{CarrierKind, FieldDefault, ProtocolSpec, Wire, SPEC_DEFAULTS};

/// `log = logging.getLogger(__name__)` -- the module logger.
pub fn log() -> crate::python::stdlib::logging::Logger {
    crate::python::stdlib::logging::Logger::get(
        "laila.policy.central.communication.protocols.ip_app.zeromq",
    )
}

/// `_LAILA_IDENTIFIABLE_ZEROMQ_COMM_PROTOCOL`'s static spec.
pub static SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_LAILA_IDENTIFIABLE_ZEROMQ_COMM_PROTOCOL",
    module: "laila.policy.central.communication.protocols.ip_app.zeromq",
    protocol_name: "zeromq",
    tokens: &["nanomsg", "nng", "zeromq", "zmq"],
    schemes: &["zmq://", "zeromq://"],
    carrier: CarrierKind::Broker,
    wire: Wire::Driver,
    supports_channels: false,
    fields: &[("endpoint_scheme", FieldDefault::Str("ipc")), ("tcp_host", FieldDefault::Str("127.0.0.1"))],
    driver_modules: &[],
    driver_extra: "",
    bases: &["_BrokerRPCProtocol", "_CarrierRPCProtocol", "_LAILA_IDENTIFIABLE_COMM_PROTOCOL"],
    tier: "driver/hardware",
    note: "The ZeroMQ transport requires pyzmq. Install it with `pip install laila-core[zmq]` (or `pip install pyzmq`).",
    ..SPEC_DEFAULTS
};

crate::protocol_class!(_LAILA_IDENTIFIABLE_ZEROMQ_COMM_PROTOCOL, SPEC);

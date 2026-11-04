//! Stub transports the communication unit tests subclass in Python
//! (`class _FakeCarrier(_CarrierRPCProtocol)`, `class
//! _DeadProtocol(_LAILA_IDENTIFIABLE_COMM_PROTOCOL)`). A Rust protocol
//! class is a static [`ProtocolSpec`] plus [`ProtocolHooks`] for the verbs
//! the Python subclass overrides; the `class_aliases` override of the test
//! file imports these under the Python names.

use std::sync::Arc;

use laila::policy::central::communication::protocols::base::{
    CarrierKind, Protocol, ProtocolHooks, ProtocolSpec, Wire, SPEC_DEFAULTS,
};
use laila::python::{Arg, Args, IntoArg, LailaError, Result};

/// `class _FakeCarrier(_CarrierRPCProtocol): protocol_name = "fake"` -- a
/// carrier with no wire: admission, inbound dispatch and the `send_rpc`
/// backoff loop are the shared carrier base's; `_send_once` is whatever
/// the test assigns (`c._send_once = fn`).
pub static FAKE_CARRIER_SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_FakeCarrier",
    module: "tests.functional.policy.communication.unit_tests.test_backpressure",
    protocol_name: "fake",
    tokens: &["fake"],
    schemes: &[],
    carrier: CarrierKind::Stream,
    wire: Wire::None,
    supports_channels: false,
    bases: &[
        "_StreamRPCProtocol",
        "_CarrierRPCProtocol",
        "_LAILA_IDENTIFIABLE_COMM_PROTOCOL",
    ],
    ..SPEC_DEFAULTS
};

laila::protocol_class!(_FakeCarrier, FAKE_CARRIER_SPEC);

/// `class _DeadProtocol(_LAILA_IDENTIFIABLE_COMM_PROTOCOL)`: holds peers in
/// `_held` (a set the test adds to) but reports every one of them dead.
pub static DEAD_PROTOCOL_SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_DeadProtocol",
    module: "tests.functional.policy.communication.unit_tests.test_liveness",
    protocol_name: "deadstub",
    tokens: &["deadstub"],
    schemes: &[],
    carrier: CarrierKind::Base,
    wire: Wire::None,
    bases: &["_LAILA_IDENTIFIABLE_COMM_PROTOCOL"],
    ..SPEC_DEFAULTS
};

struct DeadHooks;

impl DeadHooks {
    fn held(proto: &Protocol) -> Arg {
        proto.field("_held")
    }
}

impl ProtocolHooks for DeadHooks {
    fn start(&self, _proto: &Protocol) -> Option<Result<()>> {
        Some(Ok(()))
    }
    fn stop(&self, _proto: &Protocol) -> Option<Result<()>> {
        Some(Ok(()))
    }
    fn connect(&self, _proto: &Protocol, _uri: &str, _secret: &str) -> Option<Result<String>> {
        Some(Err(LailaError::not_implemented("")))
    }
    fn disconnect(&self, proto: &Protocol, peer_id: &str) -> Option<Result<()>> {
        let held = Self::held(proto);
        let _ = held.__call_method__("discard", Args::from(peer_id.into_arg()));
        Some(Ok(()))
    }
    fn ping(&self, _proto: &Protocol, _peer_id: &str, _timeout: Option<f64>) -> Option<bool> {
        Some(false) // always "dead"
    }
    fn has_peer(&self, proto: &Protocol, peer_id: &str) -> Option<bool> {
        Some(Self::held(proto).__contains__(peer_id).unwrap_or(false))
    }
    fn send_rpc(
        &self,
        _proto: &Protocol,
        _peer_id: &str,
        _path: &[String],
        _args: &[Arg],
        _kwargs: &Arg,
    ) -> Option<Result<Arg>> {
        Some(Err(LailaError::connection_error("dead")))
    }
}

/// `_DeadProtocol(**kwargs)` -- the class object / constructor.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct _DeadProtocol;

impl _DeadProtocol {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(args: impl Into<Args>) -> Result<Protocol> {
        let proto = Protocol::new(&DEAD_PROTOCOL_SPEC, args)?;
        proto.set_field("_held", laila::python::builtins::set(Args::new())?);
        proto.set_hooks(Arc::new(DeadHooks));
        Ok(proto)
    }
    pub fn class_object() -> laila::python::PyClass {
        laila::python::PyClass::with_ctor(
            DEAD_PROTOCOL_SPEC.class_name,
            DEAD_PROTOCOL_SPEC.module,
            Arc::new(|a: Args| Self::new(a).map(IntoArg::into_arg)),
        )
    }
}
impl laila::python::builtins::IsInstance for _DeadProtocol {
    fn isinstance(x: &Arg) -> bool {
        Protocol::of(x).is_some_and(|p| p.spec().class_name == DEAD_PROTOCOL_SPEC.class_name)
    }
}
impl IntoArg for _DeadProtocol {
    fn into_arg(self) -> Arg {
        Self::class_object().into_arg()
    }
}
impl laila::python::PyArg for _DeadProtocol {
    fn to_arg(&self) -> Arg {
        Self::class_object().into_arg()
    }
}
impl laila::python::builtins::CallValue for _DeadProtocol {
    fn call_value(&self, args: Args) -> Result<Arg> {
        Self::new(args).map(IntoArg::into_arg)
    }
}
impl laila::python::builtins::ClassValue for _DeadProtocol {
    fn class_value() -> Arg {
        Self::class_object().into_arg()
    }
}

/// `class _RpcOnlyLoopback(DefaultLoopbackProtocol): supports_channels =
/// False` -- the loopback carrier with its stream lanes switched off
/// (stands in for a framed-RPC-only transport). `_stream_enqueue` is never
/// reached: every lane entry point is guarded by `supports_channels`.
pub static RPC_ONLY_LOOPBACK_SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_RpcOnlyLoopback",
    module: "tests.functional.policy.communication.streaming.unit_tests.test_peer_registry",
    protocol_name: "rpconly",
    tokens: &["rpconly"],
    schemes: &["loopback://"],
    carrier: CarrierKind::Loopback,
    wire: Wire::Loopback,
    supports_channels: false,
    bases: &[
        "_LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL",
        "_CarrierRPCProtocol",
        "_LAILA_IDENTIFIABLE_COMM_PROTOCOL",
    ],
    tier: "works",
    ..SPEC_DEFAULTS
};

laila::protocol_class!(_RpcOnlyLoopback, RPC_ONLY_LOOPBACK_SPEC);

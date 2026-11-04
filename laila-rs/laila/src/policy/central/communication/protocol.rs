//! JSON-RPC 2.0 message helpers and LAILA-aware JSON encoding
//! (`laila.policy.central.communication.protocol`).
//!
//! All inter-policy communication uses the JSON-RPC 2.0 wire format. Three
//! RPC methods are defined by laila on top of that:
//!
//! - `peer.connect` -- the inbound side of an outbound peering handshake.
//!   Carries the initiating policy's `global_id` and the shared secret.
//! - `peer.disconnect` -- a *notification* (no `id`, no reply) a policy
//!   sends right before it drops a peer or shuts a transport down.
//! - `rpc.call` -- a remote attribute-chain invocation. Carries `path`
//!   (list[str]), `args` (list), `kwargs` (dict).
//!
//! The module is also a *value* ([`protocol`]) so test code written
//! against the Python module (`protocol.make_request(..)`,
//! `protocol.ERR_BUSY`) lowers unchanged.

use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::python::object::PyObject;
use crate::python::{Arg, Args, IntoArg, LailaError, PyArg, PyDictBuilder, Result};

pub const JSONRPC_VERSION: &str = "2.0";

pub const ERR_INVALID_REQUEST: i64 = -32600;
pub const ERR_METHOD_NOT_FOUND: i64 = -32601;
pub const ERR_AUTH_FAILED: i64 = -32001;
pub const ERR_EXECUTION: i64 = -32002;
/// Backpressure: the receiving policy's inbound RPC queue is at capacity.
/// Senders treat this as retryable (exponential backoff) rather than a
/// hard failure -- it is distinct from `ERR_EXECUTION` so retries stay
/// scoped to overload only.
pub const ERR_BUSY: i64 = -32003;

/// Keys that identify the self-describing `Entry.serialize(
/// transformation_base64)` envelope on the wire.
pub const _ENTRY_ENVELOPE_KEYS: [&str; 4] = ["_uuid", "_scopes", "payload", "constitution"];

/// The wire envelope for a READY `Entry`, else `None`.
///
/// The envelope is the same `Entry.serialize(transformation_base64)` form
/// future results travel in, so a peer rebuilds the real entry with
/// `build_by_scope`. Non-READY entries (nothing faithful to ship yet) and
/// non-entries yield `None` so the caller can fall through to the generic
/// encoders.
pub fn serialize_entry_arg(o: &Arg) -> Arg {
    let Arg::Entry(e) = o else {
        return Arg::None;
    };
    let mut a = Args::new();
    a.push_keyword(
        "transformations",
        crate::entry::transformation_base64().into_arg(),
    );
    match e.serialize(a) {
        Ok(v) => v,
        Err(_) => Arg::None,
    }
}

/// `True` when *value* is a dict carrying the serialized-entry signature.
pub fn is_entry_envelope(value: &Arg) -> bool {
    match value {
        Arg::Dict(d) => {
            let d = d.read();
            _ENTRY_ENVELOPE_KEYS
                .iter()
                .all(|k| d.contains_key(&Arg::Str((*k).into())))
        }
        _ => false,
    }
}

fn rehydrate_one(v: &Arg) -> Result<Arg> {
    if is_entry_envelope(v) {
        let mut a = Args::from(v.clone());
        a.push_keyword("asynchronous", false);
        return crate::entry::constitution::build_maps::build_by_scope(a);
    }
    match v {
        Arg::List(_) | Arg::Tuple(_) => {
            let items = v.__iter__()?;
            if !items.iter().any(is_entry_envelope) {
                return Ok(v.clone());
            }
            let mut out = Vec::with_capacity(items.len());
            for x in items {
                if is_entry_envelope(&x) {
                    let mut a = Args::from(x);
                    a.push_keyword("asynchronous", false);
                    out.push(crate::entry::constitution::build_maps::build_by_scope(a)?);
                } else {
                    out.push(x);
                }
            }
            Ok(match v {
                Arg::Tuple(_) => Arg::Tuple(out.into()),
                _ => out.into_arg(),
            })
        }
        other => Ok(other.clone()),
    }
}

/// Rebuild entries shipped as wire envelopes inside RPC `args` / `kwargs`.
///
/// Walks one level deep (top-level values and the items of top-level
/// lists / tuples). Returns `(list, dict)`.
pub fn rehydrate_entry_args(args: &Arg, kwargs: &Arg) -> Result<(Arg, Arg)> {
    let mut out_args = Vec::new();
    if !args.is_none() {
        for a in args.__iter__()? {
            out_args.push(rehydrate_one(&a)?);
        }
    }
    let mut out_kwargs = PyDictBuilder::new();
    if !kwargs.is_none() {
        for kv in kwargs.items()?.__iter__()? {
            let pair = kv.__iter__()?;
            if let [k, v] = pair.as_slice() {
                out_kwargs.insert(k.clone(), rehydrate_one(v)?);
            }
        }
    }
    Ok((out_args.into_arg(), out_kwargs.finish()))
}

/// JSON encoder that serialises LAILA objects via their existing hooks
/// (`json.JSONEncoder` subclass in Python; [`encode`] uses it).
#[derive(Clone, Copy, Debug, Default)]
pub struct LailaJSONEncoder;

impl LailaJSONEncoder {
    /// `LailaJSONEncoder.default(o)`: see [`json_default`].
    pub fn default(&self, o: &Arg) -> Result<Arg> {
        json_default(o)
    }
}

/// `LailaJSONEncoder.default(o)`: the JSON-able stand-in for a non-JSON
/// object, in the documented resolution order (group future envelope,
/// future envelope, READY entry envelope, `model_dump()`, `as_dict()`,
/// `identity()`, `str(o)`).
pub fn json_default(o: &Arg) -> Result<Arg> {
    if let Arg::Future(f) = o {
        let mut d = PyDictBuilder::new();
        d.insert("__laila_future__", true);
        let is_group = f.kind() == crate::policy::central::command::FutureKind::Group;
        d.insert("__is_group__", is_group);
        d.insert("global_id", f.global_id());
        d.insert("policy_id", f.policy_id().__str__());
        d.insert("taskforce_id", f.taskforce_id().__str__());
        if is_group {
            d.insert("future_ids", f.future_ids());
        }
        return Ok(d.finish());
    }
    if let Some(rf) =
        o.downcast_ref::<crate::policy::central::command::schema::future::future::RemoteFuture>()
    {
        // A remote future forwarded onwards: the same envelope its origin
        // emitted (a peer of a peer resolves it through *its* owner).
        let mut d = PyDictBuilder::new();
        d.insert("__laila_future__", true);
        d.insert("__is_group__", rf.is_group());
        d.insert("global_id", rf.global_id());
        d.insert("policy_id", rf.policy_id());
        d.insert("taskforce_id", rf.taskforce_id().__str__());
        return Ok(d.finish());
    }
    let envelope = serialize_entry_arg(o);
    if !envelope.is_none() {
        return Ok(envelope);
    }
    if let Arg::Entry(e) = o {
        // Non-READY entries fall through to ``model_dump()`` -> the dict.
        return e.as_dict();
    }
    for m in ["model_dump", "as_dict", "identity"] {
        if let Ok(v) = o.__call_method__(m, Args::new()) {
            return Ok(v);
        }
    }
    Ok(Arg::Str(o.__str__().into()))
}

fn dumps_options() -> crate::_compat::pyjson::DumpsOptions {
    crate::_compat::pyjson::DumpsOptions {
        default: Some(Arc::new(|a: Args| -> Result<Arg> {
            let o = a.positional.first().cloned().unwrap_or(Arg::None);
            json_default(&o)
        })),
        ..Default::default()
    }
}

/// Serialize *obj* to a JSON string using `LailaJSONEncoder`.
pub fn encode(obj: &Arg) -> Result<String> {
    crate::_compat::pyjson::dumps(obj, &dumps_options())
}

/// Deserialize a JSON string.
pub fn decode(raw: &str) -> Result<Arg> {
    crate::_compat::pyjson::loads(raw)
}

/// Build a JSON-RPC 2.0 request dict (`request_id=None` -> a fresh uuid4).
pub fn make_request(method: &str, params: Arg, request_id: Option<&str>) -> Arg {
    let id: String = match request_id {
        Some(r) => String::from(r),
        None => crate::python::stdlib::uuid.uuid4().__str__(),
    };
    let mut d = PyDictBuilder::new();
    d.insert("jsonrpc", JSONRPC_VERSION);
    d.insert("method", method);
    d.insert("params", params);
    d.insert("id", id);
    d.finish()
}

/// Build a JSON-RPC 2.0 notification (a request without an `id`).
pub fn make_notification(method: &str, params: Arg) -> Arg {
    let mut d = PyDictBuilder::new();
    d.insert("jsonrpc", JSONRPC_VERSION);
    d.insert("method", method);
    d.insert("params", params);
    d.finish()
}

/// Build a JSON-RPC 2.0 success response.
pub fn make_result(request_id: Arg, result: Arg) -> Arg {
    let mut d = PyDictBuilder::new();
    d.insert("jsonrpc", JSONRPC_VERSION);
    d.insert("result", result);
    d.insert("id", request_id);
    d.finish()
}

/// Build a JSON-RPC 2.0 error response.
pub fn make_error(request_id: Arg, code: i64, message: &str, data: Arg) -> Arg {
    let mut error = PyDictBuilder::new();
    error.insert("code", code);
    error.insert("message", message);
    if !data.is_none() {
        error.insert("data", data);
    }
    let mut d = PyDictBuilder::new();
    d.insert("jsonrpc", JSONRPC_VERSION);
    d.insert("error", error.finish());
    d.insert("id", request_id);
    d.finish()
}

/// `True` if *msg* is a JSON-RPC request (carries a `method` field).
pub fn is_request(msg: &Arg) -> bool {
    msg.__contains__("method").unwrap_or(false)
}

/// `True` if *msg* is a JSON-RPC response (`result` or `error`).
pub fn is_response(msg: &Arg) -> bool {
    msg.__contains__("result").unwrap_or(false) || msg.__contains__("error").unwrap_or(false)
}

/// `msg.get(key)` on a message dict (`None` for anything else).
pub fn msg_get(msg: &Arg, key: &str) -> Arg {
    match msg {
        Arg::Dict(d) => d
            .read()
            .get(&Arg::Str(key.into()))
            .cloned()
            .unwrap_or(Arg::None),
        _ => Arg::None,
    }
}

/// `err.get("message", err)`: the text of an error envelope.
pub fn error_message(err: &Arg) -> String {
    match msg_get(err, "message") {
        Arg::None => err.__str__(),
        m => m.__str__(),
    }
}

// ---------------------------------------------------------------------------
// The module as a value

/// `laila.policy.central.communication.protocol` as a Python module value.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct protocol_mod;

/// `from laila.policy.central.communication import protocol`
#[allow(non_upper_case_globals)]
pub static protocol: protocol_mod = protocol_mod;

fn opt_str(a: &Arg) -> Option<String> {
    match a {
        Arg::None => None,
        other => Some(other.__str__()),
    }
}

impl protocol_mod {
    /// `protocol.make_request(method, params, request_id=None)`
    pub fn make_request(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        let method = a
            .positional
            .first()
            .cloned()
            .unwrap_or_else(|| a.get("method"));
        let params = a
            .positional
            .get(1)
            .cloned()
            .unwrap_or_else(|| a.get("params"));
        let rid = a
            .positional
            .get(2)
            .cloned()
            .unwrap_or_else(|| a.get("request_id"));
        let rid = opt_str(&rid);
        Ok(make_request(&method.__str__(), params, rid.as_deref()))
    }
    /// `protocol.make_notification(method, params)`
    pub fn make_notification(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        let method = a
            .positional
            .first()
            .cloned()
            .unwrap_or_else(|| a.get("method"));
        let params = a
            .positional
            .get(1)
            .cloned()
            .unwrap_or_else(|| a.get("params"));
        Ok(make_notification(&method.__str__(), params))
    }
    /// `protocol.make_result(request_id, result)`
    pub fn make_result(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        let rid = a
            .positional
            .first()
            .cloned()
            .unwrap_or_else(|| a.get("request_id"));
        let result = a
            .positional
            .get(1)
            .cloned()
            .unwrap_or_else(|| a.get("result"));
        Ok(make_result(rid, result))
    }
    /// `protocol.make_error(request_id, code, message, data=None)`
    pub fn make_error(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        let rid = a
            .positional
            .first()
            .cloned()
            .unwrap_or_else(|| a.get("request_id"));
        let code = a
            .positional
            .get(1)
            .cloned()
            .unwrap_or_else(|| a.get("code"));
        let message = a
            .positional
            .get(2)
            .cloned()
            .unwrap_or_else(|| a.get("message"));
        let data = a
            .positional
            .get(3)
            .cloned()
            .unwrap_or_else(|| a.get("data"));
        Ok(make_error(
            rid,
            code.as_i64().unwrap_or(ERR_EXECUTION),
            &message.__str__(),
            data,
        ))
    }
    /// `protocol.encode(obj)` -> str
    ///
    /// (Single-value parameter on purpose: `bytes.decode()` / `str.encode()`
    /// are zero-argument `Arg` methods and the macro must not see an
    /// `Args`-taking `encode`/`decode` anywhere in the library.)
    pub fn encode(&self, obj: impl IntoArg) -> Result<Arg> {
        let obj = obj.into_arg();
        Ok(Arg::Str(encode(&obj)?.into()))
    }
    /// `protocol.decode(raw)` -> Any
    pub fn decode(&self, raw: impl IntoArg) -> Result<Arg> {
        let raw = raw.into_arg();
        match &raw {
            Arg::Bytes(b) => crate::_compat::pyjson::loads_bytes(b),
            other => decode(&other.__str__()),
        }
    }
    /// `protocol.is_request(msg)`
    pub fn is_request(&self, args: impl Into<Args>) -> Result<bool> {
        let a: Args = args.into();
        Ok(is_request(
            &a.positional.first().cloned().unwrap_or(Arg::None),
        ))
    }
    /// `protocol.is_response(msg)`
    pub fn is_response(&self, args: impl Into<Args>) -> Result<bool> {
        let a: Args = args.into();
        Ok(is_response(
            &a.positional.first().cloned().unwrap_or(Arg::None),
        ))
    }
    /// `protocol.is_entry_envelope(value)`
    pub fn is_entry_envelope(&self, args: impl Into<Args>) -> Result<bool> {
        let a: Args = args.into();
        Ok(is_entry_envelope(
            &a.positional.first().cloned().unwrap_or(Arg::None),
        ))
    }
    /// `protocol.serialize_entry_arg(o)`
    pub fn serialize_entry_arg(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        Ok(serialize_entry_arg(
            &a.positional.first().cloned().unwrap_or(Arg::None),
        ))
    }
    /// `protocol.rehydrate_entry_args(args, kwargs)` -> `(list, dict)`
    pub fn rehydrate_entry_args(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        let x = a.positional.first().cloned().unwrap_or(Arg::None);
        let y = a.positional.get(1).cloned().unwrap_or(Arg::None);
        let (l, d) = rehydrate_entry_args(&x, &y)?;
        Ok(Arg::Tuple(alloc::vec![l, d].into()))
    }
    /// `getattr(protocol, name)`
    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        PyObject::__getattr__(self, name)
    }
}

impl PyObject for protocol_mod {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "module"
    }
    fn __repr__(&self) -> String {
        String::from("<module 'laila.policy.central.communication.protocol'>")
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other.as_any().downcast_ref::<protocol_mod>().is_some()
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(0x70_72_6f_74)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        Ok(match name {
            "JSONRPC_VERSION" => Arg::Str(JSONRPC_VERSION.into()),
            "ERR_INVALID_REQUEST" => Arg::Int(ERR_INVALID_REQUEST),
            "ERR_METHOD_NOT_FOUND" => Arg::Int(ERR_METHOD_NOT_FOUND),
            "ERR_AUTH_FAILED" => Arg::Int(ERR_AUTH_FAILED),
            "ERR_EXECUTION" => Arg::Int(ERR_EXECUTION),
            "ERR_BUSY" => Arg::Int(ERR_BUSY),
            "__name__" => Arg::Str("laila.policy.central.communication.protocol".into()),
            other => {
                return Err(LailaError::attribute_error(alloc::format!(
                "module 'laila.policy.central.communication.protocol' has no attribute '{other}'"
            )))
            }
        })
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match name {
            "make_request" => self.make_request(args),
            "make_notification" => self.make_notification(args),
            "make_result" => self.make_result(args),
            "make_error" => self.make_error(args),
            "encode" => self.encode(
                args.positional
                    .first()
                    .cloned()
                    .unwrap_or_else(|| args.get("obj")),
            ),
            "decode" => self.decode(
                args.positional
                    .first()
                    .cloned()
                    .unwrap_or_else(|| args.get("raw")),
            ),
            "is_request" => self.is_request(args).map(Arg::Bool),
            "is_response" => self.is_response(args).map(Arg::Bool),
            "is_entry_envelope" => self.is_entry_envelope(args).map(Arg::Bool),
            "serialize_entry_arg" => self.serialize_entry_arg(args),
            "rehydrate_entry_args" => self.rehydrate_entry_args(args),
            other => Err(LailaError::attribute_error(alloc::format!(
                "module 'laila.policy.central.communication.protocol' has no attribute '{other}'"
            ))),
        }
    }
}
impl IntoArg for protocol_mod {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for protocol_mod {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(*self))
    }
}
impl crate::python::builtins::ModuleValue for protocol_mod {
    fn module_value(self) -> Arg {
        self.into_arg()
    }
}

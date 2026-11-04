package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocol.py -- JSON-RPC 2.0 message helpers
// and LAILA-aware JSON encoding.
//
// All inter-policy communication uses the JSON-RPC 2.0 wire format. Two
// RPC methods are defined by laila on top of that:
//
//   - ``peer.connect`` -- the inbound side of an outbound peering
//     handshake. Carries the initiating policy's ``global_id`` and the
//     shared secret.
//   - ``peer.disconnect`` -- a *notification* (no ``id``, no reply) a
//     policy sends right before it drops a peer or shuts a transport down,
//     so the other side can unregister it at once instead of waiting for a
//     liveness timeout. Carries the sender's ``global_id`` as ``from_id``.
//   - ``rpc.call`` -- a remote attribute-chain invocation. Carries
//     ``path`` (list[str]), ``args`` (list), ``kwargs`` (dict).
//
// This file provides:
//
//   - Thin constructors for requests / success responses / error responses
//     (Make_request, Make_result, Make_error).
//   - A custom JSON encoder (LailaJSONEncoder / Laila_json_default) that
//     handles laila-specific types -- in particular, futures get marked
//     with ``__laila_future__`` so the receiving side can promote them back
//     into RemoteFuture proxies.
//   - Convenience predicates (Is_request, Is_response) for the inbound
//     dispatcher.
//
// The error-code constants follow JSON-RPC 2.0 conventions: ``-32600`` for
// "invalid request", ``-32601`` for "method not found", plus
// laila-specific ``-32001`` (auth failed) and ``-32002`` (execution error).

// JSONRPC_VERSION is the ``jsonrpc`` field value.
const JSONRPC_VERSION = "2.0"

const (
	ERR_INVALID_REQUEST = -32600
	ERR_METHOD_NOT_FOUND = -32601
	ERR_AUTH_FAILED      = -32001
	ERR_EXECUTION        = -32002
	// ERR_BUSY -- backpressure: the receiving policy's inbound RPC queue is
	// at capacity. Senders treat this as retryable (exponential backoff)
	// rather than a hard failure -- it is distinct from ERR_EXECUTION so
	// retries stay scoped to overload only.
	ERR_BUSY = -32003
)

// ENTRY_ENVELOPE_KEYS_ are the keys that identify the self-describing
// ``Entry.serialize(transformation_base64)`` envelope on the wire. Every
// entry (and manifest) serialized that way carries all four; ordinary RPC
// dicts never carry ``_uuid`` *and* ``constitution`` together.
var ENTRY_ENVELOPE_KEYS_ = []string{"_uuid", "_scopes", "payload", "constitution"}

// Serialize_entry_arg returns the wire envelope for a READY Entry, else nil.
//
// The envelope is the same ``Entry.serialize(transformation_base64)`` form
// future results travel in, so a peer rebuilds the real entry with
// Build_by_scope. Non-READY entries (nothing faithful to ship yet) and
// non-entries yield nil so the caller can fall through to the generic
// encoders.
func Serialize_entry_arg(o any) *py.Dict {
	e, ok := o.(*Entry)
	if !ok || !py.Isinstance(o, Entry_class) {
		return nil
	}
	env, err := e.Serialize(Transformation_base64)
	if err != nil {
		return nil
	}
	d, ok := env.(*py.Dict)
	if !ok {
		return nil
	}
	return d
}

// Is_entry_envelope is ``True`` when value is a dict carrying the
// serialized-entry signature.
func Is_entry_envelope(value any) bool {
	var d *py.Dict
	switch x := value.(type) {
	case *py.Dict:
		d = x
	case map[string]any:
		d = py.DictFromMap(x)
	case py.Kw:
		d = py.DictFromKw(x)
	default:
		return false
	}
	for _, k := range ENTRY_ENVELOPE_KEYS_ {
		if !d.Contains(k) {
			return false
		}
	}
	return true
}

// Rehydrate_entry_args rebuilds entries shipped as wire envelopes inside
// RPC ``args`` / ``kwargs``.
//
// Walks one level deep (top-level values and the items of top-level lists
// / tuples) -- the shapes laila's memory verbs accept. Dicts that carry the
// envelope signature become real Entry objects via Build_by_scope;
// everything else is returned untouched.
func Rehydrate_entry_args(args any, kwargs any) ([]any, py.Kw, error) {
	one := func(v any) (any, error) {
		if Is_entry_envelope(v) {
			return Build_by_scope(v, py.Kw{"asynchronous": false})
		}
		switch seq := v.(type) {
		case *py.List:
			items := seq.Items()
			if !anyEnvelope(items) {
				return v, nil
			}
			out := make([]any, 0, len(items))
			for _, x := range items {
				y, err := buildIfEnvelope(x)
				if err != nil {
					return nil, err
				}
				out = append(out, y)
			}
			return py.NewList(out...), nil
		case []any:
			if !anyEnvelope(seq) {
				return v, nil
			}
			out := make([]any, 0, len(seq))
			for _, x := range seq {
				y, err := buildIfEnvelope(x)
				if err != nil {
					return nil, err
				}
				out = append(out, y)
			}
			return out, nil
		case py.Tuple:
			if !anyEnvelope([]any(seq)) {
				return v, nil
			}
			out := make(py.Tuple, 0, len(seq))
			for _, x := range seq {
				y, err := buildIfEnvelope(x)
				if err != nil {
					return nil, err
				}
				out = append(out, y)
			}
			return out, nil
		}
		return v, nil
	}
	outArgs := []any{}
	for _, a := range py.Iter(args) {
		v, err := one(a)
		if err != nil {
			return nil, nil, err
		}
		outArgs = append(outArgs, v)
	}
	outKw := py.Kw{}
	if kwargs != nil {
		kd, err := As_dict_(kwargs)
		if err != nil {
			return nil, nil, err
		}
		for _, it := range kd.Items() {
			v, err := one(it.Value)
			if err != nil {
				return nil, nil, err
			}
			outKw[py.Str(it.Key)] = v
		}
	}
	return outArgs, outKw, nil
}

func anyEnvelope(items []any) bool {
	for _, x := range items {
		if Is_entry_envelope(x) {
			return true
		}
	}
	return false
}

func buildIfEnvelope(x any) (any, error) {
	if Is_entry_envelope(x) {
		return Build_by_scope(x, py.Kw{"asynchronous": false})
	}
	return x, nil
}

// Laila_json_default is ``LailaJSONEncoder.default``: encode o using the
// resolution order documented on LailaJSONEncoder.
//
// Resolution order for non-standard objects:
//
//  1. GroupFuture -- emit a future-shaped envelope with
//     ``__laila_future__=True`` and ``__is_group__=True``, plus the child
//     future ids needed to reconstruct the proxy.
//  2. _LAILA_IDENTIFIABLE_FUTURE -- emit a future-shaped envelope with
//     ``__is_group__=False``.
//  3. A READY Entry -- emit the canonical, self-describing
//     ``Entry.serialize(transformation_base64)`` form (the same envelope
//     future results travel in), which the receiving ``_execute_rpc``
//     rehydrates into a real entry. This is what lets
//     ``laila.memorize(entry)`` work in morph mode. Non-READY entries fall
//     through to ``model_dump()``.
//  4. Pydantic v2 models -- delegate to ``model_dump()``.
//  5. Anything providing ``as_dict`` -- delegate to that.
//  6. Anything providing ``identity`` -- delegate to that.
//  7. Final fallback: ``str(o)`` so RPC payloads never raise on encoding
//     even for opaque objects.
func Laila_json_default(o any) (any, error) {
	if g, ok := o.(*GroupFuture); ok {
		d := py.NewDict()
		d.Set("__laila_future__", true)
		d.Set("__is_group__", true)
		d.Set("global_id", g.Global_id())
		d.Set("policy_id", py.Str(g.Policy_id))
		d.Set("taskforce_id", py.Str(g.Taskforce_id))
		d.Set("future_ids", append([]string(nil), g.Future_ids...))
		return d, nil
	}

	if py.Isinstance(o, LAILA_IDENTIFIABLE_FUTURE_class) {
		gid, _ := py.Getattr(o, "global_id")
		pid, _ := py.Getattr(o, "policy_id")
		tid, _ := py.Getattr(o, "taskforce_id")
		d := py.NewDict()
		d.Set("__laila_future__", true)
		d.Set("__is_group__", false)
		d.Set("global_id", gid)
		d.Set("policy_id", py.Str(pid))
		d.Set("taskforce_id", py.Str(tid))
		return d, nil
	}

	if envelope := Serialize_entry_arg(o); envelope != nil {
		return envelope, nil
	}

	// ``hasattr(o, "model_dump")``: every pydantic model -- an explicit
	// Model_dump override or, for any laila object bound to a py.Class,
	// the generic field walk (``resource``, ``batch_accelerated``, ... for
	// a pool; private attrs and ClassVars excluded exactly like pydantic).
	if d, ok := o.(py.Dumper); ok {
		return d.Model_dump(), nil
	}
	if py.Classof(o) != nil {
		return py.Model_dump(o), nil
	}
	if a, ok := o.(interface{ As_dict() *py.Dict }); ok {
		return a.As_dict(), nil
	}
	if a, ok := o.(interface{ As_dict() (*py.Dict, error) }); ok {
		return a.As_dict()
	}
	if i, ok := o.(interface{ Identity() *py.Dict }); ok {
		return i.Identity(), nil
	}
	switch x := o.(type) {
	case []byte:
		return py.Str(x), nil
	case py.UUID:
		return x.String(), nil
	}
	return py.Str(o), nil
}

// LailaJSONEncoder is the json.dumps options object carrying
// Laila_json_default.
var LailaJSONEncoder = py.JsonOpts{Default: Laila_json_default}

// Encode serializes obj to a JSON string using LailaJSONEncoder.
func Encode(obj any) (string, error) {
	return py.Json_dumps_opts(obj, LailaJSONEncoder)
}

// Decode deserializes a JSON string (str or bytes).
func Decode(raw any) (any, error) {
	switch x := raw.(type) {
	case string:
		return py.Json_loads(x)
	case []byte:
		return py.Json_loads_bytes(x)
	}
	return nil, py.TypeError.Newf("the JSON object must be str, bytes or bytearray, not %s", py.Pytypename(raw))
}

// Decode_msg is Decode plus the ``dict`` assertion the dispatchers rely on.
func Decode_msg(raw any) (*py.Dict, error) {
	v, err := Decode(raw)
	if err != nil {
		return nil, err
	}
	d, ok := v.(*py.Dict)
	if !ok {
		return nil, py.AttributeError.Newf("'%s' object has no attribute 'get'", py.Pytypename(v))
	}
	return d, nil
}

// Make_request builds a JSON-RPC 2.0 request dict. request_id is
// auto-generated when "" is passed.
func Make_request(method string, params any, request_id string) *py.Dict {
	if request_id == "" {
		request_id = py.Uuid4().String()
	}
	d := py.NewDict()
	d.Set("jsonrpc", JSONRPC_VERSION)
	d.Set("method", method)
	d.Set("params", params)
	d.Set("id", request_id)
	return d
}

// Make_notification builds a JSON-RPC 2.0 notification (a request without
// an ``id``). Per the spec the receiver must not reply to a notification;
// laila uses it for ``peer.disconnect``.
func Make_notification(method string, params any) *py.Dict {
	d := py.NewDict()
	d.Set("jsonrpc", JSONRPC_VERSION)
	d.Set("method", method)
	d.Set("params", params)
	return d
}

// Make_result builds a JSON-RPC 2.0 success response.
func Make_result(request_id any, result any) *py.Dict {
	d := py.NewDict()
	d.Set("jsonrpc", JSONRPC_VERSION)
	d.Set("result", result)
	d.Set("id", request_id)
	return d
}

// Make_error builds a JSON-RPC 2.0 error response (request_id nil for
// parse errors; data nil omits the ``data`` member).
func Make_error(request_id any, code int, message string, data any) *py.Dict {
	e := py.NewDict()
	e.Set("code", code)
	e.Set("message", message)
	if data != nil {
		e.Set("data", data)
	}
	d := py.NewDict()
	d.Set("jsonrpc", JSONRPC_VERSION)
	d.Set("error", e)
	d.Set("id", request_id)
	return d
}

// Is_request returns ``True`` if msg is a JSON-RPC request: requests carry
// a ``method`` field, responses do not.
func Is_request(msg *py.Dict) bool { return msg != nil && msg.Contains("method") }

// Is_response returns ``True`` if msg is a JSON-RPC response: identified
// by the presence of either a ``result`` key (success) or an ``error`` key
// (failure).
func Is_response(msg *py.Dict) bool {
	return msg != nil && (msg.Contains("result") || msg.Contains("error"))
}

// Msg_get is ``msg.get(key)`` with Python's None default.
func Msg_get(msg *py.Dict, key string) any {
	if msg == nil {
		return nil
	}
	return msg.GetDefault(key, nil)
}

// Msg_str is ``msg.get(key)`` coerced to str ("" for None).
func Msg_str(msg *py.Dict, key string) string {
	v := Msg_get(msg, key)
	if v == nil {
		return ""
	}
	return py.Str(v)
}

// Msg_dict is ``msg.get(key, {})`` as a *py.Dict.
func Msg_dict(msg *py.Dict, key string) *py.Dict {
	v := Msg_get(msg, key)
	if v == nil {
		return py.NewDict()
	}
	d, err := As_dict_(v)
	if err != nil {
		return py.NewDict()
	}
	return d
}

// Error_message is ``err.get("message", err)`` for an error envelope.
func Error_message(err any) string {
	if d, ok := err.(*py.Dict); ok {
		if m, ok := d.Get("message"); ok {
			return py.Str(m)
		}
	}
	return py.Str(err)
}

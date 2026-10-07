/**
 * JSON-RPC 2.0 message helpers and LAILA-aware JSON encoding.
 *
 * All inter-policy communication uses the JSON-RPC 2.0 wire format. Two RPC
 * methods are defined by laila on top of that:
 *
 * - ``peer.connect`` -- the inbound side of an outbound peering handshake.
 *   Carries the initiating policy's ``global_id`` and the shared secret.
 * - ``peer.disconnect`` -- a *notification* (no ``id``, no reply) a policy
 *   sends right before it drops a peer or shuts a transport down, so the
 *   other side can unregister it at once instead of waiting for a liveness
 *   timeout. Carries the sender's ``global_id`` as ``from_id``.
 * - ``rpc.call`` -- a remote attribute-chain invocation. Carries ``path``
 *   (list[str]), ``args`` (list), ``kwargs`` (dict).
 *
 * This module provides:
 *
 * - Thin constructors for requests / success responses / error responses
 *   (``make_request``, ``make_result``, ``make_error``).
 * - A custom JSON encoder (``LailaJSONEncoder``) that handles laila-specific
 *   types -- in particular, futures get marked with ``__laila_future__`` so
 *   the receiving side can promote them back into ``RemoteFuture`` proxies.
 * - Convenience predicates (``is_request``, ``is_response``) for the inbound
 *   dispatcher.
 *
 * The error-code constants follow JSON-RPC 2.0 conventions: ``-32600`` for
 * "invalid request", ``-32601`` for "method not found", plus laila-specific
 * ``-32001`` (auth failed) and ``-32002`` (execution error).
 */
import { TypeError as PyTypeError } from "../../../_compat/errors.js";
import { lazy } from "../../../_compat/lazy.js";
import { dumps, loads } from "../../../_compat/pyjson.js";
import { str } from "../../../_compat/pytypes.js";
import { uuid4 } from "../../../_compat/uuid.js";

export const JSONRPC_VERSION = "2.0";

export const ERR_INVALID_REQUEST = -32600;
export const ERR_METHOD_NOT_FOUND = -32601;
export const ERR_AUTH_FAILED = -32001;
export const ERR_EXECUTION = -32002;
// Backpressure: the receiving policy's inbound RPC queue is at capacity.
// Senders treat this as retryable (exponential backoff) rather than a hard
// failure -- it is distinct from ERR_EXECUTION so retries stay scoped to
// overload only.
export const ERR_BUSY = -32003;

/**
 * JSON encoder that serialises LAILA objects via their existing hooks.
 *
 * Resolution order for non-standard objects:
 *
 * 1. ``GroupFuture`` -- emit a future-shaped envelope with
 *    ``__laila_future__=True`` and ``__is_group__=True``, plus the child
 *    future ids needed to reconstruct the proxy.
 * 2. ``_LAILA_IDENTIFIABLE_FUTURE`` -- emit a future-shaped envelope with
 *    ``__is_group__=False``.
 * 3. Pydantic v2 models -- delegate to ``model_dump()``.
 * 4. Anything providing ``as_dict`` -- delegate to that.
 * 5. Anything providing ``identity`` -- delegate to that.
 * 6. Final fallback: ``str(o)`` so RPC payloads never raise on encoding even
 *    for opaque objects.
 */
export class LailaJSONEncoder {
  /** Encode *o* using the resolution order documented on the class. */
  default(o) {
    const { _LAILA_IDENTIFIABLE_FUTURE } = lazy("laila.policy.central.command.schema.future.future.future_identity");
    const { GroupFuture } = lazy("laila.policy.central.command.schema.future.future.group_future");

    if (o instanceof GroupFuture) {
      return {
        __laila_future__: true,
        __is_group__: true,
        global_id: o.global_id,
        policy_id: str(o.policy_id),
        taskforce_id: str(o.taskforce_id),
        future_ids: o.future_ids,
      };
    }

    if (o instanceof _LAILA_IDENTIFIABLE_FUTURE) {
      return {
        __laila_future__: true,
        __is_group__: false,
        global_id: o.global_id,
        policy_id: str(o.policy_id),
        taskforce_id: str(o.taskforce_id),
      };
    }

    if (o !== null && o !== undefined) {
      if (typeof o.model_dump === "function") return o.model_dump();
      if (typeof o.as_dict === "function") return o.as_dict();
      if (typeof o.identity === "function") return o.identity();
      // ``bytes`` are not JSON serialisable in Python either -> ``str(o)``.
    }
    try {
      return _base_default(o);
    } catch (e) {
      if (e instanceof PyTypeError || e instanceof globalThis.TypeError) return str(o);
      throw e;
    }
  }

  /** ``json.JSONEncoder.encode`` */
  encode(obj) {
    return dumps(obj, { default: (o) => this.default(o) });
  }
}

function _base_default(o) {
  // ``json.JSONEncoder.default`` always raises TypeError.
  throw new PyTypeError(`Object of type ${o === null || o === undefined ? "NoneType" : (o.constructor && o.constructor.name) || typeof o} is not JSON serializable`);
}

const _ENCODER = new LailaJSONEncoder();

/**
 * Serialize *obj* to a JSON string using ``LailaJSONEncoder``.
 * @param {any} obj
 * @returns {string}
 */
export function encode(obj) {
  return _ENCODER.encode(obj);
}

/**
 * Deserialize a JSON string.
 * @param {string} raw
 * @returns {any}
 */
export function decode(raw) {
  return loads(raw);
}

/**
 * Build a JSON-RPC 2.0 request dict.
 * @param {string} method RPC method name (e.g. ``"peer.connect"``, ``"rpc.call"``).
 * @param {object} params Method parameters.
 * @param {string|null} [request_id] Correlation ID. Auto-generated when omitted.
 */
export function make_request(method, params, request_id = null) {
  if (request_id === null || request_id === undefined) request_id = String(uuid4());
  return {
    jsonrpc: JSONRPC_VERSION,
    method,
    params,
    id: request_id,
  };
}

/**
 * Build a JSON-RPC 2.0 notification (a request without an ``id``).
 *
 * Per the spec the receiver must not reply to a notification; laila uses it
 * for ``peer.disconnect``.
 */
export function make_notification(method, params) {
  return { jsonrpc: JSONRPC_VERSION, method, params };
}

/** Build a JSON-RPC 2.0 success response. */
export function make_result(request_id, result) {
  return {
    jsonrpc: JSONRPC_VERSION,
    result,
    id: request_id,
  };
}

/**
 * Build a JSON-RPC 2.0 error response.
 * @param {string|null} request_id ID from the originating request (``null`` for parse errors).
 * @param {number} code Numeric error code.
 * @param {string} message Short human-readable description.
 * @param {any} [data] Additional error context.
 */
export function make_error(request_id, code, message, data = null) {
  const error = { code, message };
  if (data !== null && data !== undefined) error.data = data;
  return {
    jsonrpc: JSONRPC_VERSION,
    error,
    id: request_id,
  };
}

/**
 * Return ``true`` if *msg* is a JSON-RPC request.
 *
 * Distinguishing requests from responses uses the JSON-RPC 2.0 convention:
 * requests carry a ``method`` field, responses do not.
 */
export function is_request(msg) {
  return _has(msg, "method");
}

/**
 * Return ``true`` if *msg* is a JSON-RPC response.
 *
 * Per JSON-RPC 2.0, responses are identified by the presence of either a
 * ``result`` key (success) or an ``error`` key (failure).
 */
export function is_response(msg) {
  return _has(msg, "result") || _has(msg, "error");
}

function _has(msg, key) {
  if (msg === null || msg === undefined || typeof msg !== "object") return false;
  if (msg instanceof Map) return msg.has(key);
  return Object.prototype.hasOwnProperty.call(msg, key);
}

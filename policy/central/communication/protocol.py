"""JSON-RPC 2.0 message helpers and LAILA-aware JSON encoding.

All inter-policy communication uses the JSON-RPC 2.0 wire format.
Two RPC methods are defined by laila on top of that:

- ``peer.connect`` -- the inbound side of an outbound peering
  handshake. Carries the initiating policy's ``global_id`` and the
  shared secret.
- ``peer.disconnect`` -- a *notification* (no ``id``, no reply) a
  policy sends right before it drops a peer or shuts a transport down,
  so the other side can unregister it at once instead of waiting for a
  liveness timeout. Carries the sender's ``global_id`` as ``from_id``.
- ``rpc.call`` -- a remote attribute-chain invocation. Carries
  ``path`` (list[str]), ``args`` (list), ``kwargs`` (dict).

This module provides:

- Thin constructors for requests / success responses / error responses
  (:func:`make_request`, :func:`make_result`, :func:`make_error`).
- A custom JSON encoder (:class:`LailaJSONEncoder`) that handles
  laila-specific types -- in particular, futures get marked with
  ``__laila_future__`` so the receiving side can promote them back
  into :class:`RemoteFuture` proxies.
- Convenience predicates (:func:`is_request`, :func:`is_response`)
  for the inbound dispatcher.

The error-code constants follow JSON-RPC 2.0 conventions:
``-32600`` for "invalid request", ``-32601`` for "method not found",
plus laila-specific ``-32001`` (auth failed) and ``-32002`` (execution
error).
"""

from __future__ import annotations

import json
import uuid as _uuid
from typing import Any

JSONRPC_VERSION = "2.0"

ERR_INVALID_REQUEST = -32600
ERR_METHOD_NOT_FOUND = -32601
ERR_AUTH_FAILED = -32001
ERR_EXECUTION = -32002
# Backpressure: the receiving policy's inbound RPC queue is at capacity.
# Senders treat this as retryable (exponential backoff) rather than a
# hard failure -- it is distinct from ERR_EXECUTION so retries stay
# scoped to overload only.
ERR_BUSY = -32003

# Keys that identify the self-describing ``Entry.serialize(
# transformation_base64)`` envelope on the wire. Every entry (and
# manifest) serialized that way carries all four; ordinary RPC dicts
# never carry ``_uuid`` *and* ``constitution`` together.
_ENTRY_ENVELOPE_KEYS = frozenset({"_uuid", "_scopes", "payload", "constitution"})


def serialize_entry_arg(o: Any) -> dict | None:
    """Return the wire envelope for a READY :class:`Entry`, else ``None``.

    The envelope is the same ``Entry.serialize(transformation_base64)``
    form future results travel in, so a peer rebuilds the real entry
    with :func:`build_by_scope`. Non-READY entries (nothing faithful to
    ship yet) and non-entries yield ``None`` so the caller can fall
    through to the generic encoders.
    """
    from ....entry.entry import Entry

    if not isinstance(o, Entry):
        return None
    try:
        from ....entry import transformation_base64

        return o.serialize(transformations=transformation_base64)
    except Exception:
        return None


def is_entry_envelope(value: Any) -> bool:
    """``True`` when *value* is a dict carrying the serialized-entry signature."""
    return isinstance(value, dict) and _ENTRY_ENVELOPE_KEYS <= value.keys()


def rehydrate_entry_args(args: Any, kwargs: Any) -> tuple[list, dict]:
    """Rebuild entries shipped as wire envelopes inside RPC ``args`` / ``kwargs``.

    Walks one level deep (top-level values and the items of top-level
    lists / tuples) -- the shapes laila's memory verbs accept. Dicts that
    carry the envelope signature become real :class:`Entry` objects via
    :func:`build_by_scope`; everything else is returned untouched.
    """
    from ....entry.constitution.build_maps import build_by_scope

    def one(v: Any) -> Any:
        if is_entry_envelope(v):
            return build_by_scope(v, asynchronous=False)
        if isinstance(v, (list, tuple)) and any(is_entry_envelope(x) for x in v):
            return type(v)(
                build_by_scope(x, asynchronous=False) if is_entry_envelope(x) else x for x in v
            )
        return v

    return [one(a) for a in (args or [])], {k: one(v) for k, v in (kwargs or {}).items()}


class LailaJSONEncoder(json.JSONEncoder):
    """JSON encoder that serialises LAILA objects via their existing hooks.

    Resolution order for non-standard objects:

    1. :class:`GroupFuture` -- emit a future-shaped envelope with
       ``__laila_future__=True`` and ``__is_group__=True``, plus the
       child future ids needed to reconstruct the proxy.
    2. :class:`_LAILA_IDENTIFIABLE_FUTURE` -- emit a future-shaped
       envelope with ``__is_group__=False``.
    3. A READY :class:`Entry` -- emit the canonical, self-describing
       ``Entry.serialize(transformation_base64)`` form (the same envelope
       future results travel in), which the receiving
       ``_execute_rpc`` rehydrates into a real entry. This is what lets
       ``laila.memorize(entry)`` work in morph mode. Non-READY entries
       fall through to ``model_dump()``.
    4. Pydantic v2 models -- delegate to ``model_dump()``.
    5. Anything providing ``as_dict`` -- delegate to that.
    6. Anything providing ``identity`` -- delegate to that.
    7. Final fallback: ``str(o)`` so RPC payloads never raise on
       encoding even for opaque objects.
    """

    def default(self, o: Any) -> Any:
        """Encode *o* using the resolution order documented on the class."""
        from ..command.schema.future.future.future_identity import (
            _LAILA_IDENTIFIABLE_FUTURE,
        )
        from ..command.schema.future.future.group_future import GroupFuture

        if isinstance(o, GroupFuture):
            return {
                "__laila_future__": True,
                "__is_group__": True,
                "global_id": o.global_id,
                "policy_id": str(o.policy_id),
                "taskforce_id": str(o.taskforce_id),
                "future_ids": o.future_ids,
            }

        if isinstance(o, _LAILA_IDENTIFIABLE_FUTURE):
            return {
                "__laila_future__": True,
                "__is_group__": False,
                "global_id": o.global_id,
                "policy_id": str(o.policy_id),
                "taskforce_id": str(o.taskforce_id),
            }

        envelope = serialize_entry_arg(o)
        if envelope is not None:
            return envelope

        if hasattr(o, "model_dump"):
            return o.model_dump()
        if hasattr(o, "as_dict"):
            return o.as_dict()
        if hasattr(o, "identity"):
            return o.identity()
        try:
            return super().default(o)
        except TypeError:
            return str(o)


def encode(obj: Any) -> str:
    """Serialize *obj* to a JSON string using :class:`LailaJSONEncoder`.

    Parameters
    ----------
    obj : Any
        Object to serialize.

    Returns
    -------
    str
        JSON string.
    """
    return json.dumps(obj, cls=LailaJSONEncoder)


def decode(raw: str) -> Any:
    """Deserialize a JSON string.

    Parameters
    ----------
    raw : str
        JSON text.

    Returns
    -------
    Any
        Parsed Python object.
    """
    return json.loads(raw)


def make_request(
    method: str, params: dict[str, Any], request_id: str | None = None
) -> dict[str, Any]:
    """Build a JSON-RPC 2.0 request dict.

    Parameters
    ----------
    method : str
        RPC method name (e.g. ``"peer.connect"``, ``"rpc.call"``).
    params : dict
        Method parameters.
    request_id : str, optional
        Correlation ID.  Auto-generated when omitted.

    Returns
    -------
    dict
        JSON-RPC request message.
    """
    if request_id is None:
        request_id = str(_uuid.uuid4())
    return {
        "jsonrpc": JSONRPC_VERSION,
        "method": method,
        "params": params,
        "id": request_id,
    }


def make_notification(method: str, params: dict[str, Any]) -> dict[str, Any]:
    """Build a JSON-RPC 2.0 notification (a request without an ``id``).

    Per the spec the receiver must not reply to a notification; laila
    uses it for ``peer.disconnect``.

    Parameters
    ----------
    method : str
        RPC method name.
    params : dict
        Method parameters.

    Returns
    -------
    dict
        JSON-RPC notification message.
    """
    return {"jsonrpc": JSONRPC_VERSION, "method": method, "params": params}


def make_result(request_id: str, result: Any) -> dict[str, Any]:
    """Build a JSON-RPC 2.0 success response.

    Parameters
    ----------
    request_id : str
        ID from the originating request.
    result : Any
        Return value of the method call.

    Returns
    -------
    dict
        JSON-RPC success response.
    """
    return {
        "jsonrpc": JSONRPC_VERSION,
        "result": result,
        "id": request_id,
    }


def make_error(request_id: str | None, code: int, message: str, data: Any = None) -> dict[str, Any]:
    """Build a JSON-RPC 2.0 error response.

    Parameters
    ----------
    request_id : str or None
        ID from the originating request (``None`` for parse errors).
    code : int
        Numeric error code.
    message : str
        Short human-readable description.
    data : Any, optional
        Additional error context.

    Returns
    -------
    dict
        JSON-RPC error response.
    """
    error: dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        error["data"] = data
    return {
        "jsonrpc": JSONRPC_VERSION,
        "error": error,
        "id": request_id,
    }


def is_request(msg: dict[str, Any]) -> bool:
    """Return ``True`` if *msg* is a JSON-RPC request.

    Distinguishing requests from responses uses the JSON-RPC 2.0
    convention: requests carry a ``method`` field, responses do not.
    """
    return "method" in msg


def is_response(msg: dict[str, Any]) -> bool:
    """Return ``True`` if *msg* is a JSON-RPC response.

    Per JSON-RPC 2.0, responses are identified by the presence of
    either a ``result`` key (success) or an ``error`` key (failure).
    """
    return "result" in msg or "error" in msg

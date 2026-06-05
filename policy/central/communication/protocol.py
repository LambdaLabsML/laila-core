"""JSON-RPC 2.0 message helpers and LAILA-aware JSON encoding.

All inter-policy communication uses the JSON-RPC 2.0 wire format.
Two RPC methods are defined by laila on top of that:

- ``peer.connect`` -- the inbound side of an outbound peering
  handshake. Carries the initiating policy's ``global_id`` and the
  shared secret.
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


class LailaJSONEncoder(json.JSONEncoder):
    """JSON encoder that serialises LAILA objects via their existing hooks.

    Resolution order for non-standard objects:

    1. :class:`GroupFuture` -- emit a future-shaped envelope with
       ``__laila_future__=True`` and ``__is_group__=True``, plus the
       child future ids needed to reconstruct the proxy.
    2. :class:`_LAILA_IDENTIFIABLE_FUTURE` -- emit a future-shaped
       envelope with ``__is_group__=False``.
    3. Pydantic v2 models -- delegate to ``model_dump()``.
    4. Anything providing ``as_dict`` -- delegate to that.
    5. Anything providing ``identity`` -- delegate to that.
    6. Final fallback: ``str(o)`` so RPC payloads never raise on
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

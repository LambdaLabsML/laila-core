"""JSON-RPC 2.0 message helpers and LAILA-aware JSON encoding.

All inter-policy communication uses the JSON-RPC 2.0 wire format.
This module provides thin builders for requests, results, and errors,
plus a custom JSON encoder that handles LAILA-specific types.
"""

from __future__ import annotations

import json
import uuid as _uuid
from typing import Any, Dict, List, Optional


JSONRPC_VERSION = "2.0"

ERR_INVALID_REQUEST = -32600
ERR_METHOD_NOT_FOUND = -32601
ERR_AUTH_FAILED = -32001
ERR_EXECUTION = -32002


class LailaJSONEncoder(json.JSONEncoder):
    """JSON encoder that serialises LAILA objects via their existing hooks.

    Futures and GroupFutures are serialized with a ``__laila_future__``
    marker so the receiving side can detect them and wrap in a
    ``RemoteFuture``.  Falls back to ``str(obj)`` for otherwise
    unserializable objects so that RPC payloads never raise on encoding.
    """

    def default(self, o: Any) -> Any:
        from ..command.schema.future.future.group_future import GroupFuture
        from ..command.schema.future.future.future_identity import (
            _LAILA_IDENTIFIABLE_FUTURE,
        )

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


def make_request(method: str, params: Dict[str, Any], request_id: Optional[str] = None) -> Dict[str, Any]:
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


def make_result(request_id: str, result: Any) -> Dict[str, Any]:
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


def make_error(request_id: Optional[str], code: int, message: str, data: Any = None) -> Dict[str, Any]:
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
    error: Dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        error["data"] = data
    return {
        "jsonrpc": JSONRPC_VERSION,
        "error": error,
        "id": request_id,
    }


def is_request(msg: Dict[str, Any]) -> bool:
    """Return ``True`` if *msg* is a JSON-RPC request (has ``method`` key)."""
    return "method" in msg


def is_response(msg: Dict[str, Any]) -> bool:
    """Return ``True`` if *msg* is a JSON-RPC response (has ``result`` or ``error``)."""
    return "result" in msg or "error" in msg

"""Runtime introspection for futures across the policy universe.

This module is global — it does not use ``active_policy``.  Instead it
derives the owning policy from the future itself (via ``policy_id``) and
looks it up in ``laila.universe``.
"""

from __future__ import annotations
from typing import Any, Optional


def _resolve_future(future_ref: Any) -> Any:
    """Look up the actual future object from a reference.

    Accepts a ``RemoteFuture``, ``Future``, ``GroupFuture``,
    ``_LAILA_IDENTIFIABLE_FUTURE``, or a ``global_id`` string.
    For strings, searches all local policy future banks.
    """
    from ..policy.central.command.schema.future.future.remote_future import RemoteFuture
    from ..policy.central.command.schema.future.future.future import Future
    from ..policy.central.command.schema.future.future.group_future import GroupFuture
    from ..policy.central.command.schema.future.future.future_identity import _LAILA_IDENTIFIABLE_FUTURE

    if isinstance(future_ref, RemoteFuture):
        return future_ref
    if isinstance(future_ref, Future):
        return future_ref
    if isinstance(future_ref, GroupFuture):
        return future_ref

    if isinstance(future_ref, str):
        from .. import _local_policies
        for policy in _local_policies.values():
            if future_ref in policy.future_bank:
                return policy.future_bank[future_ref]
        raise KeyError(f"Future {future_ref} not found in any local policy bank")

    if isinstance(future_ref, _LAILA_IDENTIFIABLE_FUTURE):
        from .. import _local_policies
        gid = future_ref.global_id
        for policy in _local_policies.values():
            if gid in policy.future_bank:
                return policy.future_bank[gid]
        raise KeyError(f"Future {gid} not found in any local policy bank")

    raise TypeError(f"Cannot resolve future for {type(future_ref)}")


def status(future_ref: Any) -> Any:
    """Return the status of a future.

    Parameters
    ----------
    future_ref : RemoteFuture | Future | GroupFuture | _LAILA_IDENTIFIABLE_FUTURE | str
        The future reference to query.
    """
    return _resolve_future(future_ref).status


def result(future_ref: Any) -> Any:
    """Return the result of a future, blocking if not yet finished.

    Parameters
    ----------
    future_ref : RemoteFuture | Future | GroupFuture | _LAILA_IDENTIFIABLE_FUTURE | str
        The future reference to query.
    """
    return _resolve_future(future_ref).result


def exception(future_ref: Any) -> Optional[Exception]:
    """Return the exception of a future, or ``None`` if it succeeded.

    Parameters
    ----------
    future_ref : RemoteFuture | Future | GroupFuture | _LAILA_IDENTIFIABLE_FUTURE | str
        The future reference to query.
    """
    return _resolve_future(future_ref).exception


def wait(future_ref: Any, timeout: Optional[float] = None) -> Any:
    """Block until the future completes and return its result.

    Parameters
    ----------
    future_ref : RemoteFuture | Future | GroupFuture | _LAILA_IDENTIFIABLE_FUTURE | str
        The future reference to wait on.
    timeout : float, optional
        Maximum seconds to wait.  ``None`` waits indefinitely.
    """
    return _resolve_future(future_ref).wait(timeout)

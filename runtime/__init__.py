"""Runtime introspection for futures across the policy universe.

This module is the *policy-agnostic* facade for asking questions
about futures: status, result, exception, or simply blocking until
completion. Unlike most of laila, it deliberately does *not* consult
:func:`laila.get_active_policy` -- callers may pass a future from any
local policy and the helpers here will route the request to the
right ``future_bank`` automatically.

Acceptable references
---------------------
Every public helper accepts any of:

- :class:`Future` / :class:`GroupFuture` / :class:`RemoteFuture` -- the
  concrete future object, used as-is.
- :class:`_LAILA_IDENTIFIABLE_FUTURE` -- a future *identity* whose
  ``global_id`` is resolved against the local policy banks.
- ``str`` -- a raw ``global_id``, also resolved against the local
  policy banks.

When resolution is needed, the helpers walk every local policy in
:data:`laila._local_policies` and return the first hit. Missing ids
raise :class:`KeyError`.
"""

from __future__ import annotations

from typing import Any, Optional


def _resolve_future(future_ref: Any) -> Any:
    """Resolve *future_ref* to a concrete future object.

    Accepts :class:`Future`, :class:`GroupFuture`, :class:`RemoteFuture`,
    :class:`_LAILA_IDENTIFIABLE_FUTURE`, or a raw ``global_id`` string.
    Concrete futures are returned unchanged; identities and strings
    are looked up in every local policy's ``future_bank`` (first hit
    wins).

    Raises
    ------
    KeyError
        If *future_ref* is an identity or string that does not match
        any future in any local policy bank.
    TypeError
        If *future_ref* is none of the supported reference types.
    """
    from ..policy.central.command.schema.future.future.future import Future
    from ..policy.central.command.schema.future.future.future_identity import (
        _LAILA_IDENTIFIABLE_FUTURE,
    )
    from ..policy.central.command.schema.future.future.group_future import GroupFuture
    from ..policy.central.command.schema.future.future.remote_future import RemoteFuture

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
    """Return the current status of *future_ref* without blocking.

    For :class:`Future` and :class:`RemoteFuture` this is a single
    :class:`FutureStatus` value; for :class:`GroupFuture` it is the
    aggregate status payload (counts, percentages, top-level
    progress) defined by :attr:`GroupFuture.status`.

    Parameters
    ----------
    future_ref : Future | GroupFuture | RemoteFuture | _LAILA_IDENTIFIABLE_FUTURE | str
        Any of the supported reference types (see module docstring).
    """
    return _resolve_future(future_ref).status


def result(future_ref: Any) -> Any:
    """Return the result of *future_ref*, blocking until it is available.

    The blocking semantics are inherited from the underlying future:
    a :class:`LoopBlockingWaitError` may be raised if called from the
    same thread that owns an async taskforce loop. Use :func:`wait`
    explicitly if you need to bound the wait.

    Parameters
    ----------
    future_ref : Future | GroupFuture | RemoteFuture | _LAILA_IDENTIFIABLE_FUTURE | str
        Any of the supported reference types (see module docstring).
    """
    return _resolve_future(future_ref).result


def exception(future_ref: Any) -> Exception | None:
    """Return the exception captured by *future_ref*, or ``None``.

    Returns ``None`` for futures that completed successfully or are
    still in flight; only futures whose terminal status is ``ERROR``
    or ``CANCELLED`` carry an exception value.

    Parameters
    ----------
    future_ref : Future | GroupFuture | RemoteFuture | _LAILA_IDENTIFIABLE_FUTURE | str
        Any of the supported reference types (see module docstring).
    """
    return _resolve_future(future_ref).exception


def wait(future_ref: Any, timeout: float | None = None) -> Any:
    """Block until *future_ref* terminates and return its result.

    The wait honours the same timeout semantics as
    :meth:`Future.wait`: ``timeout=None`` waits indefinitely; a
    positive *timeout* raises :class:`concurrent.futures.TimeoutError`
    on expiry. As with :func:`result`, calling from inside an async
    taskforce loop thread raises :class:`LoopBlockingWaitError`
    instead of dead-locking.

    Parameters
    ----------
    future_ref : Future | GroupFuture | RemoteFuture | _LAILA_IDENTIFIABLE_FUTURE | str
        Any of the supported reference types (see module docstring).
    timeout : float, optional
        Maximum seconds to wait. ``None`` (default) waits forever.
    """
    return _resolve_future(future_ref).wait(timeout)

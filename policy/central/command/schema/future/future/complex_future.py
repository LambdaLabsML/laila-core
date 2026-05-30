"""ComplexFuture -- sequential composition of futures (Mode A: declarative pipeline).

A :class:`ComplexFuture` represents a pipeline of stages, executed one at a
time in declared order. The number of stages and the function that produces
each stage's :class:`Future` (given the prior stage's result) is fixed at
construction time. The actual stage :class:`Future` instances are built
lazily -- stage ``k`` is constructed when stage ``k-1`` flips to
``FutureStatus.FINISHED``.

This is "Mode A" composition (immutable ``stage_fns``, lazy stage
construction). Dynamic-shape pipelines are expressed via composition:

- A ``stage_fn`` may return another :class:`ComplexFuture` or
  :class:`GroupFuture` -- pipelines nest to arbitrary depth.
- A ``stage_fn`` may branch -- pick one of several futures based on the
  prior stage's result.
- A ``stage_fn`` may return a :class:`GroupFuture` -- fan out parallel work
  whose aggregated result feeds the next stage.

Failure semantics
-----------------
- If any stage flips to ``ERROR`` or ``CANCELLED``, the parent
  ``ComplexFuture`` propagates that status (and exception) and no
  further stages are constructed.
- Empty ``stage_fns`` is rejected at construction with
  :class:`ValueError`.
- A ``stage_fn`` that returns a non-future or raises synchronously
  when invoked transitions the parent to ``ERROR``.

Implementation notes
--------------------
- Stage callbacks are wired through :meth:`Future.add_status_callback`
  for leaf futures. Group-future stages are polled on a daemon thread
  (see :meth:`_watch_group_future`) because :class:`GroupFuture` exposes
  its status as a percentage breakdown rather than a single
  :class:`FutureStatus` enum member.
- Re-entrant locking via ``_stage_lock`` keeps the stage state-machine
  consistent under concurrent callback delivery from worker threads.
- Pydantic v2's ``validate_python`` wipes any private-attribute writes
  done before ``super().__init__`` returns, so the raw ``stage_fns``
  argument is shuttled across the boundary via the thread-local
  :data:`_PARK` dict and re-attached in :meth:`model_post_init`.
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Callable
from concurrent.futures import TimeoutError as FutureTimeoutError
from typing import Any

from pydantic import ConfigDict, Field, PrivateAttr

from .future import Future
from .future_identity import _LAILA_IDENTIFIABLE_FUTURE
from .future_status import FutureStatus
from .group_future import GroupFuture

StageFn = Callable[[Any], Future | GroupFuture | _LAILA_IDENTIFIABLE_FUTURE]


_PARK: dict[int, list] = {}
"""Thread-local hand-off used by :class:`ComplexFuture` to ferry the
``stage_fns`` argument from ``__init__`` to ``model_post_init`` across
Pydantic v2's :meth:`validate_python` boundary, which wipes any private
attribute writes made before it returns. Keyed by ``threading.get_ident()``;
populated and cleared inside :meth:`ComplexFuture.__init__`.
"""


class ComplexFuture(Future):
    """Sequential composition of Future stages with declarative shape.

    Parameters
    ----------
    stage_fns
        Ordered list of callables. Stage 0 is invoked with ``None``; every
        subsequent stage receives the prior stage's resolved result. Each
        callable must return a :class:`Future`, :class:`GroupFuture`, or
        :class:`_LAILA_IDENTIFIABLE_FUTURE` (a future identity, which is
        resolved against the active local policy's future bank).

    Raises
    ------
    ValueError
        If ``stage_fns`` is empty at construction.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    stage_future_ids: list[str] = Field(default_factory=list)

    _stage_fns: list[StageFn] = PrivateAttr(default_factory=list)
    _stage_lock: threading.RLock = PrivateAttr(default_factory=threading.RLock)
    _current_idx: int = PrivateAttr(default=-1)
    _terminated: bool = PrivateAttr(default=False)

    def __init__(self, **data: Any) -> None:
        stage_fns = data.pop("stage_fns", None) or []
        if not isinstance(stage_fns, (list, tuple)):
            raise TypeError("stage_fns must be a list or tuple of callables")
        if len(stage_fns) == 0:
            raise ValueError("ComplexFuture requires at least one stage")
        for idx, fn in enumerate(stage_fns):
            if not callable(fn):
                raise TypeError(f"stage_fns[{idx}] must be callable, got {type(fn).__name__}")
        # Pydantic v2 wipes private attributes during validate_python (called
        # inside super().__init__), so we cannot stash stage_fns on `self`
        # before the super call. Instead, hand them off via a thread-local
        # park so model_post_init can pick them up after validation.
        _PARK[threading.get_ident()] = list(stage_fns)
        try:
            super().__init__(**data)
        finally:
            _PARK.pop(threading.get_ident(), None)

    def model_post_init(self, __context: Any) -> None:
        """Register with the active policy's future bank, then kick off stage 0.

        Picks up the parked ``stage_fns`` list from :data:`_PARK` (set
        by :meth:`__init__`), reattaches it as a private attribute,
        then synchronously triggers construction of the first stage
        with ``prior_result=None``. Subsequent stages are constructed
        lazily inside the per-stage completion callbacks.
        """
        super().model_post_init(__context)
        parked = _PARK.get(threading.get_ident())
        if parked is not None:
            self._stage_fns = list(parked)
        self._kick_off_stage(0, prior_result=None)

    @property
    def current_stage_index(self) -> int:
        """0-based index of the most recently constructed stage.

        Returns ``-1`` before the first stage has been kicked off.
        """
        return self._current_idx

    @property
    def num_stages(self) -> int:
        """Total number of stages declared at construction."""
        return len(self._stage_fns)

    def stage_futures(self) -> list[Future]:
        """Resolve and return the list of stage futures constructed so far.

        Lookups go through the active policy's future bank.
        """
        from ....... import _local_policies

        out: list[Future] = []
        for fid in self.stage_future_ids:
            for policy in _local_policies.values():
                if fid in policy.future_bank:
                    out.append(policy.future_bank[fid])
                    break
        return out

    def _resolve_future(self, value: Any) -> Any:
        """Return the underlying Future or GroupFuture for *value*.

        Accepts a :class:`Future`, :class:`GroupFuture`, or a future
        identity (which is resolved against the active local policy's
        future bank).
        """
        if isinstance(value, Future):
            return value
        if isinstance(value, GroupFuture):
            return value
        if isinstance(value, _LAILA_IDENTIFIABLE_FUTURE):
            from ....... import _local_policies

            gid = value.global_id
            for policy in _local_policies.values():
                if gid in policy.future_bank:
                    return policy.future_bank[gid]
            raise KeyError(f"Future {gid} not found in any local policy bank")
        raise TypeError(
            f"stage_fn must return a Future / GroupFuture / future_identity, "
            f"got {type(value).__name__}"
        )

    def _kick_off_stage(self, idx: int, *, prior_result: Any) -> None:
        """Construct stage *idx* and register chaining callbacks on it."""
        with self._stage_lock:
            if self._terminated:
                return
            try:
                fn = self._stage_fns[idx]
                produced = fn(prior_result)
                stage_fut = self._resolve_future(produced)
            except Exception as exc:
                self._fail(exc, FutureStatus.ERROR)
                return

            self.stage_future_ids.append(stage_fut.global_id)
            self._current_idx = idx

            if isinstance(stage_fut, GroupFuture):
                self._watch_group_future(idx, stage_fut)
            else:
                stage_fut.add_status_callback(
                    FutureStatus.FINISHED,
                    lambda f, i=idx: self._on_stage_done(i, f),
                )
                stage_fut.add_status_callback(
                    FutureStatus.ERROR,
                    lambda f, i=idx: self._on_stage_failed(i, f),
                )
                stage_fut.add_status_callback(
                    FutureStatus.CANCELLED,
                    lambda f, i=idx: self._on_stage_failed(i, f),
                )

            if self._status == FutureStatus.NOT_STARTED:
                self.status = FutureStatus.RUNNING

    def _watch_group_future(self, idx: int, gf: GroupFuture) -> None:
        """Drive a :class:`GroupFuture` stage via a daemon-thread poll loop.

        ``GroupFuture`` exposes its status as a percentage breakdown rather
        than a single :class:`FutureStatus`, so it does not support the
        callback hook used for :class:`Future` chaining. We poll until all
        children are terminal, then synthesize the aggregated outcome and
        forward to :meth:`_on_stage_done` / :meth:`_on_stage_failed` exactly
        as we would for a leaf future.
        """

        def _poll():
            poll_interval_s = 0.01
            while True:
                pct = gf.status["percentages"]
                terminal_pct = pct["finished"] + pct["error"] + pct["cancelled"]
                if terminal_pct >= 100.0:
                    break
                time.sleep(poll_interval_s)
            try:
                results = gf.result
            except Exception as exc:

                class _Holder:
                    def __init__(self, e):
                        self.exception = e
                        self.status = FutureStatus.ERROR

                self._on_stage_failed(idx, _Holder(exc))
                return

            class _Holder:
                def __init__(self, r):
                    self.result = r

            self._on_stage_done(idx, _Holder(results))

        threading.Thread(target=_poll, name=f"ComplexFuture-GF-watch-{idx}", daemon=True).start()

    def _on_stage_done(self, idx: int, fut: Future) -> None:
        """Handle a stage finishing successfully — kick off the next one or finalize."""
        with self._stage_lock:
            if self._terminated:
                return
            if idx != self._current_idx:
                return
            try:
                value = fut.result
            except Exception as exc:
                self._fail(exc, FutureStatus.ERROR)
                return

            nxt = idx + 1
            if nxt >= len(self._stage_fns):
                self._terminated = True
                self.exception = None
                self.result = value
                self.status = FutureStatus.FINISHED
                return

        self._kick_off_stage(nxt, prior_result=value)

    def _on_stage_failed(self, idx: int, fut: Future) -> None:
        """Propagate a stage's ERROR/CANCELLED to the parent."""
        with self._stage_lock:
            if self._terminated:
                return
            if idx != self._current_idx:
                return
            self._fail(fut.exception, fut.status)

    def _fail(
        self,
        exc: BaseException | None,
        status: FutureStatus = FutureStatus.ERROR,
    ) -> None:
        """Mark the parent as failed/cancelled and seal further progress."""
        self._terminated = True
        self.exception = exc
        self.result = None
        self.status = status

    def wait(self, timeout: float | None = None) -> Any:
        """Block until the pipeline terminates.

        Polls the parent status (which is driven by stage callbacks). When
        the parent has reached a terminal status, returns the result or
        raises the captured exception.

        Raises
        ------
        LoopBlockingWaitError
            If called from a thread that owns an async event loop.
        """
        from ...exceptions import _check_not_loop_thread

        _check_not_loop_thread()

        deadline = None if timeout is None else time.monotonic() + timeout
        poll_interval_s = 0.01
        while True:
            with self.atomic():
                status = self._status
                exc = self._exception
                value = self._return_value

            if status == FutureStatus.FINISHED:
                return value
            if status in (FutureStatus.ERROR, FutureStatus.CANCELLED):
                if exc is not None:
                    raise exc
                raise RuntimeError(f"ComplexFuture ended with status={status} and no exception.")

            if deadline is not None and time.monotonic() >= deadline:
                self._default_callbacks[FutureStatus.POLL_TIMEOUT](self)
                raise FutureTimeoutError()

            time.sleep(poll_interval_s)

    def __await__(self):
        """Await the pipeline's terminal status by yielding to the event loop."""

        async def _await_terminal():
            poll_interval_s = 0.01
            while True:
                with self.atomic():
                    status = self._status
                    exc = self._exception
                    value = self._return_value

                if status == FutureStatus.FINISHED:
                    return value
                if status in (FutureStatus.ERROR, FutureStatus.CANCELLED):
                    if exc is not None:
                        raise exc
                    raise RuntimeError(
                        f"ComplexFuture ended with status={status} and no exception."
                    )
                await asyncio.sleep(poll_interval_s)

        return _await_terminal().__await__()

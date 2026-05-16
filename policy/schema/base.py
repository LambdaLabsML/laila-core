"""Base schema for a Laila policy and its central sub-components.

A *policy* is the unit of ownership for everything stateful in laila:
storage pools, task-forces, peer connections, futures, and the
manifests/hints that drive memory routing. The base class defined here
is the bridge between the user-facing top-level API (``laila.memory``,
``laila.command``, ...) and the four central sub-systems that actually
do the work.

Policies are themselves identifiable. Their ``global_id`` is what peers
use to address them across the network (see :class:`RemotePolicyProxy`),
and what :func:`laila.activate_policy` records as the active gid so
the top-level shortcuts know which subsystem instance to resolve
against.
"""

from __future__ import annotations
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field, ConfigDict, PrivateAttr
from ...basics.definitions.cli_capable import CLIExempt, _LAILA_CLI_CAPABLE_CLASS

from ...pool.schema.base import _LAILA_IDENTIFIABLE_POOL
from ...entry import Entry
from ..central.command.schema.base import _LAILA_IDENTIFIABLE_CENTRAL_COMMAND
from ...basics.definitions.identifiable_object import _LAILA_IDENTIFIABLE_OBJECT
from ..central.memory.schema.base import _LAILA_IDENTIFIABLE_CENTRAL_MEMORY
from ...macros.strings import _POLICY_SCOPE

class _LAILA_IDENTIFIABLE_POLICY(_LAILA_CLI_CAPABLE_CLASS, _LAILA_IDENTIFIABLE_OBJECT):
    """Top-level policy object owning central command, memory, communication, and logic.

    A policy bundles three runtime concerns into one identifiable
    object:

    1. **Memory** -- where entries live and how they're routed.
    2. **Command** -- how work runs and how its lifecycle is tracked.
    3. **Communication** -- how this policy talks to peers.

    Plus a ``future_bank`` keyed by future ``global_id`` that every
    :class:`Future` self-registers into on construction. The bank is the
    single source of truth that lets remote-policy RPCs and
    ``laila.runtime.wait`` look futures up by id.

    Lazy wiring
    -----------
    Every central sub-system has a default implementation
    (``DefaultCentralCommand`` / ``DefaultCentralMemory`` /
    ``DefaultCentralCommunication``) that :meth:`model_post_init`
    instantiates if the user did not pass one explicitly. This means
    ``policy = DefaultPolicy()`` returns a fully-functional policy with
    no further setup -- pools, taskforces, and a stopped communication
    instance ready to start on first ``add_peer``.
    """

    _scopes: list[str] = PrivateAttr(default_factory=lambda: list([_POLICY_SCOPE]))
    class Central(BaseModel):
        """Container struct for the four central sub-systems of a policy.

        Holds optional references to ``logic``, ``command``, ``memory``,
        and ``communication``. All four are :class:`CLIExempt` so they
        do not appear in ``laila.args`` resolution paths -- they are
        wired directly by :meth:`_LAILA_IDENTIFIABLE_POLICY.model_post_init`,
        not from CLI args.
        """

        logic: Optional[Any] = CLIExempt(default=None)
        command: Optional[_LAILA_IDENTIFIABLE_CENTRAL_COMMAND] = CLIExempt(default=None)
        communication: Optional[Any] = CLIExempt(default=None)
        memory: Optional[Any] = CLIExempt(default=None)

        model_config = ConfigDict(arbitrary_types_allowed=True)

    model_config = ConfigDict(arbitrary_types_allowed=True)

    # Core components
    central: Central = CLIExempt(default_factory=Central)
    future_bank: Dict[str, Any] = CLIExempt(default_factory=dict)


    def model_post_init(self, __context: Any) -> None:
        """Lazily wire the default central sub-systems if the user didn't supply them.

        For each of ``memory`` / ``command`` / ``communication``, if
        the slot is ``None`` we instantiate the corresponding
        ``Default<Subsystem>`` from :mod:`laila.macros.defaults`.

        After wiring, ``communication._local_policy`` is back-reffed to
        ``self`` so inbound RPC dispatch can find the local subsystems
        by walking attribute paths from the policy.

        ``logic`` is intentionally left ``None`` -- it is reserved for
        future higher-level orchestration.
        """
        from ...macros.defaults import (
            DefaultCentralCommand,
            DefaultCentralMemory,
            DefaultCentralCommunication,
        )

        if self.central.memory is None:
            self.central.memory = DefaultCentralMemory()

        if self.central.command is None:
            self.central.command = DefaultCentralCommand(policy_id=self.global_id)

        if self.central.communication is None:
            self.central.communication = DefaultCentralCommunication(
                policy_id=self.global_id,
            )

        self.central.communication._local_policy = self


    def extend(self, new_pool: _LAILA_IDENTIFIABLE_POOL) -> None:
        """Register a new pool with this policy's central memory.

        Equivalent to ``self.central.memory.extend(new_pool, ...)``,
        which delegates to the underlying :class:`PoolRouter`. The
        pool's ``global_id`` becomes its routing key; once registered,
        subsequent :func:`memorize` / :func:`remember` calls can target
        it with ``pool_id=`` (or with ``pool_nickname=`` if a nickname
        was supplied to :meth:`PoolRouter.extend`).

        Parameters
        ----------
        new_pool : _LAILA_IDENTIFIABLE_POOL
            The pool to register.
        """
        self.central.memory[new_pool.pool_id] = new_pool


    def remember(
        self,
        global_id: str,
        *,
        global_fetch: bool = False,
        pool_subset: Optional[Dict[str, _LAILA_IDENTIFIABLE_POOL]] = None,
        hint: Optional[str] = None,
        _remote_called: bool = False,
    ) -> Optional[Entry]:
        """Fetch a single entry from central memory by its ``global_id``.

        This is a *low-level*, blocking convenience used internally and
        from tests; most callers should use the top-level
        :func:`laila.remember` (which returns a future and supports
        cache-back into the alpha pool).

        Parameters
        ----------
        global_id : str
            The unique identifier of the entry to recall.
        global_fetch : bool, optional
            If ``True``, search across all known policies (multi-policy
            federation). Currently raises :exc:`NotImplementedError`.
        pool_subset : dict, optional
            Restrict the search to a subset of pools.
        hint : str, optional
            Routing hint forwarded to central memory's pool resolver.
        _remote_called : bool, optional
            Internal flag set when this method is invoked through an
            inbound RPC; reserved for future per-source bookkeeping.

        Returns
        -------
        Entry or None
            The recovered entry, or ``None`` if not found in any
            inspected pool.

        Raises
        ------
        NotImplementedError
            When ``global_fetch=True``.
        """
        if global_fetch:
            raise NotImplementedError
        
        entry = self.central.memory.fetch(
            key = global_id,
            pool_subset = pool_subset,
            hint = hint
        )

        return entry


    
    def memorize(
        self,
        entries: Any,
        *,
        require_local_update = False, #update only affects the main pool at first.
        require_global_update = False
    ) -> None:
        """Persist *entries* into central memory.

        Thin wrapper around ``self.central.memory.record(entries)``.
        The propagation kwargs (``require_local_update`` /
        ``require_global_update``) are placeholders for future
        replication semantics and are currently ignored -- writes
        affect only the routed pool.
        """
        return self.central.memory.record(entries)

    # ------------------------------------------------------------------
    # RPC helpers for remote future introspection
    # ------------------------------------------------------------------

    def _get_future_status(self, future_id: str) -> Any:
        """RPC: return the status of a local future.

        Invoked by :class:`RemoteFuture` (running on a peer process) to
        poll the *real* future that lives in this policy's
        :attr:`future_bank`. The returned value is unwrapped from any
        :class:`Enum` so it survives JSON serialization on the wire.

        Raises
        ------
        KeyError
            If *future_id* is not in this policy's bank (either it was
            never created here or it has been garbage-collected).
        """
        future = self.future_bank.get(future_id)
        if future is None:
            raise KeyError(f"Future {future_id} not in bank")
        status = future.status
        if hasattr(status, "value"):
            return status.value
        return status

    def _get_future_exception(self, future_id: str) -> Optional[Dict]:
        """RPC: return a JSON-serializable view of a local future's exception.

        Returns the empty payload ``None`` when the future succeeded.
        Otherwise returns ``{"type": <ExcClass>, "message": <str>}`` --
        the exception's full type isn't reconstructed on the remote
        side, but its name and message are preserved for diagnostics.
        """
        future = self.future_bank.get(future_id)
        if future is None:
            raise KeyError(f"Future {future_id} not in bank")
        exc = future.exception
        if exc is None:
            return None
        return {"type": type(exc).__name__, "message": str(exc)}

    def _get_future_result_id(self, future_id: str) -> Any:
        """RPC: return the result entry's ``global_id`` for a local future.

        Used by :class:`RemoteFuture` to discover *which entry* a peer's
        future produced, so the peer can then ``laila.remember`` it
        from the appropriate pool.

        For :class:`GroupFuture`, returns a list of child result ids
        (one per member future) -- the caller is responsible for
        zipping it with ``future.future_ids``.

        Returns ``None`` if the future has no recorded result id (e.g.
        the future is still running, or it produced a non-entry
        result).
        """
        future = self.future_bank.get(future_id)
        if future is None:
            raise KeyError(f"Future {future_id} not in bank")
        if hasattr(future, "_result_global_id"):
            return future._result_global_id
        if hasattr(future, "future_ids"):
            ids = []
            for fid in future.future_ids:
                child = self.future_bank.get(fid)
                if child and hasattr(child, "_result_global_id"):
                    ids.append(child._result_global_id)
                else:
                    ids.append(None)
            return ids
        return None

    def _wait_future(self, future_id: str, timeout: float = None) -> Any:
        """RPC: block on a local future and return its result-entry gid.

        Companion to :meth:`_get_future_result_id` used by
        :class:`RemoteFuture` to wait synchronously for completion. The
        wait happens on this policy's process; the peer's call is
        already inside its own thread of execution and is fine to block.
        """
        future = self.future_bank.get(future_id)
        if future is None:
            raise KeyError(f"Future {future_id} not in bank")
        future.wait(timeout)
        if hasattr(future, "_result_global_id"):
            return future._result_global_id
        return None


"""Laila top-level package.

Provides the public API for policy management, subsystem access
(``laila.memory``, ``laila.command``, ``laila.communication``),
future lifecycle helpers, and argument loading.
"""

import sys
import types
import uuid
import os
from typing import Optional
from dotmap import DotMap

from .entry import Entry
from . import entry
from . import policy
from .policy.central.memory.schema.manifest import Manifest
manifest = Manifest
from .macros.aliases import *
from .policy.schema.base import _LAILA_IDENTIFIABLE_POLICY
from .policy.central.command import taskforce as TaskForce

from .macros.defaults import *
from .macros.strings import _ENTRY_SCOPE

from .utils.args import ArgReader
from .utils import guarantee, guarantee_async
from .logger import (
    Logger,
    get_logger,
    enable_logging,
    disable_logging,
    set_log_level,
    _install_null_handler as _install_logger_null_handler,
)

_install_logger_null_handler()


def _is_env_load_trigger(value: "object") -> bool:
    """Return True if assigning *value* to ``laila.args.environment`` should reload.

    Two distinct cases:

    1. A *plain* ``dict`` with at least one key is interpreted as an
       explicit user-assigned environment payload and always triggers a
       load. ``_load_environment`` is then responsible for validating
       its shape (raising ``ValueError`` for malformed envs).
    2. A ``DotMap`` is treated as a load trigger only when it has a
       non-empty ``policies`` mapping or an ``active_gid`` set. This
       ensures the internal scaffolding path
       (``_refresh_args_environment`` writes an empty ``DotMap`` to
       seed ``args.environment``) does not re-enter the loader.

    Anything else (non-mapping, ``None``, empty ``dict``) is inert.
    """
    if isinstance(value, DotMap):
        try:
            policies = value.get("policies")
            active = value.get("active_gid")
        except Exception:
            return False
        if active is not None:
            return True
        if policies is None:
            return False
        try:
            return len(policies) > 0
        except Exception:
            return False
    if isinstance(value, dict):
        return len(value) > 0
    return False


class _LailaArgs(DotMap):
    """``DotMap`` subclass that watches for assignments to ``environment``.

    Setting ``laila.args.environment = my_env`` (or
    ``laila.args["environment"] = my_env``) where ``my_env.policies`` is
    a non-empty mapping triggers ``_load_environment`` -- which calls
    ``laila.terminate(...)`` and rebuilds every policy listed in
    ``my_env.policies`` with the right pool / taskforce / protocol
    subclasses.

    The mirror-update path (``_refresh_args_environment`` writing to
    ``args.environment.policies[<gid>]``) goes through DotMap's normal
    ``__setitem__`` on the inner ``policies`` DotMap and never re-enters
    this class's hooks.
    """

    def __setattr__(self, key, value):
        if key == "environment" and _is_env_load_trigger(value):
            from .basics.definitions.cli_capable import _load_environment
            _load_environment(value)
            return
        if isinstance(value, dict) and not isinstance(value, DotMap):
            value = DotMap(value)
        super().__setattr__(key, value)

    def __setitem__(self, key, value):
        if key == "environment" and _is_env_load_trigger(value):
            from .basics.definitions.cli_capable import _load_environment
            _load_environment(value)
            return
        if isinstance(value, dict) and not isinstance(value, DotMap):
            value = DotMap(value)
        super().__setitem__(key, value)


args = _LailaArgs()

arg_reader = ArgReader(target=args)

_local_policies = {}
_remote_policies = {}


def terminate(*, wait: bool = True, cancel_pending: bool = False) -> list:
    """Gracefully tear down everything ``laila`` has spawned in this process.

    Walks every locally-registered policy and, in this order:

    1. ``policy.central.communication.stop()`` -- closes WebSocket
       servers, joins event-loop threads, drops peer registrations.
    2. ``policy.central.command.shutdown(wait=..., cancel_pending=...)``
       -- joins / cancels every taskforce.
    3. ``pool.close()`` for each pool registered in
       ``policy.central.memory.pool_router.pools`` -- terminates managed
       subprocesses (Redis / Postgres / Mongo), closes file handles
       (HDF5 / SQLite / DuckDB), and disconnects cloud clients.

    Then clears ``_local_policies``, ``_remote_policies``,
    ``_active_policy_gid``, and the ``laila.args.environment.policies``
    mirror.

    Parameters
    ----------
    wait : bool, default True
        Forwarded to ``command.shutdown`` -- block until taskforce
        workers exit.
    cancel_pending : bool, default False
        Forwarded to ``command.shutdown`` -- cancel queued, un-started
        futures.

    Returns
    -------
    list[str]
        One error string per failed step. An empty list means clean
        shutdown. The function never raises -- failures are recorded
        and the next step still runs (best-effort, idempotent).
    """
    global _active_policy_gid
    errors: list = []

    for gid, policy in list(_local_policies.items()):
        try:
            comm = getattr(getattr(policy, "central", None), "communication", None)
            if comm is not None:
                comm.stop()
        except Exception as e:
            errors.append(f"communication.stop[{gid}]: {e!r}")

        try:
            cmd = getattr(getattr(policy, "central", None), "command", None)
            if cmd is not None:
                cmd.shutdown(wait=wait, cancel_pending=cancel_pending)
        except Exception as e:
            errors.append(f"command.shutdown[{gid}]: {e!r}")

        try:
            mem = getattr(getattr(policy, "central", None), "memory", None)
            router = getattr(mem, "pool_router", None) if mem is not None else None
            if router is not None:
                for pool_id, pool in list(getattr(router, "pools", {}).items()):
                    try:
                        close = getattr(pool, "close", None)
                        if callable(close):
                            close()
                    except Exception as e:
                        errors.append(f"pool.close[{pool_id}]: {e!r}")
        except Exception as e:
            errors.append(f"memory.pools[{gid}]: {e!r}")

    _local_policies.clear()
    _remote_policies.clear()
    _active_policy_gid = None

    try:
        from .policy.central.command.taskforce.base import (
            _live_taskforces_snapshot,
        )
        for tf in _live_taskforces_snapshot():
            try:
                tf.shutdown(wait=wait, cancel_pending=cancel_pending)
            except Exception as e:
                errors.append(f"orphan_taskforce.shutdown[{getattr(tf, 'global_id', '?')}]: {e!r}")
    except Exception as e:
        errors.append(f"orphan_taskforce sweep: {e!r}")

    try:
        Logger.reset_singleton()
    except Exception as e:
        errors.append(f"logger.reset_singleton: {e!r}")

    try:
        env = args.get("environment") if hasattr(args, "get") else None
        if env is not None and hasattr(env, "get"):
            policies = env.get("policies")
            if policies is not None and hasattr(policies, "clear"):
                policies.clear()
            try:
                if hasattr(env, "pop"):
                    env.pop("logger", None)
                elif "logger" in env:
                    del env["logger"]
            except Exception:
                pass
    except Exception as e:
        errors.append(f"clear environment mirror: {e!r}")

    return errors


def read_args(source, *, terminal_args=None) -> None:
    """Load user arguments from a TOML/JSON/.env/.xml file or ``terminal`` into ``laila.args``.

    Mutates ``laila.args`` in place. Always returns ``None``.
    """
    arg_reader.load(source, terminal_args=terminal_args)

_active_policy_gid: "str | None" = None
_active_namespace = None

def get_active_namespace():
    """Return the active UUID namespace, initializing it on first access."""
    global _active_namespace
    if _active_namespace is None:
        from .macros.defaults import LAILA_UNIVERSAL_NAMESPACE
        _active_namespace = LAILA_UNIVERSAL_NAMESPACE
    return _active_namespace

def set_active_namespace(namespace_key: str):
    """Set the active UUID namespace derived from *namespace_key*.

    Parameters
    ----------
    namespace_key : str
        DNS-style key used with ``uuid.uuid5`` to generate the namespace.
    """
    global _active_namespace
    _active_namespace = uuid.uuid5(uuid.NAMESPACE_DNS, namespace_key)

def get_active_policy():
    """Return the active policy, lazily creating a ``DefaultPolicy`` on first access.

    Resolves through ``universe`` (``_local_policies`` ∪ ``_remote_policies``)
    so that morphing into a peer via ``laila.active_policy = remote_proxy``
    transparently returns the proxy.  Local policies take precedence when
    a gid is registered both locally and as a remote proxy (this only
    happens when testing multiple local policies that peer with each
    other in the same process; in real cross-process usage the two
    namespaces are disjoint).
    """
    global _active_policy_gid
    if _active_policy_gid is None:
        from .macros.defaults import DefaultPolicy
        activate_policy(DefaultPolicy())
    if _active_policy_gid in _local_policies:
        return _local_policies[_active_policy_gid]
    return _remote_policies[_active_policy_gid]

def activate_policy(policy):
    """Replace the active policy.

    Accepts a local ``_LAILA_IDENTIFIABLE_POLICY`` or a
    ``RemotePolicyProxy`` obtained from ``laila.peers``.  Multiple local
    policies may coexist in the same process and the active one can be
    swapped freely; the most recent call wins.

    Equivalent to ``laila.active_policy = policy``.

    Parameters
    ----------
    policy : _LAILA_IDENTIFIABLE_POLICY | RemotePolicyProxy
        The policy instance (local or remote proxy) to activate.
    """
    global _active_policy_gid
    new_gid = str(policy.global_id)
    _active_policy_gid = new_gid

    from .policy.schema.base import _LAILA_IDENTIFIABLE_POLICY
    if isinstance(policy, _LAILA_IDENTIFIABLE_POLICY):
        _local_policies[new_gid] = policy

    try:
        from .basics.definitions.cli_capable import _refresh_args_environment
        _refresh_args_environment(policy)
    except Exception:
        pass


def _get_active_local_policy():
    """Return the local policy used for future registration.

    When the active policy is a local one, returns it directly so that
    swapping between several local policies routes future registration
    to the currently active one.  When the active policy is a remote
    proxy (morph mode), falls back to the single local policy registered
    in this process; with multiple locals and a remote active policy
    the caller must first switch back to a local policy.  When no policy
    is active yet, lazily activates a ``DefaultPolicy`` (mirroring
    ``get_active_policy``).
    """
    if _active_policy_gid is None:
        get_active_policy()
    if _active_policy_gid in _local_policies:
        return _local_policies[_active_policy_gid]
    if len(_local_policies) == 1:
        return next(iter(_local_policies.values()))
    raise RuntimeError(
        "No local policy available for future registration; "
        "set `laila.active_policy` to a local policy first."
    )


class _LailaModule(types.ModuleType):
    """Module subclass that exposes subsystem shortcuts and property-based
    access so that ``laila.active_policy = my_policy`` and
    ``laila.memory.extend(...)`` work naturally."""

    @property
    def active_policy(self):
        return get_active_policy()

    @active_policy.setter
    def active_policy(self, value):
        activate_policy(value)

    @property
    def communication(self):
        return get_active_policy().central.communication

    @property
    def memory(self):
        return get_active_policy().central.memory

    @property
    def command(self):
        return get_active_policy().central.command

    @property
    def peers(self):
        return get_active_policy().central.communication.peers

    @property
    def local_policies(self):
        """All local policies on this machine, keyed by ``global_id``."""
        return _local_policies

    @property
    def remote_policies(self):
        """All remote peer policies, keyed by ``global_id``."""
        return _remote_policies

    @property
    def universe(self):
        """Union of local and remote policies, keyed by ``global_id``.

        Locals take precedence when a gid appears in both maps (only
        possible when peering local policies in the same process).
        """
        return {**_remote_policies, **_local_policies}

    @property
    def alpha_pool(self):
        """The active policy's default (alpha) pool instance."""
        mem = get_active_policy().central.memory
        return mem.pool_router.pools[mem.alpha_pool]

    @property
    def runtime(self):
        """Runtime introspection module for futures."""
        import importlib
        return importlib.import_module("laila.runtime")

    @property
    def logger(self):
        """Process-wide :class:`laila.logger.Logger` singleton (lazy)."""
        return get_logger()

    @logger.setter
    def logger(self, value):
        """Replace the singleton with a fully-built ``Logger`` instance."""
        Logger.reset_singleton()
        Logger._singleton = value


sys.modules[__name__].__class__ = _LailaModule


def build(entry, *, taskforce_id: Optional["str"] = None):
    """Materialize *entry* by submitting its async build to the active command.

    Always submits the entry's ``_build_async`` coroutine to the alpha
    taskforce. Both build flavors compose cleanly on a loop thread:

    - ``SimpleConstitution`` entries finish their build inline (pure CPU,
      no nested awaits).
    - ``ComplexConstitution`` entries ``await`` ``laila.remember(...)``
      to fetch their manifest and then ``await`` ``manifest.async_realized``
      to materialize referenced entries — all without ever blocking the
      loop on a sync ``Future.wait()``.

    From sync callers (e.g. main thread, tests), ``ref.wait(None)`` blocks
    until completion. From async callers, ``await ref`` yields the loop
    while the build progresses.

    On completion the entry is mutated in place: ``_payload`` is populated,
    ``_constitution`` is cleared, and ``_state`` is ``READY``. The future
    also resolves to the entry for convenience.

    Parameters
    ----------
    entry : Entry
        The entry to materialize. Must have a constitution attached.
    taskforce_id : str, optional
        Target taskforce; defaults to the alpha taskforce.

    Returns
    -------
    Future
        Future identity that resolves to the (mutated) entry.

    Raises
    ------
    RuntimeError
        If *entry* has no constitution or is already built.
    """
    command = get_active_policy().central.command
    return command.submit([entry._build_async], taskforce_id=taskforce_id)


def memorize(*args, **kwargs):
    """Persist one or more entries into the active policy's memory.

    Entries are serialized, transformed, and written to the routed pool.
    Returns a future (or ``GroupFuture``) that resolves when the write
    completes.  Returns ``None`` when the default in-memory pool is used.

    Parameters
    ----------
    entries : Entry | list[Entry]
        The entry or entries to store.
    pool_id : str, optional
        Explicit pool ``global_id`` to route to.
    pool_nickname : str, optional
        Pool alias registered via ``extend``.
    """
    return get_active_policy().central.memory.memorize(*args, **kwargs)

def __resolve_nickname(kwargs):
    """Convert a nickname in *kwargs* to a deterministic ``global_id`` list."""
    if isinstance(kwargs["nickname"], str):
        args = [
            Entry.to_global_id(
                nickname = kwargs["nickname"],
                scopes = [_ENTRY_SCOPE],
                evolution = kwargs.get("evolution", None)
            )
        ]
        return args
    else:
        raise ValueError("nickname must be a string")

def remember(*args, persist: bool = True, **kwargs):
    """Retrieve one or more entries from the active policy's memory.

    Reads serialized blobs from the routed pool, applies the inverse
    transformation sequence, and returns recovered ``Entry`` objects
    wrapped in a future.

    Parameters
    ----------
    entry_ids : str | list[str]
        The ``global_id``(s) of the entries to recall.
    pool_id : str, optional
        Explicit pool ``global_id`` to read from.
    pool_nickname : str, optional
        Pool alias registered via ``extend``.
    nickname : str, optional
        Convenience alias – converted to a deterministic ``global_id``.
    persist : bool, default True
        When ``True``, the fetched entries are also memorized into the
        active policy's alpha pool, and the returned future only resolves
        after the alpha-pool write has completed.  Using
        ``with laila.guarantee:`` therefore blocks until the alpha pool
        has received the entries.  When the routed source pool already
        is the alpha pool, the write is skipped.
    """
    if "nickname" in kwargs:
        args = []
        kwargs["entry_ids"] = __resolve_nickname(kwargs)
        del kwargs["nickname"]
        if "evolution" in kwargs:
            del kwargs["evolution"]
    return get_active_policy().central.memory.remember(
        *args, persist=persist, **kwargs
    )

def forget(*args, **kwargs):
    """Delete one or more entries from the active policy's memory.

    Removes the stored blob from the routed pool.  Returns a future
    that resolves when the deletion completes.

    Parameters
    ----------
    entry_ids : str | list[str]
        The ``global_id``(s) of the entries to delete.
    pool_id : str, optional
        Explicit pool ``global_id`` to delete from.
    pool_nickname : str, optional
        Pool alias registered via ``extend``.
    nickname : str, optional
        Convenience alias – converted to a deterministic ``global_id``.
    """
    if "nickname" in kwargs:
        args = []
        kwargs["entry_ids"] = __resolve_nickname(kwargs)
        del kwargs["nickname"]
        if "evolution" in kwargs:
            del kwargs["evolution"]
    return get_active_policy().central.memory.forget(*args, **kwargs)


def add_peer(uri: str, secret: str) -> str:
    """Connect to a remote policy and register it as a peer.

    Parameters
    ----------
    uri : str
        URI of the remote policy (e.g. ``"ws://host:port"``).
    secret : str
        The remote policy's ``peer_secret_key`` (on its protocol).

    Returns
    -------
    str
        The ``global_id`` of the newly peered remote policy.
    """
    return get_active_policy().central.communication.add_peer(uri, secret)


def _resolve_future(future_ref):
    """Look up the actual future object from a reference.

    Accepts a ``RemoteFuture``, a future identity, a GroupFuture, a
    full Future, or a ``global_id`` string.  Searches all local policy
    banks when resolving by string.
    """
    from .policy.central.command.schema.future.future.remote_future import RemoteFuture
    from .policy.central.command.schema.future.future.future_identity import _LAILA_IDENTIFIABLE_FUTURE
    from .policy.central.command.schema.future.future.group_future import GroupFuture
    from .policy.central.command.schema.future.future.future import Future

    if isinstance(future_ref, RemoteFuture):
        return future_ref
    if isinstance(future_ref, GroupFuture):
        return future_ref
    if isinstance(future_ref, Future):
        return future_ref

    if isinstance(future_ref, str):
        for policy in _local_policies.values():
            if future_ref in policy.future_bank:
                return policy.future_bank[future_ref]
        bank = get_active_policy().future_bank
        return bank[future_ref]

    if isinstance(future_ref, _LAILA_IDENTIFIABLE_FUTURE):
        gid = future_ref.global_id
        for policy in _local_policies.values():
            if gid in policy.future_bank:
                return policy.future_bank[gid]
        bank = get_active_policy().future_bank
        return bank[gid]

    raise TypeError(f"Cannot resolve future for {type(future_ref)}")


def status(future_ref):
    """Return the status of a future.

    .. deprecated::
        Use ``laila.runtime.status()`` instead.
    """
    from . import runtime
    return runtime.status(future_ref)


def wait(future_ref, timeout=None):
    """Block until the future completes and return its result.

    .. deprecated::
        Use ``laila.runtime.wait()`` instead.
    """
    from . import runtime
    return runtime.wait(future_ref, timeout)


def set_default_directory(directory):
    """Override the default root directory and derived sub-directories.

    Parameters
    ----------
    directory : str
        Filesystem path (may contain ``~``) to use as the new root.
    """
    directory = os.path.expanduser(directory)
    LAILA_DEFAULT_DIRECTORIES.update({
        "root": directory,
        "pools": os.path.join(directory, "pools"),
        "logs": os.path.join(directory, "logs"),
        "secrets": os.path.join(directory, "secrets"),
    })
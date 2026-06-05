"""Laila top-level package — Lambda's Interdisciplinary Large Atlas.

This module is the single, opinionated entry point that user code
imports as ``import laila``. Every other capability — storage pools,
task-forces, peer-to-peer communication, futures, manifests,
constitutions, and the CLI/TOML environment loader — is reachable
either as a public attribute on this module or as a free function
defined below.

Major surface
-------------
**Subsystem shortcuts** (resolved against the *active* policy):

- ``laila.memory`` -> :class:`policy.central.memory.schema.base._LAILA_IDENTIFIABLE_CENTRAL_MEMORY`
- ``laila.command`` -> :class:`policy.central.command.schema.base._LAILA_IDENTIFIABLE_CENTRAL_COMMAND`
- ``laila.communication`` -> :class:`policy.central.communication.schema.base._LAILA_IDENTIFIABLE_COMMUNICATION`
- ``laila.peers`` -> dict of :class:`RemotePolicyProxy` keyed by ``global_id``
- ``laila.alpha_pool`` -> the active policy's default storage pool
- ``laila.runtime`` -> :mod:`laila.runtime` (futures introspection)
- ``laila.logger`` -> the process-wide :class:`laila.logger.Logger` singleton

**Active policy management**:

- ``laila.active_policy`` (read/write) -- get or set the active policy.
  Setting accepts both local policies and remote :class:`RemotePolicyProxy`
  objects, enabling "morph" mode where local code transparently routes
  through a peer.
- :func:`activate_policy` / :func:`get_active_policy` -- explicit forms.
- :func:`get_active_namespace` / :func:`set_active_namespace` -- control
  the UUID-5 namespace used to derive deterministic IDs from nicknames.
- ``laila.local_policies`` / ``laila.remote_policies`` / ``laila.universe``
  -- enumerate every policy reachable from this process.

**High-level memory operations** (delegate to the active policy's central
memory; ``memorize`` / ``remember`` also accept ``policy_id=`` to target a
connected peer's memory):

- :func:`memorize` -- write entries to the routed pool (or a peer's pool
  via ``policy_id=``).
- :func:`remember` -- read entries (optionally caching into the alpha pool;
  read from a peer via ``policy_id=``).
- :func:`forget`  -- delete entries.
- :func:`build`   -- materialize an entry by running its constitution.

**Lifecycle and configuration**:

- :func:`terminate` -- best-effort, idempotent tear-down of every
  subsystem this process owns.
- :func:`read_args` -- load TOML/JSON/.env/.xml/CLI args into ``laila.args``.
- ``laila.args`` -- a :class:`_LailaArgs` (DotMap subclass) used by every
  ``_LAILA_CLI_CAPABLE_CLASS`` for 4-tier parameter resolution. Assigning
  to ``laila.args.environment`` triggers a full environment reload.
- :func:`set_default_directory` -- relocate the on-disk root used by
  pools, logs, and secrets.

**Networking helpers**:

- :func:`add_peer` -- handshake with a remote policy and return its gid.

Implementation notes
--------------------
This module installs a ``__class__`` swap (``sys.modules[__name__].__class__``)
so that simple attribute access (``laila.memory``, ``laila.active_policy``)
can be backed by Python ``property`` descriptors -- see :class:`_LailaModule`.
That trick is what makes "subsystem shortcuts" work without forcing users
to call helper functions on every access.

The module also installs a null logging handler at import time so that
applications which never call :func:`laila.enable_logging` do not see
"No handlers could be found" warnings from the standard ``logging``
machinery.
"""

import os
import sys
import types
import uuid
from importlib import metadata as _metadata
from typing import Optional

from dotmap import DotMap

try:
    __version__ = _metadata.version("laila-core")
except _metadata.PackageNotFoundError:
    __version__ = "0.0.0+local"

from . import entry, policy
from .entry import Entry
from .policy.central.memory.schema.manifest import Manifest

manifest = Manifest
from .logger import (
    Logger,
    disable_logging,
    enable_logging,
    get_logger,
    set_log_level,
)
from .logger import (
    _install_null_handler as _install_logger_null_handler,
)
from .macros.aliases import *
from .macros.defaults import *
from .macros.strings import _ENTRY_SCOPE
from .policy.central.command import taskforce as TaskForce
from .policy.schema.base import _LAILA_IDENTIFIABLE_POLICY
from .utils import guarantee, guarantee_async
from .utils.args import ArgReader

_install_logger_null_handler()


def _is_env_load_trigger(value: "object") -> bool:
    """Decide whether assigning *value* to ``laila.args.environment`` should
    trigger a full :func:`_load_environment` cycle.

    The ``args.environment`` slot is overloaded -- it serves two roles
    that share the same key, and we must distinguish them at write time
    to avoid an infinite loop:

    1. **User-driven full reload.** A user (or a CLI / TOML loader)
       assigns a *fully-populated* environment payload to
       ``laila.args.environment``. We must call :func:`_load_environment`
       which tears down the current process state and rebuilds every
       policy described by the payload.

    2. **Internal mirror update.** Whenever a CLI-capable class is
       constructed or mutated, :func:`_refresh_args_environment` writes
       a snapshot of the owning policy back to
       ``laila.args.environment.policies[<gid>]``. That write must NOT
       re-enter :func:`_load_environment` -- otherwise every model
       construction during a load would recursively trigger another load.

    The two cases are distinguished as follows:

    - A *plain* ``dict`` with at least one key is always treated as a
      user-driven payload. (``_refresh_args_environment`` only ever
      writes ``DotMap`` instances, never plain dicts.) The load function
      itself is responsible for validating the dict's shape and raising
      ``ValueError`` for malformed envs.
    - A ``DotMap`` is treated as a load trigger only when it carries a
      non-empty ``policies`` mapping or an ``active_gid``. The mirror
      path can briefly leave ``args.environment`` as an *empty* DotMap
      (during initial scaffolding) and must not re-enter the loader.
    - Anything else (non-mapping, ``None``, empty ``dict``) is inert
      and silently passes through to :class:`DotMap`'s normal setter.

    Parameters
    ----------
    value : object
        The proposed new value for ``laila.args.environment``.

    Returns
    -------
    bool
        ``True`` if :func:`_load_environment` should be invoked,
        ``False`` if the assignment should fall through to the default
        DotMap behavior.
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
    """``DotMap`` subclass that watches for assignments to ``environment``
    and triggers a full process-wide reload when one arrives.

    ``laila.args`` is a single instance of this class created at import
    time. Almost every CLI-capable Pydantic model in the package walks
    ``laila.args`` during validation to fill in defaults (see
    :class:`laila.basics.definitions.cli_capable._LAILA_CLI_CAPABLE_CLASS`).
    The ``environment`` key, however, is special: it stores a
    machine-readable snapshot of the *current* runtime (which policies
    exist, which taskforces and pools are wired, which protocols are
    registered) and is also the input format for restoring that runtime
    on another process or after :func:`terminate`.

    Hooked behavior
    ---------------
    Setting ``laila.args.environment = my_env`` (or
    ``laila.args["environment"] = my_env``) where ``my_env`` carries a
    non-empty ``policies`` mapping (or an ``active_gid``) triggers a
    cascade:

    1. :func:`terminate` is invoked to shut down every existing policy,
       close every pool, and stop every taskforce.
    2. :func:`_load_environment` walks ``my_env.policies`` and, using
       the ``class_token`` recorded for each taskforce, pool, and
       protocol, instantiates the appropriate concrete subclasses with
       the recorded UUIDs preserved.
    3. The chosen policy is activated.

    Plain ``dict`` -> ``DotMap`` coercion is also performed for any
    other key, so users can write ``laila.args.foo = {"bar": 1}`` and
    immediately access ``laila.args.foo.bar``.

    The mirror-update path -- ``_refresh_args_environment`` writing to
    ``args.environment.policies[<gid>]`` -- goes through DotMap's normal
    ``__setitem__`` on the inner ``policies`` DotMap (NOT through this
    class's hooks), so the recursive loop described in
    :func:`_is_env_load_trigger` is impossible.
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

    This is the canonical "go back to a clean slate" call. It is safe
    to call from anywhere -- including inside an exception handler --
    and is automatically invoked at the start of every full environment
    reload (see :class:`_LailaArgs`).

    Order of operations
    -------------------
    For each policy in :data:`_local_policies` (a snapshot is taken first
    so concurrent mutation is harmless):

    1. ``policy.central.communication.stop()`` -- closes WebSocket
       servers, joins their event-loop threads, drops every peer
       registration. This is done first because peer-side RPCs that
       arrive after a taskforce shuts down would otherwise raise
       confusing exceptions instead of clean ``ConnectionClosed`` errors.
    2. ``policy.central.command.shutdown(wait=..., cancel_pending=...)``
       -- shuts down every registered taskforce. With ``wait=True`` we
       block until the workers drain; with ``cancel_pending=True`` any
       queued-but-unstarted submissions are dropped.
    3. ``pool.close()`` for each pool registered in
       ``policy.central.memory.pool_router.pools`` -- terminates managed
       subprocesses (Redis / Postgres / Mongo), closes file handles
       (HDF5 / SQLite / DuckDB / filesystem mounts), and disconnects
       cloud clients (S3, GCS, Azure, Backblaze, Cloudflare R2).

    After the per-policy sweep:

    4. :data:`_local_policies`, :data:`_remote_policies`, and
       :data:`_active_policy_gid` are cleared.
    5. *Orphan* taskforces -- those created via raw ``TaskForce.*``
       instantiation but never attached to a policy -- are looked up
       through :func:`_live_taskforces_snapshot` and shut down.
    6. The :class:`Logger` singleton is reset so the next call to
       :func:`get_logger` returns a fresh instance.
    7. The ``laila.args.environment.policies`` and
       ``laila.args.environment.logger`` mirrors are cleared so a
       subsequent ``laila.args.environment = {...}`` assignment is not
       contaminated by stale entries.

    Best-effort semantics
    ---------------------
    Each step is wrapped in its own ``try``/``except`` so that a failure
    in one subsystem (e.g. a hung Redis connection) does not prevent the
    others from being torn down. Failures are recorded in the returned
    list as short ``"<step>[<id>]: <repr>"`` strings; this function never
    raises directly.

    Parameters
    ----------
    wait : bool, default True
        Forwarded to ``command.shutdown`` -- block until taskforce
        workers exit. Set to ``False`` for non-blocking, fire-and-forget
        teardown (typically only useful in test harnesses).
    cancel_pending : bool, default False
        Forwarded to ``command.shutdown`` -- cancel queued, un-started
        futures. Already-running tasks are not cancelled regardless of
        this flag.

    Returns
    -------
    list[str]
        One short error string per failed step. An empty list means
        clean shutdown. The function never raises -- failures are
        recorded and the next step still runs (best-effort, idempotent).
    """
    global _active_policy_gid
    errors: list = []

    for gid, pol in list(_local_policies.items()):
        try:
            comm = getattr(getattr(pol, "central", None), "communication", None)
            if comm is not None:
                comm.stop()
        except Exception as e:
            errors.append(f"communication.stop[{gid}]: {e!r}")

        try:
            cmd = getattr(getattr(pol, "central", None), "command", None)
            if cmd is not None:
                cmd.shutdown(wait=wait, cancel_pending=cancel_pending)
        except Exception as e:
            errors.append(f"command.shutdown[{gid}]: {e!r}")

        try:
            mem = getattr(getattr(pol, "central", None), "memory", None)
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
    """Load user arguments from a file or terminal flags into ``laila.args``.

    A thin wrapper around the singleton :class:`laila.utils.args.ArgReader`
    bound to the live ``laila.args`` :class:`_LailaArgs` instance.

    Supported sources
    -----------------
    - ``"path/to/config.toml"`` -- parsed with :mod:`tomllib`.
    - ``"path/to/config.json"`` -- parsed with :mod:`json`.
    - ``"path/to/.env"``        -- parsed line-by-line as ``KEY=value``.
    - ``"path/to/config.xml"``  -- parsed as a flat XML key-value map.
    - ``"terminal"``            -- consume :data:`sys.argv` (or
      *terminal_args*) as ``--key value`` / ``--key=value`` pairs.

    For non-terminal sources, nested TOML tables map cleanly onto nested
    DotMap attributes (``[policy.central.memory]`` becomes
    ``laila.args.policy.central.memory``).

    Side effects
    ------------
    - Mutates ``laila.args`` in place. Existing keys are *merged*, not
      replaced -- callers that need a clean slate should construct a
      fresh dict and assign it to ``laila.args.environment`` to trigger
      a full reload via :class:`_LailaArgs`.
    - If the loaded payload contains an ``environment`` key with a
      non-empty ``policies`` mapping, the assignment hook fires and
      :func:`_load_environment` is invoked, which calls
      :func:`terminate` and rebuilds the entire policy graph.

    Parameters
    ----------
    source : str
        Path to a TOML/JSON/.env/.xml file, or the literal string
        ``"terminal"``.
    terminal_args : list[str], optional
        Override for :data:`sys.argv` when *source* is ``"terminal"``.
        Useful in tests; also lets callers strip off ``argv[0]``
        themselves.

    Returns
    -------
    None
        Always. Inspect ``laila.args`` for the result.
    """
    arg_reader.load(source, terminal_args=terminal_args)


_active_policy_gid: "str | None" = None
_active_namespace = None


def get_active_namespace():
    """Return the UUID-5 namespace used to derive deterministic IDs from nicknames.

    Every nickname-based identifier in laila (``Entry.constant(nickname=...)``,
    ``Entry.variable(nickname=...)``, the convenience ``nickname=`` argument
    on :func:`memorize` / :func:`remember` / :func:`forget`) is hashed
    against this namespace to produce a stable :class:`uuid.UUID`. Two
    processes that share the same namespace and nickname will compute
    *identical* global IDs, which is what makes nicknames usable for
    cross-process / cross-machine entry resolution.

    On first access the namespace is initialized to
    :data:`laila.macros.defaults.LAILA_UNIVERSAL_NAMESPACE`. Override it
    via :func:`set_active_namespace` to scope your project's nickname
    space (e.g. so two unrelated projects can't accidentally collide on
    the nickname ``"images"``).

    Returns
    -------
    uuid.UUID
        The currently active namespace UUID.
    """
    global _active_namespace
    if _active_namespace is None:
        from .macros.defaults import LAILA_UNIVERSAL_NAMESPACE

        _active_namespace = LAILA_UNIVERSAL_NAMESPACE
    return _active_namespace


def set_active_namespace(namespace_key: str):
    """Replace the active UUID-5 namespace with one derived from *namespace_key*.

    The new namespace is computed as ``uuid.uuid5(uuid.NAMESPACE_DNS,
    namespace_key)``. After this call every nickname-based identifier
    minted by laila will be derived from the new namespace.

    Typical use is once at startup, with a stable project identifier:

    .. code-block:: python

        laila.set_active_namespace("acme.research.experiments")

    Two processes that pass the same ``namespace_key`` will derive the
    same namespace UUID, so nicknames remain interoperable across them.

    Parameters
    ----------
    namespace_key : str
        DNS-style key used with :func:`uuid.uuid5` to generate the
        namespace. Any string is accepted; using a reverse-domain
        identifier you control is recommended to avoid collisions.

    See Also
    --------
    get_active_namespace : retrieve the currently active namespace.
    """
    global _active_namespace
    _active_namespace = uuid.uuid5(uuid.NAMESPACE_DNS, namespace_key)


def get_active_policy():
    """Return the active policy, lazily creating a ``DefaultPolicy`` on first access.

    The "active policy" is the implicit subject of every top-level
    laila call (``laila.memorize``, ``laila.remember``, ``laila.build``,
    ``laila.memory``, etc.). At most one policy is active per process at
    any given time, but multiple policies may coexist in
    :data:`_local_policies` and the active one can be swapped via
    :func:`activate_policy` (or, equivalently, by assigning to
    ``laila.active_policy``).

    Resolution order
    ----------------
    1. If no policy has been activated yet, instantiate a fresh
       :class:`laila.macros.defaults.DefaultPolicy` and activate it.
       This makes "import laila and use it" work without ceremony.
    2. If the active gid maps to a *local* policy, return that instance.
    3. Otherwise the active gid must map to a *remote* peer (we
       previously activated a :class:`RemotePolicyProxy`); return the
       proxy. This is "morph mode": local code transparently routes
       every memory / command / communication call through the peer.

    Local policies take precedence when a gid appears in both
    :data:`_local_policies` and :data:`_remote_policies` -- this can
    only happen when two local policies in the same process peer with
    each other (a test-only setup); in real cross-process usage the
    namespaces are disjoint.

    Returns
    -------
    _LAILA_IDENTIFIABLE_POLICY | RemotePolicyProxy
        The active policy. On first access this is always a freshly-
        constructed ``DefaultPolicy``.
    """
    global _active_policy_gid
    if _active_policy_gid is None:
        from .macros.defaults import DefaultPolicy

        activate_policy(DefaultPolicy())
    if _active_policy_gid in _local_policies:
        return _local_policies[_active_policy_gid]
    return _remote_policies[_active_policy_gid]


def activate_policy(policy):
    """Replace the active policy with *policy*.

    Mutates the module-level :data:`_active_policy_gid` and, when
    *policy* is a *local* policy, ensures it is registered in
    :data:`_local_policies`. After this call:

    - :func:`get_active_policy` returns *policy*.
    - The subsystem shortcuts ``laila.memory``, ``laila.command``,
      ``laila.communication``, and ``laila.peers`` resolve through
      *policy*.
    - High-level helpers (:func:`memorize`, :func:`remember`,
      :func:`forget`, :func:`build`) operate on *policy*.
    - The ``laila.args.environment`` mirror is best-effort refreshed
      so a subsequent restart can recreate the same configuration.

    Equivalent to ``laila.active_policy = policy``.

    Switching freely
    ----------------
    Multiple local policies may coexist in the same process; this call
    simply swings the "currently active" pointer. To use a remote
    policy ("morph mode"), pass a :class:`RemotePolicyProxy` obtained
    from ``laila.peers`` -- subsequent local calls then transparently
    RPC into the peer.

    Parameters
    ----------
    policy : _LAILA_IDENTIFIABLE_POLICY | RemotePolicyProxy
        The policy instance (local or remote proxy) to activate.
        Must expose a ``global_id`` property.

    Notes
    -----
    The args-environment refresh is wrapped in a broad ``try``/``except``
    -- failures here are non-fatal because the in-memory policy state is
    the source of truth; the environment mirror is purely for
    serialization / restart purposes.
    """
    global _active_policy_gid
    new_gid = str(policy.global_id)
    _active_policy_gid = new_gid

    if isinstance(policy, _LAILA_IDENTIFIABLE_POLICY):
        _local_policies[new_gid] = policy

    try:
        from .basics.definitions.cli_capable import _refresh_args_environment

        _refresh_args_environment(policy)
    except Exception:
        pass


def _get_active_local_policy():
    """Return the local policy that should own newly-created futures.

    Used by :class:`Future` / :class:`GroupFuture` (and other
    ``_LAILA_IDENTIFIABLE_FUTURE`` subclasses) at construction time so
    they can self-register into ``policy.future_bank``. Futures must
    always live in a *local* policy (a :class:`RemotePolicyProxy` does
    not have a real ``future_bank``), even when the *active* policy is
    a remote peer in morph mode.

    Resolution order
    ----------------
    1. If no policy is active yet, lazily activate a ``DefaultPolicy``
       via :func:`get_active_policy`. This means importing laila and
       creating a future works without any setup.
    2. If the active gid maps to a *local* policy, return it. This is
       the common case and ensures that swapping between several local
       policies routes future registration to the currently active one.
    3. Otherwise (active policy is a remote proxy):
       - if exactly one local policy exists in this process, return it
         (an obvious unambiguous fallback);
       - if multiple local policies exist, raise ``RuntimeError`` --
         the caller must first switch back to a local policy with
         :func:`activate_policy`.

    Returns
    -------
    _LAILA_IDENTIFIABLE_POLICY
        A local policy suitable for future registration.

    Raises
    ------
    RuntimeError
        If the active policy is a remote proxy and the number of local
        policies is not exactly one.
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
    """Module subclass that backs ``laila.<name>`` attribute access with
    Python ``property`` descriptors.

    Why subclass :class:`types.ModuleType`?
    ---------------------------------------
    Plain modules cannot expose computed attributes (you get a fresh
    ``laila.memory`` every call only if it is recomputed each time --
    a normal module-level binding is static). By installing this
    subclass as ``sys.modules[__name__].__class__`` at the bottom of
    this file we get the best of both worlds: ``import laila`` still
    returns a module, but every read of ``laila.memory`` /
    ``laila.command`` / ``laila.communication`` re-resolves through
    the *current* active policy, so swapping the active policy is
    instantly visible to user code without any extra plumbing.

    The same trick lets ``laila.active_policy = my_policy`` and
    ``laila.logger = MyLogger()`` work as ordinary assignments while
    still routing through :func:`activate_policy` and
    :meth:`Logger.reset_singleton`.
    """

    @property
    def active_policy(self):
        """Currently active policy (lazy ``DefaultPolicy`` on first access)."""
        return get_active_policy()

    @active_policy.setter
    def active_policy(self, value):
        """Replace the active policy via :func:`activate_policy`."""
        activate_policy(value)

    @property
    def communication(self):
        """Active policy's ``central.communication`` -- peers, protocols, RPC."""
        return get_active_policy().central.communication

    @property
    def memory(self):
        """Active policy's ``central.memory`` -- memorize/remember/forget, pools."""
        return get_active_policy().central.memory

    @property
    def command(self):
        """Active policy's ``central.command`` -- taskforces, submit, futures."""
        return get_active_policy().central.command

    @property
    def peers(self):
        """Mapping of ``global_id`` -> :class:`RemotePolicyProxy` for every connected peer."""
        return get_active_policy().central.communication.peers

    @property
    def local_policies(self):
        """All local policies on this machine, keyed by ``global_id``.

        A *local* policy is one whose subsystems live in this process.
        At any time at most one is active, but several may coexist (e.g.
        for testing peer-to-peer flows in a single process).
        """
        return _local_policies

    @property
    def remote_policies(self):
        """All remote peer policies, keyed by ``global_id``.

        Each value is a :class:`RemotePolicyProxy` that translates
        attribute access into RPC calls against the peer. Membership
        is updated automatically by handshake / disconnect events on
        the underlying communication protocols.
        """
        return _remote_policies

    @property
    def universe(self):
        """Union of local and remote policies, keyed by ``global_id``.

        Useful when you have a gid in hand and don't care whether the
        owner is local or remote. Locals take precedence when a gid
        appears in both maps (only possible when peering local
        policies in the same process for tests).
        """
        return {**_remote_policies, **_local_policies}

    @property
    def alpha_pool(self):
        """The active policy's default (alpha) pool instance.

        The alpha pool is the destination for ``laila.memorize(...)``
        when no explicit ``pool_id`` / ``pool_nickname`` is provided,
        and the cache target for ``laila.remember(..., persist=True)``.
        """
        mem = get_active_policy().central.memory
        return mem.pool_router.pools[mem.alpha_pool]

    @property
    def runtime(self):
        """The :mod:`laila.runtime` module -- future status / wait / result helpers.

        Loaded lazily via :func:`importlib.import_module` to avoid
        pulling the runtime symbols into the top-level namespace at
        import time (they would otherwise shadow the deprecated
        :func:`status` / :func:`wait` shims).
        """
        import importlib

        return importlib.import_module("laila.runtime")

    @property
    def logger(self):
        """Process-wide :class:`laila.logger.Logger` singleton (lazy).

        A singleton because every ``record_*`` call writes to the same
        underlying handlers; sharing one instance avoids duplicate
        output when several pieces of code want to log.
        """
        return get_logger()

    @logger.setter
    def logger(self, value):
        """Replace the logger singleton with a fully-built :class:`Logger`.

        Resets the existing singleton first so any cached references
        (e.g. inside CLI-capable models that captured ``get_logger()``
        during validation) become stale and re-resolve on next access.
        """
        Logger.reset_singleton()
        Logger._singleton = value


sys.modules[__name__].__class__ = _LailaModule


def build(entry, *, taskforce_id: Optional["str"] = None):
    """Materialize *entry* by running its constitution on a taskforce.

    A :class:`laila.entry.Entry` may be created with a
    :class:`Constitution` rather than a concrete payload; in that case
    its ``state`` is :class:`EntryState.STAGED` and ``entry.data`` will
    raise :class:`EntryNotBuiltError` until you call :func:`build`.

    What this function does
    -----------------------
    Always submits the entry's :meth:`Entry._build_async` coroutine to
    the chosen taskforce (alpha by default). The unified async path
    composes cleanly with the rest of the system:

    - **SimpleConstitution** entries: a pure-CPU chain of inverse
      transformations (e.g. base64-decode -> zlib-decompress ->
      msgpack-deserialize). The coroutine runs the chain inline on its
      loop thread because there is no I/O to ``await``.
    - **ComplexConstitution** entries: the user-provided builder
      function takes a :class:`Manifest`. The coroutine first
      ``await``\\ s :func:`Manifest.async_realized` to recursively
      materialize every referenced entry (each of which may itself be
      a complex build), then offloads the user's sync builder body to
      :func:`asyncio.to_thread` so that any internal blocking ``.wait()``
      calls don't deadlock the loop.

    Calling conventions
    -------------------
    The returned future identity is awaitable both ways:

    - From sync callers (main thread, tests): ``build(entry).wait()``
      blocks until completion.
    - From async callers: ``await build(entry)`` yields the loop while
      the build progresses.

    Either way, on completion the entry is mutated in place:
    ``_payload`` is populated, ``_constitution`` is cleared, and
    ``_state`` flips to ``READY``. The future *also* resolves to the
    same entry instance for convenience.

    Parameters
    ----------
    entry : Entry
        The entry to materialize. Must have a constitution attached
        (otherwise :meth:`Entry._build_async` raises ``RuntimeError``).
    taskforce_id : str, optional
        Target taskforce ``global_id``; defaults to
        ``policy.central.command.alpha_taskforce``. Pass a different
        taskforce gid to route CPU-heavy builds to a process pool.

    Returns
    -------
    Future
        Future identity that resolves to the (now-built) entry.

    Raises
    ------
    RuntimeError
        Raised inside the future when *entry* has no constitution or is
        already built. (The submission itself does not raise.)

    See Also
    --------
    laila.memorize : persist a built entry to a pool.
    laila.remember : fetch (and optionally build) an entry from a pool.
    laila.entry.Entry.variable : create an entry with a constitution.
    """
    command = get_active_policy().central.command
    return command.submit([entry._build_async], taskforce_id=taskforce_id)


def _route_memory_to_peer(proxy, op: str, args: tuple, kwargs: dict):
    """Route a memory op to a remote peer as a LOCAL (A-owned) Future.

    With the *current* active policy A (not morphed), asking peer B to
    memorize/remember produces a normal local :class:`Future` /
    :class:`GroupFuture` owned by A -- not a :class:`RemoteFuture`. The
    over-the-wire transfer runs inside one A-side task per entry; each
    task offloads the blocking wire RPC with :func:`asyncio.to_thread` so
    it never blocks the taskforce event loop.

    Entries cross the wire as their canonical, self-describing
    ``Entry.serialize(transformation_base64)`` blob and are rebuilt via
    :func:`build_by_scope` -- real entry data moves, no shared pool needed.
    (``RemoteFuture`` is reserved for *morph* mode, when A's active policy
    has been switched to B.)
    """
    import asyncio as _asyncio

    pool = kwargs.get("pool")
    if pool is not None and not isinstance(pool, str):
        raise TypeError(
            "Remote memory ops need a pool gid or nickname *string* for the "
            "peer-side pool; a standalone pool object cannot be shipped to a peer."
        )
    mem_kwargs = {"pool": pool} if pool is not None else {}
    command = get_active_policy().central.command

    if op == "memorize":
        entries = args[0] if args else kwargs.get("entries")
        entries_list = entries if isinstance(entries, list) else [entries]
        from .entry import transformation_base64

        def _make_store(entry):
            blob = entry.serialize(transformations=transformation_base64)

            async def _store():
                gids = await _asyncio.to_thread(
                    proxy.central.memory._remote_memorize, [blob], **mem_kwargs
                )
                return gids[0]

            return _store

        return command.submit([_make_store(e) for e in entries_list])

    if op == "remember":
        entry_ids = args[0] if args else kwargs.get("entry_ids")
        ids_list = entry_ids if isinstance(entry_ids, list) else [entry_ids]
        ids = [x.global_id if hasattr(x, "global_id") else str(x) for x in ids_list]
        from .entry.constitution.build_maps import build_by_scope

        def _make_fetch(eid):
            async def _fetch():
                blobs = await _asyncio.to_thread(
                    proxy.central.memory._remote_remember, [eid], **mem_kwargs
                )
                return build_by_scope(blobs[0], asynchronous=False)

            return _fetch

        return command.submit([_make_fetch(eid) for eid in ids])

    if op == "forget":
        entry_ids = args[0] if args else kwargs.get("entry_ids")
        ids_list = entry_ids if isinstance(entry_ids, list) else [entry_ids]
        ids = [x.global_id if hasattr(x, "global_id") else str(x) for x in ids_list]

        def _delete():
            return proxy.central.memory._remote_forget(ids, **mem_kwargs)

        async def _delete_async():
            return await _asyncio.to_thread(_delete)

        return command.submit([_delete_async])

    # Any other op: pass straight through (gids are JSON-safe).
    return getattr(proxy.central.memory, op)(*args, **kwargs)


def _route_to_policy(policy_id, op: str, args: tuple, kwargs: dict, comm=None):
    """Run a central-memory operation against an arbitrary policy by ``global_id``.

    This is the engine behind the ``policy_id=`` argument on
    :func:`memorize` / :func:`remember`. The target may be:

    - a *remote* peer (a :class:`RemotePolicyProxy` registered in
      :data:`_remote_policies` after a successful handshake), in which
      case the operation is dispatched as a single RPC over the peer's
      transport via the proxy's attribute-chain. The local active policy
      is intentionally left *unchanged* so the :class:`RemoteFuture`
      returned by the RPC registers into the local policy's
      ``future_bank`` (a proxy has no bank to register into); using the
      peer's proxy is itself the sanctioned remote-access path, so the
      golden rule in ``vault/agent/policy.md`` is satisfied without a
      global morph.
    - another *local* policy in this process (registered in
      :data:`_local_policies`), in which case the active policy is
      *transiently* morphed into the target -- honouring the golden rule
      "to access a policy's elements it must become the active policy" --
      and restored afterwards.

    Parameters
    ----------
    policy_id : str
        ``global_id`` of the target policy. Resolved against
        ``laila.universe`` (locals take precedence over remotes when a
        gid appears in both).
    op : str
        The central-memory method to invoke -- one of ``"memorize"``,
        ``"remember"``, ``"forget"``.
    args : tuple
        Positional arguments forwarded to the operation.
    kwargs : dict
        Keyword arguments forwarded to the operation.

    Returns
    -------
    Any
        Whatever the underlying memory operation returns -- a
        :class:`Future` / :class:`GroupFuture` for a local target, or a
        :class:`RemoteFuture` when the target is a peer.

    Raises
    ------
    ConnectionError
        If *policy_id* does not name any known local or remote policy.
    """
    global _active_policy_gid
    from .policy.central.communication.proxy import RemotePolicyProxy

    pid = str(policy_id)
    target = {**_remote_policies, **_local_policies}.get(pid)
    if target is None:
        raise ConnectionError(
            f"Unknown policy_id {pid!r}: not a local policy or a connected peer. "
            "Connect to the peer first with laila.add_peer()/add_tcpip_peer()."
        )

    if isinstance(target, RemotePolicyProxy):
        if comm is not None:
            target = target.via(comm)
        return _route_memory_to_peer(target, op, args, kwargs)

    previous_gid = _active_policy_gid
    activate_policy(target)
    try:
        memory = get_active_policy().central.memory
        return getattr(memory, op)(*args, **kwargs)
    finally:
        _active_policy_gid = previous_gid


def _resolve_policy_ref(ref):
    """Resolve a policy reference to a ``global_id`` string, or ``None``.

    Accepts any of:

    - ``None`` -> ``None`` (means "the active policy" for src, or "no
      remote target" for dst).
    - a live policy / :class:`RemotePolicyProxy` (anything exposing
      ``global_id``) -> its ``global_id``.
    - a global-id string (recognised via
      :meth:`_LAILA_IDENTIFIABLE_OBJECT.is_laila_resource`) -> returned
      as-is.
    - any other string -> treated as a *nickname* and turned into a
      deterministic policy gid via
      :meth:`to_global_id(nickname=..., scopes=[POLICY])`. Two processes
      in the same namespace derive the same gid from the same nickname,
      which is what makes nicknames usable for cross-process routing.
    """
    if ref is None:
        return None
    gid = getattr(ref, "global_id", None)
    if isinstance(gid, str):
        return gid
    from .basics.definitions.identifiable_object import _LAILA_IDENTIFIABLE_OBJECT
    from .macros.strings import _POLICY_SCOPE

    s = str(ref)
    if _LAILA_IDENTIFIABLE_OBJECT.is_laila_resource(s):
        return s
    return _LAILA_IDENTIFIABLE_OBJECT.to_global_id(nickname=s, scopes=[_POLICY_SCOPE])


def _extract_gids(args, kwargs):
    """Pull a list of entry global_ids out of a ``(entry_ids,)`` arg shape."""
    val = args[0] if args else kwargs.get("entry_ids", kwargs.get("entries"))
    items = val if isinstance(val, list) else [val]
    return [x.global_id if hasattr(x, "global_id") else str(x) for x in items]


def _relay_transfer(verb, src_gid, args, kwargs, *, src_pool, dst_gid, dst_pool, comm, persist=True):
    """Drive a 3-party transfer by commanding the *source* policy.

    With active policy A, ``src_gid`` = B, ``dst_gid`` = C: A reaches B
    (A must be peered to B) and asks B to move the entries to/from C over
    B's *own* B<->C link. A never brokers the B<->C connection.

    - ``verb == "memorize"``: B pushes ``entry_ids`` from its ``src_pool``
      into C's ``dst_pool``.
    - ``verb == "remember"``: B pulls ``entry_ids`` from C's ``dst_pool``
      and stores them into B's ``src_pool``.
    """
    from .policy.central.communication.proxy import RemotePolicyProxy

    src = {**_remote_policies, **_local_policies}.get(src_gid)
    if src is None:
        raise ConnectionError(
            f"src_policy {src_gid!r} is not a connected peer or a local policy. "
            "The active policy must be peered to the source policy "
            "(connect first with laila.add_peer())."
        )
    gids = _extract_gids(args, kwargs)

    if isinstance(src, RemotePolicyProxy):
        relay = (src.via(comm) if comm is not None else src).central.memory
        if verb == "memorize":
            return relay._relay_memorize(
                gids, src_pool=src_pool, dst_policy=dst_gid, dst_pool=dst_pool, comm=comm
            )
        return relay._relay_remember(
            gids, dst_policy=dst_gid, dst_pool=dst_pool, src_pool=src_pool,
            comm=comm, persist=persist,
        )

    global _active_policy_gid
    previous_gid = _active_policy_gid
    activate_policy(src)
    try:
        memory = get_active_policy().central.memory
        if verb == "memorize":
            return memory._relay_memorize(
                gids, src_pool=src_pool, dst_policy=dst_gid, dst_pool=dst_pool, comm=comm
            )
        return memory._relay_remember(
            gids, dst_policy=dst_gid, dst_pool=dst_pool, src_pool=src_pool,
            comm=comm, persist=persist,
        )
    finally:
        _active_policy_gid = previous_gid


def memorize(
    *args,
    src_policy=None,
    src_pool=None,
    dst_policy=None,
    dst_pool=None,
    comm=None,
    policy_id=None,
    pool_id=None,
    pool_nickname=None,
    affinity=None,
    **kwargs,
):
    """Persist one or more entries into a policy's memory.

    Thin top-level shim that forwards to
    :meth:`policy.central.memory.memorize`. The work performed there:

    1. Resolve the destination pool via the
       :class:`PoolRouter` (``pool_id`` > ``pool_nickname`` > alpha).
    2. For each entry, wrap it in a :class:`Record`, serialize the
       record + its payload through the pool's
       :class:`TransformationSequence` (e.g. msgpack -> zlib -> base64),
       and ``await`` the pool's ``_write_async`` for the actual storage
       round-trip.
    3. Submit one async coroutine per entry to the alpha taskforce so
       writes proceed concurrently without blocking the caller.

    Return shape
    ------------
    A *future identity* you can ``await`` (async) or ``.wait()`` (sync):

    - One entry -> a single :class:`Future`.
    - Many entries -> a :class:`GroupFuture` that completes when all
      child writes complete.

    Pure in-memory pools that perform writes synchronously inside the
    submission may return ``None`` -- callers that need a future-shaped
    handle should pick a non-default pool or use ``with laila.guarantee:``.

    Parameters
    ----------
    entries : Entry | list[Entry]
        The entry or entries to store. Already-list inputs are passed
        through; single entries are auto-wrapped.
    pool_id : str, optional
        Explicit pool ``global_id`` to route to.
    pool_nickname : str, optional
        Pool alias registered via :meth:`PoolRouter.extend` /
        :meth:`Policy.extend`. Falls back to the alpha pool when neither
        ``pool_id`` nor ``pool_nickname`` is given.
    affinity : float, optional
        Reserved for future affinity-based routing.
    policy_id : str, optional
        ``global_id`` of a *peer* (or another local policy) to write
        into. When supplied, the entries are stored in that policy's
        pool (selected by ``pool_id`` / ``pool_nickname`` *on the peer*)
        rather than in the active policy's memory. See
        :func:`_route_to_policy`.

    Returns
    -------
    Future or GroupFuture or None
        Future-like handle resolving when the write(s) finish. A
        :class:`RemoteFuture` when ``policy_id`` names a peer.

    See Also
    --------
    laila.remember : the inverse operation.
    laila.policy.central.memory.schema.base._LAILA_IDENTIFIABLE_CENTRAL_MEMORY.memorize :
        the concrete implementation invoked by this shim.
    """
    # Accept the leading positional via the ``entries=`` keyword too.
    if not args and "entries" in kwargs:
        args = (kwargs.pop("entries"),)

    # Back-compat: policy_id -> dst_policy, pool_id/pool_nickname -> dst_pool.
    if dst_policy is None:
        dst_policy = policy_id
    if dst_pool is None:
        dst_pool = pool_id if pool_id is not None else pool_nickname

    src_gid = _resolve_policy_ref(src_policy)
    dst_gid = _resolve_policy_ref(dst_policy)
    active_gid = get_active_policy().global_id

    # Source is another policy -> 3-party relay (B pushes src->dst).
    if src_gid is not None and src_gid != active_gid:
        return _relay_transfer(
            "memorize", src_gid, args, kwargs,
            src_pool=src_pool, dst_gid=dst_gid, dst_pool=dst_pool, comm=comm,
        )

    # Source is the active policy.
    if dst_gid is not None and dst_gid != active_gid:
        # active -> peer push (2-party).
        return _route_to_policy(
            dst_gid, "memorize", args, {"pool": dst_pool}, comm=comm
        )

    # Purely local / standalone write into the active policy's pool.
    return get_active_policy().central.memory.memorize(
        *args, pool=dst_pool, affinity=affinity, **kwargs
    )


def __resolve_nickname(kwargs):
    """Translate a ``nickname=`` (and optional ``evolution=``) kwarg pair
    into the canonical ``[global_id]`` shape consumed by
    :meth:`memory.remember` / :meth:`memory.forget`.

    Routed through :func:`Entry.to_global_id` so the resulting gid uses
    the *active* UUID-5 namespace (see :func:`get_active_namespace`).
    Anyone in the same namespace who calls with the same nickname will
    derive the same gid -- that's what makes nicknames usable for
    cross-process entry resolution.

    Parameters
    ----------
    kwargs : dict
        Caller's kwargs dict; expects ``nickname`` (str) and optionally
        ``evolution`` (int).

    Returns
    -------
    list[str]
        A single-element list containing the derived global_id.

    Raises
    ------
    ValueError
        If ``kwargs["nickname"]`` is not a string.
    """
    if isinstance(kwargs["nickname"], str):
        args = [
            Entry.to_global_id(
                nickname=kwargs["nickname"],
                scopes=[_ENTRY_SCOPE],
                evolution=kwargs.get("evolution", None),
            )
        ]
        return args
    else:
        raise ValueError("nickname must be a string")


def remember(
    *args,
    persist: bool = True,
    src_policy=None,
    src_pool=None,
    dst_policy=None,
    dst_pool=None,
    comm=None,
    policy_id=None,
    pool_id=None,
    pool_nickname=None,
    **kwargs,
):
    """Retrieve one or more entries from a policy's memory.

    Thin top-level shim that forwards to
    :meth:`policy.central.memory.remember`. The work performed there:

    1. Resolve the source pool via the :class:`PoolRouter`.
    2. For each ``entry_id``, ``await`` the pool's ``_read_async`` and
       then route the raw blob through :meth:`Record._build_async`,
       which inverts the pool's :class:`TransformationSequence` and
       hydrates a fresh :class:`Entry`.
    3. Submit one async coroutine per entry to the alpha taskforce.

    The ``persist`` cache-back semantics
    -------------------------------------
    By default (``persist=True``), if the routed source pool is *not*
    the alpha pool, the fetched entries are additionally memorized into
    the alpha pool and the returned future only resolves once that
    write completes. This means:

    - ``with laila.guarantee:`` blocks until the alpha pool has the
      entries, so subsequent reads from the alpha pool are safe.
    - The next ``remember(...)`` for the same ids hits the alpha pool
      directly and skips the slower remote round-trip.

    Pass ``persist=False`` to opt out -- useful for one-shot reads
    where you don't want to grow the alpha pool.

    Identifying entries
    -------------------
    Either pass explicit ``entry_ids=...`` or use the convenience
    ``nickname=...`` form which derives the gid via
    :func:`Entry.to_global_id` against the active namespace. The
    ``nickname`` and ``evolution`` kwargs are consumed and removed
    before the call is forwarded.

    Parameters
    ----------
    entry_ids : str | list[str]
        The ``global_id``(s) of the entries to recall.
    pool_id : str, optional
        Explicit pool ``global_id`` to read from.
    pool_nickname : str, optional
        Pool alias registered via :meth:`Policy.extend`.
    nickname : str, optional
        Convenience alias -- converted to a deterministic ``global_id``
        via :func:`Entry.to_global_id` against the active namespace.
        Mutually exclusive with ``entry_ids`` for that slot.
    evolution : int, optional
        Optional evolution suffix to append to the nickname-derived gid.
    persist : bool, default True
        Cache-back into the alpha pool. See "persist semantics" above.
        When ``policy_id`` names a peer, the cache-back happens on the
        *peer's* alpha pool, not the local one -- pass ``persist=False``
        for a clean one-shot peer read.
    policy_id : str, optional
        ``global_id`` of a *peer* (or another local policy) to read
        from. When supplied, the entries are fetched from that policy's
        memory rather than the active policy's. See :func:`_route_to_policy`.

    Returns
    -------
    Future or GroupFuture
        Future-like handle resolving to the rebuilt :class:`Entry` (or
        list of entries for multiple ids). A :class:`RemoteFuture` when
        ``policy_id`` names a peer.
    """
    if "nickname" in kwargs:
        args = ()
        kwargs["entry_ids"] = __resolve_nickname(kwargs)
        del kwargs["nickname"]
        kwargs.pop("evolution", None)

    # Accept the leading positional via the ``entry_ids=`` keyword too.
    if not args and "entry_ids" in kwargs:
        args = (kwargs.pop("entry_ids"),)

    # Back-compat: policy_id -> dst_policy, pool_id/pool_nickname -> dst_pool.
    if dst_policy is None:
        dst_policy = policy_id
    if dst_pool is None:
        dst_pool = pool_id if pool_id is not None else pool_nickname

    src_gid = _resolve_policy_ref(src_policy)
    dst_gid = _resolve_policy_ref(dst_policy)
    active_gid = get_active_policy().global_id

    # Source policy is another policy -> 3-party relay (B pulls from dst).
    if src_gid is not None and src_gid != active_gid:
        return _relay_transfer(
            "remember", src_gid, args, kwargs,
            src_pool=src_pool, dst_gid=dst_gid, dst_pool=dst_pool, comm=comm,
            persist=persist,
        )

    # Active policy pulls from a peer (2-party).
    if dst_gid is not None and dst_gid != active_gid:
        return _route_to_policy(
            dst_gid, "remember", args, {"persist": persist, "pool": dst_pool}, comm=comm
        )

    # Purely local / standalone read from the active policy's pool.
    return get_active_policy().central.memory.remember(
        *args, persist=persist, pool=dst_pool, **kwargs
    )


def forget(
    *args,
    policy=None,
    pool=None,
    comm=None,
    policy_id=None,
    pool_id=None,
    pool_nickname=None,
    **kwargs,
):
    """Delete one or more entries from the active policy's memory.

    Thin top-level shim that forwards to
    :meth:`policy.central.memory.forget`. For each gid, ``await``\\ s
    the routed pool's ``_delete_async`` (or the batch path on
    batch-accelerated pools) and yields a future per entry.

    Forgetting is *pool-local*: it only removes the blob from the
    routed pool, not from any other pool that may have a copy. To wipe
    an entry everywhere, iterate over ``laila.memory.pool_router.pools``
    explicitly.

    Identifying entries follows the same rules as :func:`remember`: pass
    either ``entry_ids`` or the convenience ``nickname`` (+ optional
    ``evolution``) form.

    Parameters
    ----------
    entry_ids : str | list[str]
        The ``global_id``(s) of the entries to delete.
    pool_id : str, optional
        Explicit pool ``global_id`` to delete from.
    pool_nickname : str, optional
        Pool alias registered via :meth:`Policy.extend`.
    nickname : str, optional
        Convenience alias -- converted to a deterministic ``global_id``
        via :func:`Entry.to_global_id`.
    evolution : int, optional
        Optional evolution suffix for the nickname form.

    Returns
    -------
    Future or GroupFuture
        Future-like handle resolving when the delete(s) finish.
    """
    if "nickname" in kwargs:
        args = ()
        kwargs["entry_ids"] = __resolve_nickname(kwargs)
        del kwargs["nickname"]
        kwargs.pop("evolution", None)

    # Accept the leading positional via the ``entry_ids=`` keyword too.
    if not args and "entry_ids" in kwargs:
        args = (kwargs.pop("entry_ids"),)

    # Back-compat: policy_id -> policy, pool_id/pool_nickname -> pool.
    if policy is None:
        policy = policy_id
    if pool is None:
        pool = pool_id if pool_id is not None else pool_nickname

    target_gid = _resolve_policy_ref(policy)
    active_gid = get_active_policy().global_id

    if target_gid is not None and target_gid != active_gid:
        return _route_to_policy(
            target_gid, "forget", args, {"pool": pool}, comm=comm
        )
    return get_active_policy().central.memory.forget(*args, pool=pool, **kwargs)


def add_peer(uri: str, secret: str) -> str:
    """Connect to a remote policy and register it as a peer of the active policy.

    Delegates to :meth:`Communication.add_peer`, which:

    1. Picks a transport protocol that can handle *uri* (currently the
       TCP/IP WebSocket protocol; matches ``ws://`` and ``wss://``).
    2. Performs the handshake -- exchanges ``peer_secret_key`` values
       and policy ``global_id``\\ s.
    3. Registers a :class:`RemotePolicyProxy` in
       ``laila.peers[<remote_gid>]`` and in :data:`_remote_policies`.

    Once registered, the proxy can be passed to
    :func:`activate_policy` ("morph mode") so subsequent local calls
    transparently RPC into the peer, or it can be used directly:

    .. code-block:: python

        gid = laila.add_peer("ws://host:9000", secret="...")
        peer = laila.peers[gid]
        peer.memorize(my_entry)  # explicit RPC, active policy unchanged

    Parameters
    ----------
    uri : str
        URI of the remote policy. Today the only supported scheme is
        WebSocket: ``"ws://host:port"`` or ``"wss://host:port"``.
    secret : str
        The remote policy's ``peer_secret_key`` (configured on the
        remote's communication protocol). Required for the handshake to
        succeed.

    Returns
    -------
    str
        The ``global_id`` of the newly peered remote policy. Use it to
        index ``laila.peers``.

    Raises
    ------
    ConnectionError
        If no registered protocol can handle *uri*, or if the underlying
        transport refuses to connect.
    PermissionError
        If the *secret* is rejected by the remote's protocol.
    """
    return get_active_policy().central.communication.add_peer(uri, secret)


def request(policy_id, comm_protocol=None):
    """Return a transport-bound proxy for a connected peer.

    The channel-aware companion to :func:`add_peer`. Given a peered
    policy's ``global_id`` it returns its :class:`RemotePolicyProxy`,
    optionally *bound* to a specific transport via *comm_protocol* so
    that every call (and every follow-up on the futures it yields)
    travels over that channel:

    .. code-block:: python

        # send this request over LoRa even if a TCP link also exists
        laila.request(gid, comm_protocol="lora").central.memory.remember(eid)

    Parameters
    ----------
    policy_id : str
        ``global_id`` of a peer registered via :func:`add_peer`.
    comm_protocol : str, optional
        A *communication id* -- a registered connection's ``global_id``
        or a protocol token (``"tcp"``, ``"lora"``, ``"ble"`` ...). When
        ``None`` the proxy uses the first transport holding the peer.

    Returns
    -------
    RemotePolicyProxy
        A proxy (optionally channel-bound) to the remote policy.

    Raises
    ------
    ConnectionError
        If *policy_id* is not a connected peer.
    """
    proxy = _remote_policies.get(str(policy_id))
    if proxy is None:
        raise ConnectionError(
            f"Unknown peer {str(policy_id)!r}: connect first with laila.add_peer()."
        )
    return proxy.via(comm_protocol) if comm_protocol is not None else proxy


def _resolve_future(future_ref):
    """Look up the actual future object from a heterogeneous reference.

    The various ``laila`` APIs accept many shapes that all *refer* to
    a future without necessarily *being* one. This helper normalizes
    them to a concrete future instance you can call ``.wait()`` /
    ``.status`` / ``.exception`` on.

    Accepted shapes
    ---------------
    - :class:`RemoteFuture` -- already a usable handle, returned as-is.
    - :class:`GroupFuture`  -- already a usable handle, returned as-is.
    - :class:`Future`       -- already a usable handle, returned as-is.
    - :class:`_LAILA_IDENTIFIABLE_FUTURE` (the lightweight identity
      shape returned by :meth:`command.submit`) -- looked up by gid in
      every local policy's ``future_bank`` and finally in the active
      policy's bank.
    - ``str`` -- treated as a future ``global_id`` and looked up the
      same way.

    The local-banks-first search lets future references created against
    a non-active local policy still resolve correctly when the active
    policy has been swapped out.

    Parameters
    ----------
    future_ref : RemoteFuture | Future | GroupFuture | _LAILA_IDENTIFIABLE_FUTURE | str
        Anything that uniquely identifies a future.

    Returns
    -------
    Future | GroupFuture | RemoteFuture
        The concrete future object.

    Raises
    ------
    KeyError
        If the gid is not found in any future bank.
    TypeError
        If *future_ref* is not one of the accepted types.
    """
    from .policy.central.command.schema.future.future.future import Future
    from .policy.central.command.schema.future.future.future_identity import (
        _LAILA_IDENTIFIABLE_FUTURE,
    )
    from .policy.central.command.schema.future.future.group_future import GroupFuture
    from .policy.central.command.schema.future.future.remote_future import RemoteFuture

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
    """Return the lifecycle status of *future_ref*.

    Backwards-compatible shim that forwards to :func:`laila.runtime.status`.
    See that function for the full type-shape contract of *future_ref*.

    .. deprecated::
        Use :func:`laila.runtime.status` directly. This top-level form
        exists only so older notebooks and tutorials keep working.
    """
    from . import runtime

    return runtime.status(future_ref)


def wait(future_ref, timeout=None):
    """Block until *future_ref* completes and return its result.

    Backwards-compatible shim that forwards to :func:`laila.runtime.wait`.

    Parameters
    ----------
    future_ref : Any
        See :func:`_resolve_future` for accepted shapes.
    timeout : float, optional
        Maximum seconds to block. ``None`` waits indefinitely.

    .. deprecated::
        Use :func:`laila.runtime.wait` directly.
    """
    from . import runtime

    return runtime.wait(future_ref, timeout)


def set_default_directory(directory):
    """Relocate every on-disk sub-directory laila uses by default.

    Mutates :data:`laila.macros.defaults.LAILA_DEFAULT_DIRECTORIES` in
    place. Pools created *after* this call resolve their backing
    directories from the new root; pools that have already opened
    files keep their existing paths (they captured the old root in
    ``model_post_init``).

    The four conventional subdirectories are derived from the new root:

    - ``root``    -- the directory itself
    - ``pools``   -- per-pool data folders (``pools/<pool_uuid>/``)
    - ``logs``    -- log files written by :class:`Logger`
    - ``secrets`` -- key material loaded by the ``crypto`` extras
    - ``indices`` -- non-memorizing query helpers (e.g. Manifest SQL
      indices), per-manifest folders (``indices/<manifest_uuid>/``)

    ``~`` and ``~user`` are expanded via :func:`os.path.expanduser`.

    Parameters
    ----------
    directory : str
        Filesystem path (may contain ``~``) to use as the new root.

    Notes
    -----
    Call this *before* instantiating any pool that you want rooted at
    the new location -- typically at the very top of your script,
    immediately after ``import laila``.
    """
    directory = os.path.expanduser(directory)
    LAILA_DEFAULT_DIRECTORIES.update(
        {
            "root": directory,
            "pools": os.path.join(directory, "pools"),
            "logs": os.path.join(directory, "logs"),
            "secrets": os.path.join(directory, "secrets"),
            "indices": os.path.join(directory, "indices"),
        }
    )

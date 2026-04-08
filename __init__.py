"""Laila top-level package.

Provides the public API for policy management, subsystem access
(``laila.memory``, ``laila.command``, ``laila.communication``),
future lifecycle helpers, and argument loading.
"""

import sys
import types
import uuid
import os
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


args = DotMap()

arg_reader = ArgReader(target=args)

_local_policies = {}
_remote_policies = {}


def read_args(source, *, terminal_args=None) -> None:
    """Load user arguments from a TOML/JSON/.env/.xml file or ``terminal`` into ``laila.args``.

    Mutates ``laila.args`` in place. Always returns ``None``.
    """
    arg_reader.load(source, terminal_args=terminal_args)

_active_policy = None
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
    """Return the active policy, lazily creating a ``DefaultPolicy`` on first access."""
    global _active_policy
    if _active_policy is None:
        from .macros.defaults import DefaultPolicy
        _active_policy = DefaultPolicy()
        _local_policies[_active_policy.global_id] = _active_policy
    return _active_policy
    
def activate_policy(policy):
    """Replace the active policy singleton.

    Accepts a local ``_LAILA_IDENTIFIABLE_POLICY`` or a
    ``RemotePolicyProxy`` obtained from ``laila.peers``.

    Equivalent to ``laila.active_policy = policy``.

    Parameters
    ----------
    policy : _LAILA_IDENTIFIABLE_POLICY | RemotePolicyProxy
        The policy instance (local or remote proxy) to activate globally.
    """
    global _active_policy
    _active_policy = policy


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
    def environment(self):
        from .basics.definitions.cli_capable import build_environment
        return build_environment(get_active_policy())

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
        """Union of local and remote policies, keyed by ``global_id``."""
        return {**_local_policies, **_remote_policies}

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


sys.modules[__name__].__class__ = _LailaModule


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

def remember(*args, **kwargs):
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
    """
    if "nickname" in kwargs:
        args = []
        kwargs["entry_ids"] = __resolve_nickname(kwargs)
        del kwargs["nickname"]
        if "evolution" in kwargs:
            del kwargs["evolution"]
    return get_active_policy().central.memory.remember(*args, **kwargs)

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
    })
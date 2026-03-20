import uuid
import os
from dotmap import DotMap

from .entry import Entry
from . import entry
from . import policy
from .macros.aliases import *
from .policy.schema.base import _LAILA_IDENTIFIABLE_POLICY
from .policy.central.command import taskforce as TaskForce

from .macros.defaults import *
from .macros.strings import _ENTRY_SCOPE

from .utils.args import ArgReader
from .utils import guarantee, guarantee_async


args = DotMap()

arg_reader = ArgReader(target=args)


def read_args(source, *, terminal_args=None) -> None:
    """Load user arguments from a TOML/JSON/.env/.xml file or ``terminal`` into ``laila.args``.

    Mutates ``laila.args`` in place. Always returns ``None``.
    """
    arg_reader.load(source, terminal_args=terminal_args)

_active_policy = None
_active_namespace = None

def get_active_namespace():
    global _active_namespace
    if _active_namespace is None:
        from .macros.defaults import LAILA_UNIVERSAL_NAMESPACE
        _active_namespace = LAILA_UNIVERSAL_NAMESPACE
    return _active_namespace

def set_active_namespace(namespace_key: str):
    global _active_namespace
    _active_namespace = uuid.uuid5(uuid.NAMESPACE_DNS, namespace_key)

def get_active_policy():
    global _active_policy
    if _active_policy is None:
        from .macros.defaults import DefaultPolicy
        _active_policy = DefaultPolicy()
    return _active_policy
    
def __getattr__(name):
    if name == "active_policy":
        return get_active_policy()
    raise AttributeError(name)

def activate_policy(policy: _LAILA_IDENTIFIABLE_POLICY):
    global _active_policy
    _active_policy = policy


def add_pool(*args, **kwargs):
    """Register a storage pool with the active policy's memory system.

    Parameters
    ----------
    pool : _LAILA_IDENTIFIABLE_POOL
        The pool instance to register (e.g. ``S3Pool``, ``HDF5Pool``).
    affinity : float, optional
        Routing priority. Higher values mean the pool is preferred.
    pool_nickname : str, optional
        A human-readable alias used to reference this pool later.
    """
    return get_active_policy().central.memory.add_pool(*args, **kwargs)

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
        Pool alias registered via ``add_pool``.
    nickname : str, optional
        Convenience alias – converted to a deterministic ``global_id``.
    """
    return get_active_policy().central.memory.memorize(*args, **kwargs)

def __resolve_nickname(kwargs):
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
        Pool alias registered via ``add_pool``.
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
        Pool alias registered via ``add_pool``.
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

def set_default_directory(directory):
    directory = os.path.expanduser(directory)
    LAILA_DEFAULT_DIRECTORIES.update({
        "root": directory,
        "pools": os.path.join(directory, "pools"),
        "logs": os.path.join(directory, "logs"),
    })
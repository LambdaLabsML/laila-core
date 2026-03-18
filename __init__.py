
from .entry import Entry
from . import entry
from . import policy
from .macros.aliases import *
from .policy.schema.base import _LAILA_IDENTIFIABLE_POLICY
from .policy.central.command import taskforce as TaskForce

from .macros.defaults import *
from .macros.strings import _ENTRY_SCOPE

from dotmap import DotMap
from .utils.args import ArgsReader
from .utils import guarantee, guarantee_async

args = DotMap()

args_reader = ArgsReader(target=args)


def read_args(source, *, terminal_args=None):
    return args_reader.load(source, terminal_args=terminal_args)

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
    return get_active_policy().central.memory.add_pool(*args, **kwargs)

def memorize(*args, **kwargs):
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
    if "nickname" in kwargs:
        args = []
        kwargs["entry_ids"] = __resolve_nickname(kwargs)
        del kwargs["nickname"]
        if "evolution" in kwargs:
            del kwargs["evolution"]
    return get_active_policy().central.memory.remember(*args, **kwargs)

def forget(*args, **kwargs):
    if "nickname" in kwargs:
        args = []
        kwargs["entry_ids"] = __resolve_nickname(kwargs)
        del kwargs["nickname"]
        if "evolution" in kwargs:
            del kwargs["evolution"]
    return get_active_policy().central.memory.forget(*args, **kwargs)

 
"""Default implementations and compile-time constants for laila.

Single source of truth for the "what does laila ship with by
default?" question. The aliases at the top of the module name the
*concrete* class that backs each abstract role:

==========================  ==================================================
Default name                Concrete class
==========================  ==================================================
DefaultTaskForce            :class:`PythonAsyncThreadPoolTaskForce`
DefaultCentralCommand       :class:`_LAILA_IDENTIFIABLE_CENTRAL_COMMAND`
DefaultCentralCommunication :class:`_LAILA_IDENTIFIABLE_COMMUNICATION`
DefaultTCPIPProtocol        :class:`_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL`
DefaultLoRaProtocol         :class:`_LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL` (scaffold)
DefaultBluetoothProtocol    :class:`_LAILA_IDENTIFIABLE_BLUETOOTH_COMM_PROTOCOL` (scaffold)
DefaultCentralMemory        :class:`_LAILA_IDENTIFIABLE_CENTRAL_MEMORY`
DefaultPolicy               :class:`_LAILA_IDENTIFIABLE_POLICY`
DefaultPool                 :class:`_LAILA_IDENTIFIABLE_POOL`
DefaultPoolRouter           :class:`_LAILA_IDENTIFIABLE_POOL_ROUTER`
==========================  ==================================================

Other constants:

- :data:`AUTO_INITIALIZE_POLICY` -- whether ``import laila`` should
  spin up a default policy automatically. Toggling this off is
  useful for unit-test rigs that want full control over policy
  lifecycle.
- :data:`LAILA_UNIVERSAL_NAMESPACE` -- the UUID-5 namespace used as
  the *root* of laila's deterministic-id tree. Touch only if you
  know exactly what you're doing -- changing it invalidates every
  nickname-derived id ever produced.
- :data:`LAILA_DEFAULT_DIRECTORIES` -- the on-disk layout under
  ``~/.laila``: per-pool storage under ``pools/``, log files under
  ``logs/``, key material under ``secrets/``, and non-memorizing
  query helpers (e.g. Manifest SQL indices) under ``indices/``. Each
  directory is created at import time if missing.
"""

import os
import uuid

from ..policy.central.command.schema.base import _LAILA_IDENTIFIABLE_CENTRAL_COMMAND
from ..policy.central.command.taskforce.async_thread_pool_executor import (
    PythonAsyncThreadPoolTaskForce,
)
from ..policy.central.communication.protocols.bluetooth import (
    _LAILA_IDENTIFIABLE_BLUETOOTH_COMM_PROTOCOL,
)
from ..policy.central.communication.protocols.lora import _LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL
from ..policy.central.communication.protocols.tcpip import _LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL
from ..policy.central.communication.schema.base import _LAILA_IDENTIFIABLE_COMMUNICATION
from ..policy.central.memory.router.pool_router import _LAILA_IDENTIFIABLE_POOL_ROUTER
from ..policy.central.memory.schema.base import _LAILA_IDENTIFIABLE_CENTRAL_MEMORY
from ..policy.schema.base import _LAILA_IDENTIFIABLE_POLICY
from ..pool.schema.base import _LAILA_IDENTIFIABLE_POOL

DefaultTaskForce = PythonAsyncThreadPoolTaskForce
DefaultCentralCommand = _LAILA_IDENTIFIABLE_CENTRAL_COMMAND
DefaultCentralCommunication = _LAILA_IDENTIFIABLE_COMMUNICATION
DefaultTCPIPProtocol = _LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL
DefaultLoRaProtocol = _LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL
DefaultBluetoothProtocol = _LAILA_IDENTIFIABLE_BLUETOOTH_COMM_PROTOCOL
DefaultCentralMemory = _LAILA_IDENTIFIABLE_CENTRAL_MEMORY
DefaultPolicy = _LAILA_IDENTIFIABLE_POLICY
DefaultPool = _LAILA_IDENTIFIABLE_POOL
DefaultPoolRouter = _LAILA_IDENTIFIABLE_POOL_ROUTER


AUTO_INITIALIZE_POLICY = True

# ============================================================
# DO NOT CHANGE THIS VALUE UNLESS YOU KNOW WHAT YOU ARE DOING
LAILA_UNIVERSAL_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_DNS, "laila")
# ============================================================

_DEFAULT_ROOT = os.path.expanduser("~/.laila")

LAILA_DEFAULT_DIRECTORIES = {
    "root": _DEFAULT_ROOT,
    "pools": os.path.join(_DEFAULT_ROOT, "pools"),
    "logs": os.path.join(_DEFAULT_ROOT, "logs"),
    "secrets": os.path.join(_DEFAULT_ROOT, "secrets"),
    "indices": os.path.join(_DEFAULT_ROOT, "indices"),
}

for _dir in LAILA_DEFAULT_DIRECTORIES.values():
    os.makedirs(_dir, exist_ok=True)

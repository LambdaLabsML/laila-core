from ..policy.central.memory.schema.base import _LAILA_IDENTIFIABLE_CENTRAL_MEMORY
from ..policy.central.command.taskforce.thread_pool_executor import PythonThreadPoolTaskForce
from ..policy.central.command.schema.base import _LAILA_IDENTIFIABLE_CENTRAL_COMMAND
from ..policy.schema.base import _LAILA_IDENTIFIABLE_POLICY
from ..pool.schema.base import _LAILA_IDENTIFIABLE_POOL
from ..policy.central.memory.router.pool_router import _LAILA_IDENTIFIABLE_POOL_ROUTER
import uuid

DefaultTaskForce = PythonThreadPoolTaskForce
DefaultCentralCommand = _LAILA_IDENTIFIABLE_CENTRAL_COMMAND
DefaultCentralMemory = _LAILA_IDENTIFIABLE_CENTRAL_MEMORY
DefaultPolicy = _LAILA_IDENTIFIABLE_POLICY
DefaultPool = _LAILA_IDENTIFIABLE_POOL
DefaultPoolRouter = _LAILA_IDENTIFIABLE_POOL_ROUTER


AUTO_INITIALIZE_POLICY = True

#============================================================
#DO NOT CHANGE THIS VALUE UNLESS YOU KNOW WHAT YOU ARE DOING
LAILA_UNIVERSAL_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_DNS, "laila")
#============================================================


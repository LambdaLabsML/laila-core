"""Taskforce sub-package — async-thread-pool and process-pool backends.

The async-thread-pool taskforce is the canonical general-purpose backend;
sync callables submitted to it are auto-wrapped at submission time and
run inline on whichever loop thread the dispatcher picks. The legacy
``PythonThreadPoolTaskForce`` has been removed in favor of the unified
async backend.
"""

from .async_thread_pool_executor import PythonAsyncThreadPoolTaskForce
from .process_pool_executor import PythonProcessPoolTaskForce
from .thread_pool_executor.future import ConcurrentPackageFuture

from __future__ import annotations

from ..thread_pool_executor.future import ConcurrentPackageFuture


class ProcessPackageFuture(ConcurrentPackageFuture):
    """Wrapper around a `ProcessPoolExecutor` native future."""

    pass

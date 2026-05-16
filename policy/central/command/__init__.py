"""Command sub-package -- task-forces, futures, and submission machinery.

Layout:

- :mod:`schema <laila.policy.central.command.schema>` -- the central
  command class itself, the future hierarchy (``Future``,
  ``GroupFuture``, ``RemoteFuture``, ``ComplexFuture``), and the
  shared :class:`FutureStatus` enum.
- :mod:`taskforce <laila.policy.central.command.taskforce>` -- the
  abstract :class:`_LAILA_IDENTIFIABLE_TASK_FORCE` base plus three
  concrete backends:

  - :mod:`async_thread_pool_executor` -- the default. A pool of
    daemon threads each running its own asyncio event loop;
    coroutines are routed to a loop and awaited there. Best for
    mixed sync/async I/O workloads.
  - :mod:`thread_pool_executor` -- classic :class:`ThreadPoolExecutor`
    semantics for legacy sync-only workloads.
  - :mod:`process_pool_executor` -- isolated subprocess workers;
    suitable for CPU-bound jobs that benefit from skipping the GIL,
    at the cost of pickling each task across the process boundary.
"""

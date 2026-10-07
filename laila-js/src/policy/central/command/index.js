/**
 * Command sub-package -- task-forces, futures, and submission machinery.
 *
 * Layout:
 *
 * - ``schema`` -- the central command class itself, the future hierarchy
 *   (``Future``, ``GroupFuture``, ``RemoteFuture``, ``ComplexFuture``), and
 *   the shared ``FutureStatus`` enum.
 * - ``taskforce`` -- the abstract ``_LAILA_IDENTIFIABLE_TASK_FORCE`` base
 *   plus the concrete backends:
 *
 *   - ``async_thread_pool_executor`` -- the default. A pool of daemon threads
 *     each running its own asyncio event loop; coroutines are routed to a
 *     loop and awaited there. Best for mixed sync/async I/O workloads.
 *   - ``thread_pool_executor`` -- the ``ConcurrentPackageFuture`` wrapper
 *     shared by the backends (the legacy thread-pool taskforce was removed).
 *   - ``process_pool_executor`` -- isolated subprocess workers; suitable for
 *     CPU-bound jobs, at the cost of pickling each task across the process
 *     boundary.
 */

export * from "./schema/index.js";
export * from "./taskforce/index.js";
export { _LAILA_IDENTIFIABLE_TASK_FORCE, _live_taskforces_snapshot } from "./taskforce/base.js";
export { TaskForceStatus } from "./taskforce/status.js";
export { ProcessPackageFuture } from "./taskforce/process_pool_executor/future.js";

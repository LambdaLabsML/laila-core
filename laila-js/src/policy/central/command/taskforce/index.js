/**
 * Taskforce sub-package -- async-thread-pool and process-pool backends.
 *
 * The async-thread-pool taskforce is the canonical general-purpose backend;
 * sync callables submitted to it are auto-wrapped at submission time and
 * offloaded to the taskforce's sync executor so they never block a loop
 * thread. Tasks that wait on other laila futures *park* their slot for the
 * duration (see ``laila.policy.central.command.schema.parking``), which makes
 * nested submissions deadlock-free. The legacy ``PythonThreadPoolTaskForce``
 * has been removed in favor of the unified async backend.
 */

export { PythonAsyncThreadPoolTaskForce } from "./async_thread_pool_executor/index.js";
export { PythonProcessPoolTaskForce } from "./process_pool_executor/index.js";
export { ConcurrentPackageFuture } from "./thread_pool_executor/future.js";

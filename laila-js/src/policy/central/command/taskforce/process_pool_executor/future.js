/** Concrete future wrapping ``concurrent.futures.Future`` for process-pool execution. */
import { register } from "../../../../../_compat/lazy.js";
import { finalize_model } from "../../../../../_compat/pydantic.js";
import { ConcurrentPackageFuture } from "../thread_pool_executor/future.js";

/** Wrapper around a ``ProcessPoolExecutor`` native future. */
export class ProcessPackageFuture extends ConcurrentPackageFuture {
  static {
    finalize_model(this);
  }
}

register("laila.policy.central.command.taskforce.process_pool_executor.future", { ProcessPackageFuture });

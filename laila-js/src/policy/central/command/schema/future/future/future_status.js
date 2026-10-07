/**
 * Enumeration of future lifecycle status codes.
 *
 * These string values are the canonical wire representation -- they are
 * sent verbatim through RPC frames (see
 * ``_LAILA_IDENTIFIABLE_POLICY._get_future_status``) and rendered in log
 * records, so renaming a member is a breaking change for any peer or log
 * consumer that hard-codes the value.
 */
import { Enum } from "../../../../../../_compat/enum.js";

/**
 * Lifecycle states for every laila future.
 *
 * - ``UNKNOWN``: status could not be determined (typically a transient state
 *   for newly-created ``RemoteFuture`` handles before the first poll).
 * - ``NOT_STARTED``: constructed and registered, task not yet executing.
 * - ``RUNNING``: the underlying task is actively executing.
 * - ``POLL_TIMEOUT``: a status poll on a remote/concurrent backing primitive
 *   timed out without a definitive answer.
 * - ``FINISHED``: completed successfully; ``result`` is set.
 * - ``ERROR``: the task raised; ``exception`` is set.
 * - ``CANCELLED``: cancelled before completion.
 *
 * Members are ``str`` (``JSON.stringify(FutureStatus.RUNNING)`` -> ``"running"``).
 */
export const FutureStatus = Enum("FutureStatus", {
  UNKNOWN: "unknown",
  NOT_STARTED: "not_started",
  RUNNING: "running",
  POLL_TIMEOUT: "poll_timeout",
  FINISHED: "finished",
  ERROR: "error",
  CANCELLED: "cancelled",
});

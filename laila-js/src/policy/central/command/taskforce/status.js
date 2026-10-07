/**
 * Lifecycle status codes for task-forces.
 *
 * Transitions::
 *
 *     NOT_STARTED --start()--> RUNNING --pause()--> PAUSED --start()--> RUNNING
 *                                  \--shutdown()--> STOPPED
 *                                  \--(backend failure)--> CRASHED
 *
 * - ``NOT_STARTED``: constructed, backend resources not yet allocated.
 * - ``RUNNING``: actively dispatching submitted tasks.
 * - ``PAUSED``: quiesced via ``pause()``; resources still allocated but no
 *   new work is dispatched. Back to ``RUNNING`` via ``start()``.
 * - ``STOPPED``: terminal; resources released by ``shutdown()``.
 * - ``CRASHED``: terminal; an unrecoverable backend error was observed.
 */
import { Enum } from "../../../../_compat/enum.js";

export const TaskForceStatus = Enum("TaskForceStatus", {
  NOT_STARTED: "not_started",
  RUNNING: "running",
  PAUSED: "paused",
  STOPPED: "stopped",
  CRASHED: "crashed",
});

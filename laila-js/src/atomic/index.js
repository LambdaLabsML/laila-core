/**
 * Thread-safe atomic data types and base definitions.
 *
 * Two layers live here:
 *
 * - ``definitions`` -- mixin base classes that wrap any object with a
 *   reentrant lock (``_LAILA_LOCALLY_ATOMIC_OBJECT``,
 *   ``_LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT``,
 *   ``_LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT``). All identifiable objects
 *   in laila that need critical sections inherit from one of these and gain a
 *   uniform ``with_(self.atomic(), () => ...)`` context manager.
 * - ``types`` -- thread-safe wrappers around common data structures:
 *
 *   ============  =================================================
 *   Type          Wrapper for
 *   ============  =================================================
 *   AtomicDict    ``dict``
 *   AtomicList    ``list``
 *   AtomicStr     ``str``
 *   AtomicInt     ``int``
 *   AtomicFlag    boolean (single bit, atomically toggled)
 *   AtomicDotMap  ``DotMap``
 *   ============  =================================================
 *
 *   Each wrapper exposes the same surface as the underlying type but guards
 *   every mutation with an internal ``RLock``, so they are safe to share
 *   between worker threads.
 */

export { AtomicDict } from "./types/atomic_dict.js";
export { AtomicDotMap } from "./types/atomic_dotmap.js";
export { AtomicFlag } from "./types/atomic_flag.js";
export { AtomicInt } from "./types/atomic_int.js";
export { AtomicList } from "./types/atomic_list.js";
export { AtomicStr } from "./types/atomic_str.js";

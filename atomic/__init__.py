"""Thread-safe atomic data types and base definitions.

Two layers live here:

- :mod:`.definitions` -- mixin base classes that wrap any object with a
  reentrant lock (:class:`_LAILA_LOCALLY_ATOMIC_OBJECT`,
  :class:`_LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT`,
  :class:`_LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT`). All
  identifiable objects in laila that need critical sections inherit
  from one of these and gain a uniform ``with self.atomic(): ...``
  context manager.
- :mod:`.types` -- thread-safe wrappers around common Python data
  structures:

  ============  =================================================
  Type          Wrapper for
  ============  =================================================
  AtomicDict    ``dict``
  AtomicList    ``list``
  AtomicStr     ``str``
  AtomicInt     ``int``
  AtomicFlag    boolean (single bit, atomically toggled)
  AtomicDotMap  :class:`dotmap.DotMap`
  ============  =================================================

  Each wrapper exposes the same surface as the underlying type but
  guards every mutation with an internal :class:`threading.RLock`,
  so they are safe to share between worker threads.
"""

from .types.atomic_dict import AtomicDict
from .types.atomic_flag import AtomicFlag
from .types.atomic_str import AtomicStr
from .types.atomic_int import AtomicInt
from .types.atomic_list import AtomicList
from .types.atomic_dotmap import AtomicDotMap

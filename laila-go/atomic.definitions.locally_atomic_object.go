package laila

import (
	"strconv"
	"sync/atomic"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// atomic/definitions/locally_atomic_object.py -- locally-atomic base class
// providing reentrant per-instance locking.
//
// "Locally" means one lock per instance, in the current process. The lock
// is a re-entrant py.RLock so the same goroutine can nest critical
// sections (``with self.atomic(): self.helper()`` where helper also takes
// ``with self.atomic()``) without dead-locking.
//
// Port notes
//
//   - The locking surface lives on Locally_atomic_mixin so that the diamond
//     ``_LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT(LAO, IDO)`` carries a single
//     copy of the shared LAILA_OBJECT_ state (Go embedding has no MRO).
//   - ``with obj.atomic() as locked:`` becomes
//     ``locked, release, err := obj.Atomic(); defer release()``.
//   - ``lock(timeout_s=None)`` is ``Lock()``; ``lock(timeout_s=1.5)`` is
//     ``Lock(1.5)``.
//   - __deepcopy__ / __getstate__ / __setstate__: the port's py.Deepcopy and
//     pickling never copy a lock -- every *py.Lock / *py.RLock reachable
//     from the instance is replaced by a fresh one of the same type, which
//     is exactly what _LOCK_FACTORIES does in Python.

// Locally_atomic_mixin holds the lazily-created per-instance RLock
// (``_local_lock``) and the ``lock`` / ``unlock`` / ``locked`` helpers.
type Locally_atomic_mixin struct {
	// Local_lock_ is ``_local_lock``: created on first access, stored
	// outside pydantic's field machinery (object.__setattr__).
	Local_lock_ atomic.Pointer[py.RLock] `py:"-"`
}

// Ensure_local_lock_ returns the instance's RLock, creating it lazily.
func (m *Locally_atomic_mixin) Ensure_local_lock_() *py.RLock {
	if lock := m.Local_lock_.Load(); lock != nil {
		return lock
	}
	fresh := py.NewRLock()
	if m.Local_lock_.CompareAndSwap(nil, fresh) {
		return fresh
	}
	return m.Local_lock_.Load()
}

// Atomic_ is the shared body of ``atomic(*, scope="local", timeout_s=None)``:
// holds the local lock and yields self. Concrete classes expose it as
// Atomic(kw...) so the yielded value is the outermost object.
//
// ValueError if scope is not "local"; TimeoutError if the lock cannot be
// acquired within timeout_s.
func (m *Locally_atomic_mixin) Atomic_(self any, kw py.Kw) (any, func(), error) {
	scope := py.Str(kw.Get("scope", "local"))
	if scope != "local" {
		return nil, nil, py.ValueError.New("Invalid scope for _LAILA_LOCALLY_ATOMIC_OBJECT.atomic() call.")
	}
	if !m.lock_opt_(kw.Get("timeout_s", nil)) {
		return nil, nil, py.TimeoutError.New("Timed out acquiring local lock.")
	}
	return self, m.Unlock, nil
}

// lock_opt_ is Lock with a Python-style ``timeout_s`` value (None or number).
func (m *Locally_atomic_mixin) lock_opt_(timeout_s any) bool {
	if t, ok := Optional_float_(timeout_s); ok {
		return m.Lock(t)
	}
	return m.Lock()
}

// Lock acquires the per-instance reentrant lock. With no argument it
// blocks indefinitely (``timeout_s=None``); otherwise it waits at most
// ``timeout_s`` seconds. Returns false if the timeout elapsed first.
func (m *Locally_atomic_mixin) Lock(timeout_s ...float64) bool {
	local_lock := m.Ensure_local_lock_()
	if len(timeout_s) == 0 {
		local_lock.Lock_()
		return true
	}
	return local_lock.Acquire(true, timeout_s[0])
}

// Unlock releases the per-instance reentrant lock once. A goroutine that
// called Lock N times must call Unlock N times.
func (m *Locally_atomic_mixin) Unlock() { _ = m.Ensure_local_lock_().Release() }

// Locked returns true if the lock is currently held by some goroutine.
func (m *Locally_atomic_mixin) Locked() bool { return Rlock_locked_(m.Ensure_local_lock_()) }

// Rlock_locked_ is the portable ``RLock.locked()`` (``_rlock_locked``).
//
// ``threading.RLock.locked`` only exists on Python >= 3.14; the Python
// helper falls back to ``_is_owned`` plus a non-blocking acquire probe on
// older interpreters. The port's py.RLock always answers the question
// directly, so the probe is never needed here.
func Rlock_locked_(lock *py.RLock) bool { return lock.Locked() }

// LAILA_LOCALLY_ATOMIC_OBJECT_ is the pydantic base model giving subclasses
// thread-safe per-instance locking. Derives from LAILA_OBJECT_, so every
// atomic object also carries a creation timestamp.
type LAILA_LOCALLY_ATOMIC_OBJECT_ struct {
	LAILA_OBJECT_
	Locally_atomic_mixin
}

// LAILA_LOCALLY_ATOMIC_OBJECT_class is the class descriptor of
// _LAILA_LOCALLY_ATOMIC_OBJECT.
var LAILA_LOCALLY_ATOMIC_OBJECT_class = py.NewClass("_LAILA_LOCALLY_ATOMIC_OBJECT", (*LAILA_LOCALLY_ATOMIC_OBJECT_)(nil), LAILA_OBJECT_class)

// New_LAILA_LOCALLY_ATOMIC_OBJECT_ is ``_LAILA_LOCALLY_ATOMIC_OBJECT(**data)``.
func New_LAILA_LOCALLY_ATOMIC_OBJECT_(kw ...py.Kw) (*LAILA_LOCALLY_ATOMIC_OBJECT_, error) {
	o := LAILA_LOCALLY_ATOMIC_OBJECT_class.New().(*LAILA_LOCALLY_ATOMIC_OBJECT_)
	if err := o.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return o, nil
}

// Init_ forwards all keyword arguments to pydantic; the lock itself is
// created lazily on first access via Ensure_local_lock_.
func (o *LAILA_LOCALLY_ATOMIC_OBJECT_) Init_(data py.Kw) error {
	return o.LAILA_OBJECT_.Init_(data)
}

// Model_post_init is the cooperative post-init hook (keeps the chain
// intact in mixin diamonds).
func (o *LAILA_LOCALLY_ATOMIC_OBJECT_) Model_post_init(ctx any) error {
	return o.LAILA_OBJECT_.Model_post_init(ctx)
}

// Atomic holds the local lock for the duration of the block
// (``with obj.atomic(scope="local", timeout_s=None) as locked``). Returns
// the locked instance and the release function to defer.
func (o *LAILA_LOCALLY_ATOMIC_OBJECT_) Atomic(kw ...py.Kw) (any, func(), error) {
	return o.Atomic_(o.Self(), py.MergeKw(kw...))
}

// Optional_float_ interprets a Python ``float | None`` argument: (value,
// true) for numbers (and numeric strings), (0, false) for None.
func Optional_float_(v any) (float64, bool) {
	switch x := v.(type) {
	case nil:
		return 0, false
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case int32:
		return float64(x), true
	case uint:
		return float64(x), true
	case uint64:
		return float64(x), true
	case string:
		if f, err := strconv.ParseFloat(x, 64); err == nil {
			return f, true
		}
	}
	return 0, false
}

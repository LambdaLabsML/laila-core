package laila

import "github.com/amirzadehlambda/laila/laila-go/py"

// policy/central/memory/hint/hint.py -- hint model used to guide memory
// routing decisions.
//
// A MemoryHint is a soft, declarative knob the caller can pass into a
// memory operation to bias the PoolRouter's pool selection without
// hard-coding a specific ``pool_id``. The router is free to ignore any
// field it doesn't understand -- hints are advisory, never authoritative.
// (For an authoritative selection, pass ``pool_id`` or ``pool_nickname``
// directly to ``memorize`` / ``remember`` / ``forget``.)

// MemoryHint holds routing hints that bias which pools are searched or
// written to.
//
// All fields are optional. Each is a soft preference; the PoolRouter may
// honor or ignore them based on the routing strategy currently in effect.
type MemoryHint struct {
	py.Object

	// Pools: which pools to search first.
	Pools *py.Set `py:"pools"`
	// Keywords: keywords that help find the entry in memory.
	Keywords *py.Set `py:"keywords"`
	// Semantics: vector semantics that help find the entry in memory.
	Semantics any `py:"semantics"`
	// Purpose: if certain pools are uniquely used for a special purpose.
	Purpose *string `py:"purpose"`
	// Affinity: closeness to local/nearby memory caches (0-1).
	Affinity *float64 `py:"affinity,ge=0.0,le=1.0"`
}

// MemoryHint_class is the class descriptor.
var MemoryHint_class = py.NewClass("MemoryHint", (*MemoryHint)(nil))

// New_MemoryHint is ``MemoryHint(**data)``.
func New_MemoryHint(kw ...py.Kw) (*MemoryHint, error) {
	h := MemoryHint_class.New().(*MemoryHint)
	if err := py.Init(h, MemoryHint_class, py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return h, nil
}

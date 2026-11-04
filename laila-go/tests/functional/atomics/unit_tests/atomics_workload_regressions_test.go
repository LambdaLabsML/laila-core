// Port of tests/functional/atomics/unit_tests/test_atomics_workload_regressions.py.
//
//   - F-25: the global-lock mixin gates *public subclass callables* only
//     while globally locked, raises PermissionError on a failed secret
//     check, and reference-counts nested holds.
//   - F-26 / F-28: AtomicDict ordering is read from ``data`` (the
//     insertion-ordered dict), keyed deletes are O(1), and out-of-band
//     edits to ``data`` are visible without Reindex().
//   - F-27: AtomicStr.Append builds a long string in amortised linear
//     time, not quadratic.
package unit_tests

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// Counter is the test's ``_Counter`` subclass of
// _LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT.
type Counter struct {
	laila.LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT_
	N int `py:"n" default:"0"`
}

var Counter_class = py.NewClass("_Counter", (*Counter)(nil), laila.LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT_class)

func New_Counter(kw ...py.Kw) (*Counter, error) {
	c := Counter_class.New().(*Counter)
	if err := c.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return c, nil
}

// Bump is ``bump(self, *, secret=None)``: a gated public subclass method.
func (c *Counter) Bump(kw ...py.Kw) (int, error) {
	if err := c.Gate_("bump", py.MergeKw(kw...)); err != nil {
		return 0, err
	}
	c.N += 1
	return c.N, nil
}

// Peek is ``peek(self)``: gated, takes no secret kwarg.
func (c *Counter) Peek(kw ...py.Kw) (int, error) {
	if err := c.Gate_("peek", py.MergeKw(kw...)); err != nil {
		return 0, err
	}
	return c.N, nil
}

// Internal_ is ``_internal(self)``: private helpers are never gated.
func (c *Counter) Internal_() int { return c.N }

func newCounter(t *testing.T) *Counter {
	t.Helper()
	c, err := New_Counter()
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func mustBump(t *testing.T, c *Counter, kw ...py.Kw) int {
	t.Helper()
	n, err := c.Bump(kw...)
	if err != nil {
		t.Fatalf("bump: %v", err)
	}
	return n
}

func TestGlobalLockGate(t *testing.T) {
	t.Run("unlocked_calls_pass_without_secret", func(t *testing.T) {
		c := newCounter(t)
		if mustBump(t, c) != 1 {
			t.Error("bump")
		}
		if n, err := c.Peek(); err != nil || n != 1 {
			t.Error("peek")
		}
	})
	t.Run("locked_calls_without_secret_raise", func(t *testing.T) {
		c := newCounter(t)
		secret, err := c.Lock_global("LAILA:POLICY:a")
		if err != nil || secret == nil {
			t.Fatalf("lock_global: %v %v", secret, err)
		}
		func() {
			defer c.Unlock_global(secret)
			if _, err := c.Bump(); !errors.Is(err, py.PermissionError) {
				t.Errorf("bump: want PermissionError, got %v", err)
			}
			if _, err := c.Peek(); !errors.Is(err, py.PermissionError) {
				t.Errorf("peek: want PermissionError, got %v", err)
			}
			// dynamic dispatch path is gated identically
			m, err := py.Getattr(c, "bump")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := py.Call(m, nil, nil); !errors.Is(err, py.PermissionError) {
				t.Errorf("dynamic bump: want PermissionError, got %v", err)
			}
			// private helpers, dunders, fields and mixin methods are never gated
			if c.Internal_() != 0 || c.N != 0 {
				t.Error("private/field access gated")
			}
			if py.Repr(c) == "" || c.Global_id() == "" {
				t.Error("repr/global_id")
			}
		}()
		if mustBump(t, c) != 1 {
			t.Error("bump after unlock")
		}
	})
	t.Run("locked_calls_with_secret_kwarg_pass", func(t *testing.T) {
		c := newCounter(t)
		secret, _ := c.Lock_global("LAILA:POLICY:a")
		defer c.Unlock_global(secret)
		if mustBump(t, c, py.Kw{"secret": *secret}) != 1 {
			t.Error("bump with secret")
		}
		if _, err := c.Bump(py.Kw{"secret": "nope"}); !errors.Is(err, py.PermissionError) {
			t.Errorf("want PermissionError, got %v", err)
		}
		m, _ := py.Getattr(c, "bump")
		if v, err := py.Call(m, nil, py.Kw{"secret": *secret}); err != nil || v != 2 {
			t.Errorf("dynamic bump with secret: %v %v", v, err)
		}
	})
	t.Run("atomic_global_block_passes_gate_for_holder", func(t *testing.T) {
		c := newCounter(t)
		func() {
			lockedAny, release, err := c.Atomic(py.Kw{"scope": "global", "policy_id": "LAILA:POLICY:a"})
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			locked := lockedAny.(*Counter)
			if mustBump(t, locked) != 1 {
				t.Error("bump in block")
			}
			if n, err := locked.Peek(); err != nil || n != 1 {
				t.Error("peek in block")
			}
		}()
		if c.Global_lock_ {
			t.Error("still locked")
		}
		if mustBump(t, c) != 2 {
			t.Error("bump after block")
		}
	})
	t.Run("other_policy_cannot_acquire_or_call", func(t *testing.T) {
		c := newCounter(t)
		_, release, err := c.Atomic(py.Kw{"scope": "global", "policy_id": "LAILA:POLICY:a"})
		if err != nil {
			t.Fatal(err)
		}
		defer release()
		if s, err := c.Lock_global("LAILA:POLICY:b", py.Kw{"timeout_s": 0.05}); err != nil || s != nil {
			t.Errorf("other policy acquired: %v %v", s, err)
		}
		if _, rel, err := c.Atomic(py.Kw{"scope": "global", "policy_id": "LAILA:POLICY:b", "timeout_s": 0.05}); !errors.Is(err, py.PermissionError) {
			if rel != nil {
				rel()
			}
			t.Errorf("want PermissionError, got %v", err)
		}
		// a goroutine without the context secret is blocked too
		seen := make(chan string, 1)
		go func() {
			if _, err := c.Bump(); errors.Is(err, py.PermissionError) {
				seen <- "denied"
			} else {
				seen <- "ok"
			}
		}()
		select {
		case v := <-seen:
			if v != "denied" {
				t.Errorf("other thread: %s", v)
			}
		case <-time.After(5 * time.Second):
			t.Error("other thread hung")
		}
	})
	t.Run("nested_hold_is_reference_counted", func(t *testing.T) {
		c := newCounter(t)
		s1, _ := c.Lock_global("LAILA:POLICY:a")
		s2, _ := c.Lock_global("LAILA:POLICY:a")
		if s1 == nil || s2 == nil || *s1 != *s2 {
			t.Fatal("secrets differ")
		}
		if c.Hold_count_ != 2 {
			t.Errorf("hold_count %d", c.Hold_count_)
		}
		c.Unlock_global(s1)
		if !c.Global_lock_ || c.Hold_count_ != 1 {
			t.Error("first unlock")
		}
		c.Unlock_global(s1)
		if c.Global_lock_ || c.Hold_count_ != 0 {
			t.Error("second unlock")
		}
		// wrong secret never releases
		s3, _ := c.Lock_global("LAILA:POLICY:a")
		c.Unlock_global(py.Ptr("wrong"))
		if !c.Global_lock_ {
			t.Error("wrong secret released")
		}
		c.Unlock_global(s3)
		if c.Global_lock_ {
			t.Error("still locked")
		}
	})
	t.Run("nested_atomic_global_blocks", func(t *testing.T) {
		c := newCounter(t)
		func() {
			_, release, err := c.Atomic(py.Kw{"scope": "global", "policy_id": "LAILA:POLICY:a"})
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			func() {
				_, release, err := c.Atomic(py.Kw{"scope": "global", "policy_id": "LAILA:POLICY:a"})
				if err != nil {
					t.Fatal(err)
				}
				defer release()
				if mustBump(t, c) != 1 {
					t.Error("inner bump")
				}
			}()
			if !c.Global_lock_ {
				t.Error("outer block released the lock")
			}
			if mustBump(t, c) != 2 {
				t.Error("outer bump")
			}
		}()
		if c.Global_lock_ {
			t.Error("still locked")
		}
	})
}

func TestAtomicDictOrder(t *testing.T) {
	t.Run("keys_follow_data_insertion_order", func(t *testing.T) {
		d := newDict(t)
		want := make([]any, 10)
		for i := 0; i < 10; i++ {
			d.Setitem(fmt.Sprintf("k%d", i), i)
			want[i] = fmt.Sprintf("k%d", i)
		}
		if !py.Eq(d.Keys(), want) {
			t.Errorf("keys = %v", d.Keys())
		}
		var itemKeys []any
		for _, it := range d.Items() {
			itemKeys = append(itemKeys, it.Key)
		}
		if !py.Eq(itemKeys, d.Keys()) || !py.Eq(d.Iter(), d.Keys()) {
			t.Error("items/iter order")
		}
	})
	t.Run("keyed_delete_and_pop_keep_order", func(t *testing.T) {
		d := newDict(t)
		for i := 0; i < 6; i++ {
			d.Setitem(i, i)
		}
		_ = d.Delitem(2)
		if v, err := d.Pop(4); err != nil || v != 4 {
			t.Error("pop")
		}
		if !py.Eq(d.Keys(), []any{0, 1, 3, 5}) {
			t.Errorf("keys = %v", d.Keys())
		}
		if k, _ := d.Key_at(2); k != 3 {
			t.Error("key_at")
		}
		if it, _ := d.Item_at(-1); it.Key != 5 || it.Value != 5 {
			t.Error("item_at(-1)")
		}
	})
	t.Run("out_of_band_data_edit_is_visible_without_reindex", func(t *testing.T) {
		d := newDict(t)
		d.Setitem("a", 1)
		d.Data.Set("b", 2)
		_ = d.Data.Del("a")
		if !py.Eq(d.Keys(), []any{"b"}) {
			t.Errorf("keys = %v", d.Keys())
		}
		if k, _ := d.Key_at(0); k != "b" {
			t.Error("key_at")
		}
		if d.Len() != 1 {
			t.Error("len")
		}
	})
	t.Run("delete_from_front_is_not_quadratic", func(t *testing.T) {
		run := func(n int) float64 {
			d := newDict(t)
			for i := 0; i < n; i++ {
				d.Setitem(i, i)
			}
			t0 := time.Now()
			for i := 0; i < n; i++ {
				_ = d.Delitem(i)
			}
			return time.Since(t0).Seconds()
		}
		small := run(5_000)
		large := run(20_000)
		// 4x the keys must cost well under 16x (quadratic) -- allow 8x slack
		if limit := max(small*8, 0.05) + 0.05; large >= limit {
			t.Errorf("small=%v large=%v", small, large)
		}
	})
	t.Run("positional_accessors_after_interleaved_ops", func(t *testing.T) {
		d := newDict(t)
		for i := 0; i < 5; i++ {
			d.Setitem(i, i)
		}
		_, _ = d.Pop(0)
		d.Setitem(9, 9)
		d.Trim(py.Ptr(1), nil)
		if !py.Eq(d.Keys(), []any{2, 3, 4, 9}) {
			t.Errorf("keys = %v", d.Keys())
		}
		if v, _ := d.Value_at(0); v != 2 {
			t.Error("value_at(0)")
		}
		if v, _ := d.Value_at(-1); v != 9 {
			t.Error("value_at(-1)")
		}
	})
}

func TestAtomicStrAppendLinear(t *testing.T) {
	t.Run("long_append_sequence_is_amortised_linear", func(t *testing.T) {
		run := func(n int) float64 {
			s, err := laila.New_AtomicStr()
			if err != nil {
				t.Fatal(err)
			}
			chunk := strings.Repeat("x", 8)
			t0 := time.Now()
			for i := 0; i < n; i++ {
				s.Append(chunk)
			}
			if s.Length() != 8*n {
				t.Errorf("length %d", s.Length())
			}
			return time.Since(t0).Seconds()
		}
		small := run(10_000)
		large := run(40_000)
		if limit := max(small*8, 0.05) + 0.05; large >= limit {
			t.Errorf("small=%v large=%v", small, large)
		}
	})
	t.Run("append_still_returns_and_stores_the_value", func(t *testing.T) {
		s, _ := laila.New_AtomicStr()
		s.Set("ab")
		if s.Append("cd") != "abcd" || s.Get() != "abcd" {
			t.Error("append")
		}
		if s.Append(1) != "abcd1" {
			t.Error("append(1)")
		}
	})
}

// Port of tests/functional/policy/communication/unit_tests/test_owner_bound_memory.py.
//
// Owner-bound central memory (regression guard for F-W18-01 / F-W18-02).
//
// ``laila.memorize(..., dst_policy=B)`` with ``B`` a *local* policy used
// to transiently ``activate_policy(B)`` and restore the previous active
// policy afterwards. That global mutation was observable by other
// threads, triggered an environment-mirror refresh per routed op, and
// made the peer-side memory op run on the *caller's* taskforce.
//
// Now ``central.memory`` keeps a weak back-reference to its owner and
// every memory op submits to the owner's command; the active policy is
// never touched.
package unit_tests

import (
	"sync"
	"testing"
	"time"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// pool_of_ is ``_pool_of(policy, nickname)``.
func pool_of_(t *testing.T, policy *laila.LAILA_IDENTIFIABLE_POLICY_, nickname string) *laila.LAILA_IDENTIFIABLE_POOL_ {
	t.Helper()
	router := laila.As_central_memory_(policy.Central.Memory).Pool_router.(*laila.LAILA_IDENTIFIABLE_POOL_ROUTER_)
	gid, ok := router.Pools_nicknames.Get(nickname)
	if !ok {
		t.Fatalf("KeyError: %s", nickname)
	}
	pool, ok := router.Pools.Get(gid)
	if !ok {
		t.Fatalf("KeyError: %s", py.Repr(gid))
	}
	return laila.As_pool_(pool)
}

func memory_of_(p *laila.LAILA_IDENTIFIABLE_POLICY_) *laila.LAILA_IDENTIFIABLE_CENTRAL_MEMORY_ {
	return laila.As_central_memory_(p.Central.Memory)
}

type ownerBound_ struct {
	A, B *laila.LAILA_IDENTIFIABLE_POLICY_
}

func setUpOwnerBound_(t *testing.T) *ownerBound_ {
	t.Helper()
	original := must(laila.Get_active_policy())
	s := &ownerBound_{
		A: must(laila.New_LAILA_IDENTIFIABLE_POLICY_()),
		B: must(laila.New_LAILA_IDENTIFIABLE_POLICY_()),
	}
	laila.Activate_policy(s.B)
	must(0, memory_of_(s.B).Extend(must(laila.New_DefaultPool()), py.Kw{"pool_nickname": "b-store"}))
	laila.Activate_policy(s.A)
	t.Cleanup(func() { laila.Activate_policy(original) })
	return s
}

func TestOwnerBoundMemory_memory_resolves_its_owner(t *testing.T) {
	s := setUpOwnerBound_(t)
	if must(memory_of_(s.B).Policy_()) != s.B.Self() {
		t.Fatal("memory._policy() is not B")
	}
	cmd := s.B.Central.Command.(interface {
		Central_command_base_() *laila.LAILA_IDENTIFIABLE_CENTRAL_COMMAND_
	}).Central_command_base_()
	if must(memory_of_(s.B).Command_()) != cmd {
		t.Fatal("memory._command() is not B.central.command")
	}
	assertEq(t, must(memory_of_(s.B).Owner_gid_()), s.B.Global_id())
}

func TestOwnerBoundMemory_routed_memorize_never_switches_active_policy(t *testing.T) {
	s := setUpOwnerBound_(t)
	var mu sync.Mutex
	seen := map[string]bool{}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			gid := laila.Active_policy_gid_()
			mu.Lock()
			seen[gid] = true
			mu.Unlock()
			time.Sleep(500 * time.Microsecond)
		}
	}()
	entries := []*laila.Entry{}
	func() {
		defer func() {
			close(stop)
			wg.Wait()
		}()
		for i := 0; i < 50; i++ {
			entries = append(entries, must(laila.Constant(i)))
		}
		for _, e := range entries {
			f := must(laila.Memorize(e, py.Kw{"dst_policy": s.B.Global_id(), "dst_pool": "b-store"}))
			wait_if_future_(t, f)
		}
	}()

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 1 || !seen[s.A.Global_id()] {
		t.Fatalf("active policy drifted during routed ops: %v", seen)
	}
	assertEq(t, active_gid_(t), s.A.Global_id())
	store := pool_of_(t, s.B, "b-store")
	for _, e := range entries {
		if !must(store.Contains(e.Global_id())) {
			t.Fatalf("%s not in b-store", e.Global_id())
		}
	}
}

func TestOwnerBoundMemory_routed_op_records_owner_as_recorder(t *testing.T) {
	s := setUpOwnerBound_(t)
	e := must(laila.Constant(py.DictOf("who", "b")))
	f := must(laila.Memorize(e, py.Kw{"dst_policy": s.B.Global_id(), "dst_pool": "b-store"}))
	wait_if_future_(t, f)
	raw := must(pool_of_(t, s.B, "b-store").Getitem(e.Global_id()))
	rec := must(laila.Record_build_sync_(raw))
	assertEq(t, rec.GetDefault("recorder", nil), s.B.Global_id())
}

func TestOwnerBoundMemory_relay_legs_use_source_memory_not_active(t *testing.T) {
	s := setUpOwnerBound_(t)
	C := must(laila.New_LAILA_IDENTIFIABLE_POLICY_())
	laila.Activate_policy(C)
	must(0, memory_of_(C).Extend(must(laila.New_DefaultPool()), py.Kw{"pool_nickname": "c-store"}))
	laila.Activate_policy(s.A)

	e := must(laila.Constant(py.DictOf("relay", 1)))
	laila.Activate_policy(s.B)
	func() {
		defer laila.Activate_policy(s.A)
		f := must(laila.Memorize(e, py.Kw{"dst_pool": "b-store"}))
		wait_if_future_(t, f)
	}()

	// A has no pool named "b-store"/"c-store": if either relay leg bound
	// to the active policy this would raise KeyError.
	must(laila.Memorize(e.Global_id(), py.Kw{
		"src_policy": s.B.Global_id(),
		"src_pool":   "b-store",
		"dst_policy": C.Global_id(),
		"dst_pool":   "c-store",
	}))
	if !must(pool_of_(t, C, "c-store").Contains(e.Global_id())) {
		t.Fatalf("%s not in c-store", e.Global_id())
	}
	assertEq(t, active_gid_(t), s.A.Global_id())
}

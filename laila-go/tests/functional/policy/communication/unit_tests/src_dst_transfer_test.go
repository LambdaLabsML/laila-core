// Port of tests/functional/policy/communication/unit_tests/test_src_dst_transfer.py.
//
// Tests for the source/destination memory-transfer API.
//
// Covers the augmented ``laila.memorize`` / ``laila.remember`` /
// ``laila.forget`` surface:
//
//  1. ``_resolve_policy_ref`` -- gid / nickname / object / ``None``.
//  2. ``dst_policy`` / ``dst_pool`` routing into a second *local* policy
//     (the 2-party case), with active-policy restoration, addressed by both
//     gid and nickname.
//  3. ``forget(policy=..., pool=...)`` deleting from a target policy.
//  4. Standalone-pool reads/writes: handing a live, *unregistered* pool
//     object straight to ``dst_pool`` on the active policy.
//  5. Back-compat: the classic ``policy_id`` / ``pool_nickname`` aliases
//     keep working.
//
// The 2-party / local-target cases run in-process because active-policy
// restoration is exercised directly; faithful cross-process remote
// semantics live in peer_routing_test.go and emulated_peers_memory_test.go.
package unit_tests

import (
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// ------------------------------------------------------------------ #
// TestResolvePolicyRef
// ------------------------------------------------------------------ #

func TestResolvePolicyRef_none_passes_through(t *testing.T) {
	if v := must(laila.Resolve_policy_ref_(nil)); !py.IsNone(v) {
		t.Fatal(py.Repr(v))
	}
}

func TestResolvePolicyRef_gid_passes_through(t *testing.T) {
	p := must(laila.New_LAILA_IDENTIFIABLE_POLICY_())
	assertEq(t, must(laila.Resolve_policy_ref_(p.Global_id())), p.Global_id())
}

func TestResolvePolicyRef_object_yields_its_gid(t *testing.T) {
	p := must(laila.New_LAILA_IDENTIFIABLE_POLICY_())
	assertEq(t, must(laila.Resolve_policy_ref_(p)), p.Global_id())
}

func TestResolvePolicyRef_nickname_is_deterministic(t *testing.T) {
	first := must(laila.Resolve_policy_ref_("edge-sensor"))
	second := must(laila.Resolve_policy_ref_("edge-sensor"))
	assertEq(t, first, second)
	if !laila.Is_laila_resource(py.Str(first)) {
		t.Fatal(py.Repr(first))
	}
	// A policy whose uuid derives from the same nickname resolves to it.
	p := must(laila.New_LAILA_IDENTIFIABLE_POLICY_())
	p.Set_uuid(laila.Generate_uuid_from_nickname("edge-sensor"))
	assertEq(t, p.Global_id(), first)
}

// ------------------------------------------------------------------ #
// TestDstRoutingLocalTarget
// ------------------------------------------------------------------ #

type dstRouting_ struct {
	A, L *laila.LAILA_IDENTIFIABLE_POLICY_
}

func setUpDstRouting_(t *testing.T) *dstRouting_ {
	t.Helper()
	original := must(laila.Get_active_policy())
	s := &dstRouting_{
		A: must(laila.New_LAILA_IDENTIFIABLE_POLICY_()),
		L: must(laila.New_LAILA_IDENTIFIABLE_POLICY_()),
	}
	laila.Activate_policy(s.L)
	must(0, memory_of_(s.L).Extend(must(laila.New_DefaultPool()), py.Kw{"pool_nickname": "l-store"}))
	laila.Activate_policy(s.A)
	t.Cleanup(func() { laila.Activate_policy(original) })
	return s
}

func assertInPool_(t *testing.T, gid string, pool *laila.LAILA_IDENTIFIABLE_POOL_) {
	t.Helper()
	if !must(pool.Contains(gid)) {
		t.Fatalf("%s not in pool", gid)
	}
}

func assertNotInPool_(t *testing.T, gid string, pool *laila.LAILA_IDENTIFIABLE_POOL_) {
	t.Helper()
	if must(pool.Contains(gid)) {
		t.Fatalf("%s unexpectedly in pool", gid)
	}
}

func TestDstRoutingLocalTarget_memorize_into_dst_policy(t *testing.T) {
	s := setUpDstRouting_(t)
	entry := must(laila.Constant(py.DictOf("x", 1), py.Kw{"nickname": "sd-e1"}))
	wait_if_future_(t, must(laila.Memorize(entry, py.Kw{"dst_policy": s.L.Global_id(), "dst_pool": "l-store"})))
	assertEq(t, active_gid_(t), s.A.Global_id())
	assertInPool_(t, entry.Global_id(), pool_of_(t, s.L, "l-store"))
}

func TestDstRoutingLocalTarget_remember_from_dst_policy(t *testing.T) {
	s := setUpDstRouting_(t)
	entry := must(laila.Constant(py.DictOf("msg", "rt"), py.Kw{"nickname": "sd-e2"}))
	wait_if_future_(t, must(laila.Memorize(entry, py.Kw{"dst_policy": s.L.Global_id(), "dst_pool": "l-store"})))
	res := must(laila.Remember(entry.Global_id(), py.Kw{
		"dst_policy": s.L.Global_id(),
		"dst_pool":   "l-store",
		"persist":    false,
	}))
	assertEq(t, active_gid_(t), s.A.Global_id())
	assertEq(t, data_of_(t, res), py.DictOf("msg", "rt"))
}

func TestDstRoutingLocalTarget_forget_on_dst_policy(t *testing.T) {
	s := setUpDstRouting_(t)
	entry := must(laila.Constant(py.DictOf("d", 9), py.Kw{"nickname": "sd-e3"}))
	wait_if_future_(t, must(laila.Memorize(entry, py.Kw{"dst_policy": s.L.Global_id(), "dst_pool": "l-store"})))
	assertInPool_(t, entry.Global_id(), pool_of_(t, s.L, "l-store"))
	wait_if_future_(t, must(laila.Forget(entry.Global_id(), py.Kw{"policy": s.L.Global_id(), "pool": "l-store"})))
	assertEq(t, active_gid_(t), s.A.Global_id())
	assertNotInPool_(t, entry.Global_id(), pool_of_(t, s.L, "l-store"))
}

func TestDstRoutingLocalTarget_dst_policy_by_nickname(t *testing.T) {
	s := setUpDstRouting_(t)
	// Give L a deterministic identity matching the nickname, then
	// (re-)register it under that gid in the local-policy registry.
	s.L.Set_uuid(laila.Generate_uuid_from_nickname("sd-edge"))
	laila.Activate_policy(s.L)
	laila.Activate_policy(s.A)
	entry := must(laila.Constant(py.DictOf("via", "nickname"), py.Kw{"nickname": "sd-e4"}))
	wait_if_future_(t, must(laila.Memorize(entry, py.Kw{"dst_policy": "sd-edge", "dst_pool": "l-store"})))
	assertInPool_(t, entry.Global_id(), pool_of_(t, s.L, "l-store"))
}

func TestDstRoutingLocalTarget_back_compat_policy_id_alias(t *testing.T) {
	s := setUpDstRouting_(t)
	entry := must(laila.Constant(py.DictOf("legacy", true), py.Kw{"nickname": "sd-e5"}))
	wait_if_future_(t, must(laila.Memorize(py.Kw{
		"entries": entry, "policy_id": s.L.Global_id(), "pool_nickname": "l-store",
	})))
	assertInPool_(t, entry.Global_id(), pool_of_(t, s.L, "l-store"))
}

// ------------------------------------------------------------------ #
// TestThreePartyRelayLocal
// ------------------------------------------------------------------ #

// threePartyRelay_ is ``TestThreePartyRelayLocal``: 3-party transfer
// driven by an orchestrator A: src B moves data to/from dst C. Exercised
// with three *local* policies so the relay's morph/restore bookkeeping is
// verified end-to-end in one process; the cross-process variant rides the
// same code over real peer links.
type threePartyRelay_ struct {
	A, B, C *laila.LAILA_IDENTIFIABLE_POLICY_
}

func setUpThreePartyRelay_(t *testing.T) *threePartyRelay_ {
	t.Helper()
	original := must(laila.Get_active_policy())
	s := &threePartyRelay_{
		A: must(laila.New_LAILA_IDENTIFIABLE_POLICY_()),
		B: must(laila.New_LAILA_IDENTIFIABLE_POLICY_()),
		C: must(laila.New_LAILA_IDENTIFIABLE_POLICY_()),
	}
	laila.Activate_policy(s.B)
	must(0, memory_of_(s.B).Extend(must(laila.New_DefaultPool()), py.Kw{"pool_nickname": "b-store"}))
	laila.Activate_policy(s.C)
	must(0, memory_of_(s.C).Extend(must(laila.New_DefaultPool()), py.Kw{"pool_nickname": "c-store"}))
	laila.Activate_policy(s.A)
	t.Cleanup(func() { laila.Activate_policy(original) })
	return s
}

// seed_ is ``_seed(policy, nickname, entry)``.
func (s *threePartyRelay_) seed_(t *testing.T, policy *laila.LAILA_IDENTIFIABLE_POLICY_, nickname string, entry *laila.Entry) {
	t.Helper()
	laila.Activate_policy(policy)
	defer laila.Activate_policy(s.A)
	wait_if_future_(t, must(laila.Memorize(entry, py.Kw{"dst_pool": nickname})))
}

func TestThreePartyRelayLocal_push_relay_src_to_dst(t *testing.T) {
	s := setUpThreePartyRelay_(t)
	entry := must(laila.Constant(py.DictOf("relay", "push"), py.Kw{"nickname": "sd-r1"}))
	s.seed_(t, s.B, "b-store", entry)

	must(laila.Memorize(entry.Global_id(), py.Kw{
		"src_policy": s.B.Global_id(),
		"src_pool":   "b-store",
		"dst_policy": s.C.Global_id(),
		"dst_pool":   "c-store",
	}))
	assertEq(t, active_gid_(t), s.A.Global_id())
	assertInPool_(t, entry.Global_id(), pool_of_(t, s.C, "c-store"))
}

func TestThreePartyRelayLocal_pull_relay_dst_into_src(t *testing.T) {
	s := setUpThreePartyRelay_(t)
	entry := must(laila.Constant(py.DictOf("relay", "pull"), py.Kw{"nickname": "sd-r2"}))
	s.seed_(t, s.C, "c-store", entry)

	must(laila.Remember(entry.Global_id(), py.Kw{
		"src_policy": s.B.Global_id(),
		"src_pool":   "b-store",
		"dst_policy": s.C.Global_id(),
		"dst_pool":   "c-store",
	}))
	assertEq(t, active_gid_(t), s.A.Global_id())
	assertInPool_(t, entry.Global_id(), pool_of_(t, s.B, "b-store"))
}

func TestThreePartyRelayLocal_unknown_src_policy_raises(t *testing.T) {
	s := setUpThreePartyRelay_(t)
	_, err := laila.Memorize(
		"LAILA:POLICY:00000000-0000-0000-0000-000000000000",
		py.Kw{
			"src_policy": "LAILA:POLICY:11111111-1111-1111-1111-111111111111",
			"dst_policy": s.C.Global_id(),
		})
	assertRaises(t, py.ConnectionError, err)
}

// ------------------------------------------------------------------ #
// TestStandalonePool
// ------------------------------------------------------------------ #

type standalonePool_ struct {
	A          *laila.LAILA_IDENTIFIABLE_POLICY_
	standalone *laila.LAILA_IDENTIFIABLE_POOL_
}

func setUpStandalonePool_(t *testing.T) *standalonePool_ {
	t.Helper()
	original := must(laila.Get_active_policy())
	s := &standalonePool_{A: must(laila.New_LAILA_IDENTIFIABLE_POLICY_())}
	laila.Activate_policy(s.A)
	// A standalone pool the caller configured -- NOT registered in the
	// active policy's router.
	s.standalone = laila.As_pool_(must(laila.New_DefaultPool()))
	t.Cleanup(func() { laila.Activate_policy(original) })
	return s
}

func TestStandalonePool_memorize_into_standalone_pool_object(t *testing.T) {
	s := setUpStandalonePool_(t)
	entry := must(laila.Constant(py.DictOf("s", "andalone"), py.Kw{"nickname": "sd-s1"}))
	wait_if_future_(t, must(laila.Memorize(entry, py.Kw{"dst_pool": s.standalone.Self()})))
	assertInPool_(t, entry.Global_id(), s.standalone)
}

func TestStandalonePool_remember_from_standalone_pool_object(t *testing.T) {
	s := setUpStandalonePool_(t)
	entry := must(laila.Constant(py.DictOf("hello", "pool"), py.Kw{"nickname": "sd-s2"}))
	wait_if_future_(t, must(laila.Memorize(entry, py.Kw{"dst_pool": s.standalone.Self()})))
	res := must(laila.Remember(entry.Global_id(), py.Kw{"dst_pool": s.standalone.Self(), "persist": false}))
	assertEq(t, data_of_(t, res), py.DictOf("hello", "pool"))
}

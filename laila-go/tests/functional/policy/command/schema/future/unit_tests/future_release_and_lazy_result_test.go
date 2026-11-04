// Port of tests/functional/policy/command/schema/future/unit_tests/
// test_future_release_and_lazy_result.py -- functional tests for explicit
// ``release()``, lazy result wrapping, cheap identity construction and the
// single-task ``submit`` return type.
package unit_tests

import (
	"fmt"
	"reflect"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

const T_ = 30.0

// bank_ is ``_bank()``: ``laila.get_active_policy().future_bank``.
func bank_(t *testing.T) *py.Dict {
	t.Helper()
	policy, err := laila.Get_active_policy()
	if err != nil {
		t.Fatal(err)
	}
	return policy.(interface{ Get_future_bank_() *py.Dict }).Get_future_bank_()
}

// command_ is ``laila.command``.
func command_(t *testing.T) *laila.LAILA_IDENTIFIABLE_CENTRAL_COMMAND_ {
	t.Helper()
	c, err := laila.Command()
	if err != nil {
		t.Fatal(err)
	}
	return c.(*laila.LAILA_IDENTIFIABLE_CENTRAL_COMMAND_)
}

// submit_ is ``laila.command.submit([...])``.
func submit_(t *testing.T, tasks ...any) any {
	t.Helper()
	out, err := command_(t).Submit(tasks)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func submit_cpf(t *testing.T, tasks ...any) *laila.ConcurrentPackageFuture {
	t.Helper()
	out := submit_(t, tasks...)
	fut, ok := out.(*laila.ConcurrentPackageFuture)
	if !ok {
		t.Fatalf("expected ConcurrentPackageFuture, got %T", out)
	}
	return fut
}

func submit_group(t *testing.T, tasks ...any) *laila.GroupFuture {
	t.Helper()
	out := submit_(t, tasks...)
	gf, ok := out.(*laila.GroupFuture)
	if !ok {
		t.Fatalf("expected GroupFuture, got %T", out)
	}
	return gf
}

func wait_(t *testing.T, f interface{ Wait(...any) (any, error) }, timeout float64) any {
	t.Helper()
	out, err := f.Wait(timeout)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func wait_data(t *testing.T, f interface{ Wait(...any) (any, error) }, timeout float64) any {
	t.Helper()
	d, err := data_of(wait_(t, f, timeout))
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func must_data(t *testing.T, f interface{ Data() (any, error) }) any {
	t.Helper()
	d, err := f.Data()
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func assert_in_bank(t *testing.T, gid string) {
	t.Helper()
	if !bank_(t).Contains(gid) {
		t.Fatalf("%s expected in bank", gid)
	}
}

func assert_not_in_bank(t *testing.T, gid string) {
	t.Helper()
	if bank_(t).Contains(gid) {
		t.Fatalf("%s expected not in bank", gid)
	}
}

func entry_constant(t *testing.T, data any) *laila.Entry {
	t.Helper()
	e, err := laila.Entry_constant(data)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// ---------------------------------------------------------------------------
// TestFutureRelease
// ---------------------------------------------------------------------------

func TestFutureRelease_001_single_future_release_removes_from_bank(t *testing.T) {
	fut := submit_cpf(t, lam(1))
	gid := fut.Global_id()
	assert_in_bank(t, gid)
	wait_(t, fut, T_)
	// Nothing is released automatically -- waiting / reading keeps it.
	assert_in_bank(t, gid)
	_ = must_data(t, fut)
	assert_in_bank(t, gid)
	fut.Release()
	assert_not_in_bank(t, gid)
	// Idempotent.
	fut.Release()
	assert_not_in_bank(t, gid)
	// The handle itself still works after release.
	if as_int(t, must_data(t, fut)) != 1 {
		t.Fatal("data after release")
	}
}

func TestFutureRelease_002_group_release_releases_children_by_default(t *testing.T) {
	gf := submit_group(t, lam(1), lam(2), lam(3))
	ids := append([]string(nil), gf.Future_ids...)
	wait_(t, gf, T_)
	for _, fid := range ids {
		assert_in_bank(t, fid)
	}
	gf.Release()
	assert_not_in_bank(t, gf.Global_id())
	for _, fid := range ids {
		assert_not_in_bank(t, fid)
	}
	gf.Release() // idempotent
}

func TestFutureRelease_003_group_release_children_false_keeps_children(t *testing.T) {
	gf := submit_group(t, lam(1), lam(2))
	ids := append([]string(nil), gf.Future_ids...)
	wait_(t, gf, T_)
	gf.Release_with(py.Kw{"children": false})
	assert_not_in_bank(t, gf.Global_id())
	for _, fid := range ids {
		assert_in_bank(t, fid)
		child, err := bank_(t).Getitem(fid)
		if err != nil {
			t.Fatal(err)
		}
		child.(interface{ Release() }).Release()
		assert_not_in_bank(t, fid)
	}
}

func TestFutureRelease_004_early_release_does_not_break_completion(t *testing.T) {
	fut := submit_cpf(t, func() (any, error) { py.Sleep_sync(0.2); return 42, nil })
	fut.Release()
	assert_not_in_bank(t, fut.Global_id())
	if as_int(t, wait_data(t, fut, T_)) != 42 {
		t.Fatal("data")
	}
	if fut.Status() != laila.FutureStatus_FINISHED {
		t.Fatalf("status %v", fut.Status())
	}
}

func TestFutureRelease_005_identity_handle_release_delegates_to_concrete_future(t *testing.T) {
	fut := submit_cpf(t, lam(5))
	wait_(t, fut, T_)
	ident, err := fut.Future_identity()
	if err != nil {
		t.Fatal(err)
	}
	if !py.Isinstance(ident, laila.LAILA_IDENTIFIABLE_FUTURE_class) {
		t.Fatal("identity must be a _LAILA_IDENTIFIABLE_FUTURE")
	}
	if py.Isinstance(ident, laila.Future_class) {
		t.Fatal("identity must not be a Future")
	}
	assert_in_bank(t, fut.Global_id())
	ident.Release()
	assert_not_in_bank(t, fut.Global_id())
	ident.Release() // no-op once gone
}

func TestFutureRelease_006_complex_future_release_releases_stages(t *testing.T) {
	cmd, alpha, _, pid := cf_ids(t)
	s1 := stage(func(_ any) (any, error) { return submit1(cmd, alpha, lam(7)) })
	s2 := stage(func(prev any) (any, error) {
		v, err := data_of(prev)
		if err != nil {
			return nil, err
		}
		return submit1(cmd, alpha, lam(as_int(t, v)*6))
	})
	cf := new_cf(t, alpha, pid, []any{s1, s2})
	if as_int(t, wait_data(t, cf, T_)) != 42 {
		t.Fatal("data")
	}
	stage_ids := append([]string(nil), cf.Stage_future_ids...)
	if len(stage_ids) != 2 {
		t.Fatalf("stage ids %v", stage_ids)
	}
	for _, sid := range stage_ids {
		assert_in_bank(t, sid)
	}
	cf.Release()
	assert_not_in_bank(t, cf.Global_id())
	for _, sid := range stage_ids {
		assert_not_in_bank(t, sid)
	}
}

// manifest_of_leaves is ``Manifest(data={f"k{i}": e for i, e in enumerate(leaves)})``
// over ``leaves = [Entry.constant(data=i) for i in range(n)]``.
func manifest_of_leaves(t *testing.T, n int) *laila.Manifest {
	t.Helper()
	data := py.NewDict()
	for i := 0; i < n; i++ {
		e, err := laila.Entry_constant(i)
		if err != nil {
			t.Fatal(err)
		}
		data.Set(fmt.Sprintf("k%d", i), e)
	}
	m, err := laila.New_Manifest(py.Kw{"data": data})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestFutureRelease_007_manifest_realized_releases_its_own_futures(t *testing.T) {
	command_(t) // laila.get_active_policy()
	manifest := manifest_of_leaves(t, 20)
	ref, err := manifest.Memorize()
	if err != nil {
		t.Fatal(err)
	}
	wait_(t, ref, T_)
	ref.Release()
	before := bank_(t).Len()
	realized, err := manifest.Realized()
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, v := range realized.(*py.Dict).Values() {
		n, err := py.Int(must_data(t, laila.As_entry_(v)))
		if err != nil {
			t.Fatal(err)
		}
		total += n
	}
	if total != 19*20/2 {
		t.Fatalf("sum %d", total)
	}
	// realized() released the remember futures it created for itself.
	if got := bank_(t).Len(); got != before {
		t.Fatalf("bank grew: %d -> %d", before, got)
	}
}

func TestFutureRelease_008_manifest_memorize_group_owns_all_children(t *testing.T) {
	command_(t) // laila.get_active_policy()
	manifest := manifest_of_leaves(t, 300) // > one 128 batch
	before := bank_(t).Len()
	ref, err := manifest.Memorize()
	if err != nil {
		t.Fatal(err)
	}
	wait_(t, ref, T_)
	if got := bank_(t).Len(); got <= before {
		t.Fatalf("bank did not grow: %d -> %d", before, got)
	}
	ref.Release()
	// No orphaned per-batch group shells or children remain.
	if got := bank_(t).Len(); got != before {
		t.Fatalf("orphans left in bank: %d -> %d", before, got)
	}
}

// ---------------------------------------------------------------------------
// TestLazyResultWrap
// ---------------------------------------------------------------------------

func TestLazyResultWrap_001_raw_result_is_not_wrapped_until_read(t *testing.T) {
	fut := submit_cpf(t, lam(py.DictOf("x", 1)))
	done := py.NewEvent()
	fut.Add_status_callback(laila.FutureStatus_FINISHED, func(_ any) { done.Set() })
	if !done.Wait(T_) {
		t.Fatal("timed out waiting for FINISHED")
	}
	// Producer stored the raw value; no Entry exists yet.
	func() {
		_, exit, err := fut.Atomic()
		if err != nil {
			t.Fatal(err)
		}
		defer exit()
		if !fut.Result_pending_wrap_ {
			t.Fatal("_result_pending_wrap should be True")
		}
		if fut.Result_global_id_ != nil {
			t.Fatal("_result_global_id should be None")
		}
		if !py.Eq(fut.Return_value_, py.DictOf("x", 1)) {
			t.Fatalf("_return_value %v", py.Repr(fut.Return_value_))
		}
	}()
	res, err := fut.Result()
	if err != nil {
		t.Fatal(err)
	}
	entry, ok := res.(*laila.Entry)
	if !ok {
		t.Fatalf("expected Entry, got %T", res)
	}
	if !py.Eq(must_data(t, entry), py.DictOf("x", 1)) {
		t.Fatal("entry data")
	}
	if fut.Result_pending_wrap_ {
		t.Fatal("_result_pending_wrap should be False after read")
	}
	rid, err := fut.Result_global_id()
	if err != nil {
		t.Fatal(err)
	}
	if rid == nil || *rid != entry.Global_id() {
		t.Fatalf("result_global_id %v != %s", rid, entry.Global_id())
	}
	// Cached: same wrapper on every read.
	res2, err := fut.Result()
	if err != nil {
		t.Fatal(err)
	}
	if res2 != res {
		t.Fatal("result wrapper must be cached")
	}
	if !py.Eq(must_data(t, fut), py.DictOf("x", 1)) {
		t.Fatal("fut.data")
	}
	fut.Release()
}

func TestLazyResultWrap_002_wait_and_await_return_entries(t *testing.T) {
	fut := submit_cpf(t, lam(3))
	out := wait_(t, fut, T_)
	entry, ok := out.(*laila.Entry)
	if !ok {
		t.Fatalf("expected Entry, got %T", out)
	}
	if as_int(t, must_data(t, entry)) != 3 {
		t.Fatal("data")
	}
	fut.Release()

	// asyncio.run(_go())
	res, err := py.Run(func() (any, error) {
		f := submit_cpf(t, lam(4))
		defer f.Release()
		return f.Await()
	})
	if err != nil {
		t.Fatal(err)
	}
	entry, ok = res.(*laila.Entry)
	if !ok {
		t.Fatalf("expected Entry, got %T", res)
	}
	if as_int(t, must_data(t, entry)) != 4 {
		t.Fatal("await data")
	}
}

func TestLazyResultWrap_003_entry_results_are_stored_as_is(t *testing.T) {
	e := entry_constant(t, 9)
	fut := submit_cpf(t, lam(e))
	out := wait_(t, fut, T_)
	if out != any(e) {
		t.Fatal("entry result must be stored as-is")
	}
	if fut.Result_pending_wrap_ {
		t.Fatal("_result_pending_wrap should be False")
	}
	rid, err := fut.Result_global_id()
	if err != nil {
		t.Fatal(err)
	}
	if rid == nil || *rid != e.Global_id() {
		t.Fatal("result_global_id")
	}
	fut.Release()
}

func TestLazyResultWrap_004_rpc_helpers_force_wrap(t *testing.T) {
	policy, err := laila.Get_active_policy()
	if err != nil {
		t.Fatal(err)
	}
	rpc := policy.(interface {
		Get_future_result_id_(future_id string) (any, error)
	})
	fut := submit_cpf(t, lam("abc"))
	wait_(t, fut, T_)
	rid, err := rpc.Get_future_result_id_(fut.Global_id())
	if err != nil {
		t.Fatal(err)
	}
	if py.IsNone(rid) {
		t.Fatal("rid is None")
	}
	res, err := fut.Result()
	if err != nil {
		t.Fatal(err)
	}
	if py.Str(rid) != res.(*laila.Entry).Global_id() {
		t.Fatalf("rid %v != %s", rid, res.(*laila.Entry).Global_id())
	}
	gf := submit_group(t, lam(1), lam(2))
	wait_(t, gf, T_)
	ids, err := rpc.Get_future_result_id_(gf.Global_id())
	if err != nil {
		t.Fatal(err)
	}
	items := py.Iter(ids)
	if len(items) != 2 {
		t.Fatalf("ids %v", py.Repr(ids))
	}
	for _, i := range items {
		if py.IsNone(i) {
			t.Fatal("child result id is None")
		}
	}
	fut.Release()
	gf.Release()
}

func TestLazyResultWrap_005_complex_future_chain_sees_entries(t *testing.T) {
	cmd, alpha, _, pid := cf_ids(t)
	s1 := stage(func(_ any) (any, error) { return submit1(cmd, alpha, lam(7)) })
	s2 := stage(func(prev any) (any, error) {
		v, err := data_of(prev)
		if err != nil {
			return nil, err
		}
		return submit1(cmd, alpha, lam(as_int(t, v)+1))
	})
	cf := new_cf(t, alpha, pid, []any{s1, s2})
	out := wait_(t, cf, T_)
	entry, ok := out.(*laila.Entry)
	if !ok {
		t.Fatalf("expected Entry, got %T", out)
	}
	if as_int(t, must_data(t, entry)) != 8 {
		t.Fatal("data")
	}
	cf.Release()
}

func TestLazyResultWrap_006_none_result_has_no_id(t *testing.T) {
	fut := submit_cpf(t, lam(nil))
	wait_(t, fut, T_)
	rid, err := fut.Result_global_id()
	if err != nil {
		t.Fatal(err)
	}
	if rid != nil {
		t.Fatalf("result_global_id %v", *rid)
	}
	res, err := fut.Result()
	if err != nil {
		t.Fatal(err)
	}
	if !py.IsNone(res) {
		t.Fatalf("result %v", py.Repr(res))
	}
	fut.Release()
}

// ---------------------------------------------------------------------------
// TestSubmitReturnTypeAndIdentity
// ---------------------------------------------------------------------------

func TestSubmitReturnType_001_single_task_submit_returns_concrete_future(t *testing.T) {
	out := submit_(t, lam(1))
	fut, ok := out.(*laila.ConcurrentPackageFuture)
	if !ok {
		t.Fatalf("expected ConcurrentPackageFuture, got %T", out)
	}
	if !py.Isinstance(fut, laila.LAILA_IDENTIFIABLE_FUTURE_class) {
		t.Fatal("must be a _LAILA_IDENTIFIABLE_FUTURE")
	}
	if bank_(t).GetDefault(fut.Global_id(), nil) != any(fut) {
		t.Fatal("bank entry must be the same object")
	}
	if as_int(t, wait_data(t, fut, T_)) != 1 {
		t.Fatal("data")
	}
	fut.Release()
}

func TestSubmitReturnType_002_imap_yields_concrete_futures(t *testing.T) {
	cmd := command_(t)
	tf, err := cmd.Taskforces.Getitem(cmd.Alpha_taskforce)
	if err != nil {
		t.Fatal(err)
	}
	it, err := tf.(interface{ Imap(tasks any) (py.Iterator, error) }).Imap([]any{lam(1), lam(2)})
	if err != nil {
		t.Fatal(err)
	}
	var futs []*laila.ConcurrentPackageFuture
	for f, err := range it {
		if err != nil {
			t.Fatal(err)
		}
		cpf, ok := f.(*laila.ConcurrentPackageFuture)
		if !ok {
			t.Fatalf("expected ConcurrentPackageFuture, got %T", f)
		}
		futs = append(futs, cpf)
	}
	got := []int{}
	for _, f := range futs {
		got = append(got, as_int(t, wait_data(t, f, T_)))
	}
	if !reflect.DeepEqual(got, []int{1, 2}) {
		t.Fatalf("got %v", got)
	}
	for _, f := range futs {
		f.Release()
	}
}

func TestSubmitReturnType_003_remember_single_returns_concrete_future(t *testing.T) {
	command_(t) // laila.get_active_policy()
	e, err := laila.Entry_constant(11)
	if err != nil {
		t.Fatal(err)
	}
	m, err := laila.Memorize(e)
	if err != nil {
		t.Fatal(err)
	}
	wait_(t, m.(interface{ Wait(...any) (any, error) }), T_)
	m.(interface{ Release() }).Release()
	r, err := laila.Remember(e.Global_id())
	if err != nil {
		t.Fatal(err)
	}
	if !py.Isinstance(r, laila.Future_class) {
		t.Fatalf("expected Future, got %T", r)
	}
	f := r.(interface {
		Wait(...any) (any, error)
		Release()
	})
	if got := wait_data(t, f, T_); !py.Eq(got, 11) {
		t.Fatalf("got %v", got)
	}
	f.Release()
}

func TestSubmitReturnType_004_explicit_uuid_is_honoured_on_futures(t *testing.T) {
	u := py.Uuid4().String()
	_, tf, _, pid := cf_ids(t)
	f, err := laila.New_ConcurrentPackageFuture(py.Kw{"taskforce_id": tf, "policy_id": pid, "uuid": u})
	if err != nil {
		t.Fatal(err)
	}
	if f.Uuid() != u {
		t.Fatalf("uuid %s != %s", f.Uuid(), u)
	}
	if !hasSuffix(f.Global_id(), u) {
		t.Fatalf("global_id %s does not end with %s", f.Global_id(), u)
	}
	assert_in_bank(t, f.Global_id())
	g, err := laila.New_GroupFuture(py.Kw{"taskforce_id": tf, "policy_id": pid, "uuid": u, "future_ids": []string{}})
	if err != nil {
		t.Fatal(err)
	}
	if g.Uuid() != u {
		t.Fatal("group uuid")
	}
	if f.Global_id() == g.Global_id() {
		t.Fatal("different scopes must yield different global ids")
	}
	if !reflect.DeepEqual(f.Scopes(), []string{"FUTURE"}) {
		t.Fatalf("scopes %v", f.Scopes())
	}
	if !reflect.DeepEqual(g.Scopes(), []string{"GROUP_FUTURE"}) {
		t.Fatalf("scopes %v", g.Scopes())
	}
	f.Release()
	g.Release()
}

func hasSuffix(s, suffix string) bool {
	return len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix
}

func TestSubmitReturnType_005_default_scopes_per_class(t *testing.T) {
	e := entry_constant(t, 1)
	if !reflect.DeepEqual(e.Scopes(), []string{"ENTRY"}) {
		t.Fatalf("scopes %v", e.Scopes())
	}
	m, err := laila.New_Manifest(py.Kw{"data": py.DictOf("a", e)})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(m.Scopes(), []string{"MANIFEST"}) {
		t.Fatalf("manifest scopes %v", m.Scopes())
	}
	custom, err := laila.New_Entry(py.Kw{"data": 1, "scopes": []string{"X", "Y"}})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(custom.Scopes(), []string{"X", "Y"}) {
		t.Fatalf("scopes %v", custom.Scopes())
	}
	p, err := laila.Get_active_policy()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(p.(interface{ Scopes() []string }).Scopes(), []string{"POLICY"}) {
		t.Fatal("policy scopes")
	}
	if !reflect.DeepEqual(command_(t).Scopes(), []string{"CENTRAL_COMMAND"}) {
		t.Fatal("command scopes")
	}
}

func TestSubmitReturnType_006_default_callbacks_shared_and_status_callbacks_per_instance(t *testing.T) {
	_, tf, _, pid := cf_ids(t)
	a, err := laila.New_ConcurrentPackageFuture(py.Kw{"taskforce_id": tf, "policy_id": pid})
	if err != nil {
		t.Fatal(err)
	}
	b, err := laila.New_ConcurrentPackageFuture(py.Kw{"taskforce_id": tf, "policy_id": pid})
	if err != nil {
		t.Fatal(err)
	}
	if reflect.ValueOf(a.Default_callbacks_).Pointer() != reflect.ValueOf(b.Default_callbacks_).Pointer() {
		t.Fatal("_default_callbacks must be shared")
	}
	if reflect.ValueOf(a.Status_callbacks_).Pointer() == reflect.ValueOf(b.Status_callbacks_).Pointer() {
		t.Fatal("_status_callbacks must be per instance")
	}
	a.Default_callbacks_[laila.FutureStatus_FINISHED](a)
	if a.Status() != laila.FutureStatus_FINISHED {
		t.Fatalf("a status %v", a.Status())
	}
	if b.Status() != laila.FutureStatus_NOT_STARTED {
		t.Fatalf("b status %v", b.Status())
	}
	a.Release()
	b.Release()
}

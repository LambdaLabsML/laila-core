// Port of tests/functional/entry/constitution/unit_tests/test_constitution_build.py.
//
// End-to-end tests for constitution-driven Entry building and persistence.
//
// Covers:
//   - ``Entry.variable(constitution=..., manifest=...)`` construction.
//   - ``laila.build(entry)`` submits and materialises the payload, clearing
//     the attached constitution on success.
//   - ``Entry.data`` raises EntryNotBuiltError until the entry has been
//     built (no implicit lazy build any more).
//   - Memorize on a non-READY (STAGED) complex entry is rejected; building
//     first then memorizing the materialized result succeeds.
//
// Constitution *source* is still the Python text (it is what gets stored
// and compared); the Go body bound to it through Register_constitution
// is what runs.
package unit_tests

import (
	"errors"
	"testing"
	"time"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// must unwraps (v, err), failing via panic so it can be used inline.
func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func assertEq(t *testing.T, got, want any) {
	t.Helper()
	if !py.Eq(got, want) {
		t.Fatalf("got %s, want %s", py.Repr(got), py.Repr(want))
	}
}

func assertIs(t *testing.T, got, want any) {
	t.Helper()
	if got != want {
		t.Fatalf("got %s is not %s", py.Repr(got), py.Repr(want))
	}
}

type waiter interface{ Wait(timeout ...any) (any, error) }

// wait is ``fut.wait(None)``.
func wait(t *testing.T, fut any) any {
	t.Helper()
	return must(fut.(waiter).Wait(nil))
}

const CONSTITUTION_SRC_ = "def combine(manifest):\n    d = manifest.realized\n    return d['a'].data + d['b'].data\n"

// combine_ is the Go body of CONSTITUTION_SRC_.
func combine_(arg any) (any, error) {
	d, err := laila.As_manifest_(arg).Realized()
	if err != nil {
		return nil, err
	}
	a, err := laila.As_entry_(must(d.(*py.Dict).Getitem("a"))).Data()
	if err != nil {
		return nil, err
	}
	b, err := laila.As_entry_(must(d.(*py.Dict).Getitem("b"))).Data()
	if err != nil {
		return nil, err
	}
	return py.Add(a, b)
}

type buildCase struct {
	e1, e2   *laila.Entry
	manifest *laila.Manifest
}

func setUpBuild(t *testing.T) *buildCase {
	t.Helper()
	must(laila.Get_active_policy())
	laila.Register_constitution(CONSTITUTION_SRC_, combine_)
	t.Cleanup(func() { laila.Unregister_constitution(CONSTITUTION_SRC_) })

	c := &buildCase{}
	c.e1 = must(laila.Entry_constant(10))
	c.e2 = must(laila.Entry_constant(32))
	c.manifest = must(laila.New_Manifest(py.Kw{"data": py.DictOf("a", c.e1, "b", c.e2)}))
	must(must(c.manifest.Memorize()).Wait())
	return c
}

func (c *buildCase) variable(t *testing.T) *laila.Entry {
	t.Helper()
	return must(laila.Variable(py.Kw{"constitution": CONSTITUTION_SRC_, "manifest": c.manifest}))
}

// ---------------------------------------------------------------------------
// TestConstitutionBuild: constitution-driven build on freshly constructed
// entries.
// ---------------------------------------------------------------------------

func TestVariableWithConstitutionIsStagedAndCarriesConstitution(t *testing.T) {
	c := setUpBuild(t)
	entry := c.variable(t)
	assertEq(t, entry.State(), laila.EntryState_STAGED)
	if entry.Constitution() == nil {
		t.Fatal("constitution is None")
	}
	cc, ok := entry.Constitution().(*laila.ComplexConstitution)
	if !ok {
		t.Fatalf("constitution is %T, want *ComplexConstitution", entry.Constitution())
	}
	assertEq(t, *cc.Code(), CONSTITUTION_SRC_)
	assertEq(t, *cc.Manifest_global_id(), c.manifest.Global_id())
}

func TestBuildReturnsFutureAndMaterializesPayload(t *testing.T) {
	c := setUpBuild(t)
	entry := c.variable(t)
	future := must(laila.Build(entry))
	if py.IsNone(future) {
		t.Fatal("build returned None")
	}
	wait(t, future)

	assertEq(t, entry.State(), laila.EntryState_READY)
	assertEq(t, must(entry.Data()), 42)
	if entry.Constitution() != nil {
		t.Fatalf("constitution not cleared: %v", entry.Constitution())
	}
}

func TestBuildViaTaskforceIdString(t *testing.T) {
	c := setUpBuild(t)
	entry := c.variable(t)
	policy := must(laila.Get_active_policy())
	// policy.central.command.alpha_taskforce
	cmd := must(py.Getattr(must(py.Getattr(policy, "central")), "command")).(*laila.LAILA_IDENTIFIABLE_CENTRAL_COMMAND_)
	wait(t, must(laila.Build(entry, py.Kw{"taskforce_id": cmd.Alpha_taskforce})))
	assertEq(t, must(entry.Data()), 42)
}

func TestBuildTwiceIsIdempotent(t *testing.T) {
	c := setUpBuild(t)
	entry := c.variable(t)
	wait(t, must(laila.Build(entry)))
	first := must(entry.Data())
	// A second build of a READY entry is a no-op resolving to the entry.
	again := wait(t, must(laila.Build(entry)))
	assertIs(t, laila.As_entry_(again), entry)
	assertEq(t, must(entry.Data()), first)
}

func TestBuildOfReadyEntryIsNoop(t *testing.T) {
	setUpBuild(t)
	entry := must(laila.Entry_constant("already-built"))
	assertIs(t, laila.As_entry_(wait(t, must(laila.Build(entry)))), entry)
	assertEq(t, must(entry.Data()), "already-built")
}

func TestBuildWithoutConstitutionRaises(t *testing.T) {
	setUpBuild(t)
	entry := must(laila.Contingent(py.Kw{"data": "staged-without-constitution"}))
	fut := must(laila.Build(entry))
	_, err := fut.(waiter).Wait(nil)
	if !errors.Is(err, py.RuntimeError) {
		t.Fatalf("expected RuntimeError, got %v", err)
	}
}

func TestConcurrentBuildsCoalesce(t *testing.T) {
	c := setUpBuild(t)
	src := "def build(m):\n" +
		"    import time\n" +
		"    time.sleep(0.2)\n" +
		"    return 42\n"
	laila.Register_constitution(src, func(any) (any, error) {
		time.Sleep(200 * time.Millisecond)
		return 42, nil
	})
	t.Cleanup(func() { laila.Unregister_constitution(src) })
	entry := must(laila.Variable(py.Kw{"constitution": src, "manifest": c.manifest}))
	futs := make([]any, 0, 6)
	for i := 0; i < 6; i++ {
		futs = append(futs, must(laila.Build(entry)))
	}
	ids := map[any]struct{}{}
	for _, f := range futs {
		ids[f] = struct{}{}
	}
	assertEq(t, len(ids), 1)
	for _, f := range futs {
		wait(t, f)
	}
	assertEq(t, must(entry.Data()), 42)
}

func TestDataRaisesUntilBuilt(t *testing.T) {
	c := setUpBuild(t)
	entry := c.variable(t)
	_, err := entry.Data()
	if !errors.Is(err, laila.EntryNotBuiltError) {
		t.Fatalf("expected EntryNotBuiltError, got %v", err)
	}
	wait(t, must(laila.Build(entry)))
	assertEq(t, must(entry.Data()), 42)
	assertEq(t, entry.State(), laila.EntryState_READY)
	if entry.Constitution() != nil {
		t.Fatalf("constitution not cleared: %v", entry.Constitution())
	}
}

// ---------------------------------------------------------------------------
// TestConstitutionSerializeReadyOnly: a complex entry must be built before
// it can be serialized/memorized.
// ---------------------------------------------------------------------------

type serializeCase struct {
	*buildCase
	transformations *laila.TransformationSequence
}

func setUpSerialize(t *testing.T) *serializeCase {
	t.Helper()
	return &serializeCase{
		buildCase: setUpBuild(t),
		transformations: must(laila.New_TransformationSequence(py.Kw{
			"transformations": []any{must(laila.New_Base64())},
		})),
	}
}

func TestSerializeOnStagedComplexEntryRaises(t *testing.T) {
	c := setUpSerialize(t)
	entry := c.variable(t)
	_, err := entry.Serialize(c.transformations)
	if !errors.Is(err, py.RuntimeError) {
		t.Fatalf("expected RuntimeError, got %v", err)
	}
}

func TestMemorizeOnStagedComplexEntryRaises(t *testing.T) {
	c := setUpSerialize(t)
	entry := c.variable(t)
	ref := must(laila.Memorize(entry))
	if _, err := ref.(waiter).Wait(nil); err == nil {
		t.Fatal("expected an exception from wait()")
	}
}

func TestBuildThenMemorizeThenRememberSucceeds(t *testing.T) {
	c := setUpSerialize(t)
	entry := c.variable(t)
	wait(t, must(laila.Build(entry)))
	assertEq(t, entry.State(), laila.EntryState_READY)
	assertEq(t, must(entry.Data()), 42)

	must(must(laila.Memorize(entry)).(waiter).Wait())

	fut := must(laila.Remember(entry.Global_id(), py.Kw{"persist": false}))
	recovered := wait(t, fut)
	if lst, ok := recovered.(*py.List); ok {
		recovered = must(lst.Get(0))
	}
	rec := laila.As_entry_(recovered)
	assertEq(t, must(rec.Data()), 42)
	assertEq(t, rec.State(), laila.EntryState_READY)
}

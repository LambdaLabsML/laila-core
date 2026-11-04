// Port of tests/functional/pools/data_container/unit_tests/test_data_container.py.
package unit_tests

import (
	"errors"
	"strings"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

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

func assertNotImplemented(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, py.NotImplementedError) {
		t.Fatalf("want NotImplementedError, got %v", err)
	}
}

func newContainer(t *testing.T, kw ...py.Kw) *laila.LAILA_IDENTIFIABLE_DATA_CONTAINER_ {
	t.Helper()
	return must(laila.New_LAILA_IDENTIFIABLE_DATA_CONTAINER_(kw...))
}

// ---- TestDataContainerIsVirtual ----

func TestInstantiableAsAType(t *testing.T) {
	container := newContainer(t)
	if !py.Isinstance(container, laila.LAILA_IDENTIFIABLE_DATA_CONTAINER_class) {
		t.Fatal("isinstance")
	}
}

func TestGetitemRaisesNotImplemented(t *testing.T) {
	_, err := newContainer(t).Getitem("k")
	assertNotImplemented(t, err)
}

func TestSetitemRaisesNotImplemented(t *testing.T) {
	assertNotImplemented(t, newContainer(t).Setitem("k", 1))
}

func TestEmptyRaisesNotImplemented(t *testing.T) {
	assertNotImplemented(t, newContainer(t).Empty())
}

// Half is ``class Half(_LAILA_IDENTIFIABLE_DATA_CONTAINER): pass``.
type Half struct {
	laila.LAILA_IDENTIFIABLE_DATA_CONTAINER_
}

var Half_class = py.NewClass("Half", (*Half)(nil), laila.LAILA_IDENTIFIABLE_DATA_CONTAINER_class)

func TestErrorMessageNamesTheConcreteClass(t *testing.T) {
	h := Half_class.New().(*Half)
	if err := h.Init_(py.Kw{}); err != nil {
		t.Fatal(err)
	}
	_, err := h.Getitem(0)
	assertNotImplemented(t, err)
	if !strings.Contains(err.Error(), "Half") {
		t.Fatalf("message %q should name Half", err.Error())
	}
}

// ---- TestDataContainerIdentity ----

func TestScopeIsDataContainer(t *testing.T) {
	assertEq(t, newContainer(t).Scopes(), []string{laila.DATA_CONTAINER_SCOPE_})
	assertEq(t, laila.DATA_CONTAINER_SCOPE_, "DATA_CONTAINER")
}

func TestGlobalIdCarriesScope(t *testing.T) {
	if !strings.Contains(newContainer(t).Global_id(), laila.DATA_CONTAINER_SCOPE_) {
		t.Fatal("global_id should carry the scope")
	}
}

func TestNicknameDerivedUuidIsDeterministic(t *testing.T) {
	a := newContainer(t, py.Kw{"nickname": "dc-nick"})
	b := newContainer(t, py.Kw{"nickname": "dc-nick"})
	assertEq(t, a.Uuid(), b.Uuid())
}

func TestAtomicLockAvailable(t *testing.T) {
	container := newContainer(t)
	_, r1, err := container.Atomic()
	if err != nil {
		t.Fatal(err)
	}
	defer r1()
	_, r2, err := container.Atomic()
	if err != nil {
		t.Fatal(err)
	}
	defer r2()
}

// ---- TestHierarchy ----

func TestPoolIsADataContainer(t *testing.T) {
	if !laila.LAILA_IDENTIFIABLE_POOL_class.Issubclass(laila.LAILA_IDENTIFIABLE_DATA_CONTAINER_class) {
		t.Fatal("issubclass")
	}
	if !py.Isinstance(must(laila.New_LAILA_IDENTIFIABLE_POOL_()), laila.LAILA_IDENTIFIABLE_DATA_CONTAINER_class) {
		t.Fatal("isinstance")
	}
}

func TestMultibufferIsADataContainer(t *testing.T) {
	if !laila.MultiBuffer_class.Issubclass(laila.LAILA_IDENTIFIABLE_DATA_CONTAINER_class) {
		t.Fatal("issubclass")
	}
	if !py.Isinstance(must(laila.New_MultiBuffer()), laila.LAILA_IDENTIFIABLE_DATA_CONTAINER_class) {
		t.Fatal("isinstance")
	}
}

func TestMultibufferIsNotAPool(t *testing.T) {
	if laila.MultiBuffer_class.Issubclass(laila.LAILA_IDENTIFIABLE_POOL_class) {
		t.Fatal("MultiBuffer must not be a pool")
	}
}

func TestSubclassesKeepTheirOwnScopes(t *testing.T) {
	assertEq(t, must(laila.New_LAILA_IDENTIFIABLE_POOL_()).Scopes(), []string{laila.POOL_SCOPE_})
	assertEq(t, must(laila.New_MultiBuffer()).Scopes(), []string{laila.MULTI_BUFFER_SCOPE_})
	assertEq(t, laila.MULTI_BUFFER_SCOPE_, "MULTI_BUFFER")
}

func TestPoolOverridesAreConcrete(t *testing.T) {
	pool := must(laila.New_LAILA_IDENTIFIABLE_POOL_())
	if err := pool.Setitem("k", "v"); err != nil {
		t.Fatal(err)
	}
	assertEq(t, must(pool.Getitem("k")), "v")
	if err := pool.Empty(); err != nil {
		t.Fatal(err)
	}
	assertEq(t, must(pool.Getitem("k")), nil)
}

func TestDefaultPoolAliasStillPointsAtPool(t *testing.T) {
	if laila.DefaultPool != laila.LAILA_IDENTIFIABLE_POOL_class {
		t.Fatal("DefaultPool")
	}
	if laila.DefaultMultiBuffer != laila.MultiBuffer_class {
		t.Fatal("DefaultMultiBuffer")
	}
}

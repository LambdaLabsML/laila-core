// Go twin of examples/simple/data_types.ipynb: wrap one value of every
// Python data type laila handles as a constant Entry, memorize the lot
// into the active policy's alpha pool, remember each one back and check
// that the round trip preserves both the value and its type.
//
// Python                                   Go
// ------                                   --
// laila.constant(data=42)                  laila.Constant(42)
// laila.constant(data=[1, 2, 3, "four"])   laila.Constant(py.NewList(1, 2, 3, "four"))
// laila.constant(data=("a", "b"))          laila.Constant(py.Tuple{"a", "b"})
// laila.constant(data={"x", "y"})          laila.Constant(py.NewSet("x", "y"))
// laila.constant(data={"name": "laila"})   laila.Constant(py.DictOf("name", "laila"))
// laila.constant(data=np.array(...))       laila.Constant(py.NDArray_float64(shape, data))
// with laila.guarantee: laila.memorize(e)  laila.Guarantee.With(func() error { _, err := laila.Memorize(e); return err })
// laila.remember(gid).data                 laila.Remember(gid) -> Wait -> .Data()
//
// The torch tensor from the notebook is omitted: the port has no torch
// runtime (see README "Deviations").
//
// Run: go run ./examples/data_types
package main

import (
	"fmt"
	"os"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
	"github.com/amirzadehlambda/laila/laila-go/runtime"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	defer laila.Terminate()

	// Every laila verb routes through the active policy; create one
	// explicitly so the example does not depend on laila.json / LAILA_* env.
	policy, err := laila.New_DefaultPolicy()
	if err != nil {
		return err
	}
	laila.Activate_policy(policy)

	// The same sample values as the notebook (minus torch).
	samples := []struct {
		name string
		data any
	}{
		{"int", 42},
		{"float", 3.14159},
		{"str", "hello laila"},
		{"bool", true},
		{"none", nil},
		{"list", py.NewList(1, 2, 3, "four")},
		{"tuple", py.Tuple{"alpha", "beta", "gamma"}},
		{"set", py.NewSet("x", "y", "z")},
		{"dict", py.DictOf("name", "laila", "version", 1, "active", true)},
		{"numpy", py.NDArray_float64([]int{2, 2}, []float64{1, 2, 3, 4})},
	}

	// Wrap each value as a LAILA constant to assign global ids.
	wrapped := map[string]*laila.Entry{}
	for _, s := range samples {
		e, err := laila.Constant(s.data, py.Kw{"nickname": "sample-" + s.name})
		if err != nil {
			return err
		}
		wrapped[s.name] = e
		fmt.Printf("%12s -> %s\n", s.name, e.Global_id())
	}

	// ``with laila.guarantee: laila.memorize(entries=[...])`` -- the
	// guarantee scope waits for every future submitted inside it.
	fmt.Println("\nmemorizing into the alpha pool ...")
	err = laila.Guarantee.With(func() error {
		for _, s := range samples {
			if _, err := laila.Memorize(wrapped[s.name]); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Read every entry back by global id and compare with the original.
	fmt.Println("remembering ...")
	for _, s := range samples {
		fut, err := laila.Remember(wrapped[s.name].Global_id())
		if err != nil {
			return err
		}
		recalled, err := runtime.Wait(fut)
		if err != nil {
			return err
		}
		data, err := recalled.(*laila.Entry).Data()
		if err != nil {
			return err
		}
		same := py.Eq(data, s.data)
		if arr, ok := data.(*py.NDArray); ok {
			same = arr.Eq(s.data.(*py.NDArray))
		}
		fmt.Printf("%12s -> %-14s %s  equal=%v\n", s.name, py.Pytypename(data), py.Repr(data), same)
		if !same {
			return fmt.Errorf("round trip changed %s: %s != %s", s.name, py.Repr(data), py.Repr(s.data))
		}
	}
	fmt.Println("\nall", len(samples), "data types round-tripped through the pool")
	return nil
}

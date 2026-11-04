//go:build workload

// Port of tests/workloads/_w12_workers.py: top-level picklable worker
// functions for the process-pool workloads.
//
// Each function is registered under its Python qualified name
// (``_w12_workers.<name>``) so the parent and the re-executed worker
// binary agree on the task identity, the way a spawned worker re-imports
// the module.
package workloads

import (
	"bytes"
	"fmt"
	"os"
	"syscall"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// W is the ``_w12_workers`` module namespace.
var W = struct {
	Ident, Square, Sleep_then, Pid, Big_bytes, Echo_bytes, Raise_value, Cpu_burn,
	Nested_dict, Numpy_sum, Exit_hard, Segv, Memory_hog, Laila_in_child, Callable,
	Partial_target *py.ProcessTask
}{
	Ident:          laila.Register_process_task("_w12_workers.ident", w12_ident),
	Square:         laila.Register_process_task("_w12_workers.square", w12_square),
	Sleep_then:     laila.Register_process_task("_w12_workers.sleep_then", w12_sleep_then),
	Pid:            laila.Register_process_task("_w12_workers.pid", w12_pid),
	Big_bytes:      laila.Register_process_task("_w12_workers.big_bytes", w12_big_bytes),
	Echo_bytes:     laila.Register_process_task("_w12_workers.echo_bytes", w12_echo_bytes),
	Raise_value:    laila.Register_process_task("_w12_workers.raise_value", w12_raise_value),
	Cpu_burn:       laila.Register_process_task("_w12_workers.cpu_burn", w12_cpu_burn),
	Nested_dict:    laila.Register_process_task("_w12_workers.nested_dict", w12_nested_dict),
	Numpy_sum:      laila.Register_process_task("_w12_workers.numpy_sum", w12_numpy_sum),
	Exit_hard:      laila.Register_process_task("_w12_workers.exit_hard", w12_exit_hard),
	Segv:           laila.Register_process_task("_w12_workers.segv", w12_segv),
	Memory_hog:     laila.Register_process_task("_w12_workers.memory_hog", w12_memory_hog),
	Laila_in_child: laila.Register_process_task("_w12_workers.laila_in_child", w12_laila_in_child),
	Callable:       laila.Register_process_task("_w12_workers.Callable", w12_callable),
	Partial_target: laila.Register_process_task("_w12_workers.partial_target", w12_partial_target),
}

// w12_kw splits a trailing ``py.Kw`` off the positional arguments.
func w12_kw(args []any) ([]any, py.Kw) {
	if n := len(args); n > 0 {
		if kw, ok := args[n-1].(py.Kw); ok {
			return args[:n-1], kw
		}
	}
	return args, nil
}

func w12_ident(args ...any) (any, error) { return args[0], nil }

func w12_square(args ...any) (any, error) {
	x, err := py.Int(args[0])
	if err != nil {
		return nil, err
	}
	return x * x, nil
}

// w12_sleep_then is ``sleep_then(x, s=0.01)``.
func w12_sleep_then(args ...any) (any, error) {
	pos, kw := w12_kw(args)
	s := 0.01
	if len(pos) > 1 {
		s = py.Must(py.Float(pos[1]))
	} else if v, ok := kw["s"]; ok {
		s = py.Must(py.Float(v))
	}
	py.Sleep_sync(s)
	return pos[0], nil
}

func w12_pid(args ...any) (any, error) { return os.Getpid(), nil }

func w12_big_bytes(args ...any) (any, error) {
	n, err := py.Int(args[0])
	if err != nil {
		return nil, err
	}
	return bytes.Repeat([]byte{0x7f}, n), nil
}

func w12_echo_bytes(args ...any) (any, error) { return py.Len(args[0]) }

func w12_raise_value(args ...any) (any, error) {
	return nil, py.ValueError.Newf("proc-boom-%s", py.Str(args[0]))
}

func w12_cpu_burn(args ...any) (any, error) {
	n, err := py.Int(args[0])
	if err != nil {
		return nil, err
	}
	acc := 0
	for i := 0; i < n; i++ {
		acc = (acc*31 + i) & 0xFFFFFFFF
	}
	return acc, nil
}

func w12_nested_dict(args ...any) (any, error) {
	depth, err := py.Int(args[0])
	if err != nil {
		return nil, err
	}
	var d any = py.DictOf("leaf", 1)
	for i := 0; i < depth; i++ {
		d = py.DictOf(fmt.Sprintf("k%d", i), d)
	}
	return d, nil
}

// w12_numpy_sum is ``float(np.arange(n, dtype=np.float64).sum())``.
func w12_numpy_sum(args ...any) (any, error) {
	n, err := py.Int(args[0])
	if err != nil {
		return nil, err
	}
	acc := 0.0
	for i := 0; i < n; i++ {
		acc += float64(i)
	}
	return acc, nil
}

func w12_exit_hard(args ...any) (any, error) {
	code, err := py.Int(args[0])
	if err != nil {
		return nil, err
	}
	py.Exit_(code)
	return nil, nil
}

// w12_segv is ``ctypes.string_at(0)``: the worker dies of SIGSEGV.
func w12_segv(args ...any) (any, error) {
	_ = syscall.Kill(os.Getpid(), syscall.SIGSEGV)
	py.Sleep_sync(5)
	return nil, nil
}

func w12_memory_hog(args ...any) (any, error) {
	mb, err := py.Int(args[0])
	if err != nil {
		return nil, err
	}
	buf := make([]byte, mb*1024*1024)
	return len(buf), nil
}

func w12_laila_in_child(args ...any) (any, error) {
	e, err := laila.Entry_constant(py.DictOf("from", os.Getpid()))
	if err != nil {
		return nil, err
	}
	f, err := laila.Memorize(e)
	if err != nil {
		return nil, err
	}
	if _, err := Wait(f, 30); err != nil {
		return nil, err
	}
	g, err := laila.Remember(e.Global_id())
	if err != nil {
		return nil, err
	}
	out, err := Wait(g, 30)
	if err != nil {
		return nil, err
	}
	out = Unwrap(out)
	laila.Terminate()
	from, err := out.(*py.Dict).Getitem("from")
	if err != nil {
		return nil, err
	}
	return py.Eq(from, os.Getpid()), nil
}

// w12_callable is ``Callable(k)()``: ``k * 3``.
func w12_callable(args ...any) (any, error) {
	k, err := py.Int(args[0])
	if err != nil {
		return nil, err
	}
	return k * 3, nil
}

// w12_partial_target is ``partial_target(a, b)``.
func w12_partial_target(args ...any) (any, error) {
	pos, kw := w12_kw(args)
	var b any
	if len(pos) > 1 {
		b = pos[1]
	} else {
		b = kw["b"]
	}
	return py.Add(pos[0], b)
}

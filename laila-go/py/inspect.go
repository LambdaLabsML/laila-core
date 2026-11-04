package py

import (
	"reflect"
	"runtime"
	"strings"
)

// Qualname is ``fn.__qualname__`` for a Go func value: the function's
// name within its package (``Outer.func1`` for closures, ``T.Method-fm``
// trimmed to ``T.Method`` for bound methods). Non-func values report their
// Python type name, like ``type(obj).__qualname__``.
func Qualname(fn any) string {
	if fn == nil {
		return "NoneType"
	}
	if q, ok := fn.(interface{ Qualname() string }); ok {
		return q.Qualname()
	}
	if bm, ok := fn.(*BoundMethod); ok {
		return Pytypename(bm.Recv) + "." + bm.Name
	}
	rv := reflect.ValueOf(fn)
	if rv.Kind() != reflect.Func {
		return Pytypename(fn)
	}
	rf := runtime.FuncForPC(rv.Pointer())
	if rf == nil {
		return "<lambda>"
	}
	name := rf.Name()
	// strip the package path: "github.com/x/y/pkg.Outer.func1" -> "Outer.func1"
	if i := strings.LastIndex(name, "/"); i >= 0 {
		name = name[i+1:]
	}
	if i := strings.Index(name, "."); i >= 0 {
		name = name[i+1:]
	}
	name = strings.TrimSuffix(name, "-fm")
	name = strings.TrimPrefix(strings.TrimPrefix(name, "(*"), "(")
	name = strings.Replace(name, ")", "", 1)
	return name
}

// Func_name is ``fn.__name__``: the last component of Qualname.
func Func_name(fn any) string {
	q := Qualname(fn)
	if i := strings.LastIndex(q, "."); i >= 0 {
		return q[i+1:]
	}
	return q
}

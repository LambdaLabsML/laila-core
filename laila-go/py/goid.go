//go:build !laila_slowgls

package py

import "github.com/petermattis/goid"

// Goid returns the current goroutine's id. Fast path: an assembly read of
// runtime.g.goid maintained per Go release by petermattis/goid (3 ns on
// Go 1.27); the library itself falls back to runtime.Stack parsing on an
// unknown Go version. Build with -tags laila_slowgls to force the parser.
func Goid() int64 { return goid.Get() }

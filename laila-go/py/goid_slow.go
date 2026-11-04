//go:build laila_slowgls

package py

import (
	"bytes"
	"runtime"
	"strconv"
)

// Goid returns the current goroutine's id by parsing runtime.Stack
// ("goroutine 123 [running]:"). Portable but ~1-7 us per call.
func Goid() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	fields := bytes.Fields(buf[:n])
	if len(fields) < 2 {
		return 0
	}
	id, _ := strconv.ParseInt(string(fields[1]), 10, 64)
	return id
}

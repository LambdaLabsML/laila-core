//go:build !linux

package py

import "os"

// Openpty is ``os.openpty()``; only implemented on Linux in this port.
func Openpty() (master *os.File, slave *os.File, err error) {
	return nil, nil, OSError.New("openpty is not supported on this platform")
}

// Ttyname is ``os.ttyname(fd)``.
func Ttyname(f *os.File) string { return f.Name() }

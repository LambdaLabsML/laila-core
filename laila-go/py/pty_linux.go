//go:build linux

package py

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// pty_linux.go -- ``os.openpty()`` / ``os.ttyname()``: the pseudo-terminal
// helpers the hardware-free serial tests use (a laila UART transport
// opens the *slave* as its port; the test drives the *master*).

// Openpty is ``os.openpty()``: returns the (master, slave) pair. The slave
// file's Name() is its ``/dev/pts/N`` path (see Ttyname).
func Openpty() (master *os.File, slave *os.File, err error) {
	m, err := os.OpenFile("/dev/ptmx", os.O_RDWR|unix.O_NOCTTY, 0)
	if err != nil {
		return nil, nil, OSError.Newf("openpty: %s", err.Error()).From(err)
	}
	// Use the raw fd (not m.Fd(), which would flip the master to blocking
	// mode and make Close unable to wake a parked Read).
	if err := With_raw_fd(m, func(fd int) error { return unix.IoctlSetPointerInt(fd, unix.TIOCSPTLCK, 0) }); err != nil {
		_ = m.Close()
		return nil, nil, OSError.Newf("openpty: unlockpt: %s", err.Error()).From(err)
	}
	var n int
	err = With_raw_fd(m, func(fd int) error { var e error; n, e = unix.IoctlGetInt(fd, unix.TIOCGPTN); return e })
	if err != nil {
		_ = m.Close()
		return nil, nil, OSError.Newf("openpty: ptsname: %s", err.Error()).From(err)
	}
	name := fmt.Sprintf("/dev/pts/%d", n)
	s, err := os.OpenFile(name, os.O_RDWR|unix.O_NOCTTY, 0)
	if err != nil {
		_ = m.Close()
		return nil, nil, OSError.Newf("openpty: %s", err.Error()).From(err)
	}
	return m, s, nil
}

// Ttyname is ``os.ttyname(fd)`` for a file opened by Openpty (or any
// terminal device file): its device path.
func Ttyname(f *os.File) string { return f.Name() }

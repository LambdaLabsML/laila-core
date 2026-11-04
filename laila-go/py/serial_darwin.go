//go:build darwin

package py

import "golang.org/x/sys/unix"

const (
	ioctl_get_termios_ = unix.TIOCGETA
	ioctl_set_termios_ = unix.TIOCSETA
)

func set_baud_(t *unix.Termios, code uint32) {
	t.Ispeed = uint64(code)
	t.Ospeed = uint64(code)
}

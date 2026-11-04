//go:build linux

package py

import "golang.org/x/sys/unix"

const (
	ioctl_get_termios_ = unix.TCGETS
	ioctl_set_termios_ = unix.TCSETS
)

func set_baud_(t *unix.Termios, code uint32) {
	t.Cflag &^= unix.CBAUD
	t.Cflag |= code
	t.Ispeed = code
	t.Ospeed = code
}

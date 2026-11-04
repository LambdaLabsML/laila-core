//go:build linux || darwin

package py

import (
	"os"
	"sync"

	"golang.org/x/sys/unix"
)

// serial_unix.go -- the subset of ``pyserial`` (``serial.Serial``) the
// serial-line transports use: open a tty in raw 8N1 mode at a baud rate,
// non-blocking, plus ``read`` / ``write`` / ``close`` / ``fileno``.
//
// Opening a pty slave (``os.openpty()``) works exactly like a real device,
// which is what the hardware-free serial tests rely on.

// Serial is ``serial.Serial(port, baudrate, timeout=0)``.
type Serial struct {
	Port     string
	Baudrate int

	f    *os.File
	mu   sync.Mutex
	once sync.Once
	// Rs485_mode mirrors ``ser.rs485_mode = RS485Settings()`` (recorded;
	// direction control is driver-level on Linux).
	Rs485_mode bool
}

var baud_table_ = map[int]uint32{
	0: unix.B0, 50: unix.B50, 75: unix.B75, 110: unix.B110, 134: unix.B134, 150: unix.B150,
	200: unix.B200, 300: unix.B300, 600: unix.B600, 1200: unix.B1200, 1800: unix.B1800,
	2400: unix.B2400, 4800: unix.B4800, 9600: unix.B9600, 19200: unix.B19200,
	38400: unix.B38400, 57600: unix.B57600, 115200: unix.B115200, 230400: unix.B230400,
}

// Serial_open is ``serial.Serial(port, baudrate, timeout=0)``: opens the
// device, puts it in raw 8N1 mode at *baudrate* and returns the handle.
// Errors are ``serial.SerialException`` in Python; here OSError.
func Serial_open(port string, baudrate int) (*Serial, error) {
	f, err := os.OpenFile(port, os.O_RDWR|unix.O_NOCTTY, 0)
	if err != nil {
		return nil, OSError.Newf("could not open port %s: %s", Repr(port), err.Error()).From(err)
	}
	s := &Serial{Port: port, Baudrate: baudrate, f: f}
	if err := s.configure_(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return s, nil
}

func (s *Serial) configure_() error {
	// NOTE: never call ``f.Fd()`` here -- it switches the *os.File to
	// blocking mode, after which ``Close`` can no longer wake a goroutine
	// parked in ``Read`` (and a re-opened port reusing the fd number would
	// have its bytes stolen by the stale reader). Use the raw fd via
	// SyscallConn instead, which keeps the file on the runtime poller.
	var t *unix.Termios
	err := With_raw_fd(s.f, func(fd int) error {
		var e error
		t, e = unix.IoctlGetTermios(fd, ioctl_get_termios_)
		return e
	})
	if err != nil {
		return OSError.Newf("could not configure port %s: %s", Repr(s.Port), err.Error()).From(err)
	}
	// cfmakeraw + 8N1, no flow control, local line.
	t.Iflag &^= unix.IGNBRK | unix.BRKINT | unix.PARMRK | unix.ISTRIP | unix.INLCR | unix.IGNCR | unix.ICRNL | unix.IXON | unix.IXOFF | unix.IXANY
	t.Oflag &^= unix.OPOST
	t.Lflag &^= unix.ECHO | unix.ECHONL | unix.ICANON | unix.ISIG | unix.IEXTEN
	t.Cflag &^= unix.CSIZE | unix.PARENB | unix.CSTOPB | unix.CRTSCTS
	t.Cflag |= unix.CS8 | unix.CREAD | unix.CLOCAL
	t.Cc[unix.VMIN] = 1
	t.Cc[unix.VTIME] = 0
	if code, ok := baud_table_[s.Baudrate]; ok {
		set_baud_(t, code)
	}
	if err := With_raw_fd(s.f, func(fd int) error { return unix.IoctlSetTermios(fd, ioctl_set_termios_, t) }); err != nil {
		return OSError.Newf("could not configure port %s: %s", Repr(s.Port), err.Error()).From(err)
	}
	return nil
}

// With_raw_fd runs fn with the file's descriptor *without* taking the
// file out of non-blocking/poller mode (unlike ``(*os.File).Fd``).
func With_raw_fd(f *os.File, fn func(fd int) error) error {
	rc, err := f.SyscallConn()
	if err != nil {
		return err
	}
	var inner error
	if err := rc.Control(func(fd uintptr) { inner = fn(int(fd)) }); err != nil {
		return err
	}
	return inner
}

// Raw_fd returns the descriptor number without switching the file to
// blocking mode (``f.fileno()``).
func Raw_fd(f *os.File) int {
	fd := -1
	_ = With_raw_fd(f, func(n int) error { fd = n; return nil })
	return fd
}

// Fileno is ``ser.fileno()``.
func (s *Serial) Fileno() int { return Raw_fd(s.f) }

// File is the underlying *os.File (pollable; reads unblock on Close).
func (s *Serial) File() *os.File { return s.f }

// Read is ``ser.read(n)`` (blocking until at least one byte or close).
func (s *Serial) Read(p []byte) (int, error) { return s.f.Read(p) }

// Write is ``ser.write(data)``.
func (s *Serial) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.f.Write(p)
}

// Close is ``ser.close()`` (idempotent).
func (s *Serial) Close() error {
	var err error
	s.once.Do(func() { err = s.f.Close() })
	return err
}

// Is_open is ``ser.is_open``.
func (s *Serial) Is_open() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return With_raw_fd(s.f, func(fd int) error { _, e := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); return e }) == nil
}

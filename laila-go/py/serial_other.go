//go:build !linux && !darwin

package py

import "os"

// Serial is unavailable on this platform (``pyserial`` absent).
type Serial struct {
	Port       string
	Baudrate   int
	Rs485_mode bool
}

func Serial_open(port string, baudrate int) (*Serial, error) {
	return nil, OSError.New("serial ports are not supported on this platform")
}
func (s *Serial) Fileno() int                  { return -1 }
func (s *Serial) File() *os.File               { return nil }
func (s *Serial) Read(p []byte) (int, error)   { return 0, OSError.New("closed") }
func (s *Serial) Write(p []byte) (int, error)  { return 0, OSError.New("closed") }
func (s *Serial) Close() error                 { return nil }
func (s *Serial) Is_open() bool                { return false }

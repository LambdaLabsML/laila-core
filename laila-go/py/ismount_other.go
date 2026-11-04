//go:build !unix

package py

// Ismount is os.path.ismount(path); unsupported platforms report false.
func Ismount(path string) bool { return false }

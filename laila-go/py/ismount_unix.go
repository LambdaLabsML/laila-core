//go:build unix

package py

import (
	"os"
	"path/filepath"
	"syscall"
)

// Ismount is os.path.ismount(path): true when path is a mount point (its
// device differs from its parent's, or it is the same inode as its parent
// -- the root of a filesystem).
func Ismount(path string) bool {
	st, err := os.Lstat(path)
	if err != nil {
		return false
	}
	if st.Mode()&os.ModeSymlink != 0 {
		return false
	}
	parent := filepath.Join(path, "..")
	pst, err := os.Lstat(parent)
	if err != nil {
		return false
	}
	s1, ok1 := st.Sys().(*syscall.Stat_t)
	s2, ok2 := pst.Sys().(*syscall.Stat_t)
	if !ok1 || !ok2 {
		return false
	}
	if s1.Dev != s2.Dev {
		return true
	}
	return s1.Ino == s2.Ino
}

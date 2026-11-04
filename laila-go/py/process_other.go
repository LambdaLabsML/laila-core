//go:build !linux

package py

import "os/exec"

// setPdeathsig is Linux-only; other platforms rely on pipe EOF to retire
// orphaned workers.
func setPdeathsig(cmd *exec.Cmd) {}

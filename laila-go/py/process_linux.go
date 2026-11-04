//go:build linux

package py

import (
	"os/exec"
	"syscall"
)

// setPdeathsig asks the kernel to SIGKILL the worker if this process dies;
// multiprocessing workers are daemonic for the same reason.
func setPdeathsig(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Pdeathsig = syscall.SIGKILL
}

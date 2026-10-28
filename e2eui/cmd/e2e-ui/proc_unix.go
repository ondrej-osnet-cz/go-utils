//go:build unix

package main

import (
	"os/exec"
	"syscall"
)

// killProcessGroup starts cmd in its own process group and makes cancelling
// kill the whole group, so the test binary that go test starts stops too.
func killProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
}

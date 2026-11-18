//go:build unix

package experiment

import (
	"os"
	"os/exec"
	"syscall"
)

func prepareProcess(*exec.Cmd) {}

func signalProcess(process *os.Process) error {
	return process.Signal(os.Interrupt)
}

func faultExit(state *os.ProcessState) bool {
	status, ok := state.Sys().(syscall.WaitStatus)
	return ok && status.Signaled() && status.Signal() == syscall.SIGKILL
}

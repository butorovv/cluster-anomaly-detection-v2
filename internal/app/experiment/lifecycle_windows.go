package experiment

import (
	"os"
	"os/exec"
	"syscall"
)

var generateConsoleCtrlEvent = syscall.NewLazyDLL("kernel32.dll").NewProc("GenerateConsoleCtrlEvent")

func prepareProcess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP}
}

func signalProcess(process *os.Process) error {
	// CTRL_BREAK_EVENT is directed to the child's group on the shared console.
	if sent, _, err := generateConsoleCtrlEvent.Call(syscall.CTRL_BREAK_EVENT, uintptr(process.Pid)); sent == 0 {
		return err
	}
	return nil
}

func faultExit(state *os.ProcessState) bool {
	return state.ExitCode() == 1 // os.Process.Kill uses TerminateProcess with code 1.
}

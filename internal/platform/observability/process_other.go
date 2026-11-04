//go:build !windows && !linux

package observability

import "os"

func process() Process { return Process{PID: os.Getpid()} }

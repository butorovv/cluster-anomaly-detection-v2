//go:build linux

package observability

import (
	"os"
	"strconv"
	"strings"
	"syscall"
)

func process() Process {
	p := Process{PID: os.Getpid()}
	var r syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &r) == nil {
		seconds := float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
		p.CPUSeconds = &seconds
	}
	if b, err := os.ReadFile("/proc/self/statm"); err == nil {
		fields := strings.Fields(string(b))
		if len(fields) > 1 {
			if pages, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
				rss := pages * uint64(os.Getpagesize())
				p.RSSBytes = &rss
			}
		}
	}
	return p
}

//go:build windows

package observability

import (
	"golang.org/x/sys/windows"
	"os"
	"unsafe"
)

var memoryInfo = windows.NewLazySystemDLL("psapi.dll").NewProc("GetProcessMemoryInfo")

type memoryCounters struct {
	Size, Faults                                                                                 uint32
	PeakWorkingSet, WorkingSet, PeakPaged, Paged, PeakNonpaged, Nonpaged, Pagefile, PeakPagefile uintptr
}

func process() Process {
	p := Process{PID: os.Getpid()}
	h := windows.CurrentProcess()
	var created, exited, kernel, user windows.Filetime
	if windows.GetProcessTimes(h, &created, &exited, &kernel, &user) == nil {
		seconds := float64((uint64(kernel.HighDateTime)<<32|uint64(kernel.LowDateTime))+(uint64(user.HighDateTime)<<32|uint64(user.LowDateTime))) / 1e7
		p.CPUSeconds = &seconds
	}
	var m memoryCounters
	m.Size = uint32(unsafe.Sizeof(m))
	ok, _, _ := memoryInfo.Call(uintptr(h), uintptr(unsafe.Pointer(&m)), uintptr(m.Size))
	if ok != 0 {
		rss := uint64(m.WorkingSet)
		p.RSSBytes = &rss
	}
	return p
}

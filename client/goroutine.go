package client

import (
	"bytes"
	"runtime"
	"strconv"
)

// goroutineID returns the calling goroutine's id, parsed from its stack header
// ("goroutine 42 [running]:"). It is only used to tell whether a loop-bound
// sender is being used on the event-loop goroutine.
func goroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	field := bytes.TrimPrefix(buf[:n], []byte("goroutine "))
	if i := bytes.IndexByte(field, ' '); i >= 0 {
		field = field[:i]
	}
	id, _ := strconv.ParseUint(string(field), 10, 64)
	return id
}

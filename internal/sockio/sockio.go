// Package sockio is the small set of raw socket calls the client and server
// loops make on a descriptor they poll: non-blocking reads and writes, accept,
// close, and a wake descriptor another goroutine signals to interrupt a wait.
// On Unix it is a thin layer over golang.org/x/sys/unix and a pipe; on Windows
// it calls Winsock directly, and the wake descriptor is a loopback UDP socket,
// since WSAPoll watches only sockets.
package sockio

// Wake is a descriptor the loop polls for readability. Signal makes it
// readable from any goroutine; Drain, on the loop, makes it quiet again.
type Wake struct {
	wake
}

// FD is the descriptor to add to the poller.
func (w *Wake) FD() int { return w.fd() }

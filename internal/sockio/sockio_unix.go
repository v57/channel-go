//go:build !windows

package sockio

import (
	"os"

	"golang.org/x/sys/unix"
)

// SetNonblock puts fd in non-blocking mode.
func SetNonblock(fd int) error { return unix.SetNonblock(fd, true) }

// Read reads from fd; 0 bytes and no error means the peer closed.
func Read(fd int, p []byte) (int, error) { return unix.Read(fd, p) }

// Write writes to fd.
func Write(fd int, p []byte) (int, error) { return unix.Write(fd, p) }

// Close closes fd.
func Close(fd int) error { return unix.Close(fd) }

// WouldBlock reports whether err means a non-blocking call has nothing to do now.
func WouldBlock(err error) bool { return err == unix.EAGAIN || err == unix.EWOULDBLOCK }

// Accept accepts one connection on a non-blocking listening socket.
func Accept(fd int) (int, error) {
	nfd, _, err := unix.Accept(fd)
	return nfd, err
}

// AcceptRetry reports whether an Accept error is transient or about one
// connection only, so the server keeps listening.
func AcceptRetry(err error) bool {
	switch err {
	case unix.EAGAIN, unix.EINTR, unix.ECONNABORTED, unix.EMFILE, unix.ENFILE:
		return true
	}
	return false
}

type wake struct {
	reader *os.File
	writer *os.File
}

// NewWake makes a non-blocking pipe.
func NewWake() (*Wake, error) {
	reader, writer, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	for _, f := range []*os.File{reader, writer} {
		if err := unix.SetNonblock(int(f.Fd()), true); err != nil {
			_ = reader.Close()
			_ = writer.Close()
			return nil, err
		}
	}
	return &Wake{wake{reader: reader, writer: writer}}, nil
}

func (w *wake) fd() int { return int(w.reader.Fd()) }

// Signal makes the wake descriptor readable.
func (w *Wake) Signal() { _, _ = w.writer.Write([]byte{1}) }

// Drain reads what Signal wrote.
func (w *Wake) Drain() {
	var buffer [256]byte
	_, _ = unix.Read(w.fd(), buffer[:])
}

// Close closes both ends.
func (w *Wake) Close() error {
	_ = w.writer.Close()
	return w.reader.Close()
}

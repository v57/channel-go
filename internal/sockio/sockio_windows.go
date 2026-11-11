//go:build windows

package sockio

import (
	"errors"
	"sync"
	"unsafe"

	"golang.org/x/sys/windows"
)

var (
	ws2         = windows.NewLazySystemDLL("ws2_32.dll")
	procIoctl   = ws2.NewProc("ioctlsocket")
	procRecv    = ws2.NewProc("recv")
	procSend    = ws2.NewProc("send")
	procAccept  = ws2.NewProc("accept")
	invalidSock = ^uintptr(0)
)

const (
	fionbio = 0x8004667e
	// SOCKET_ERROR: recv, send, ioctlsocket and WSAPoll return a 32-bit int,
	// so only the low 32 bits of the returned register count.
	socketError  = int32(-1)
	wsaeIntr     = windows.Errno(10004)
	wsaeMFile    = windows.Errno(10024)
	wsaeConnRst  = windows.Errno(10054)
	wsaeNoBufs   = windows.Errno(10055)
	wsaeConnAbrt = windows.Errno(10053)
)

func lastError(errno error) error {
	if e, ok := errno.(windows.Errno); ok && e != 0 {
		return e
	}
	// WSAGetLastError is GetLastError, which Call already read.
	return errors.New("winsock call failed")
}

// SetNonblock puts the socket in non-blocking mode (FIONBIO).
func SetNonblock(fd int) error {
	mode := uint32(1)
	r, _, errno := procIoctl.Call(uintptr(fd), fionbio, uintptr(unsafe.Pointer(&mode)))
	if int32(r) == socketError {
		return lastError(errno)
	}
	return nil
}

// Read receives from the socket; 0 bytes and no error means the peer closed.
func Read(fd int, p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	r, _, errno := procRecv.Call(uintptr(fd), uintptr(unsafe.Pointer(&p[0])), uintptr(len(p)), 0)
	if int32(r) == socketError {
		return -1, lastError(errno)
	}
	return int(int32(r)), nil
}

// Write sends on the socket.
func Write(fd int, p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	r, _, errno := procSend.Call(uintptr(fd), uintptr(unsafe.Pointer(&p[0])), uintptr(len(p)), 0)
	if int32(r) == socketError {
		return -1, lastError(errno)
	}
	return int(int32(r)), nil
}

// Close closes the socket.
func Close(fd int) error { return windows.Closesocket(windows.Handle(fd)) }

// WouldBlock reports whether err is WSAEWOULDBLOCK.
func WouldBlock(err error) bool { return err == windows.WSAEWOULDBLOCK }

// Accept accepts one connection on a non-blocking listening socket. The new
// socket inherits the listener's non-blocking mode.
func Accept(fd int) (int, error) {
	r, _, errno := procAccept.Call(uintptr(fd), 0, 0)
	if r == invalidSock {
		return 0, lastError(errno)
	}
	return int(r), nil
}

// AcceptRetry reports whether an Accept error is transient or about one
// connection only, so the server keeps listening.
func AcceptRetry(err error) bool {
	switch err {
	case windows.WSAEWOULDBLOCK, wsaeIntr, wsaeConnAbrt, wsaeConnRst, wsaeMFile, wsaeNoBufs:
		return true
	}
	return false
}

// The wake descriptor is a UDP socket bound to 127.0.0.1 and connected to
// itself: Signal sends it a datagram, Drain reads them all.
type wake struct {
	sock   windows.Handle
	mu     sync.Mutex
	closed bool
}

// NewWake makes the self-connected loopback UDP socket.
func NewWake() (*Wake, error) {
	sock, err := windows.Socket(windows.AF_INET, windows.SOCK_DGRAM, windows.IPPROTO_UDP)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*Wake, error) {
		_ = windows.Closesocket(sock)
		return nil, err
	}
	if err := windows.Bind(sock, &windows.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}); err != nil {
		return fail(err)
	}
	local, err := windows.Getsockname(sock)
	if err != nil {
		return fail(err)
	}
	if err := windows.Connect(sock, local); err != nil {
		return fail(err)
	}
	if err := SetNonblock(int(sock)); err != nil {
		return fail(err)
	}
	return &Wake{wake{sock: sock}}, nil
}

func (w *wake) fd() int { return int(w.sock) }

// Signal makes the wake socket readable.
func (w *Wake) Signal() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return
	}
	_, _ = Write(int(w.sock), []byte{1})
}

// Drain reads every pending datagram.
func (w *Wake) Drain() {
	var buffer [16]byte
	for i := 0; i < 1024; i++ {
		if _, err := Read(int(w.sock), buffer[:]); err != nil {
			return
		}
	}
}

// Close closes the socket.
func (w *Wake) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	return windows.Closesocket(w.sock)
}

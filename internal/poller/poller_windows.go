//go:build windows

package poller

import (
	"fmt"
	"sync"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

// The Windows poller is level-triggered WSAPoll over the registered sockets,
// the same semantics as the epoll and kqueue pollers (which are also level-
// triggered here). WSAPoll watches only sockets, so every descriptor added
// must be a SOCKET (the wake descriptor is a loopback UDP socket, see sockio).
type wsaPoller struct {
	mu     sync.Mutex
	fds    []pollFD
	index  map[int]int
	closed bool
}

// WSAPOLLFD
type pollFD struct {
	fd      uintptr
	events  int16
	revents int16
}

const (
	pollRdNorm = 0x0100
	pollRdBand = 0x0200
	pollWrNorm = 0x0010
	pollErr    = 0x0001
	pollHup    = 0x0002
	pollNval   = 0x0004
)

var (
	ws2          = windows.NewLazySystemDLL("ws2_32.dll")
	procWSAPoll  = ws2.NewProc("WSAPoll")
	errClosed    = fmt.Errorf("poller closed")
	socketError  = int32(-1) // WSAPoll returns a 32-bit int
	maxWaitSlice = 50 * time.Millisecond
)

func New() (Poller, error) {
	if err := procWSAPoll.Find(); err != nil {
		return nil, err
	}
	// Winsock must be started before any raw socket call. Go's net package does
	// it too, but a loop can run before anything in net has.
	var data windows.WSAData
	if err := windows.WSAStartup(uint32(0x202), &data); err != nil {
		return nil, err
	}
	return &wsaPoller{index: make(map[int]int)}, nil
}

func (p *wsaPoller) Add(fd int, readable bool, writable bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.index[fd]; ok {
		return fmt.Errorf("poller: fd %d already added", fd)
	}
	p.index[fd] = len(p.fds)
	p.fds = append(p.fds, pollFD{fd: uintptr(fd), events: makeEvents(readable, writable)})
	return nil
}

func (p *wsaPoller) Modify(fd int, readable bool, writable bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	i, ok := p.index[fd]
	if !ok {
		return fmt.Errorf("poller: fd %d not added", fd)
	}
	p.fds[i].events = makeEvents(readable, writable)
	return nil
}

func (p *wsaPoller) Delete(fd int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	i, ok := p.index[fd]
	if !ok {
		return nil
	}
	last := len(p.fds) - 1
	if i != last {
		p.fds[i] = p.fds[last]
		p.index[int(p.fds[i].fd)] = i
	}
	p.fds = p.fds[:last]
	delete(p.index, fd)
	return nil
}

func (p *wsaPoller) Wait(timeout time.Duration) ([]Event, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, errClosed
	}
	fds := make([]pollFD, len(p.fds))
	copy(fds, p.fds)
	p.mu.Unlock()
	if len(fds) == 0 {
		if timeout < 0 {
			timeout = maxWaitSlice
		}
		time.Sleep(timeout)
		return nil, nil
	}

	timeoutMS := -1
	if timeout >= 0 {
		timeoutMS = int(timeout / time.Millisecond)
		if timeout > 0 && timeoutMS == 0 {
			timeoutMS = 1
		}
	}
	r, _, errno := procWSAPoll.Call(uintptr(unsafe.Pointer(&fds[0])), uintptr(len(fds)), uintptr(int32(timeoutMS)))
	if int32(r) == socketError {
		if e, ok := errno.(windows.Errno); ok && e == windows.Errno(10004) { // WSAEINTR
			return nil, nil
		}
		return nil, errno
	}
	n := int(int32(r))
	if n <= 0 {
		return nil, nil
	}
	out := make([]Event, 0, n)
	for _, raw := range fds {
		flags := raw.revents
		if flags == 0 {
			continue
		}
		out = append(out, Event{
			FD:       int(raw.fd),
			Readable: flags&(pollRdNorm|pollRdBand|pollHup) != 0,
			Writable: flags&pollWrNorm != 0,
			Error:    flags&(pollErr|pollHup|pollNval) != 0,
		})
	}
	return out, nil
}

func (p *wsaPoller) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	p.fds = nil
	p.index = nil
	return windows.WSACleanup()
}

func makeEvents(readable bool, writable bool) int16 {
	var events int16
	if readable {
		events |= pollRdNorm
	}
	if writable {
		events |= pollWrNorm
	}
	return events
}

//go:build linux

package poller

import (
	"time"

	"golang.org/x/sys/unix"
)

type epollPoller struct {
	fd     int
	events []unix.EpollEvent
}

func New() (Poller, error) {
	fd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		return nil, err
	}
	return &epollPoller{
		fd:     fd,
		events: make([]unix.EpollEvent, 128),
	}, nil
}

func (p *epollPoller) Add(fd int, readable bool, writable bool) error {
	return unix.EpollCtl(p.fd, unix.EPOLL_CTL_ADD, fd, p.makeEvent(fd, readable, writable))
}

func (p *epollPoller) Modify(fd int, readable bool, writable bool) error {
	return unix.EpollCtl(p.fd, unix.EPOLL_CTL_MOD, fd, p.makeEvent(fd, readable, writable))
}

func (p *epollPoller) Delete(fd int) error {
	return unix.EpollCtl(p.fd, unix.EPOLL_CTL_DEL, fd, nil)
}

func (p *epollPoller) Wait(timeout time.Duration) ([]Event, error) {
	timeoutMS := -1
	if timeout >= 0 {
		timeoutMS = int(timeout / time.Millisecond)
		if timeout > 0 && timeoutMS == 0 {
			timeoutMS = 1
		}
	}
	n, err := unix.EpollWait(p.fd, p.events, timeoutMS)
	if err != nil {
		if err == unix.EINTR {
			return nil, nil
		}
		return nil, err
	}
	out := make([]Event, 0, n)
	for _, raw := range p.events[:n] {
		flags := raw.Events
		out = append(out, Event{
			FD:       int(raw.Fd),
			Readable: flags&(unix.EPOLLIN|unix.EPOLLPRI|unix.EPOLLHUP) != 0,
			Writable: flags&unix.EPOLLOUT != 0,
			Error:    flags&(unix.EPOLLERR|unix.EPOLLHUP) != 0,
		})
	}
	return out, nil
}

func (p *epollPoller) Close() error {
	return unix.Close(p.fd)
}

func (p *epollPoller) makeEvent(fd int, readable bool, writable bool) *unix.EpollEvent {
	var events uint32
	if readable {
		events |= unix.EPOLLIN | unix.EPOLLPRI
	}
	if writable {
		events |= unix.EPOLLOUT
	}
	return &unix.EpollEvent{
		Events: events,
		Fd:     int32(fd),
	}
}

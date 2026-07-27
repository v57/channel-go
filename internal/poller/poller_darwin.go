//go:build darwin

package poller

import (
	"time"

	"golang.org/x/sys/unix"
)

type kqueuePoller struct {
	fd     int
	events []unix.Kevent_t
}

func New() (Poller, error) {
	fd, err := unix.Kqueue()
	if err != nil {
		return nil, err
	}
	return &kqueuePoller{
		fd:     fd,
		events: make([]unix.Kevent_t, 128),
	}, nil
}

func (p *kqueuePoller) Add(fd int, readable bool, writable bool) error {
	_, err := unix.Kevent(p.fd, p.makeChanges(fd, readable, writable), nil, nil)
	return err
}

func (p *kqueuePoller) Modify(fd int, readable bool, writable bool) error {
	_, err := unix.Kevent(p.fd, p.makeChanges(fd, readable, writable), nil, nil)
	return err
}

func (p *kqueuePoller) Delete(fd int) error {
	changes := []unix.Kevent_t{
		p.makeDelete(fd, unix.EVFILT_READ),
		p.makeDelete(fd, unix.EVFILT_WRITE),
	}
	_, err := unix.Kevent(p.fd, changes, nil, nil)
	if err == unix.ENOENT {
		return nil
	}
	return err
}

func (p *kqueuePoller) Wait(timeout time.Duration) ([]Event, error) {
	var ts *unix.Timespec
	if timeout >= 0 {
		spec := unix.NsecToTimespec(timeout.Nanoseconds())
		ts = &spec
	}
	n, err := unix.Kevent(p.fd, nil, p.events, ts)
	if err != nil {
		if err == unix.EINTR {
			return nil, nil
		}
		return nil, err
	}
	indexByFD := make(map[int]*Event, n)
	out := make([]Event, 0, n)
	for _, raw := range p.events[:n] {
		fd := int(raw.Ident)
		event := indexByFD[fd]
		if event == nil {
			out = append(out, Event{FD: fd})
			event = &out[len(out)-1]
			indexByFD[fd] = event
		}
		if raw.Filter == unix.EVFILT_READ {
			event.Readable = true
		}
		if raw.Filter == unix.EVFILT_WRITE {
			event.Writable = true
		}
		if raw.Flags&unix.EV_EOF != 0 {
			event.Error = true
			event.Readable = true
		}
		if raw.Flags&unix.EV_ERROR != 0 {
			event.Error = true
		}
	}
	return out, nil
}

func (p *kqueuePoller) Close() error {
	return unix.Close(p.fd)
}

func (p *kqueuePoller) makeChanges(fd int, readable bool, writable bool) []unix.Kevent_t {
	return []unix.Kevent_t{
		p.makeFilter(fd, unix.EVFILT_READ, readable),
		p.makeFilter(fd, unix.EVFILT_WRITE, writable),
	}
}

func (p *kqueuePoller) makeFilter(fd int, filter int16, enabled bool) unix.Kevent_t {
	flags := uint16(unix.EV_ADD)
	if enabled {
		flags |= unix.EV_ENABLE
	} else {
		flags |= unix.EV_DISABLE
	}
	return unix.Kevent_t{
		Ident:  uint64(fd),
		Filter: filter,
		Flags:  flags,
	}
}

func (p *kqueuePoller) makeDelete(fd int, filter int16) unix.Kevent_t {
	return unix.Kevent_t{
		Ident:  uint64(fd),
		Filter: filter,
		Flags:  unix.EV_DELETE,
	}
}

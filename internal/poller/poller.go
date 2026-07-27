package poller

import "time"

type Event struct {
	FD       int
	Readable bool
	Writable bool
	Error    bool
}

type Poller interface {
	Add(fd int, readable bool, writable bool) error
	Modify(fd int, readable bool, writable bool) error
	Delete(fd int) error
	Wait(timeout time.Duration) ([]Event, error)
	Close() error
}

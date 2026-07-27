package channelgo

import (
	"context"
	"sync"
)

// Body is passed to post and stream handlers.
type Body struct {
	Body    any
	Sender  *Sender
	State   any
	Path    string
	Context any
	Task    *Task
}

// Function handles request/response calls.
type Function func(body Body) (any, error)

// StreamEmitter emits stream values to the remote caller.
type StreamEmitter interface {
	Emit(value any) error
	IsCancelled() bool
}

// StreamFunction handles stream calls by emitting values synchronously.
type StreamFunction func(body Body, emit StreamEmitter) error

// EventBody receives event payloads for subscriptions.
type EventBody func(body any)

// StreamIterator is the stream abstraction used by channel.
type StreamIterator interface {
	Next(ctx context.Context) (value any, done bool, err error)
	Return() error
}

// Cancellable is returned by subscription APIs.
type Cancellable interface {
	Cancel()
}

// SendingRequest is a request handle with explicit cancellation.
type SendingRequest struct {
	once     sync.Once
	done     chan struct{}
	cancelFn func()
	value    any
	err      error
}

func (r *SendingRequest) resolve(value any, err error) {
	r.once.Do(func() {
		r.value = value
		r.err = err
		close(r.done)
	})
}

func (r *SendingRequest) Wait() (any, error) {
	<-r.done
	return r.value, r.err
}

func (r *SendingRequest) Cancel() {
	if r.cancelFn != nil {
		r.cancelFn()
	}
}

// IteratorValue is returned by Values.Next.
type IteratorValue struct {
	Value any
	Done  bool
}

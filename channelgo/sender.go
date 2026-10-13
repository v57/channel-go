package channelgo

import (
	"context"
	"fmt"
	"sync"
)

type connectionInterface interface {
	Send(body any) int
	Sent(id int)
	Cancel(id int) bool
	Notify(body any)
	AddTopic(topic string, event EventBody) func() bool
	Stop()
}

type waitConnection interface {
	InLoop() bool
	Wait(ctx context.Context, ready func() bool) error
}

// closedConnection lets process transports fail fast after Stop without
// invoking the handler (the honest equivalent of the old inline fast path:
// it checks connection state only, never bypasses request routing).
type closedConnection interface {
	SendClosed() bool
}

// Sender is a remote execution handle used by handlers and clients.
type Sender struct {
	streams    *iteratorStore
	requests   *taskStore
	ch         *Channel
	connection connectionInterface
}

func MakeSender(ch *Channel, connection connectionInterface) *Sender {
	return &Sender{
		streams:    newIteratorStore(),
		requests:   newTaskStore(),
		ch:         ch,
		connection: connection,
	}
}

func (s *Sender) Send(path string, body any, contextValue ...any) (any, error) {
	if closed, ok := s.connection.(closedConnection); ok && closed.SendClosed() {
		return nil, fmt.Errorf("process connection is closed")
	}

	request := s.Request(path, body, contextValue...)
	if waiter, ok := s.connection.(waitConnection); ok && waiter.InLoop() {
		if err := waiter.Wait(context.Background(), func() bool { return isDone(request.done) }); err != nil {
			return nil, err
		}
		return request.value, request.err
	}
	return request.Wait()
}

// SendUntracked sends a request without storing local response state.
func (s *Sender) SendUntracked(path string, body any, contextValue ...any) {
	var contextBody any
	if len(contextValue) > 0 {
		contextBody = contextValue[0]
	}

	// No id: the receiver runs the handler without replying, as the
	// TypeScript channel does. An id from a separate counter used to collide
	// with tracked requests, which then received this message's reply.
	_ = s.connection.Send(Message{
		Path:    strPtr(path),
		Body:    body,
		Context: contextBody,
	})
}

// SendAsync issues a request without blocking the caller and invokes onDone when a response arrives.
func (s *Sender) SendAsync(path string, body any, onDone func(error), contextValue ...any) {
	var contextBody any
	if len(contextValue) > 0 {
		contextBody = contextValue[0]
	}

	request := s.ch.MakeRequest(path, body, contextBody, func(message Message) {
		if onDone == nil {
			return
		}
		if message.Error != nil {
			onDone(asError(message.Error))
			return
		}
		onDone(nil)
	}, s)
	_ = s.connection.Send(request)
}

func (s *Sender) Request(path string, body any, contextValue ...any) *SendingRequest {
	var contextBody any
	if len(contextValue) > 0 {
		contextBody = contextValue[0]
	}

	response := &SendingRequest{done: make(chan struct{})}
	state := struct {
		sync.Mutex
		transportID int
		rid         int
		completed   bool
	}{
		transportID: -1,
		rid:         -1,
	}

	request := s.ch.MakeRequest(path, body, contextBody, func(message Message) {
		if message.Error != nil {
			response.resolve(nil, asError(message.Error))
		} else {
			response.resolve(message.Body, nil)
		}
		state.Lock()
		state.completed = true
		transportID := state.transportID
		state.transportID = -1
		state.rid = -1
		state.Unlock()
		if transportID >= 0 {
			s.connection.Sent(transportID)
		}
	}, s)

	if request.ID != nil {
		state.rid = *request.ID
	}
	transportID := s.connection.Send(request)
	state.Lock()
	completed := state.completed
	if !completed {
		state.transportID = transportID
	}
	state.Unlock()
	if completed && transportID >= 0 {
		s.connection.Sent(transportID)
	}

	response.cancelFn = func() {
		state.Lock()
		rid := state.rid
		if rid < 0 {
			state.Unlock()
			return
		}
		transportID := state.transportID
		state.completed = true
		state.transportID = -1
		state.rid = -1
		state.Unlock()
		if transportID >= 0 {
			cancelled := s.connection.Cancel(transportID)
			if !cancelled {
				s.connection.Notify(Message{Cancel: intPtr(rid)})
			}
		} else {
			s.connection.Notify(Message{Cancel: intPtr(rid)})
		}
		s.ch.Cancel(rid)
		response.resolve(nil, fmt.Errorf("cancelled"))
	}
	return response
}

func (s *Sender) Values(path string, body any, contextValue ...any) *Values {
	var contextBody any
	if len(contextValue) > 0 {
		contextBody = contextValue[0]
	}
	return &Values{
		ch:      s.ch,
		path:    path,
		body:    body,
		context: contextBody,
		sender:  s,
		onSend: func(message any) int {
			return s.connection.Send(message)
		},
		onCancel: func(rid int, transportID int, hasTransportID bool) {
			if hasTransportID {
				if !s.connection.Cancel(transportID) {
					s.connection.Send(Message{Cancel: intPtr(rid)})
				}
			} else {
				s.connection.Send(Message{Cancel: intPtr(rid)})
			}
			s.ch.Cancel(rid)
		},
		waiter: func() waitConnection {
			if waiter, ok := s.connection.(waitConnection); ok {
				return waiter
			}
			return nil
		}(),
	}
}

func (s *Sender) Subscribe(path string, body any, event EventBody, contextValue ...any) (Cancellable, error) {
	var contextBody any
	if len(contextValue) > 0 {
		contextBody = contextValue[0]
	}

	result := make(chan struct {
		cancel Cancellable
		err    error
	}, 1)

	request := s.ch.MakeSubscription(path, body, contextBody, func(message Message) {
		if message.Error != nil {
			result <- struct {
				cancel Cancellable
				err    error
			}{err: asError(message.Error)}
			return
		}
		topic := ""
		if message.Topic != nil {
			topic = *message.Topic
		}
		remove := s.connection.AddTopic(topic, event)
		result <- struct {
			cancel Cancellable
			err    error
		}{cancel: &cancellableFunc{fn: func() {
			if remove() {
				s.connection.Notify(Message{Unsub: strPtr(topic)})
			}
		}}}
	}, s)
	_ = s.connection.Send(request)

	if waiter, ok := s.connection.(waitConnection); ok && waiter.InLoop() {
		if err := waiter.Wait(context.Background(), func() bool { return len(result) > 0 }); err != nil {
			return nil, err
		}
	}
	value := <-result
	return value.cancel, value.err
}

func (s *Sender) Stop() {
	s.connection.Stop()
}

func (s *Sender) RequestsSize() int {
	return s.requests.Size()
}

func (s *Sender) StreamsSize() int {
	return s.streams.Size()
}

type cancellableFunc struct {
	once sync.Once
	fn   func()
}

func (c *cancellableFunc) Cancel() {
	if c == nil {
		return
	}
	c.once.Do(func() {
		if c.fn != nil {
			c.fn()
		}
	})
}

func asError(value any) error {
	if value == nil {
		return nil
	}
	if err, ok := value.(error); ok {
		return err
	}
	return fmt.Errorf("%v", value)
}

// Values reads stream responses lazily and supports cancellation.
type Values struct {
	ch       *Channel
	path     string
	body     any
	context  any
	sender   *Sender
	onSend   func(any) int
	onCancel func(rid int, transportID int, hasTransportID bool)
	waiter   waitConnection

	mu             sync.Mutex
	isRunning      bool
	cancelled      bool
	pending        []chan Message
	queued         []Message
	rid            int
	hasRID         bool
	transportID    int
	hasTransportID bool
}

func (v *Values) start() {
	v.mu.Lock()
	if v.isRunning || v.cancelled {
		v.mu.Unlock()
		return
	}
	v.isRunning = true

	request := v.ch.MakeStream(v.path, v.body, v.context, func(message Message) {
		v.mu.Lock()
		if v.cancelled {
			v.mu.Unlock()
			return
		}
		if len(v.pending) > 0 {
			next := v.pending[0]
			v.pending = v.pending[1:]
			v.mu.Unlock()
			next <- message
			close(next)
			return
		}
		v.queued = append(v.queued, message)
		v.mu.Unlock()
	}, v.sender)
	if request.ID != nil {
		v.rid = *request.ID
		v.hasRID = true
	}
	v.mu.Unlock()

	transportID := v.onSend(request)
	v.mu.Lock()
	v.transportID = transportID
	v.hasTransportID = transportID >= 0
	v.mu.Unlock()
}

func (v *Values) processResponse(response Message) (IteratorValue, error) {
	if response.Error != nil {
		return IteratorValue{}, asError(response.Error)
	}
	done := response.Done != nil && *response.Done
	return IteratorValue{Value: response.Body, Done: done}, nil
}

func (v *Values) Next(ctx context.Context) (IteratorValue, error) {
	v.mu.Lock()
	if v.cancelled {
		v.mu.Unlock()
		return IteratorValue{Done: true}, nil
	}
	if len(v.queued) > 0 {
		response := v.queued[0]
		v.queued = v.queued[1:]
		v.mu.Unlock()
		return v.processResponse(response)
	}

	responseCh := make(chan Message, 1)
	v.pending = append(v.pending, responseCh)
	v.mu.Unlock()

	v.start()

	if v.waiter != nil && v.waiter.InLoop() {
		if err := v.waiter.Wait(ctx, func() bool { return len(responseCh) > 0 }); err != nil {
			return IteratorValue{}, err
		}
		response := <-responseCh
		return v.processResponse(response)
	}

	select {
	case <-ctx.Done():
		return IteratorValue{}, ctx.Err()
	case response := <-responseCh:
		return v.processResponse(response)
	}
}

func isDone(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func (v *Values) Return() {
	v.mu.Lock()
	if v.cancelled {
		v.mu.Unlock()
		return
	}
	v.cancelled = true
	if !v.hasRID {
		pending := append([]chan Message{}, v.pending...)
		v.pending = nil
		v.mu.Unlock()
		for _, wait := range pending {
			wait <- Message{Done: boolPtr(true)}
			close(wait)
		}
		return
	}
	rid := v.rid
	transportID := v.transportID
	hasTransportID := v.hasTransportID
	pending := append([]chan Message{}, v.pending...)
	v.pending = nil
	v.hasRID = false
	v.mu.Unlock()

	v.onCancel(rid, transportID, hasTransportID)
	for _, wait := range pending {
		wait <- Message{Done: boolPtr(true)}
		close(wait)
	}
}

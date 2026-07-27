package process

import (
	"sync"
	"sync/atomic"

	"github.com/v57/channel-go/channelgo"
)

// Connect creates an in-process sender that loops requests into the same channel.
func Connect(ch *channelgo.Channel) *channelgo.Sender {
	connection := &loopConnection{ch: ch}
	sender := channelgo.MakeSender(ch, connection)
	state := map[string]any{}
	var controller *channelgo.Controller

	controller = &channelgo.Controller{
		ResponseFn: func(body channelgo.Message) {
			ch.Receive(body, controller)
		},
		SubscribeFn:   func(string) {},
		UnsubscribeFn: func(string) {},
		EventFn:       func(string, any) {},
		SenderValue:   sender,
		StateValue:    state,
	}
	connection.controller.Store(controller)
	connection.sender = sender
	return sender
}

type loopConnection struct {
	mu         sync.Mutex
	ch         *channelgo.Channel
	controller atomic.Pointer[channelgo.Controller]
	sender     *channelgo.Sender
	closed     atomic.Bool
}

func (i *loopConnection) Send(body any) int {
	if i.closed.Load() {
		return -1
	}
	controller := i.controller.Load()
	ch := i.ch
	if controller == nil || ch == nil {
		return -1
	}
	ch.Receive(body, controller)
	return 0
}

func (i *loopConnection) Sent(int) {}

func (i *loopConnection) Cancel(int) bool {
	return false
}

func (i *loopConnection) Notify(body any) {
	if i.closed.Load() {
		return
	}
	controller := i.controller.Load()
	ch := i.ch
	if controller == nil || ch == nil {
		return
	}
	ch.Receive(body, controller)
}

func (i *loopConnection) LocalController() *channelgo.Controller {
	if i.closed.Load() {
		return nil
	}
	return i.controller.Load()
}

func (i *loopConnection) AddTopic(string, channelgo.EventBody) func() bool {
	return func() bool { return false }
}

func (i *loopConnection) Stop() {
	i.mu.Lock()
	if i.closed.Load() {
		i.mu.Unlock()
		return
	}
	i.closed.Store(true)
	controller := i.controller.Swap(nil)
	ch := i.ch
	sender := i.sender
	i.mu.Unlock()

	if controller != nil && ch != nil && sender != nil {
		ch.Disconnect(controller.State(), sender)
	}
}

package process

import (
	"context"
	"testing"
	"time"

	"github.com/v57/channel-go/channelgo"
)

func waitFor(t *testing.T, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition was not met before timeout")
}

func TestProcessStopClearsPendingRequests(t *testing.T) {
	ch := channelgo.NewChannel().Post("wait", func(body channelgo.Body) (any, error) {
		done := make(chan struct{})
		body.Task.OnCancel(func() {
			close(done)
		})
		<-done
		return nil, nil
	})
	sender := Connect(ch)
	done := make(chan struct{}, 10)

	for i := 0; i < 10; i++ {
		go func() {
			sender.Request("wait", nil)
			done <- struct{}{}
		}()
	}

	waitFor(t, func() bool {
		return sender.RequestsSize() > 0 && ch.RequestsSize() > 0
	})

	sender.Stop()

	waitFor(t, func() bool {
		return sender.RequestsSize() == 0 && ch.RequestsSize() == 0
	})

	for i := 0; i < 10; i++ {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("request goroutine did not finish")
		}
	}
}

func TestValuesReturnClearsPendingStreamRequestEvenWithoutTransportCancelSupport(t *testing.T) {
	nextID := 100
	transportSendBodies := make([]any, 0)
	transportCancelIDs := make([]int, 0)

	ch := channelgo.NewChannel()
	stuckSender := channelgo.MakeSender(ch, &stuckConnection{
		send: func(body any) int {
			transportSendBodies = append(transportSendBodies, body)
			id := nextID
			nextID++
			return id
		},
		sent: func(id int) {
			_ = id
		},
		cancel: func(id int) bool {
			transportCancelIDs = append(transportCancelIDs, id)
			return false
		},
		notify: func(body any) {
			_ = body
		},
		addTopic: func(topic string, event channelgo.EventBody) func() bool {
			_ = topic
			_ = event
			return func() bool { return true }
		},
		stop: func() {},
	})

	values := stuckSender.Values("stream/stuck", nil)
	go func() {
		_, _ = values.Next(context.Background())
	}()

	waitFor(t, func() bool {
		return ch.RequestsSize() == 1
	})

	values.Return()

	waitFor(t, func() bool {
		if ch.RequestsSize() != 0 {
			return false
		}
		if len(transportCancelIDs) != 1 || transportCancelIDs[0] != 100 {
			return false
		}
		for _, body := range transportSendBodies {
			if message, ok := body.(channelgo.Message); ok && message.Cancel != nil {
				return true
			}
		}
		return false
	})
}

type stuckConnection struct {
	send     func(any) int
	sent     func(int)
	cancel   func(int) bool
	notify   func(any)
	addTopic func(string, channelgo.EventBody) func() bool
	stop     func()
}

func (c *stuckConnection) Send(body any) int {
	return c.send(body)
}

func (c *stuckConnection) Sent(id int) {
	c.sent(id)
}

func (c *stuckConnection) Cancel(id int) bool {
	return c.cancel(id)
}

func (c *stuckConnection) Notify(body any) {
	c.notify(body)
}

func (c *stuckConnection) AddTopic(topic string, event channelgo.EventBody) func() bool {
	return c.addTopic(topic, event)
}

func (c *stuckConnection) Stop() {
	c.stop()
}

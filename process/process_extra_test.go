package process

import (
	"testing"
	"time"

	"github.com/v57/channel-go/channelgo"
)

func TestLoopConnectionHelpers(t *testing.T) {
	ch := channelgo.NewChannel().Post("echo", func(body channelgo.Body) (any, error) {
		return body.Body, nil
	})
	sender := Connect(ch)
	defer sender.Stop()

	// exercising Notify branch via loop transport.
	value, err := sender.Send("echo", "x")
	if err != nil || value != "x" {
		t.Fatalf("unexpected process send result: %v %v", value, err)
	}

	lc := &loopConnection{}
	lc.Sent(1)
	if lc.Cancel(1) {
		t.Fatalf("loop cancel should be false")
	}
	if removed := lc.AddTopic("x", func(any) {})(); removed {
		t.Fatalf("loop add topic cancellation should be false")
	}

	sender.Stop() // second stop should hit already-closed branch in loopConnection.Stop
	sender.Stop()
}

func TestLoopConnectionSendNotifyGuardsAndActivePath(t *testing.T) {
	lc := &loopConnection{}
	if id := lc.Send(channelgo.Message{}); id != -1 {
		t.Fatalf("expected send to fail when loop is inactive")
	}
	lc.Notify(channelgo.Message{})

	handled := 0
	ch := channelgo.NewChannel().Post("x", func(channelgo.Body) (any, error) {
		handled++
		return "ok", nil
	})
	sender := channelgo.MakeSender(ch, &stuckConnection{
		send:     func(any) int { return 0 },
		sent:     func(int) {},
		cancel:   func(int) bool { return false },
		notify:   func(any) {},
		addTopic: func(string, channelgo.EventBody) func() bool { return func() bool { return true } },
		stop:     func() {},
	})
	p := "x"
	id := 1
	lc = &loopConnection{ch: ch}
	lc.controller.Store(&channelgo.Controller{
		ResponseFn:    func(channelgo.Message) {},
		SubscribeFn:   func(string) {},
		UnsubscribeFn: func(string) {},
		EventFn:       func(string, any) {},
		SenderValue:   sender,
		StateValue:    map[string]any{},
	})
	lc.Notify(channelgo.Message{ID: &id, Path: &p})
	waitFor(t, func() bool {
		return handled > 0
	})
	if handled == 0 {
		t.Fatalf("expected notify to route active message")
	}

	lc.closed.Store(true)
	lc.Notify(channelgo.Message{ID: &id, Path: &p})
}

func TestProcessSendFastPathSuccess(t *testing.T) {
	ch := channelgo.NewChannel().Post("ok", func(body channelgo.Body) (any, error) {
		return body.Body, nil
	})
	sender := Connect(ch)
	defer sender.Stop()

	value, err := sender.Send("ok", "payload")
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if value != "payload" {
		t.Fatalf("expected payload response, got %v", value)
	}
}

func TestProcessSendFastPathMissingRoute(t *testing.T) {
	sender := Connect(channelgo.NewChannel())
	defer sender.Stop()

	_, err := sender.Send("missing", nil)
	if err == nil || err.Error() != "api not found" {
		t.Fatalf("expected api not found error, got %v", err)
	}
}

func TestProcessSendFastPathPanicReturnsError(t *testing.T) {
	ch := channelgo.NewChannel().Post("panic", func(channelgo.Body) (any, error) {
		panic("boom")
	})
	sender := Connect(ch)
	defer sender.Stop()

	_, err := sender.Send("panic", nil)
	if err == nil || err.Error() != "boom" {
		t.Fatalf("expected panic message as error, got %v", err)
	}
}

func TestProcessSendAfterStopReturnsQuickly(t *testing.T) {
	ch := channelgo.NewChannel().Post("ok", func(channelgo.Body) (any, error) {
		return 1, nil
	})
	sender := Connect(ch)
	sender.Stop()

	done := make(chan error, 1)
	go func() {
		_, err := sender.Send("ok", nil)
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil || err.Error() != "process connection is closed" {
			t.Fatalf("expected process closed error, got %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("send blocked after stop")
	}
}

func TestProcessSendContextAndTask(t *testing.T) {
	var defaultContext any = "unset"
	var providedContext any
	var defaultTask *channelgo.Task
	var providedTask *channelgo.Task
	expectedContext := &struct {
		K string
	}{
		K: "v",
	}

	ch := channelgo.NewChannel().
		Post("default", func(body channelgo.Body) (any, error) {
			defaultContext = body.Context
			defaultTask = body.Task
			return "ok", nil
		}).
		Post("provided", func(body channelgo.Body) (any, error) {
			providedContext = body.Context
			providedTask = body.Task
			return "ok", nil
		})

	sender := Connect(ch)
	defer sender.Stop()

	if _, err := sender.Send("default", nil); err != nil {
		t.Fatalf("default send failed: %v", err)
	}
	if _, err := sender.Send("provided", nil, expectedContext); err != nil {
		t.Fatalf("provided send failed: %v", err)
	}

	// Honest process path: default context is an empty map (the receive-path
	// default), provided context passes through unchanged, and both requests
	// carry a real cancellable task (the full request/response machinery).
	if defaultContext == nil {
		t.Fatalf("expected default context to be an empty map, got nil")
	}
	if providedContext != expectedContext {
		t.Fatalf("expected provided context to pass through unchanged")
	}
	if defaultTask == nil || providedTask == nil {
		t.Fatalf("expected non-nil task for process requests, got default=%v provided=%v", defaultTask, providedTask)
	}
}

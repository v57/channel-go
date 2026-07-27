package channelgo

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

type fakeConnection struct {
	mu                 sync.Mutex
	nextID             int
	sendReturnsMissing bool
	cancelResult       bool
	sentIDs            []int
	cancelIDs          []int
	notifyBodies       []any
}

func (c *fakeConnection) Send(body any) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sendReturnsMissing {
		return -1
	}
	id := c.nextID
	c.nextID++
	_ = body
	return id
}

func (c *fakeConnection) Sent(id int) {
	c.mu.Lock()
	c.sentIDs = append(c.sentIDs, id)
	c.mu.Unlock()
}

func (c *fakeConnection) Cancel(id int) bool {
	c.mu.Lock()
	c.cancelIDs = append(c.cancelIDs, id)
	result := c.cancelResult
	c.mu.Unlock()
	return result
}

func (c *fakeConnection) Notify(body any) {
	c.mu.Lock()
	c.notifyBodies = append(c.notifyBodies, body)
	c.mu.Unlock()
}

func (c *fakeConnection) AddTopic(topic string, event EventBody) func() bool {
	_ = topic
	_ = event
	return func() bool { return true }
}

func (c *fakeConnection) Stop() {}

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

func firstRequestID(ch *Channel) int {
	ch.mu.RLock()
	defer ch.mu.RUnlock()
	for key := range ch.requests {
		return key
	}
	return -1
}

func TestPostOtherAndStreamOtherRouteDynamicPaths(t *testing.T) {
	ch := NewChannel()
	ch.PostOther(func(path string) bool { return len(path) >= 8 && path[:8] == "dynamic/" }, func(body Body) (any, error) {
		return fmt.Sprintf("%s:%v", body.Path, body.Body), nil
	})
	ch.StreamOther(func(path string) bool { return len(path) >= 7 && path[len(path)-7:] == "/stream" }, func(body Body, emit StreamEmitter) error {
		return emit.Emit(body.Body)
	})

	sender := MakeSender(ch, &fakeConnection{nextID: 1})
	responses := make([]Message, 0)
	mu := sync.Mutex{}
	controller := &Controller{
		ResponseFn: func(response Message) {
			mu.Lock()
			responses = append(responses, response)
			mu.Unlock()
		},
		SubscribeFn:   func(string) {},
		UnsubscribeFn: func(string) {},
		EventFn:       func(string, any) {},
		SenderValue:   sender,
		StateValue:    map[string]any{},
	}

	ch.ReceiveOne(Message{ID: intPtr(1), Path: strPtr("dynamic/echo"), Body: "ok"}, controller)
	ch.ReceiveOne(Message{ID: intPtr(2), Stream: strPtr("dynamic/stream"), Body: 7}, controller)

	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		if len(responses) < 3 {
			return false
		}
		havePost := false
		haveStream := false
		haveDone := false
		for _, response := range responses {
			if response.ID != nil && *response.ID == 1 && response.Body == "dynamic/echo:ok" {
				havePost = true
			}
			if response.ID != nil && *response.ID == 2 && response.Body == 7 {
				haveStream = true
			}
			if response.ID != nil && *response.ID == 2 && response.Done != nil && *response.Done {
				haveDone = true
			}
		}
		return havePost && haveStream && haveDone
	})
}

func TestDisconnectRunsCleanupCallbacksAndClearsPendingMaps(t *testing.T) {
	disconnected := 0
	ch := NewChannel().OnDisconnect(func(state any, sender *Sender) {
		disconnected++
		panic("ignored")
	})

	sender := MakeSender(ch, &fakeConnection{nextID: 1})

	closedStreams := 0
	sender.streams.Set(1, &FuncIterator{
		NextFn: func(ctx context.Context) (any, bool, error) {
			_ = ctx
			return nil, true, nil
		},
		ReturnFn: func() error {
			closedStreams++
			return nil
		},
	})

	cancelActions := 0
	task := newTask(nil)
	task.OnCancel(func() {
		cancelActions++
	})
	task.OnCancel(func() {
		cancelActions++
		panic("ignored")
	})
	sender.requests.Set(10, task)

	ch.MakeRequest("pending", nil, nil, func(Message) {}, sender)
	ch.Disconnect(map[string]any{}, sender)

	if disconnected != 1 {
		t.Fatalf("expected disconnect callback once, got %d", disconnected)
	}
	if closedStreams != 1 {
		t.Fatalf("expected stream return once, got %d", closedStreams)
	}
	if cancelActions != 2 {
		t.Fatalf("expected two cancel callbacks, got %d", cancelActions)
	}
	if sender.StreamsSize() != 0 {
		t.Fatalf("expected sender streams to be empty")
	}
	if sender.RequestsSize() != 0 {
		t.Fatalf("expected sender requests to be empty")
	}
	if ch.RequestsSize() != 0 {
		t.Fatalf("expected channel requests to be empty")
	}
}

func TestCancelRemovesRequestByID(t *testing.T) {
	ch := NewChannel()
	request := ch.MakeRequest("hello", nil, nil, func(Message) {}, nil)
	if ch.RequestsSize() != 1 {
		t.Fatalf("expected one request")
	}
	ch.Cancel(*request.ID)
	if ch.RequestsSize() != 0 {
		t.Fatalf("expected request to be removed")
	}
}

func TestReceiveOneHandlesAsyncPostRejection(t *testing.T) {
	ch := NewChannel().Post("fails", func(Body) (any, error) {
		return nil, fmt.Errorf("boom")
	})
	sender := MakeSender(ch, &fakeConnection{nextID: 1})
	responses := make([]Message, 0)
	mu := sync.Mutex{}
	controller := &Controller{
		ResponseFn: func(response Message) {
			mu.Lock()
			responses = append(responses, response)
			mu.Unlock()
		},
		SubscribeFn:   func(string) {},
		UnsubscribeFn: func(string) {},
		EventFn:       func(string, any) {},
		SenderValue:   sender,
		StateValue:    map[string]any{},
	}

	ch.ReceiveOne(Message{ID: intPtr(1), Path: strPtr("fails")}, controller)

	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, response := range responses {
			if response.ID != nil && *response.ID == 1 {
				return fmt.Sprintf("%v", response.Error) == "boom"
			}
		}
		return false
	})
}

func TestReceiveOneCancelRunsEveryTaskOnCancelAction(t *testing.T) {
	onCancelCalls := make([]int, 0)
	done := make(chan struct{})
	requestFinished := make(chan struct{})

	ch := NewChannel().Post("wait", func(body Body) (any, error) {
		body.Task.OnCancel(func() { onCancelCalls = append(onCancelCalls, 1) })
		body.Task.OnCancel(func() { onCancelCalls = append(onCancelCalls, 2); close(done) })
		<-done
		return nil, nil
	})
	conn := &fakeConnection{nextID: 1}
	sender := MakeSender(ch, conn)
	responses := make([]Message, 0)
	mu := sync.Mutex{}
	controller := &Controller{
		ResponseFn: func(response Message) {
			mu.Lock()
			responses = append(responses, response)
			mu.Unlock()
		},
		SubscribeFn:   func(string) {},
		UnsubscribeFn: func(string) {},
		EventFn:       func(string, any) {},
		SenderValue:   sender,
		StateValue:    map[string]any{},
	}

	go func() {
		ch.ReceiveOne(Message{ID: intPtr(5), Path: strPtr("wait")}, controller)
		close(requestFinished)
	}()
	waitFor(t, func() bool { return sender.RequestsSize() == 1 })
	ch.ReceiveOne(Message{Cancel: intPtr(5)}, controller)

	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		if len(onCancelCalls) != 2 {
			return false
		}
		for _, response := range responses {
			if response.ID != nil && *response.ID == 5 {
				return fmt.Sprintf("%v", response.Error) == "cancelled"
			}
		}
		return false
	})
	<-requestFinished
	if sender.RequestsSize() != 0 {
		t.Fatalf("expected sender requests to be empty")
	}
}

func TestSubscriptionTopicAndBodyCanBothReplaceTopicFormatter(t *testing.T) {
	subscription := NewSubscription()
	events := make([]SubscriptionEvent, 0)
	subscription.AddPublisher(&testPublisher{publish: func(event SubscriptionEvent) {
		events = append(events, event)
	}})

	subscription.Topic(func(body any) string { return "topic-" + fmt.Sprintf("%v", body) })
	subscription.Body(func(body any) any { return "body-" + fmt.Sprintf("%v", body) })
	subscription.Send("x", "payload")

	if len(events) != 1 {
		t.Fatalf("expected one event, got %d", len(events))
	}
	if events[0].Topic != "body-x" {
		t.Fatalf("expected body-x topic, got %s", events[0].Topic)
	}
	if events[0].Body != "payload" {
		t.Fatalf("expected payload body")
	}
}

func TestMakeSenderRequestResolvesAndMarksSentID(t *testing.T) {
	ch := NewChannel()
	connection := &fakeConnection{nextID: 200}
	sender := MakeSender(ch, connection)
	request := sender.Request("hello", nil)
	rid := firstRequestID(ch)
	controller := &Controller{SenderValue: sender, StateValue: map[string]any{}}

	ch.ReceiveOne(Message{ID: intPtr(rid), Body: "world"}, controller)
	value, err := request.Wait()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if value != "world" {
		t.Fatalf("expected world, got %v", value)
	}
	if len(connection.sentIDs) != 1 || connection.sentIDs[0] != 200 {
		t.Fatalf("expected sent id 200, got %#v", connection.sentIDs)
	}
}

func TestMakeSenderRequestRejectsAndMarksSentID(t *testing.T) {
	ch := NewChannel()
	connection := &fakeConnection{nextID: 200}
	sender := MakeSender(ch, connection)
	request := sender.Request("hello", nil)
	rid := firstRequestID(ch)
	controller := &Controller{SenderValue: sender, StateValue: map[string]any{}}

	ch.ReceiveOne(Message{ID: intPtr(rid), Error: "failed"}, controller)
	_, err := request.Wait()
	if err == nil || err.Error() != "failed" {
		t.Fatalf("expected failed error, got %v", err)
	}
	if len(connection.sentIDs) != 1 || connection.sentIDs[0] != 200 {
		t.Fatalf("expected sent id 200, got %#v", connection.sentIDs)
	}
}

func TestMakeSenderRequestCancelNotifiesWhenTransportCancelFails(t *testing.T) {
	ch := NewChannel()
	connection := &fakeConnection{nextID: 200, cancelResult: false}
	sender := MakeSender(ch, connection)
	request := sender.Request("slow", nil)
	rid := firstRequestID(ch)

	request.Cancel()
	_, _ = request.Wait()

	if len(connection.cancelIDs) != 1 || connection.cancelIDs[0] != 200 {
		t.Fatalf("expected transport cancel for id 200, got %#v", connection.cancelIDs)
	}
	if len(connection.notifyBodies) != 1 {
		t.Fatalf("expected one notify body")
	}
	msg, ok := connection.notifyBodies[0].(Message)
	if !ok || msg.Cancel == nil || *msg.Cancel != rid {
		t.Fatalf("expected notify cancel for rid %d, got %#v", rid, connection.notifyBodies[0])
	}
	if ch.RequestsSize() != 0 {
		t.Fatalf("expected channel requests to be empty")
	}
}

func TestMakeSenderRequestCancelNotifiesWhenSendIDMissing(t *testing.T) {
	ch := NewChannel()
	connection := &fakeConnection{nextID: 200, sendReturnsMissing: true}
	sender := MakeSender(ch, connection)
	request := sender.Request("slow", nil)
	rid := firstRequestID(ch)

	request.Cancel()
	_, _ = request.Wait()

	if len(connection.cancelIDs) != 0 {
		t.Fatalf("expected no transport cancel, got %#v", connection.cancelIDs)
	}
	if len(connection.notifyBodies) != 1 {
		t.Fatalf("expected one notify body")
	}
	msg, ok := connection.notifyBodies[0].(Message)
	if !ok || msg.Cancel == nil || *msg.Cancel != rid {
		t.Fatalf("expected notify cancel for rid %d, got %#v", rid, connection.notifyBodies[0])
	}
	if ch.RequestsSize() != 0 {
		t.Fatalf("expected channel requests to be empty")
	}
}

type testPublisher struct {
	publish func(SubscriptionEvent)
}

func (p *testPublisher) Publish(event SubscriptionEvent) {
	p.publish(event)
}

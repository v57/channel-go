package channelgo

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

type connSpy struct {
	mu sync.Mutex

	nextID int

	sentBodies   []any
	notifyBodies []any
	sentIDs      []int
	cancelIDs    []int
	cancelOK     bool
	stopped      int

	topics  map[string]map[int]EventBody
	topicID int
}

func newConnSpy() *connSpy {
	return &connSpy{nextID: 1, topics: map[string]map[int]EventBody{}}
}

func (c *connSpy) Send(body any) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sentBodies = append(c.sentBodies, body)
	id := c.nextID
	c.nextID++
	return id
}

func (c *connSpy) Sent(id int) {
	c.mu.Lock()
	c.sentIDs = append(c.sentIDs, id)
	c.mu.Unlock()
}

func (c *connSpy) Cancel(id int) bool {
	c.mu.Lock()
	c.cancelIDs = append(c.cancelIDs, id)
	ok := c.cancelOK
	c.mu.Unlock()
	return ok
}

func (c *connSpy) Notify(body any) {
	c.mu.Lock()
	c.notifyBodies = append(c.notifyBodies, body)
	c.mu.Unlock()
}

func (c *connSpy) AddTopic(topic string, event EventBody) func() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.topicID++
	id := c.topicID
	if c.topics[topic] == nil {
		c.topics[topic] = map[int]EventBody{}
	}
	c.topics[topic][id] = event
	return func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		h := c.topics[topic]
		delete(h, id)
		if len(h) > 0 {
			return false
		}
		delete(c.topics, topic)
		return true
	}
}

func (c *connSpy) Stop() {
	c.mu.Lock()
	c.stopped++
	c.mu.Unlock()
}

func waitUntil(t *testing.T, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met")
}

func requestIDByKind(ch *Channel, kind string) int {
	ch.mu.RLock()
	defer ch.mu.RUnlock()
	for id, req := range ch.requests {
		switch kind {
		case "path":
			if req.request.Path != nil {
				return id
			}
		case "stream":
			if req.request.Stream != nil {
				return id
			}
		case "sub":
			if req.request.Sub != nil {
				return id
			}
		}
	}
	return -1
}

func TestReceiveVariantsAndTopicDelivery(t *testing.T) {
	ch := NewChannel().Post("echo", func(b Body) (any, error) {
		return b.Body, nil
	})
	conn := newConnSpy()
	sender := MakeSender(ch, conn)
	events := 0
	responses := 0
	controller := &Controller{
		ResponseFn: func(Message) {
			responses++
		},
		SubscribeFn:   func(string) {},
		UnsubscribeFn: func(string) {},
		EventFn: func(topic string, body any) {
			if topic == "abc" && body == "payload" {
				events++
			}
		},
		SenderValue: sender,
		StateValue:  map[string]any{},
	}

	ch.Receive([]any{map[string]any{"id": float64(1), "path": "echo", "body": "x"}}, controller)
	waitUntil(t, func() bool { return responses >= 1 })

	ch.Receive([]Message{{ID: intPtr(2), Path: strPtr("echo"), Body: "y"}}, controller)
	ch.Receive([]*Message{{ID: intPtr(3), Path: strPtr("echo"), Body: "z"}}, controller)
	waitUntil(t, func() bool { return responses >= 3 })
	ch.Receive(map[string]any{"id": float64(4), "path": "echo", "body": "w"}, controller) // default branch asMessage=true
	waitUntil(t, func() bool { return responses >= 4 })
	ch.Receive("not-a-message", controller) // default branch with unsupported type

	req := ch.MakeRequest("echo", nil, nil, func(Message) {}, sender)
	ch.ReceiveOne(Message{ID: req.ID, Topic: strPtr("abc"), Body: "payload"}, controller)
	if events != 1 {
		t.Fatalf("expected topic event dispatch")
	}
}

func TestStreamAndSubscriptionBranches(t *testing.T) {
	sub := NewSubscription()
	sub.Topic(func(request any) string { return fmt.Sprintf("t/%v", request) })
	sub.BodyFn(func(request any) (any, error) {
		return fmt.Sprintf("body:%v", request), nil
	})

	streamHits := 0
	ch := NewChannel().
		Stream("ok", func(_ Body, emit StreamEmitter) error {
			streamHits++
			if err := emit.Emit(1); err != nil {
				return err
			}
			if err := emit.Emit(2); err != nil {
				return err
			}
			return nil
		}).
		Stream("nil", func(Body, StreamEmitter) error {
			return nil
		}).
		Stream("nextErr", func(Body, StreamEmitter) error {
			return errors.New("next failed")
		}).
		Stream("err", func(Body, StreamEmitter) error {
			return errors.New("stream fail")
		}).
		Events(map[string]*Subscription{"s": sub})

	conn := newConnSpy()
	sender := MakeSender(ch, conn)
	responses := make([]Message, 0)
	subscribed := ""
	unsubscribed := ""
	controller := &Controller{
		ResponseFn: func(m Message) {
			responses = append(responses, m)
		},
		SubscribeFn: func(topic string) {
			subscribed = topic
		},
		UnsubscribeFn: func(topic string) {
			unsubscribed = topic
		},
		EventFn:     func(string, any) {},
		SenderValue: sender,
		StateValue:  map[string]any{},
	}

	ch.ReceiveOne(Message{ID: intPtr(1), Stream: strPtr("missing")}, controller)
	if len(responses) == 0 || fmt.Sprintf("%v", responses[len(responses)-1].Error) != "api not found" {
		t.Fatalf("expected api not found for missing stream")
	}

	ch.ReceiveOne(Message{ID: intPtr(2), Stream: strPtr("err")}, controller)
	waitUntil(t, func() bool {
		for _, r := range responses {
			if r.ID != nil && *r.ID == 2 && fmt.Sprintf("%v", r.Error) == "stream fail" {
				return true
			}
		}
		return false
	})

	ch.ReceiveOne(Message{ID: intPtr(3), Stream: strPtr("ok")}, controller)
	waitUntil(t, func() bool {
		haveBody := 0
		haveDone := false
		for _, r := range responses {
			if r.ID != nil && *r.ID == 3 {
				if r.Done != nil && *r.Done {
					haveDone = true
				}
				if r.Body != nil {
					haveBody++
				}
			}
		}
		return haveBody >= 2 && haveDone
	})
	if streamHits != 1 {
		t.Fatalf("expected stream handler to run once")
	}

	ch.ReceiveOne(Message{ID: intPtr(6), Stream: strPtr("nil")}, controller)
	waitUntil(t, func() bool {
		for _, r := range responses {
			if r.ID != nil && *r.ID == 6 && r.Done != nil && *r.Done {
				return true
			}
		}
		return false
	})
	ch.ReceiveOne(Message{ID: intPtr(7), Stream: strPtr("nextErr")}, controller)
	waitUntil(t, func() bool {
		for _, r := range responses {
			if r.ID != nil && *r.ID == 7 && fmt.Sprintf("%v", r.Error) == "next failed" {
				return true
			}
		}
		return false
	})
	ch.ReceiveOne(Message{Stream: strPtr("ok")}, controller) // id=nil stream early return

	ch.ReceiveOne(Message{ID: intPtr(4), Sub: strPtr("missing"), Body: "x"}, controller)
	if fmt.Sprintf("%v", responses[len(responses)-1].Error) != "subscription not found" {
		t.Fatalf("expected missing subscription error")
	}

	ch.ReceiveOne(Message{ID: intPtr(5), Sub: strPtr("s"), Body: "x"}, controller)
	if subscribed != "s/t/x" {
		t.Fatalf("unexpected subscribed topic: %s", subscribed)
	}
	if responses[len(responses)-1].Body != "body:x" {
		t.Fatalf("unexpected subscription body")
	}

	ch.ReceiveOne(Message{Unsub: strPtr("s/t/x")}, controller)
	if unsubscribed != "s/t/x" {
		t.Fatalf("expected unsubscribe callback")
	}
}

func TestReceiveCancelCancelsStreamAndTask(t *testing.T) {
	ch := NewChannel()
	conn := newConnSpy()
	sender := MakeSender(ch, conn)

	returned := 0
	sender.streams.Set(7, &FuncIterator{ReturnFn: func() error {
		returned++
		return nil
	}})
	cancelled := 0
	task := newTask(func() { cancelled++ })
	sender.requests.Set(7, task)

	controller := &Controller{SenderValue: sender, StateValue: map[string]any{}}
	ch.ReceiveOne(Message{Cancel: intPtr(7)}, controller)

	if returned != 1 {
		t.Fatalf("expected stream return once")
	}
	if cancelled != 1 {
		t.Fatalf("expected task cancel once")
	}
	if sender.streams.Get(7) != nil || sender.requests.Get(7) != nil {
		t.Fatalf("expected request and stream removed")
	}
}

func TestEventsMergeAndHelpers(t *testing.T) {
	base := NewChannel()
	a := NewSubscription()
	base.Events(map[string]*Subscription{"a": a})
	if a.Prefix() != "a" {
		t.Fatalf("expected event prefix to be set")
	}

	base.EventsWithPrefix(map[string]*Subscription{"b": NewSubscription()}, "root")
	base.EventsWithPrefix(map[string]*Subscription{"nil": nil}, "root")
	count := 0
	base.ForEachEvent(func(string, *Subscription) { count++ })
	base.ForEachEvent(nil)
	if count < 2 {
		t.Fatalf("expected merged event map")
	}

	anyTree := map[string]any{"x": map[string]any{"y": NewSubscription()}}
	base.EventsAny(anyTree, "p")

	other := NewChannel().
		Post("hello", func(Body) (any, error) { return "ok", nil }).
		Stream("s", func(Body, StreamEmitter) error { return nil }).
		Events(map[string]*Subscription{"ev": NewSubscription()})
	base.Merge(other, "m")
	base.Merge(nil, "unused")
	other.eventsAPI["skip"] = nil
	base.Merge(other, "m2")

	if base.lookupPost("m/hello") == nil {
		t.Fatalf("expected merged post route")
	}
	if base.lookupPost("missing") != nil {
		t.Fatalf("expected missing post route to be nil")
	}
	if base.lookupStream("m/s") == nil {
		t.Fatalf("expected merged stream route")
	}
	if base.lookupStream("missing") != nil {
		t.Fatalf("expected missing stream route to be nil")
	}
	if base.lookupSubscription("m/ev") == nil {
		t.Fatalf("expected merged event route")
	}
	if base.lookupSubscription("missing") != nil {
		t.Fatalf("expected missing subscription route to be nil")
	}

	if joinPath("", "a") != "a" || joinPath("a", "") != "a/" || joinPath("a", "b") != "a/b" {
		t.Fatalf("joinPath mismatch")
	}
	if joinPathNoEmpty("", "a") != "a" || joinPathNoEmpty("a", "") != "a" || joinPathNoEmpty("a", "b") != "a/b" {
		t.Fatalf("joinPathNoEmpty mismatch")
	}

	parsed := map[string]*Subscription{}
	parseSubscriptions(parsed, map[string]any{"k": map[string]any{"v": NewSubscription()}}, "z")
	if parsed["z/k/v"] == nil {
		t.Fatalf("expected parsed nested subscription")
	}
	parseSubscriptions(parsed, map[string]*Subscription{"direct": NewSubscription()}, "p2")
	if parsed["p2/direct"] == nil {
		t.Fatalf("expected parsed direct subscription map")
	}
	parseSubscriptions(parsed, map[string]*Subscription{"nil": nil}, "p3")

	if _, ok := asMessage(map[string]any{"id": float64(1), "path": "p", "stream": "s", "done": true}); !ok {
		t.Fatalf("expected map -> message decode")
	}
	if parsedMsg, ok := asMessage(map[string]any{
		"id":      float64(2),
		"path":    "p",
		"body":    "b",
		"context": "ctx",
		"error":   "e",
		"cancel":  float64(3),
		"sub":     "s",
		"unsub":   "u",
		"topic":   "t",
		"stream":  "st",
		"done":    true,
	}); !ok || parsedMsg.Sub == nil || *parsedMsg.Sub != "s" || parsedMsg.Unsub == nil || *parsedMsg.Unsub != "u" || parsedMsg.Topic == nil || *parsedMsg.Topic != "t" || parsedMsg.Stream == nil || *parsedMsg.Stream != "st" || parsedMsg.Cancel == nil || *parsedMsg.Cancel != 3 {
		t.Fatalf("expected full map message decode")
	}
	msg := Message{Path: strPtr("p")}
	if _, ok := asMessage(msg); !ok {
		t.Fatalf("expected direct message conversion")
	}
	if _, ok := asMessage(&msg); !ok {
		t.Fatalf("expected pointer message conversion")
	}
	var nilMsg *Message
	if _, ok := asMessage(nilMsg); ok {
		t.Fatalf("expected nil pointer message conversion to fail")
	}
	if _, ok := asMessage(struct{}{}); ok {
		t.Fatalf("expected unsupported message to fail")
	}

	if n, ok := asInt(int(1)); !ok || n != 1 {
		t.Fatalf("asInt int failed")
	}
	if n, ok := asInt(int32(2)); !ok || n != 2 {
		t.Fatalf("asInt int32 failed")
	}
	if n, ok := asInt(int64(4)); !ok || n != 4 {
		t.Fatalf("asInt int64 failed")
	}
	if n, ok := asInt(float32(5)); !ok || n != 5 {
		t.Fatalf("asInt float32 failed")
	}
	if n, ok := asInt(float64(3)); !ok || n != 3 {
		t.Fatalf("asInt float64 failed")
	}
	if _, ok := asInt("x"); ok {
		t.Fatalf("asInt string should fail")
	}

	if valueOr(nil, "x") != "x" || valueOr("y", "x") != "y" {
		t.Fatalf("valueOr mismatch")
	}

	if isTruthy(nil) || isTruthy(false) || isTruthy(0) || isTruthy("") {
		t.Fatalf("expected falsy values")
	}
	if !isTruthy(true) || !isTruthy(1) || !isTruthy(int32(1)) || !isTruthy(int64(1)) || !isTruthy(float32(1)) || !isTruthy(float64(1)) || !isTruthy("ok") || !isTruthy(struct{}{}) {
		t.Fatalf("expected truthy values")
	}
}

func TestSenderSendValuesSubscribeStop(t *testing.T) {
	ch := NewChannel()
	conn := newConnSpy()
	sender := MakeSender(ch, conn)

	// send path
	sendDone := make(chan struct{})
	var sendValue any
	var sendErr error
	go func() {
		defer close(sendDone)
		sendValue, sendErr = sender.Send("hello", "body", "ctx")
	}()
	waitUntil(t, func() bool { return ch.RequestsSize() == 1 })
	rid := requestIDByKind(ch, "path")
	ch.mu.RLock()
	if req := ch.requests[rid].request; req.Context != "ctx" {
		ch.mu.RUnlock()
		t.Fatalf("expected request context propagation")
	}
	ch.mu.RUnlock()
	ch.ReceiveOne(Message{ID: intPtr(rid), Body: "world"}, &Controller{SenderValue: sender, StateValue: map[string]any{}})
	<-sendDone
	if sendErr != nil || sendValue != "world" {
		t.Fatalf("unexpected send result: %v %v", sendValue, sendErr)
	}

	// values path
	values := sender.Values("stream", nil)
	resCh := make(chan IteratorValue, 3)
	errCh := make(chan error, 3)
	go func() {
		v, err := values.Next(context.Background())
		if err != nil {
			errCh <- err
			return
		}
		resCh <- v
	}()
	waitUntil(t, func() bool { return requestIDByKind(ch, "stream") >= 0 })
	sid := requestIDByKind(ch, "stream")
	ch.ReceiveOne(Message{ID: intPtr(sid), Body: 1}, &Controller{SenderValue: sender, StateValue: map[string]any{}})
	select {
	case got := <-resCh:
		if got.Value != 1 || got.Done {
			t.Fatalf("unexpected stream value: %#v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for stream value")
	}

	go func() {
		v, err := values.Next(context.Background())
		if err != nil {
			errCh <- err
			return
		}
		resCh <- v
	}()
	ch.ReceiveOne(Message{ID: intPtr(sid), Done: boolPtr(true)}, &Controller{SenderValue: sender, StateValue: map[string]any{}})
	select {
	case got := <-resCh:
		if !got.Done {
			t.Fatalf("expected done response")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for done")
	}

	// error path (new values object)
	values2 := sender.Values("stream", nil)
	go func() {
		_, err := values2.Next(context.Background())
		errCh <- err
	}()
	waitUntil(t, func() bool { return requestIDByKind(ch, "stream") >= 0 })
	sid2 := requestIDByKind(ch, "stream")
	ch.ReceiveOne(Message{ID: intPtr(sid2), Error: "boom"}, &Controller{SenderValue: sender, StateValue: map[string]any{}})
	select {
	case err := <-errCh:
		if err == nil || err.Error() != "boom" {
			t.Fatalf("expected boom, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for stream error")
	}
	values2.Return()
	values2.Return()
	values.Return()
	sender.Values("noop", nil).Return() // hasRID=false early return branch
	valuesWithCtx := sender.Values("ctx-stream", nil, "ctx")
	go func() {
		_, _ = valuesWithCtx.Next(context.Background())
	}()
	waitUntil(t, func() bool { return requestIDByKind(ch, "stream") >= 0 })
	sidCtx := requestIDByKind(ch, "stream")
	ch.mu.RLock()
	ctxReq := ch.requests[sidCtx].request.Context
	ch.mu.RUnlock()
	if ctxReq != "ctx" {
		t.Fatalf("expected values context propagation, got %#v", ctxReq)
	}
	valuesWithCtx.Return()
	valuesWithCtx.Return()

	// subscribe path
	subDone := make(chan struct{})
	var cancel Cancellable
	var subErr error
	hit := 0
	go func() {
		defer close(subDone)
		cancel, subErr = sender.Subscribe("updates", "req", func(body any) {
			if body == "evt" {
				hit++
			}
		})
	}()
	waitUntil(t, func() bool { return requestIDByKind(ch, "sub") >= 0 })
	subID := requestIDByKind(ch, "sub")
	topic := "updates/1"
	ch.ReceiveOne(Message{ID: intPtr(subID), Topic: &topic, Body: "ok"}, &Controller{SenderValue: sender, StateValue: map[string]any{}})
	<-subDone
	if subErr != nil || cancel == nil {
		t.Fatalf("unexpected subscribe result: %v", subErr)
	}
	conn.mu.Lock()
	handlers := conn.topics[topic]
	conn.mu.Unlock()
	for _, fn := range handlers {
		fn("evt")
	}
	if hit != 1 {
		t.Fatalf("expected topic callback")
	}
	cancel.Cancel()
	conn.mu.Lock()
	notifyCount := len(conn.notifyBodies)
	conn.mu.Unlock()
	if notifyCount == 0 {
		t.Fatalf("expected unsubscribe notify")
	}
	cancel.Cancel() // idempotent cancel path

	sender.Stop()
	conn.mu.Lock()
	stopped := conn.stopped
	conn.mu.Unlock()
	if stopped != 1 {
		t.Fatalf("expected sender stop to call connection stop")
	}

	if asError(nil) != nil {
		t.Fatalf("asError(nil) should be nil")
	}
	wrapped := asError(errors.New("x"))
	if wrapped == nil || wrapped.Error() != "x" {
		t.Fatalf("asError(error) failed")
	}

	// request cancel branch where transport cancel succeeds (no notify fallback)
	conn2 := newConnSpy()
	conn2.cancelOK = true
	sender2 := MakeSender(NewChannel(), conn2)
	reqOK := sender2.Request("ok", nil)
	reqOK.Cancel()
	_, _ = reqOK.Wait()
	reqOK.Cancel() // rid<0 branch after completion/cancel
	conn2.mu.Lock()
	notifyLen := len(conn2.notifyBodies)
	conn2.mu.Unlock()
	if notifyLen != 0 {
		t.Fatalf("expected no notify fallback when transport cancel succeeds")
	}

	// subscribe with nil topic and remove=false branch (no notify)
	sender3 := MakeSender(NewChannel(), conn2)
	doneSub2 := make(chan struct{})
	go func() {
		defer close(doneSub2)
		cancel2, err := sender3.Subscribe("x", "body", func(any) {}, "ctx")
		if err != nil {
			t.Errorf("unexpected subscribe error: %v", err)
			return
		}
		cancel2.Cancel()
	}()
	waitUntil(t, func() bool { return requestIDByKind(sender3.ch, "sub") >= 0 })
	sub2 := requestIDByKind(sender3.ch, "sub")
	sender3.ch.mu.RLock()
	if req := sender3.ch.requests[sub2].request; req.Context != "ctx" {
		sender3.ch.mu.RUnlock()
		t.Fatalf("expected subscribe context propagation")
	}
	sender3.ch.mu.RUnlock()
	sender3.ch.ReceiveOne(Message{ID: intPtr(sub2), Body: "ok"}, &Controller{SenderValue: sender3, StateValue: map[string]any{}})
	<-doneSub2

	// subscribe error path
	errDone := make(chan error, 1)
	go func() {
		_, err := sender3.Subscribe("x", "body", func(any) {})
		errDone <- err
	}()
	waitUntil(t, func() bool { return requestIDByKind(sender3.ch, "sub") >= 0 })
	subErrID := requestIDByKind(sender3.ch, "sub")
	sender3.ch.ReceiveOne(Message{ID: intPtr(subErrID), Error: "sub-failed"}, &Controller{SenderValue: sender3, StateValue: map[string]any{}})
	if err := <-errDone; err == nil || err.Error() != "sub-failed" {
		t.Fatalf("expected subscribe error path, got %v", err)
	}

	// values.Next context cancellation branch
	valuesTimeout := sender.Values("timeout", nil)
	ctx, cancelCtx := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancelCtx()
	_, err := valuesTimeout.Next(ctx)
	if err == nil {
		t.Fatalf("expected context cancellation error from values.Next")
	}
	valuesTimeout.Return()

	// values.start early-return branches
	valuesCancelled := sender.Values("cancelled", nil)
	valuesCancelled.cancelled = true
	valuesCancelled.start()
	valuesRunning := sender.Values("running", nil)
	valuesRunning.isRunning = true
	valuesRunning.start()

	// values callback cancelled branch and next-cancelled branch.
	valuesCancelled2 := sender.Values("cancel-branch", nil)
	go func() {
		_, _ = valuesCancelled2.Next(context.Background())
	}()
	waitUntil(t, func() bool { return requestIDByKind(ch, "stream") >= 0 })
	streamCancelID := requestIDByKind(ch, "stream")
	valuesCancelled2.Return()
	ch.ReceiveOne(Message{ID: intPtr(streamCancelID), Body: 1}, &Controller{SenderValue: sender, StateValue: map[string]any{}})
	if got, err := valuesCancelled2.Next(context.Background()); err != nil || !got.Done {
		t.Fatalf("expected cancelled values.Next to return done, got %#v %v", got, err)
	}

	// values Return with hasRID=false pending queue branch.
	manual := &Values{pending: []chan Message{make(chan Message, 1), make(chan Message, 1)}}
	manual.Return()
}

func TestValuesCancelWithoutTransportAndCancelledCallback(t *testing.T) {
	ch := NewChannel()
	conn := newConnSpy()
	conn.nextID = -1 // first send returns -1 so hasTransportID=false.
	sender := MakeSender(ch, conn)

	values := sender.Values("stream", nil)
	values.start()
	ch.mu.RLock()
	rid := ch.id - 1
	ch.mu.RUnlock()
	values.Return()

	waitUntil(t, func() bool {
		conn.mu.Lock()
		defer conn.mu.Unlock()
		for _, body := range conn.sentBodies {
			message, ok := body.(Message)
			if ok && message.Cancel != nil && *message.Cancel == rid {
				return true
			}
		}
		return false
	})
	if ch.RequestsSize() != 0 {
		t.Fatalf("expected request to be cancelled and removed")
	}

	// Callback branch where values are already cancelled when response arrives.
	values2 := sender.Values("stream", nil)
	values2.start()
	ch.mu.RLock()
	rid2 := ch.id - 1
	response := ch.requests[rid2].response
	ch.mu.RUnlock()

	values2.mu.Lock()
	values2.cancelled = true
	values2.mu.Unlock()
	response(Message{Body: "ignored"})

	values2.mu.Lock()
	queued := len(values2.queued)
	values2.mu.Unlock()
	if queued != 0 {
		t.Fatalf("expected cancelled values callback to skip queueing, got %d", queued)
	}
	ch.Cancel(rid2)
}

func TestReceivePathAdditionalBranches(t *testing.T) {
	ch := NewChannel()
	calls := 0
	ch.Post("noid", func(Body) (any, error) {
		calls++
		return nil, nil
	})
	ch.Post("panic", func(Body) (any, error) {
		panic("boom")
	})
	ch.Post("cancelled", func(b Body) (any, error) {
		if b.Task != nil {
			b.Task.Cancel()
		}
		return "ignored", nil
	})

	conn := newConnSpy()
	sender := MakeSender(ch, conn)
	responses := make([]Message, 0)
	controller := &Controller{
		ResponseFn: func(m Message) {
			responses = append(responses, m)
		},
		SenderValue: sender,
		StateValue:  map[string]any{},
	}

	ch.ReceiveOne(Message{Path: strPtr("noid"), Body: 1}, controller)
	waitUntil(t, func() bool { return calls == 1 })

	ch.ReceiveOne(Message{ID: intPtr(10), Path: strPtr("panic")}, controller)
	waitUntil(t, func() bool {
		for _, r := range responses {
			if r.ID != nil && *r.ID == 10 && fmt.Sprintf("%v", r.Error) == "boom" {
				return true
			}
		}
		return false
	})

	ch.ReceiveOne(Message{ID: intPtr(11), Path: strPtr("cancelled")}, controller)
	waitUntil(t, func() bool {
		for _, r := range responses {
			if r.ID != nil && *r.ID == 11 && fmt.Sprintf("%v", r.Error) == "cancelled" {
				return true
			}
		}
		return false
	})

	// no-id missing API branch.
	ch.ReceiveOne(Message{Path: strPtr("missing")}, controller)
	// id missing API branch.
	ch.ReceiveOne(Message{ID: intPtr(12), Path: strPtr("missing")}, controller)
	waitUntil(t, func() bool {
		for _, r := range responses {
			if r.ID != nil && *r.ID == 12 && fmt.Sprintf("%v", r.Error) == "api not found" {
				return true
			}
		}
		return false
	})

	// response with unknown request id branch.
	ch.ReceiveOne(Message{ID: intPtr(999), Body: "missing"}, controller)

	// ReceiveOne guard branches.
	ch.ReceiveOne(Message{Path: strPtr("noid")}, nil)
	ch.ReceiveOne(Message{Path: strPtr("noid")}, &Controller{})

	// Disconnect nil sender branch.
	ch.Disconnect(map[string]any{}, nil)
}

func TestControllerAndIterators(t *testing.T) {
	controller := &Controller{}
	controller.Response(Message{})
	controller.Subscribe("x")
	controller.Unsubscribe("x")
	controller.Event("x", 1)
	if controller.Sender() != nil || controller.State() != nil {
		t.Fatalf("expected nil sender/state")
	}
	controller.SenderValue = &Sender{}
	controller.StateValue = "state"
	if controller.Sender() == nil || controller.State() != "state" {
		t.Fatalf("expected sender/state passthrough")
	}
	var nilController *Controller
	if nilController.Sender() != nil || nilController.State() != nil {
		t.Fatalf("expected nil receiver sender/state")
	}

	called := 0
	f := &FuncIterator{
		NextFn: func(context.Context) (any, bool, error) {
			called++
			return "v", true, nil
		},
		ReturnFn: func() error { return nil },
	}
	v, done, err := f.Next(context.Background())
	if err != nil || v != "v" || !done || called != 1 {
		t.Fatalf("func iterator next failed")
	}
	_ = f.Return()
	_ = (&FuncIterator{}).Return()
	if _, done, _ := (&FuncIterator{}).Next(context.Background()); !done {
		t.Fatalf("nil func iterator should return done")
	}

	si := NewSliceIterator(1, 2).OnYield(func(any) { called++ })
	_, _, _ = si.Next(context.Background())
	_ = si.Return()
	_, doneClosed, _ := si.Next(context.Background())
	if !doneClosed {
		t.Fatalf("expected closed slice iterator to be done")
	}

	valueCh := make(chan any, 1)
	errCh := make(chan error, 1)
	valueCh <- 10
	close(valueCh)
	close(errCh)
	ci := &ChannelIterator{Values: valueCh, Errors: errCh, cancel: func() { called++ }}
	vv, dd, ee := ci.Next(context.Background())
	if ee != nil || dd || vv != 10 {
		t.Fatalf("channel iterator value path failed")
	}
	_ = ci.Return()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = (&ChannelIterator{Values: make(chan any), Errors: make(chan error)}).Next(ctx)
	if err == nil {
		t.Fatalf("expected ctx cancellation error")
	}
	errs := make(chan error, 1)
	errs <- errors.New("boom")
	close(errs)
	_, _, err = (&ChannelIterator{Values: make(chan any), Errors: errs}).Next(context.Background())
	if err == nil || err.Error() != "boom" {
		t.Fatalf("expected propagated channel error, got %v", err)
	}
	if err := (&ChannelIterator{}).Return(); err == nil {
		t.Fatalf("expected return error for nil cancel")
	}
	var nilCI *ChannelIterator
	if _, doneNil, errNil := nilCI.Next(context.Background()); errNil != nil || !doneNil {
		t.Fatalf("expected nil channel iterator to return done")
	}
	emptyVals := make(chan any)
	close(emptyVals)
	emptyErrs := make(chan error)
	close(emptyErrs)
	_, done2, err2 := (&ChannelIterator{Values: emptyVals, Errors: emptyErrs}).Next(context.Background())
	if err2 != nil || !done2 {
		t.Fatalf("expected done on fully closed channels")
	}
	closedErrsOnly := make(chan error)
	close(closedErrsOnly)
	_, done3, err3 := (&ChannelIterator{Values: nil, Errors: closedErrsOnly}).Next(context.Background())
	if err3 != nil || !done3 {
		t.Fatalf("expected done when errors channel closes and values is nil")
	}
}

func TestSubscriptionHelpersAndPublisherRemoval(t *testing.T) {
	s := NewSubscription()
	p1 := &testPub{}
	p2 := &testPub{}
	s.AddPublisher(p1)
	s.AddPublisher(p2)
	if s.PublisherCount() != 2 {
		t.Fatalf("expected 2 publishers")
	}
	s.RemovePublisher(p1)
	if s.PublisherCount() != 1 {
		t.Fatalf("expected 1 publisher")
	}
	s.RemovePublisher(p2)
	if s.PublisherCount() != 0 {
		t.Fatalf("expected 0 publishers")
	}

	s.BodyFn(func(any) (any, error) { return nil, errors.New("x") })
	s.BodyFn(nil)
	s.Send("k") // ignored on bodyFn error
	s.Send(nil, "payload")
	s.Send(nil) // default bodyFn nil result
	s.Publish("x", nil)
	s.Topic(nil)
	s.Body(nil)

	var nilSub *Subscription
	if nilSub == nil {
		s2 := NewSubscription()
		if s2.topicFn(nil) != "" || s2.topicFn("x") != "x" {
			t.Fatalf("expected default topic formatter behavior")
		}
		s2.Send("abc") // default bodyFn (nil,nil) and value assignment branch
		s2.setPrefix("pref")
		hits := 0
		s2.AddPublisher(&testPubWithHook{hook: func(event SubscriptionEvent) {
			if event.Topic == "pref/topic" && event.Body == "body" {
				hits++
			}
		}})
		s2.Publish("topic", "body") // prefix branch
		if hits != 1 {
			t.Fatalf("expected prefixed publish event")
		}
		if got := s2.Prefix(); got != "pref" {
			t.Fatalf("expected pref prefix after setPrefix, got %q", got)
		}
	}
}

func TestTaskDoubleCancelAndOnCancelNil(t *testing.T) {
	tk := newTask(nil)
	count := 0
	tk.OnCancel(nil)
	tk.OnCancel(func() { count++ })
	tk.Cancel()
	tk.Cancel()
	if count != 1 {
		t.Fatalf("expected single cancel callback, got %d", count)
	}
}

func TestCancellableFuncNilFnAndNilReceiver(t *testing.T) {
	var nilCancelable *cancellableFunc
	nilCancelable.Cancel()

	c := &cancellableFunc{}
	c.Cancel() // nil function path
}

type testPub struct{}

func (*testPub) Publish(SubscriptionEvent) {}

type testPubWithHook struct {
	hook func(SubscriptionEvent)
}

func (p *testPubWithHook) Publish(event SubscriptionEvent) {
	if p.hook != nil {
		p.hook(event)
	}
}

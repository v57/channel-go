package client

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/v57/channel-go/channelgo"
	"github.com/v57/channel-go/process"
	"github.com/v57/channel-go/server"
)

func waitForClient(t *testing.T, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition was not met before timeout")
}

func TestResolveAddressVariants(t *testing.T) {
	cases := []struct {
		in   any
		want string
	}{
		{in: 2049, want: "ws://127.0.0.1:2049"},
		{in: "ws://example.com", want: "ws://example.com"},
		{in: "wss://example.com", want: "wss://example.com"},
		{in: "127.0.0.1:5555", want: "ws://127.0.0.1:5555"},
		{in: "8080", want: "ws://127.0.0.1:8080"},
		{in: struct{}{}, want: "ws://127.0.0.1:80"},
	}
	for _, tc := range cases {
		got := resolveAddress(tc.in)
		if got != tc.want {
			t.Fatalf("resolveAddress(%#v) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestDecodePayload(t *testing.T) {
	if _, ok := decodePayload(nil); ok {
		t.Fatalf("expected empty payload to fail")
	}
	if _, ok := decodePayload([]byte(`{"id":1,"path":"x"}`)); !ok {
		t.Fatalf("expected object payload to decode")
	}
	if value, ok := decodePayload([]byte(`[{"id":1,"path":"x"}]`)); !ok || value == nil {
		t.Fatalf("expected array payload to decode")
	}
	if _, ok := decodePayload([]byte(`{"bad":`)); ok {
		t.Fatalf("expected invalid payload to fail")
	}
	if _, ok := decodePayload([]byte(`[{`)); ok {
		t.Fatalf("expected invalid array payload to fail")
	}
}

func TestWebSocketClientPendingAndTopicHelpers(t *testing.T) {
	c := &WebSocketClient{
		pending:    map[int]pendingSend{1: {body: "a", requestID: 7}},
		subscribed: map[string]map[int]channelgo.EventBody{},
	}
	c.running.Store(true)

	if !c.Cancel(1) {
		t.Fatalf("expected pending cancel to succeed")
	}
	if c.Cancel(1) {
		t.Fatalf("expected second cancel to fail")
	}

	id := c.Send(map[string]any{"id": 11, "path": "x"})
	c.pendingMu.Lock()
	pending, ok := c.pending[id]
	c.pendingMu.Unlock()
	if !ok {
		t.Fatalf("expected pending entry after send")
	}
	if pending.body == nil {
		t.Fatalf("expected pending body to be retained")
	}

	hitsA := 0
	hitsB := 0
	cancelA := c.AddTopic("t", func(body any) {
		if body == "x" {
			hitsA++
		}
	})
	cancelB := c.AddTopic("t", func(body any) {
		if body == "x" {
			hitsB++
		}
	})

	c.ReceivedEvent("t", "x")
	if hitsA != 1 || hitsB != 1 {
		t.Fatalf("expected both handlers to run")
	}
	if cancelA() {
		t.Fatalf("first cancel should keep topic subscribed")
	}
	if !cancelB() {
		t.Fatalf("last cancel should remove topic")
	}
	if cancelB() {
		t.Fatalf("second cancel should return false when handlers map is missing")
	}

	c.Notify(channelgo.Message{Path: strPtrLocalClient("x")})
	c.publishMu.Lock()
	c.publishQueue = append(c.publishQueue, channelgo.SubscriptionEvent{Topic: "topic/1", Body: "event"})
	c.publishMu.Unlock()
	c.Throttle()
}

func TestWebSocketClientLifecycleCallbacks(t *testing.T) {
	srv, err := server.Listen(channelgo.NewChannel().Post("hello", func(channelgo.Body) (any, error) {
		return "world", nil
	}), 0, server.Options{})
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}

	ws := NewWebSocketClient("ws://"+srv.Addr(), nil)
	defer ws.Stop()

	var opened atomic.Int64
	var closed atomic.Int64
	ws.OnOpen = func() { opened.Add(1) }
	ws.OnClose = func() { closed.Add(1) }
	ws.start()

	waitForClient(t, func() bool { return ws.IsConnected() && opened.Load() > 0 })

	if err := srv.Stop(); err != nil {
		t.Fatalf("stop failed: %v", err)
	}
	waitForClient(t, func() bool { return !ws.IsConnected() && closed.Load() > 0 })
}

func TestConnectedWebSocketClientStopReturns(t *testing.T) {
	srv, err := server.Listen(channelgo.NewChannel(), 0, server.Options{})
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer srv.Stop()

	ws := NewWebSocketClient("ws://"+srv.Addr(), nil)
	ws.start()
	waitForClient(t, ws.IsConnected)

	stopped := make(chan struct{})
	go func() {
		ws.Stop()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatalf("connected client Stop did not return")
	}
}

func TestConnectOnConnectCallback(t *testing.T) {
	srv, err := server.Listen(channelgo.NewChannel().Post("hello", func(channelgo.Body) (any, error) {
		return "world", nil
	}), 0, server.Options{})
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer srv.Stop()

	var onConnect atomic.Int64
	ch := channelgo.NewChannel()
	clientConn := Connect(ch, srv.Addr(), Options{
		OnConnect: func(*channelgo.Sender) {
			onConnect.Add(1)
		},
	})
	defer clientConn.Sender.Stop()

	waitForClient(t, func() bool { return onConnect.Load() > 0 && clientConn.WS.IsConnected() })
}

func TestReconnectReplaysQueuedRequests(t *testing.T) {
	serverChannel := channelgo.NewChannel().Post("hello", func(channelgo.Body) (any, error) {
		return "world", nil
	})

	srv, err := server.Listen(serverChannel, 0, server.Options{})
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	addr := srv.Addr()

	clientConn := Connect(channelgo.NewChannel(), addr, Options{})
	defer clientConn.Sender.Stop()
	waitForClient(t, func() bool { return clientConn.WS.IsConnected() })

	if err := srv.Stop(); err != nil {
		t.Fatalf("stop failed: %v", err)
	}
	waitForClient(t, func() bool { return !clientConn.WS.IsConnected() })

	type result struct {
		value any
		err   error
	}
	resultCh := make(chan result, 1)
	go func() {
		value, err := clientConn.Sender.Send("hello", nil)
		resultCh <- result{value: value, err: err}
	}()

	select {
	case <-resultCh:
		t.Fatalf("request should remain pending while disconnected")
	case <-time.After(150 * time.Millisecond):
	}

	srv, err = server.Listen(serverChannel, addr, server.Options{})
	if err != nil {
		t.Fatalf("relisten failed: %v", err)
	}
	defer srv.Stop()
	waitForClient(t, func() bool { return clientConn.WS.IsConnected() })

	select {
	case got := <-resultCh:
		if got.err != nil {
			t.Fatalf("unexpected send error after reconnect: %v", got.err)
		}
		if got.value != "world" {
			t.Fatalf("unexpected send value after reconnect: %#v", got.value)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for replayed request")
	}
}

func TestClientSenderDelegatesSendHelpers(t *testing.T) {
	conn := &ClientSender{
		Sender: process.Connect(channelgo.NewChannel().Post("hello", func(channelgo.Body) (any, error) {
			return "world", nil
		})),
		WS: &WebSocketClient{},
	}
	defer conn.Stop()

	response, err := conn.Send("hello", nil)
	if err != nil {
		t.Fatalf("send failed: %v", err)
	}
	if response != "world" {
		t.Fatalf("unexpected response: %#v", response)
	}

	typed, err := channelgo.Send[string](conn, "hello", nil)
	if err != nil {
		t.Fatalf("typed send failed: %v", err)
	}
	if typed != "world" {
		t.Fatalf("unexpected typed response: %#v", typed)
	}
}

func strPtrLocalClient(v string) *string { return &v }

// A blocking Send inside OnConnect runs a nested loop step that drains the wake
// pipe. A Stop landing while that nested step waits for a response used to
// leave the outer step blocked in the poller forever.
func TestStopDuringBlockingSendInOnConnect(t *testing.T) {
	requested := make(chan struct{}, 1)
	release := make(chan struct{})
	done := make(chan struct{})
	srv, err := server.Listen(channelgo.NewChannel().Post("hello", func(channelgo.Body) (any, error) {
		requested <- struct{}{}
		// Hold the response so nothing but Stop's wake can end the nested wait.
		select {
		case <-release:
		case <-done:
		}
		return "world", nil
	}), 0, server.Options{})
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer srv.Stop()
	defer close(done)

	for i := 0; i < 20; i++ {
		clientConn := Connect(channelgo.NewChannel(), srv.Addr(), Options{
			OnConnect: func(sender *channelgo.Sender) {
				_, _ = sender.Send("hello", nil)
			},
		})
		<-requested
		stopped := make(chan struct{})
		go func() {
			clientConn.Sender.Stop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(2 * time.Second):
			t.Fatalf("iteration %d: Stop did not return", i)
		}
		release <- struct{}{}
	}
}

// The sender passed to OnConnect is loop-bound. Kept and used from another
// goroutine it must take the queued path instead of stepping the loop
// concurrently (run with -race).
func TestOnConnectSenderUsedFromAnotherGoroutine(t *testing.T) {
	srv, err := server.Listen(channelgo.NewChannel().Post("hello", func(channelgo.Body) (any, error) {
		return "world", nil
	}), 0, server.Options{})
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer srv.Stop()

	senders := make(chan *channelgo.Sender, 1)
	clientConn := Connect(channelgo.NewChannel(), srv.Addr(), Options{
		OnConnect: func(sender *channelgo.Sender) {
			senders <- sender
		},
	})
	defer clientConn.Sender.Stop()

	sender := <-senders
	done := make(chan error, 4)
	for range 4 {
		go func() {
			for range 50 {
				value, err := sender.Send("hello", nil)
				if err != nil {
					done <- err
					return
				}
				if value != "world" {
					done <- fmt.Errorf("unexpected response %v", value)
					return
				}
			}
			done <- nil
		}()
	}
	for range 4 {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("sends from a kept OnConnect sender did not complete")
		}
	}
}

// A long-lived stream handler used to run on the client's event loop and block
// every other request on that connection (hub-net's net/users stream froze the
// agent). Handlers now run off the loop.
func TestOpenStreamHandlerDoesNotBlockConnection(t *testing.T) {
	serverChannel := channelgo.NewChannel()
	serverChannel.Post("check", func(body channelgo.Body) (any, error) {
		values := body.Sender.Values("forever", nil)
		defer values.Return()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if _, err := values.Next(ctx); err != nil {
			return nil, fmt.Errorf("first stream value: %w", err)
		}
		// The stream stays open; the client must still answer a post.
		return body.Sender.Send("hello", nil)
	})
	srv, err := server.Listen(serverChannel, 0, server.Options{})
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer srv.Stop()

	clientChannel := channelgo.NewChannel()
	clientChannel.Post("hello", func(channelgo.Body) (any, error) { return "world", nil })
	clientChannel.Stream("forever", func(body channelgo.Body, emit channelgo.StreamEmitter) error {
		if err := emit.Emit("first"); err != nil {
			return err
		}
		for !emit.IsCancelled() {
			time.Sleep(10 * time.Millisecond)
		}
		return nil
	})
	clientConn := Connect(clientChannel, srv.Addr(), Options{})
	defer clientConn.Sender.Stop()

	result := make(chan any, 1)
	go func() {
		value, err := clientConn.Sender.Send("check", nil)
		if err != nil {
			value = err
		}
		result <- value
	}()
	select {
	case value := <-result:
		if value != "world" {
			t.Fatalf("check = %v, want world", value)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("an open stream handler blocked the client connection")
	}
}

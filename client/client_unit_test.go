package client

import (
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

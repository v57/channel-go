package server

import (
	"bytes"
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/v57/channel-go/channelgo"
)

func waitForServer(t *testing.T, predicate func() bool) {
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

func TestResolveAddressStringFallback(t *testing.T) {
	if (&Server{}).Addr() != "" {
		t.Fatalf("expected empty addr on nil listener")
	}

	h, p := resolveAddress(2049)
	if h != "127.0.0.1" || p != "2049" {
		t.Fatalf("unexpected int host/port: %s:%s", h, p)
	}

	h, p = resolveAddress("1.2.3.4:8080")
	if h != "1.2.3.4" || p != "8080" {
		t.Fatalf("unexpected host/port parse: %s:%s", h, p)
	}

	h, p = resolveAddress("bad")
	if h != "127.0.0.1" || p != "80" {
		t.Fatalf("unexpected fallback host/port: %s:%s", h, p)
	}
}

func TestDecodePayloadBranches(t *testing.T) {
	if _, ok := decodePayload(nil); ok {
		t.Fatalf("expected empty payload failure")
	}
	if _, ok := decodePayload([]byte(`{"id":1}`)); !ok {
		t.Fatalf("expected object decode success")
	}
	if _, ok := decodePayload([]byte(`[{"id":1}]`)); !ok {
		t.Fatalf("expected array decode success")
	}
	if _, ok := decodePayload([]byte(`{"bad":`)); ok {
		t.Fatalf("expected invalid json failure")
	}
	if _, ok := decodePayload([]byte(`[{`)); ok {
		t.Fatalf("expected invalid array json failure")
	}
}

func TestListenErrorAndHooks(t *testing.T) {
	occupied, err := Listen(channelgo.NewChannel(), 0, Options{})
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer occupied.Stop()

	if _, err := Listen(channelgo.NewChannel(), occupied.Addr(), Options{}); err == nil {
		t.Fatalf("expected listen failure on in-use address")
	}

	var connected atomic.Int64
	var disconnected atomic.Int64
	var seenQuery atomic.Value
	seenQuery.Store("")

	srv, err := Listen(channelgo.NewChannel(), 0, Options{
		State: func(headers map[string]string) any {
			seenQuery.Store(headers["q"])
			return map[string]any{}
		},
		OnConnect: func(*BodyContext) {
			connected.Add(1)
		},
		OnDisconnect: func(*BodyContext) {
			disconnected.Add(1)
		},
	})
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer srv.Stop()

	conn, _, _, err := ws.Dialer{}.Dial(context.Background(), "ws://"+srv.Addr()+"/?q=1")
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	if err := wsutil.WriteClientText(conn, []byte("{")); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	_ = conn.Close()

	waitForServer(t, func() bool { return connected.Load() > 0 && disconnected.Load() > 0 })
	if got := seenQuery.Load().(string); got != "1" {
		t.Fatalf("expected query header passthrough, got %q", got)
	}
}

func TestStopIsIdempotent(t *testing.T) {
	srv, err := Listen(channelgo.NewChannel(), 0, Options{})
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	if err := srv.Stop(); err != nil {
		t.Fatalf("unexpected stop error: %v", err)
	}
	if err := srv.Stop(); err != nil {
		t.Fatalf("unexpected second stop error: %v", err)
	}
}

func TestSubscriptionsAddTopicRemoveBranches(t *testing.T) {
	subs := NewSubscriptions()
	r1 := subs.AddTopic("t", func(any) {})
	r2 := subs.AddTopic("t", func(any) {})
	if r1() {
		t.Fatalf("expected first remove to keep topic")
	}
	if !r2() {
		t.Fatalf("expected second remove to clear topic")
	}
	if r2() {
		t.Fatalf("expected removing missing handler map to return false")
	}
}

func TestHeadersOverrideQueryParameters(t *testing.T) {
	var seen atomic.Value
	seen.Store("")
	srv, err := Listen(channelgo.NewChannel(), 0, Options{
		State: func(headers map[string]string) any {
			seen.Store(headers["x-user-id"] + "|" + headers["only-query"])
			return map[string]any{}
		},
	})
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer srv.Stop()

	dialer := ws.Dialer{Header: ws.HandshakeHeaderHTTP{"X-User-Id": []string{"proxy"}}}
	conn, _, _, err := dialer.Dial(context.Background(), "ws://"+srv.Addr()+"/?x-user-id=admin&only-query=1")
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()
	waitForServer(t, func() bool { return seen.Load().(string) != "" })
	if got := seen.Load().(string); got != "proxy|1" {
		t.Fatalf("expected header to win over query, got %q", got)
	}
}

func TestServerSurvivesHostileFrames(t *testing.T) {
	ch := channelgo.NewChannel()
	ch.Post("echo", func(body channelgo.Body) (any, error) { return body.Body, nil })
	srv, err := Listen(ch, 0, Options{})
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer srv.Stop()

	hostile := [][]byte{
		{0x81, 0xff, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 1, 2, 3, 4}, // 2^63-1
		{0x81, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 1, 2, 3, 4}, // 2^64-1
		{0x81, 0xff, 0, 0, 0, 0, 0x40, 0, 0, 0, 1, 2, 3, 4},                         // 1 GiB
	}
	for i, frame := range hostile {
		conn, _, _, err := ws.Dialer{}.Dial(context.Background(), "ws://"+srv.Addr())
		if err != nil {
			t.Fatalf("case %d: dial failed: %v", i, err)
		}
		if _, err := conn.Write(frame); err != nil {
			t.Fatalf("case %d: write failed: %v", i, err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		if _, err := conn.Read(make([]byte, 16)); err == nil {
			t.Fatalf("case %d: expected server to close the connection", i)
		}
		_ = conn.Close()
	}

	conn, _, _, err := ws.Dialer{}.Dial(context.Background(), "ws://"+srv.Addr())
	if err != nil {
		t.Fatalf("server died after hostile frames: %v", err)
	}
	defer conn.Close()
	if err := wsutil.WriteClientText(conn, []byte(`{"id":1,"path":"echo","body":"ok"}`)); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	reply, err := wsutil.ReadServerText(conn)
	if err != nil || !bytes.Contains(reply, []byte(`"ok"`)) {
		t.Fatalf("expected echo reply, got %q err=%v", reply, err)
	}
}

func TestServerClosesEndlessHandshake(t *testing.T) {
	srv, err := Listen(channelgo.NewChannel(), 0, Options{})
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer srv.Stop()
	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()
	_, _ = conn.Write([]byte("GET / HTTP/1.1\r\nX: "))
	chunk := bytes.Repeat([]byte("a"), 16*1024)
	closed := false
	for i := 0; i < 256; i++ { // up to 4 MiB
		_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
		if _, err := conn.Write(chunk); err != nil {
			closed = true
			break
		}
	}
	if !closed {
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, err := conn.Read(make([]byte, 1))
		closed = err != nil
	}
	if !closed {
		t.Fatalf("expected server to close an endless handshake")
	}
}

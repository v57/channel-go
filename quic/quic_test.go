package quic

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	quicgo "github.com/quic-go/quic-go"
	"github.com/v57/channel-go/channelgo"
	"github.com/v57/channel-go/client"
	"github.com/v57/channel-go/internal/testutil"
	"github.com/v57/channel-go/server"
)

// The TypeScript reference's test certificate: localhost, 127.0.0.1 and ::1
var (
	certDir     = filepath.Join("..", "..", "channel", "tests", "quic")
	fingerprint = "DD:D2:37:00:BD:8A:BD:29:14:15:D8:65:84:BE:C2:80:10:FF:21:DA:A1:C5:9D:B3:4A:7F:36:63:EF:4B:55:9B"
)

func readPEM(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(certDir, name))
	if err != nil {
		t.Skipf("the TypeScript reference's test certificate isn't next to this checkout: %v", err)
	}
	return data
}

func waitFor(t *testing.T, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ready() {
		if time.Now().After(deadline) {
			t.Fatal("timed out")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

type fixture struct {
	server  *Server
	ticks   *channelgo.Subscription
	cert    []byte
	key     []byte
	stats   struct{ connected, disconnected atomic.Int64 }
	dropped atomic.Bool
}

func newServerChannel(f *fixture) *channelgo.Channel {
	ch := channelgo.NewChannel()
	ch.Post("hello", func(channelgo.Body) (any, error) { return "world", nil })
	ch.Post("echo", func(body channelgo.Body) (any, error) { return body.Body, nil })
	ch.Post("name", func(body channelgo.Body) (any, error) {
		return body.State.(map[string]string)["x-name"], nil
	})
	ch.Post("mirror", func(body channelgo.Body) (any, error) { return body.Sender.Send("hello", nil) })
	ch.Post("slow", func(channelgo.Body) (any, error) {
		time.Sleep(300 * time.Millisecond)
		return "slow", nil
	})
	// Once: the client sends unanswered requests again after reconnecting
	ch.Post("disconnect", func(body channelgo.Body) (any, error) {
		if f.dropped.CompareAndSwap(false, true) {
			body.Sender.Stop()
		}
		return "dropped before", nil
	})
	ch.Stream("count", func(body channelgo.Body, emit channelgo.StreamEmitter) error {
		for i := 0; i < 3; i++ {
			if err := emit.Emit(i); err != nil {
				return err
			}
		}
		return nil
	})
	ch.Post("tick", func(channelgo.Body) (any, error) {
		f.ticks.Send("a", "tick")
		return nil, nil
	})
	ch.Events(map[string]*channelgo.Subscription{"ticks": f.ticks})
	return ch
}

func listen(t *testing.T, address any) *fixture {
	t.Helper()
	f := &fixture{ticks: channelgo.NewSubscription(), cert: readPEM(t, "cert.pem"), key: readPEM(t, "key.pem")}
	srv, err := Listen(newServerChannel(f), address, ListenOptions{
		Options: server.Options{
			State: func(headers map[string]string) any { return headers },
			OnConnect: func(*server.BodyContext) {
				f.stats.connected.Add(1)
			},
			OnDisconnect: func(*server.BodyContext) {
				f.stats.disconnected.Add(1)
			},
		},
		Key:  f.key,
		Cert: f.cert,
	})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	f.server = srv
	t.Cleanup(func() { _ = srv.Stop() })
	return f
}

func connect(t *testing.T, ch *channelgo.Channel, address any, options Options) *ClientSender {
	t.Helper()
	sender, err := Connect(ch, address, options)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(sender.Stop)
	return sender
}

func clientChannel() *channelgo.Channel {
	return channelgo.NewChannel().Post("hello", func(channelgo.Body) (any, error) { return "client world", nil })
}

func TestFrames(t *testing.T) {
	for _, size := range []int{0, 1, 63, 64, 16383, 16384, 70000} {
		payload := bytes.Repeat([]byte{'x'}, size)
		frame := appendFrame(nil, payload)
		header := len(frame) - size
		want := map[bool]int{size < 64: 1, size >= 64 && size < 16384: 2, size >= 16384: 4}[true]
		if header != want {
			t.Fatalf("size %d: %d header bytes, want %d", size, header, want)
		}
		// A keepalive in front is skipped
		reader := newFrameReader(bytes.NewReader(append([]byte{0}, appendFrame(frame, []byte("next"))...)), 0)
		if size > 0 {
			got, err := reader.next()
			if err != nil || !bytes.Equal(got, payload) {
				t.Fatalf("size %d: read %d bytes, %v", size, len(got), err)
			}
		}
		if got, err := reader.next(); err != nil || string(got) != "next" {
			t.Fatalf("size %d: next frame %q, %v", size, got, err)
		}
	}
	// 8-byte lengths decode too, and anything above the limit is refused
	big := []byte{0xc0, 0, 0, 0, 0, 0, 0, 5, 'h', 'e', 'l', 'l', 'o'}
	if got, err := newFrameReader(bytes.NewReader(big), 0).next(); err != nil || string(got) != "hello" {
		t.Fatalf("8-byte length: %q, %v", got, err)
	}
	if _, err := newFrameReader(bytes.NewReader(appendFrame(nil, make([]byte, 11))), 10).next(); err != errFrameTooLarge {
		t.Fatalf("over the limit: %v", err)
	}
}

func TestClientAndServer(t *testing.T) {
	f := listen(t, "127.0.0.1:0")
	c := connect(t, clientChannel(), fmt.Sprintf("quic://127.0.0.1:%d", f.server.Port()), Options{
		Options: client.Options{Headers: func() map[string]string { return map[string]string{"X-Name": "go"} }},
		CA:      f.cert,
	})

	t.Run("post", func(t *testing.T) {
		if got, err := c.Send("hello", nil); err != nil || got != "world" {
			t.Fatalf("hello: %v, %v", got, err)
		}
		body := map[string]any{"list": []any{1.0, "two", nil}, "text": strings.Repeat("é", 20000)}
		got, err := c.Send("echo", body)
		if err != nil || fmt.Sprint(got) != fmt.Sprint(body) {
			t.Fatalf("echo: %v", err)
		}
	})
	t.Run("hello headers become the state, names lowercased", func(t *testing.T) {
		if got, err := c.Send("name", nil); err != nil || got != "go" {
			t.Fatalf("name: %v, %v", got, err)
		}
	})
	t.Run("server calls the client back", func(t *testing.T) {
		if got, err := c.Send("mirror", nil); err != nil || got != "client world" {
			t.Fatalf("mirror: %v, %v", got, err)
		}
	})
	t.Run("stream", func(t *testing.T) {
		values := c.Values("count", nil)
		var got []string
		for {
			next, err := values.Next(context.Background())
			if err != nil {
				t.Fatalf("stream: %v", err)
			}
			if next.Done {
				break
			}
			got = append(got, fmt.Sprint(next.Value))
		}
		if strings.Join(got, ",") != "0,1,2" {
			t.Fatalf("stream values %v", got)
		}
	})
	t.Run("events", func(t *testing.T) {
		var count atomic.Int64
		subscription, err := c.Subscribe("ticks", "a", func(any) { count.Add(1) })
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}
		f.ticks.Send("a", "tick")
		f.ticks.Send("b", "not for us")
		f.ticks.Send("a", "tick")
		waitFor(t, func() bool { return count.Load() == 2 })
		subscription.Cancel()
		time.Sleep(20 * time.Millisecond)
		f.ticks.Send("a", "after cancel")
		time.Sleep(50 * time.Millisecond)
		if count.Load() != 2 {
			t.Fatalf("%d events after cancel", count.Load())
		}
	})
	t.Run("many in flight", func(t *testing.T) {
		requests := make([]*channelgo.SendingRequest, 2000)
		for i := range requests {
			requests[i] = c.Request("echo", i)
		}
		for i, request := range requests {
			if got, err := request.Wait(); err != nil || got != float64(i) {
				t.Fatalf("request %d: %v, %v", i, got, err)
			}
		}
	})
}

func TestCertificateTrust(t *testing.T) {
	f := listen(t, "127.0.0.1:0")
	address := fmt.Sprintf("quic://127.0.0.1:%d", f.server.Port())

	pinned := connect(t, clientChannel(), address, Options{Fingerprint: strings.ToLower(strings.ReplaceAll(fingerprint, ":", ""))})
	if got, err := pinned.Send("hello", nil); err != nil || got != "world" {
		t.Fatalf("pinned: %v, %v", got, err)
	}
	byName := connect(t, clientChannel(), address, Options{CA: f.cert, ServerName: "localhost"})
	if got, err := byName.Send("hello", nil); err != nil || got != "world" {
		t.Fatalf("server name: %v, %v", got, err)
	}

	for name, options := range map[string]Options{
		"system roots":     {},
		"wrong pin":        {Fingerprint: strings.Repeat("00", 32)},
		"name not in cert": {CA: f.cert, ServerName: "example.com"},
	} {
		c := connect(t, clientChannel(), address, options)
		time.Sleep(300 * time.Millisecond)
		if c.Client.IsConnected() {
			t.Fatalf("%s: connected", name)
		}
	}
	if _, err := Connect(clientChannel(), address, Options{Fingerprint: "nope"}); err == nil {
		t.Fatal("an invalid fingerprint is accepted")
	}
	if _, err := Connect(clientChannel(), "ws://127.0.0.1:1", Options{}); err == nil {
		t.Fatal("a ws:// address is accepted")
	}
}

// rawSession dials the server without the Channel client, to check the wire rules.
func rawSession(t *testing.T, f *fixture) (*quicgo.Conn, *quicgo.Stream) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := quicgo.DialAddr(ctx, f.server.Addr(), &tls.Config{InsecureSkipVerify: true, NextProtos: []string{ALPN}}, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.CloseWithError(0, "") })
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	return conn, stream
}

func closed(conn *quicgo.Conn) bool {
	select {
	case <-conn.Context().Done():
		return true
	case <-time.After(2 * time.Second):
		return false
	}
}

func TestWireRules(t *testing.T) {
	f := listen(t, "127.0.0.1:0")

	t.Run("raw frames", func(t *testing.T) {
		_, stream := rawSession(t, f)
		hello, _ := json.Marshal(map[string]any{"hello": map[string]any{"X-Name": "raw", "ignored": 1}})
		frames := appendFrame(nil, hello)
		frames = append(frames, 0) // keepalive
		frames = appendFrame(frames, []byte(`[{"id":1,"path":"name"},{"id":2,"path":"hello"}]`))
		if _, err := stream.Write(frames); err != nil {
			t.Fatal(err)
		}
		reader := newFrameReader(stream, 0)
		got := map[float64]any{}
		for len(got) < 2 {
			payload, err := reader.next()
			if err != nil {
				t.Fatal(err)
			}
			var message map[string]any
			_ = json.Unmarshal(payload, &message)
			got[message["id"].(float64)] = message["body"]
		}
		if got[1] != "raw" || got[2] != "world" {
			t.Fatalf("responses %v", got)
		}
	})
	t.Run("one stream per connection", func(t *testing.T) {
		conn, stream := rawSession(t, f)
		hello, _ := json.Marshal(map[string]any{"hello": map[string]string{}})
		_, _ = stream.Write(appendFrame(nil, hello))
		if _, err := conn.OpenStream(); err == nil {
			t.Fatal("a second stream opened")
		}
		if _, err := conn.OpenUniStream(); err == nil {
			t.Fatal("a unidirectional stream opened")
		}
	})
	for name, first := range map[string][]byte{
		"a first frame that isn't a hello closes": appendFrame(nil, []byte(`{"id":1,"path":"hello"}`)),
		"invalid JSON closes":                     appendFrame(appendFrame(nil, []byte(`{"hello":{}}`)), []byte(`{nope`)),
		"a frame over the limit closes":           append(appendFrame(nil, []byte(`{"hello":{}}`)), 0x80|0x01, 0, 0, 1),
	} {
		t.Run(name, func(t *testing.T) {
			conn, stream := rawSession(t, f)
			_, _ = stream.Write(first)
			if !closed(conn) {
				t.Fatal("still open")
			}
		})
	}
}

func TestReconnectReplaysRequests(t *testing.T) {
	f := listen(t, "127.0.0.1:0")
	port := f.server.Port()
	c := connect(t, clientChannel(), port, Options{CA: f.cert})
	if _, err := c.Send("hello", nil); err != nil {
		t.Fatal(err)
	}

	// The server drops the connection on the first try; the client reconnects and sends it again
	if got, err := c.Send("disconnect", nil); err != nil || got != "dropped before" {
		t.Fatalf("disconnect: %v, %v", got, err)
	}
	if f.stats.connected.Load() != 2 {
		t.Fatalf("%d connections, want 2", f.stats.connected.Load())
	}
	if got, err := c.Send("hello", nil); err != nil || got != "world" {
		t.Fatalf("after a drop: %v, %v", got, err)
	}

	// A request in flight when the server goes away is sent again to the next one on the same port
	request := c.Request("slow", nil)
	time.Sleep(50 * time.Millisecond)
	_ = f.server.Stop()
	waitFor(t, func() bool { return !c.Client.IsConnected() })
	listen(t, fmt.Sprintf("127.0.0.1:%d", port))
	if got, err := request.Wait(); err != nil || got != "slow" {
		t.Fatalf("replayed: %v, %v", got, err)
	}

	// Stop fails what still waits
	c.Client.Stop()
	if _, err := c.Send("hello", nil); err == nil {
		t.Fatal("sent after Stop")
	}
}

func TestStopWhileDisconnectedFailsRequests(t *testing.T) {
	cert := readPEM(t, "cert.pem")
	// Nothing listens there
	c, err := Connect(clientChannel(), "quic://127.0.0.1:1", Options{CA: cert})
	if err != nil {
		t.Fatal(err)
	}
	request := c.Request("hello", nil)
	time.Sleep(50 * time.Millisecond)
	c.Stop()
	done := make(chan error, 1)
	go func() { _, err := request.Wait(); done <- err }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("request succeeded")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("request still waits after Stop")
	}
}

// TestInteropTypeScriptServer runs against the TypeScript reference's fixture (channel/tests/server.ts), which
// serves QUIC on UDP 2049 next to its WebSocket on TCP 2049.
func TestInteropTypeScriptServer(t *testing.T) {
	if err := testutil.WaitForTCP("127.0.0.1:2049", 3*time.Second); err != nil {
		t.Skipf("TypeScript fixture isn't running on 127.0.0.1:2049: %v", err)
	}
	cert := readPEM(t, "cert.pem")
	names := channelgo.NewSubscription()
	ch := clientChannel()
	ch.Stream("stream/values", func(body channelgo.Body, emit channelgo.StreamEmitter) error {
		for i := 0; i < 3; i++ {
			if err := emit.Emit(i); err != nil {
				return err
			}
		}
		return nil
	})
	ch.Events(map[string]*channelgo.Subscription{"names": names})
	c := connect(t, ch, "quic://127.0.0.1:2049", Options{
		Options: client.Options{Headers: func() map[string]string { return map[string]string{"X-Port": "go"} }},
		CA:      cert,
	})
	ready := make(chan struct{})
	go func() {
		for !c.Client.IsConnected() {
			time.Sleep(5 * time.Millisecond)
		}
		close(ready)
	}()
	select {
	case <-ready:
	case <-time.After(3 * time.Second):
		t.Skip("the fixture on 2049 doesn't serve QUIC (an older channel checkout)")
	}

	expect := func(path string, body any, want string) {
		t.Helper()
		got, err := c.Send(path, body)
		if err != nil || fmt.Sprint(got) != want {
			t.Fatalf("%s: %v, %v (want %s)", path, got, err, want)
		}
	}
	expect("hello", nil, "world")
	expect("echo", map[string]any{"a": []any{1, "b"}}, "map[a:[1 b]]")
	expect("mirror", nil, "client world")
	expect("auth", map[string]any{"name": "quic"}, "quic")
	expect("auth/name", nil, "quic")
	headers, err := c.Send("v2/headers", nil)
	if err != nil || headers.(map[string]any)["x-port"] != "go" {
		t.Fatalf("hello headers: %v, %v", headers, err)
	}

	values := c.Values("mirror/stream", nil)
	var got []string
	for {
		next, err := values.Next(context.Background())
		if err != nil {
			t.Fatalf("mirror/stream: %v", err)
		}
		if next.Done {
			break
		}
		got = append(got, fmt.Sprint(next.Value))
	}
	if strings.Join(got, ",") != "0,1,2" {
		t.Fatalf("mirror/stream values %v", got)
	}

	// The fixture subscribes to this client's names/1 and cancels after the first event
	if _, err := c.Send("mirror/events", nil); err != nil {
		t.Fatal(err)
	}
	names.Send("1", "h")

	if _, err := c.Send("v2/error/code", nil); err == nil {
		t.Fatal("v2/error/code succeeded")
	}
	// The fixture drops the connection without answering; cancelled, the request isn't sent again after the
	// client reconnects
	expect("v2/drops/reset", nil, "<nil>")
	drop := c.Request("v2/drop", nil)
	waitFor(t, func() bool { return !c.Client.IsConnected() })
	drop.Cancel()
	expect("hello", nil, "world")
	time.Sleep(100 * time.Millisecond)
	expect("v2/drops", nil, "1")
}

// TestInteropTypeScriptClient serves QUIC from Go and calls it with the TypeScript reference's client.
func TestInteropTypeScriptClient(t *testing.T) {
	bun, err := exec.LookPath("bun")
	if err != nil {
		t.Skip("bun isn't installed")
	}
	reference := filepath.Join("..", "..", "channel", "quic.ts")
	if _, err := os.Stat(reference); err != nil {
		t.Skipf("the TypeScript reference isn't next to this checkout: %v", err)
	}
	f := listen(t, "127.0.0.1:0")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, bun, filepath.Join("testdata", "client.ts"), fmt.Sprint(f.server.Port()), filepath.Join(certDir, "cert.pem"))
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		t.Fatalf("bun: %v\n%s", err, stderr.String())
	}
	var result map[string]any
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("output %q: %v", output, err)
	}
	want := `map[echo:map[a:[1 b]] event:tick hello:world mirror:ts world name:ts values:[0 1 2]]`
	if fmt.Sprint(result) != want {
		t.Fatalf("got %v\nwant %s", result, want)
	}
	waitFor(t, func() bool { return f.stats.disconnected.Load() == f.stats.connected.Load() })
}

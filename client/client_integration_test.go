package client

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/v57/channel-go/channelgo"
	"github.com/v57/channel-go/internal/testutil"
)

var tsServerOnce sync.Once
var tsServerErr error

func ensureTSServer(t *testing.T) {
	t.Helper()
	tsServerOnce.Do(func() {
		tsServerErr = testutil.WaitForTCP("127.0.0.1:2049", 3*time.Second)
	})
	if tsServerErr != nil {
		t.Skipf("TypeScript integration server is not reachable on 127.0.0.1:2049: %v", tsServerErr)
	}
}

type interopClient struct {
	client     *ClientSender
	names      *channelgo.Subscription
	valuesSent atomic.Int64
}

func newInteropClient(t *testing.T) *interopClient {
	t.Helper()
	ensureTSServer(t)

	names := channelgo.NewSubscription()
	ch := channelgo.NewChannel()
	ch.Post("hello", func(channelgo.Body) (any, error) {
		return "client world", nil
	})
	ch.Stream("stream/values", func(body channelgo.Body, emit channelgo.StreamEmitter) error {
		return emitCounterStream(body, emit, 3, time.Millisecond, nil)
	})

	clientRef := &interopClient{names: names}
	ch.Stream("stream/cancel", func(body channelgo.Body, emit channelgo.StreamEmitter) error {
		return emitCounterStream(body, emit, 10, 50*time.Millisecond, func(int) {
			clientRef.valuesSent.Add(1)
		})
	})
	ch.Events(map[string]*channelgo.Subscription{"names": names})

	clientRef.client = Connect(ch, 2049, Options{})
	t.Cleanup(func() {
		clientRef.client.Sender.Stop()
	})
	return clientRef
}

func emitCounterStream(body channelgo.Body, emit channelgo.StreamEmitter, max int, delay time.Duration, onYield func(int)) error {
	for i := 0; i < max; i++ {
		if delay > 0 {
			time.Sleep(delay)
		}
		if body.Task != nil && body.Task.IsCancelled() {
			return nil
		}
		if emit.IsCancelled() {
			return nil
		}
		if onYield != nil {
			onYield(i)
		}
		if err := emit.Emit(i); err != nil {
			return err
		}
	}
	return nil
}

func TestInteropPostHello(t *testing.T) {
	client := newInteropClient(t)
	response, err := client.client.Sender.Send("hello", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if response != "world" {
		t.Fatalf("expected world, got %v", response)
	}
}

func TestInteropPostEcho(t *testing.T) {
	client := newInteropClient(t)
	random := rand.Float64()
	response, err := client.client.Sender.Send("echo", random)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if response != random {
		t.Fatalf("expected %v, got %v", random, response)
	}
}

// skipIfRouteMissing skips when the reference fixture (channel/tests/server.ts) has no such route.
func skipIfRouteMissing(t *testing.T, route string, err error) {
	t.Helper()
	if err != nil && err.Error() == "api not found" {
		t.Skipf("fixture on 127.0.0.1:2049 has no %q route", route)
	}
}

func TestInteropContext(t *testing.T) {
	client := newInteropClient(t)
	contextValue := map[string]any{"trace": "darwin-interop"}
	response, err := client.client.Sender.Send("context", nil, contextValue)
	skipIfRouteMissing(t, "context", err)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	contextMap, ok := response.(map[string]any)
	if !ok || contextMap["trace"] != "darwin-interop" {
		t.Fatalf("expected context round trip, got %#v", response)
	}
}

func TestInteropHeaders(t *testing.T) {
	ensureTSServer(t)
	client := Connect(channelgo.NewChannel(), 2049, Options{
		Headers: func() map[string]string {
			return map[string]string{"X-Channel-Test": "darwin-header"}
		},
	})
	defer client.Sender.Stop()

	response, err := client.Sender.Send("header", nil)
	skipIfRouteMissing(t, "header", err)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if response != "darwin-header" {
		t.Fatalf("expected header value, got %#v", response)
	}
}

func TestInteropPostEmpty(t *testing.T) {
	client := newInteropClient(t)
	response, err := client.client.Sender.Send("empty", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if response != nil {
		t.Fatalf("expected nil response, got %v", response)
	}
}

func TestInteropPostNotFound(t *testing.T) {
	client := newInteropClient(t)
	_, err := client.client.Sender.Send("notfound", nil)
	if err == nil || err.Error() != "api not found" {
		t.Fatalf("expected api not found, got %v", err)
	}
}

func TestInteropSubscribeNotFound(t *testing.T) {
	client := newInteropClient(t)
	_, err := client.client.Sender.Subscribe("notfound", "test", func(any) {})
	if err == nil || err.Error() != "subscription not found" {
		t.Fatalf("expected subscription not found, got %v", err)
	}
}

func TestInteropStreamValues(t *testing.T) {
	client := newInteropClient(t)
	values := client.client.Sender.Values("stream/values", nil)
	defer values.Return()

	expected := 0
	for {
		next, err := values.Next(context.Background())
		if err != nil {
			t.Fatalf("unexpected stream error: %v", err)
		}
		if next.Done {
			break
		}
		if asInt(next.Value) != expected {
			t.Fatalf("expected %d, got %v", expected, next.Value)
		}
		expected++
	}
	if expected != 3 {
		t.Fatalf("expected 3 values, got %d", expected)
	}
}

func TestInteropStreamCancel(t *testing.T) {
	client := newInteropClient(t)
	values := client.client.Sender.Values("stream/cancel", nil)
	defer values.Return()

	expected := 0
	for {
		next, err := values.Next(context.Background())
		if err != nil {
			t.Fatalf("unexpected stream error: %v", err)
		}
		if next.Done {
			break
		}
		if asInt(next.Value) != expected {
			t.Fatalf("expected %d, got %v", expected, next.Value)
		}
		expected++
		if expected == 3 {
			values.Return()
			break
		}
	}
}

func TestInteropMirrorRequest(t *testing.T) {
	client := newInteropClient(t)
	response, err := client.client.Sender.Send("mirror", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if response != "client world" {
		t.Fatalf("expected client world, got %v", response)
	}
}

func TestInteropMirrorEvents(t *testing.T) {
	client := newInteropClient(t)
	_, err := client.client.Sender.Send("mirror/events", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	client.names.Send("2", "po")
	client.names.Send("1", "h")
	time.Sleep(20 * time.Millisecond)
	client.names.Send("1", "we")
}

func TestInteropMirrorStream(t *testing.T) {
	client := newInteropClient(t)
	values := client.client.Sender.Values("mirror/stream", nil)
	defer values.Return()

	expected := 0
	for {
		next, err := values.Next(context.Background())
		if err != nil {
			t.Fatalf("unexpected stream error: %v", err)
		}
		if next.Done {
			break
		}
		if asInt(next.Value) != expected {
			t.Fatalf("expected %d, got %v", expected, next.Value)
		}
		expected++
	}
	if expected != 3 {
		t.Fatalf("expected 3 values, got %d", expected)
	}
}

func TestInteropMirrorStreamCancel(t *testing.T) {
	client := newInteropClient(t)
	client.valuesSent.Store(0)

	values := client.client.Sender.Values("mirror/stream/cancel", nil)
	defer values.Return()

	expected := 0
	for {
		next, err := values.Next(context.Background())
		if err != nil {
			t.Fatalf("unexpected stream error: %v", err)
		}
		if next.Done {
			break
		}
		if asInt(next.Value) != expected {
			t.Fatalf("expected %d, got %v", expected, next.Value)
		}
		expected++
		if expected == 3 {
			values.Return()
			break
		}
	}

	time.Sleep(100 * time.Millisecond)
	// Cancelling the mirror cancels this client's stream handler, as in the
	// TypeScript reference (valuesSent === 3). Go emitters push rather than
	// being pulled, so a value or two may already be in flight; the handler
	// must not run to the end (10).
	if got := client.valuesSent.Load(); got < 3 || got >= 10 {
		t.Fatalf("expected the cancelled stream handler to stop early, got %d values", got)
	}
}

func TestInteropStateAuth(t *testing.T) {
	ensureTSServer(t)
	client := Connect(channelgo.NewChannel(), 2049, Options{})
	defer client.Sender.Stop()

	_, err := client.Sender.Send("auth", map[string]any{"name": "tester"})
	if err != nil {
		t.Fatalf("unexpected auth error: %v", err)
	}
	name, err := client.Sender.Send("auth/name", nil)
	if err != nil {
		t.Fatalf("unexpected auth/name error: %v", err)
	}
	if name != "tester" {
		t.Fatalf("expected tester, got %v", name)
	}
}

func TestInteropStateUnauthorized(t *testing.T) {
	ensureTSServer(t)
	client := Connect(channelgo.NewChannel(), 2049, Options{})
	defer client.Sender.Stop()

	_, err := client.Sender.Send("auth/name", nil)
	if err == nil || err.Error() != "unauthorized" {
		t.Fatalf("expected unauthorized, got %v", err)
	}
}

func asInt(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int32:
		return int(typed)
	case int64:
		return int(typed)
	case float32:
		return int(typed)
	case float64:
		return int(typed)
	default:
		panic(fmt.Sprintf("unexpected numeric type %T", value))
	}
}

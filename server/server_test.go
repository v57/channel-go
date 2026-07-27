package server

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/v57/channel-go/channelgo"
	"github.com/v57/channel-go/client"
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

type testFixture struct {
	server       *Server
	client       *client.ClientSender
	serverEvent  *channelgo.Subscription
	clientNames  *channelgo.Subscription
	valuesSent   atomic.Int64
	mirrorEvents atomic.Int64
	streamCancel atomic.Int64
}

func newFixture(t *testing.T) *testFixture {
	t.Helper()
	serverEvents := map[string]*channelgo.Subscription{
		"hello": channelgo.NewSubscription(),
	}

	fixture := &testFixture{serverEvent: serverEvents["hello"]}
	channel := channelgo.NewChannel()
	channel.Post("hello", func(channelgo.Body) (any, error) {
		return "world", nil
	})
	channel.Post("mirror", func(body channelgo.Body) (any, error) {
		return body.Sender.Send("hello", nil)
	})
	channel.Post("echo", func(body channelgo.Body) (any, error) {
		return body.Body, nil
	})
	channel.Post("empty", func(channelgo.Body) (any, error) {
		return nil, nil
	})
	channel.Post("disconnect", func(body channelgo.Body) (any, error) {
		body.Sender.Stop()
		return nil, nil
	})
	channel.Post("auth", func(body channelgo.Body) (any, error) {
		payload := body.Body.(map[string]any)
		state := body.State.(map[string]any)
		state["name"] = payload["name"]
		return payload["name"], nil
	})
	channel.Post("auth/name", func(body channelgo.Body) (any, error) {
		state := body.State.(map[string]any)
		if value, ok := state["name"]; ok {
			return value, nil
		}
		return nil, fmt.Errorf("unauthorized")
	})
	channel.Stream("stream/values", func(body channelgo.Body, emit channelgo.StreamEmitter) error {
		return emitCounterStream(body, emit, 3, time.Millisecond, nil)
	})
	channel.Post("mirror/events", func(body channelgo.Body) (any, error) {
		var sub channelgo.Cancellable
		created, err := body.Sender.Subscribe("names", "1", func(any) {
			fixture.mirrorEvents.Add(1)
			if sub != nil {
				sub.Cancel()
			}
		})
		if err != nil {
			return nil, err
		}
		sub = created
		return nil, nil
	})
	channel.Stream("mirror/stream", func(body channelgo.Body, emit channelgo.StreamEmitter) error {
		values := body.Sender.Values("stream/values", nil)
		return forwardValues(body, emit, values)
	})
	channel.Stream("stream/cancel", func(body channelgo.Body, emit channelgo.StreamEmitter) error {
		return emitCounterStream(body, emit, 10, 50*time.Millisecond, func(int) {
			fixture.streamCancel.Add(1)
		})
	})
	channel.Stream("mirror/stream/cancel", func(body channelgo.Body, emit channelgo.StreamEmitter) error {
		values := body.Sender.Values("stream/cancel", nil)
		return forwardValues(body, emit, values)
	})
	channel.Events(serverEvents)

	serverInstance, err := Listen(channel, 0, Options{State: func(headers map[string]string) any {
		_ = headers
		return map[string]any{}
	}})
	if err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	fixture.server = serverInstance
	t.Cleanup(func() {
		_ = fixture.server.Stop()
	})

	clientEvents := map[string]*channelgo.Subscription{
		"names": channelgo.NewSubscription(),
	}
	clientChannel := channelgo.NewChannel()
	clientChannel.Post("hello", func(channelgo.Body) (any, error) {
		return "client world", nil
	})
	clientChannel.Stream("stream/values", func(body channelgo.Body, emit channelgo.StreamEmitter) error {
		return emitCounterStream(body, emit, 3, time.Millisecond, nil)
	})
	clientChannel.Stream("stream/cancel", func(body channelgo.Body, emit channelgo.StreamEmitter) error {
		return emitCounterStream(body, emit, 10, 50*time.Millisecond, func(int) {
			fixture.valuesSent.Add(1)
		})
	})
	clientChannel.Events(clientEvents)

	fixture.client = client.Connect(clientChannel, serverInstance.Addr(), client.Options{})
	fixture.clientNames = clientEvents["names"]
	t.Cleanup(func() {
		fixture.client.Sender.Stop()
	})
	fixture.serverEvent = serverEvents["hello"]
	return fixture
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

func forwardValues(body channelgo.Body, emit channelgo.StreamEmitter, values *channelgo.Values) error {
	if body.Task != nil {
		body.Task.OnCancel(values.Return)
	}
	defer values.Return()
	for {
		if body.Task != nil && body.Task.IsCancelled() {
			return nil
		}
		next, err := values.Next(context.Background())
		if err != nil {
			if body.Task != nil && body.Task.IsCancelled() {
				return nil
			}
			return err
		}
		if next.Done {
			return nil
		}
		if err := emit.Emit(next.Value); err != nil {
			return err
		}
	}
}

func TestGoClientServerInteroperability(t *testing.T) {
	fixture := newFixture(t)

	t.Run("post/hello", func(t *testing.T) {
		response, err := fixture.client.Sender.Send("hello", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if response != "world" {
			t.Fatalf("expected world, got %v", response)
		}
	})

	t.Run("post/echo", func(t *testing.T) {
		random := rand.Float64()
		response, err := fixture.client.Sender.Send("echo", random)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if response != random {
			t.Fatalf("expected %v, got %v", random, response)
		}
	})

	t.Run("post/empty", func(t *testing.T) {
		response, err := fixture.client.Sender.Send("empty", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if response != nil {
			t.Fatalf("expected nil, got %v", response)
		}
	})

	t.Run("post/notfound", func(t *testing.T) {
		_, err := fixture.client.Sender.Send("notfound", nil)
		if err == nil || err.Error() != "api not found" {
			t.Fatalf("expected api not found, got %v", err)
		}
	})

	t.Run("events", func(t *testing.T) {
		const topicKey = "test-events"
		var count atomic.Int64
		subscription, err := fixture.client.Sender.Subscribe("hello", topicKey, func(event any) {
			count.Add(1)
			_ = event
		})
		if err != nil {
			t.Fatalf("unexpected subscribe error: %v", err)
		}
		fixture.serverEvent.Send(topicKey, "event 1")
		fixture.serverEvent.Send(topicKey, "event 2")
		waitFor(t, func() bool { return count.Load() == 2 })
		subscription.Cancel()
	})

	t.Run("events/cancel", func(t *testing.T) {
		const topicKey = "test-cancel"
		var count atomic.Int64
		subscription, err := fixture.client.Sender.Subscribe("hello", topicKey, func(event any) {
			count.Add(1)
			_ = event
		})
		if err != nil {
			t.Fatalf("unexpected subscribe error: %v", err)
		}
		fixture.serverEvent.Send(topicKey, "event 1")
		waitFor(t, func() bool { return count.Load() == 1 })
		subscription.Cancel()
		fixture.serverEvent.Send(topicKey, "event 2")
		time.Sleep(20 * time.Millisecond)
		if got := count.Load(); got != 1 {
			t.Fatalf("expected no more events after cancel, got %d", got)
		}
	})

	t.Run("events/notfound", func(t *testing.T) {
		_, err := fixture.client.Sender.Subscribe("notfound", "test", func(any) {})
		if err == nil || err.Error() != "subscription not found" {
			t.Fatalf("expected subscription not found, got %v", err)
		}
	})

	t.Run("stream", func(t *testing.T) {
		values := fixture.client.Sender.Values("stream/values", nil)
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
	})

	t.Run("stream/cancel", func(t *testing.T) {
		values := fixture.client.Sender.Values("stream/cancel", nil)
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
		if got := fixture.streamCancel.Load(); got != 10 {
			t.Fatalf("expected stream handler to drain to 10 values, got %d", got)
		}
	})

	t.Run("server/post", func(t *testing.T) {
		response, err := fixture.client.Sender.Send("mirror", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if response != "client world" {
			t.Fatalf("expected client world, got %v", response)
		}
	})

	t.Run("server/subscribe", func(t *testing.T) {
		_, err := fixture.client.Sender.Send("mirror/events", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		fixture.clientNames.Send("2", "po")
		fixture.clientNames.Send("1", "h")
		waitFor(t, func() bool { return fixture.mirrorEvents.Load() == 1 })
		fixture.clientNames.Send("1", "we")
		time.Sleep(20 * time.Millisecond)
		if fixture.mirrorEvents.Load() != 1 {
			t.Fatalf("expected exactly one mirrored event")
		}
	})

	t.Run("server/stream", func(t *testing.T) {
		values := fixture.client.Sender.Values("mirror/stream", nil)
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
	})

	t.Run("server/stream/cancel", func(t *testing.T) {
		fixture.valuesSent.Store(0)
		values := fixture.client.Sender.Values("mirror/stream/cancel", nil)
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
		if got := fixture.valuesSent.Load(); got != 10 {
			t.Fatalf("expected mirrored stream handler to drain to 10 values, got %d", got)
		}
	})

	t.Run("state/auth", func(t *testing.T) {
		another := client.Connect(channelgo.NewChannel(), fixture.server.Addr(), client.Options{})
		defer another.Sender.Stop()
		_, err := another.Sender.Send("auth", map[string]any{"name": "tester"})
		if err != nil {
			t.Fatalf("unexpected auth error: %v", err)
		}
		name, err := another.Sender.Send("auth/name", nil)
		if err != nil {
			t.Fatalf("unexpected auth/name error: %v", err)
		}
		if name != "tester" {
			t.Fatalf("expected tester, got %v", name)
		}
	})

	t.Run("state/unauthorized", func(t *testing.T) {
		another := client.Connect(channelgo.NewChannel(), fixture.server.Addr(), client.Options{})
		defer another.Sender.Stop()
		_, err := another.Sender.Send("auth/name", nil)
		if err == nil || err.Error() != "unauthorized" {
			t.Fatalf("expected unauthorized, got %v", err)
		}
	})
}

func TestListenStopCleansUpSubscriptionPublishers(t *testing.T) {
	updates := channelgo.NewSubscription()
	ch := channelgo.NewChannel().Events(map[string]*channelgo.Subscription{"updates": updates})

	if updates.PublisherCount() != 0 {
		t.Fatalf("expected no publishers before listen")
	}

	first, err := Listen(ch, 0, Options{})
	if err != nil {
		t.Fatalf("failed to start first listener: %v", err)
	}
	if updates.PublisherCount() != 1 {
		t.Fatalf("expected one publisher after first listen")
	}
	_ = first.Stop()
	waitFor(t, func() bool { return updates.PublisherCount() == 0 })

	second, err := Listen(ch, 0, Options{})
	if err != nil {
		t.Fatalf("failed to start second listener: %v", err)
	}
	if updates.PublisherCount() != 1 {
		t.Fatalf("expected one publisher after second listen")
	}
	_ = second.Stop()
	waitFor(t, func() bool { return updates.PublisherCount() == 0 })
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
		panic(fmt.Sprintf("unexpected number type %T", value))
	}
}

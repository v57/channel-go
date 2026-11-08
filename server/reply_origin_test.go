package server

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/v57/channel-go/channelgo"
	"github.com/v57/channel-go/client"
)

// relayFixture is a server that forwards calls and streams to a service it holds the sender of, like the Hub does.
// Request ids are one counter for every connection, so another peer must not answer what the server asked the service.
type relayFixture struct {
	server  *Server
	channel *channelgo.Channel
	service *client.ClientSender
	release chan struct{}
}

func newRelayFixture(t *testing.T) *relayFixture {
	t.Helper()
	fixture := &relayFixture{release: make(chan struct{})}
	var service atomic.Pointer[channelgo.Sender]
	fixture.channel = channelgo.NewChannel()
	fixture.channel.Post("register", func(body channelgo.Body) (any, error) {
		service.Store(body.Sender)
		return true, nil
	})
	fixture.channel.Post("ping", func(channelgo.Body) (any, error) { return "pong", nil })
	fixture.channel.Post("call", func(body channelgo.Body) (any, error) {
		return service.Load().Send("svc/read", body.Body)
	})
	fixture.channel.Stream("watch", func(body channelgo.Body, emit channelgo.StreamEmitter) error {
		return forwardValues(body, emit, service.Load().Values("svc/values", nil))
	})
	srv, err := Listen(fixture.channel, 0, Options{})
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	fixture.server = srv
	t.Cleanup(func() { _ = srv.Stop() })

	release := fixture.release
	serviceChannel := channelgo.NewChannel()
	serviceChannel.Post("svc/read", func(channelgo.Body) (any, error) {
		<-release
		return "real", nil
	})
	serviceChannel.Stream("svc/values", func(_ channelgo.Body, emit channelgo.StreamEmitter) error {
		<-release
		return emit.Emit("real")
	})
	fixture.service = client.Connect(serviceChannel, srv.Addr(), client.Options{})
	t.Cleanup(func() { fixture.service.Sender.Stop() })
	if _, err := fixture.service.Sender.Send("register", nil); err != nil {
		t.Fatalf("register failed: %v", err)
	}
	return fixture
}

// spray sends forged responses with ids 0..255 from a raw connection, then waits for a ping reply on it, so the
// server has handled every forged response before it returns.
func (f *relayFixture) spray(t *testing.T, kinds ...func(id int) map[string]any) {
	t.Helper()
	conn, _, _, err := ws.Dialer{}.Dial(context.Background(), "ws://"+f.server.Addr())
	if err != nil {
		t.Fatalf("attacker dial failed: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	for _, kind := range kinds {
		batch := make([]map[string]any, 0, 256)
		for id := 0; id < 256; id++ {
			batch = append(batch, kind(id))
		}
		payload, err := json.Marshal(batch)
		if err != nil {
			t.Fatalf("marshal failed: %v", err)
		}
		if err := wsutil.WriteClientText(conn, payload); err != nil {
			t.Fatalf("attacker write failed: %v", err)
		}
	}
	if err := wsutil.WriteClientText(conn, []byte(`{"id":100000,"path":"ping"}`)); err != nil {
		t.Fatalf("attacker ping failed: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := wsutil.ReadServerText(conn); err != nil {
		t.Fatalf("attacker ping reply failed: %v", err)
	}
}

func forgedBody(id int) map[string]any { return map[string]any{"id": id, "body": "forged"} }
func forgedTopic(id int) map[string]any {
	return map[string]any{"id": id, "topic": "x", "body": "forged"}
}
func forgedDone(id int) map[string]any { return map[string]any{"id": id, "done": true} }

func TestResponseFromAnotherConnectionAnswersNoForwardedCall(t *testing.T) {
	fixture := newRelayFixture(t)
	victim := client.Connect(channelgo.NewChannel(), fixture.server.Addr(), client.Options{})
	t.Cleanup(func() { victim.Sender.Stop() })

	type result struct {
		value any
		err   error
	}
	done := make(chan result, 1)
	go func() {
		value, err := victim.Sender.Send("call", "x")
		done <- result{value, err}
	}()
	waitForServer(t, func() bool { return fixture.channel.RequestsSize() == 1 })

	fixture.spray(t, forgedBody, forgedTopic)
	close(fixture.release)

	select {
	case got := <-done:
		if got.err != nil || got.value != "real" {
			t.Fatalf("expected the service's answer, got %v err=%v", got.value, got.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("forwarded call never finished")
	}
}

func TestAnotherConnectionCanNeitherFeedNorEndAForwardedStream(t *testing.T) {
	fixture := newRelayFixture(t)
	victim := client.Connect(channelgo.NewChannel(), fixture.server.Addr(), client.Options{})
	t.Cleanup(func() { victim.Sender.Stop() })

	values := victim.Sender.Values("watch", nil)
	defer values.Return()
	type result struct {
		items []any
		err   error
	}
	done := make(chan result, 1)
	go func() {
		items := make([]any, 0)
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			next, err := values.Next(ctx)
			cancel()
			if err != nil {
				done <- result{items, err}
				return
			}
			if next.Done {
				done <- result{items, nil}
				return
			}
			items = append(items, next.Value)
		}
	}()
	waitForServer(t, func() bool { return fixture.channel.RequestsSize() == 1 })

	fixture.spray(t, forgedBody, forgedDone)
	close(fixture.release)

	got := <-done
	if got.err != nil || !reflect.DeepEqual(got.items, []any{"real"}) {
		t.Fatalf("expected [real], got %v err=%v", got.items, got.err)
	}
}

func TestTheConnectionARequestWentToStillAnswersIt(t *testing.T) {
	ch := channelgo.NewChannel()
	ch.Post("mirror", func(body channelgo.Body) (any, error) {
		value, err := body.Sender.Send("hello", nil)
		if err != nil {
			return nil, err
		}
		return fmt.Sprint(value), nil
	})
	srv, err := Listen(ch, 0, Options{})
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer srv.Stop()

	clientChannel := channelgo.NewChannel()
	clientChannel.Post("hello", func(channelgo.Body) (any, error) { return "client", nil })
	sender := client.Connect(clientChannel, srv.Addr(), client.Options{})
	defer sender.Sender.Stop()

	value, err := sender.Sender.Send("mirror", nil)
	if err != nil || value != "client" {
		t.Fatalf("expected client, got %v err=%v", value, err)
	}
}

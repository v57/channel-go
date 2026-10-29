package quic

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	quicgo "github.com/quic-go/quic-go"
	"github.com/v57/channel-go/channelgo"
	"github.com/v57/channel-go/client"
)

// Options configures a QUIC client: the WebSocket client's options plus certificate trust.
type Options struct {
	client.Options
	// CA is a PEM certificate (or CA) to trust, for self-signed servers. Without CA or Fingerprint the server
	// certificate must be trusted by the system.
	CA []byte
	// Fingerprint pins the SHA-256 of the server certificate (hex, colons optional) instead of verifying it.
	Fingerprint string
	// ServerName overrides the name sent and checked; it defaults to the address host.
	ServerName string
}

// ClientSender bundles Sender with its QUIC transport.
type ClientSender struct {
	Sender *channelgo.Sender
	Client *Client
}

func (c *ClientSender) Send(path string, body any, contextValue ...any) (any, error) {
	return c.Sender.Send(path, body, contextValue...)
}

func (c *ClientSender) Request(path string, body any, contextValue ...any) *channelgo.SendingRequest {
	return c.Sender.Request(path, body, contextValue...)
}

func (c *ClientSender) Values(path string, body any, contextValue ...any) *channelgo.Values {
	return c.Sender.Values(path, body, contextValue...)
}

func (c *ClientSender) Subscribe(path string, body any, event channelgo.EventBody, contextValue ...any) (channelgo.Cancellable, error) {
	return c.Sender.Subscribe(path, body, event, contextValue...)
}

func (c *ClientSender) Stop() {
	c.Sender.Stop()
}

// Connect opens a reconnecting QUIC connection to address (quic://host:port, host:port or a local port) and binds
// it to the channel. It fails only for an invalid address or certificate option.
func Connect(ch *channelgo.Channel, address any, options Options) (*ClientSender, error) {
	hostPort, host, err := resolveClientAddress(address)
	if err != nil {
		return nil, err
	}
	tlsConfig, err := clientTLS(host, options)
	if err != nil {
		return nil, err
	}
	c := &Client{
		address:    hostPort,
		tls:        tlsConfig,
		headers:    options.Headers,
		maxPayload: options.MaxPayload,
		OnMessage:  options.OnMessage,
		channel:    ch,
		state:      map[string]any{},
		pending:    make(map[int]*pendingSend),
		subscribed: make(map[string]map[int]channelgo.EventBody),
		remote:     make(map[string]struct{}),
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
	}
	c.sender = channelgo.MakeSender(ch, c)
	c.controller = &channelgo.Controller{
		// Handlers run on their own goroutines, so one that calls back over this connection doesn't block reading
		Async:         true,
		ResponseFn:    func(body channelgo.Message) { c.Notify(body) },
		SubscribeFn:   c.addRemoteTopic,
		UnsubscribeFn: c.removeRemoteTopic,
		EventFn:       c.ReceivedEvent,
		SenderValue:   c.sender,
		StateValue:    c.state,
	}
	if options.OnConnect != nil {
		c.OnOpen = func() { options.OnConnect(c.sender) }
	}
	publisher := &clientPublisher{client: c}
	var subscriptions []*channelgo.Subscription
	ch.ForEachEvent(func(_ string, subscription *channelgo.Subscription) {
		subscription.AddPublisher(publisher)
		subscriptions = append(subscriptions, subscription)
	})
	c.cleanup = func() {
		for _, subscription := range subscriptions {
			subscription.RemovePublisher(publisher)
		}
	}
	c.running.Store(true)
	go c.run()
	return &ClientSender{Sender: c.sender, Client: c}, nil
}

type pendingSend struct {
	body      any
	requestID int
	// written is true once the message went out on the current connection
	written bool
}

// Client is a reconnecting QUIC transport: one connection, one stream, frames both ways.
type Client struct {
	address    string
	tls        *tls.Config
	headers    func() map[string]string
	maxPayload int

	OnOpen    func()
	OnClose   func()
	OnMessage func(message any)

	channel    *channelgo.Channel
	state      map[string]any
	sender     *channelgo.Sender
	controller *channelgo.Controller
	cleanup    func()

	running   atomic.Bool
	connected atomic.Bool
	stopOnce  sync.Once
	stop      chan struct{}
	done      chan struct{}

	// mu guards the current connection's writer and the messages waiting for a response, which are sent again
	// after a reconnect
	mu      sync.Mutex
	writer  *frameWriter
	conn    *quicgo.Conn
	nextID  int
	order   []int
	pending map[int]*pendingSend

	subsMu     sync.RWMutex
	subscribed map[string]map[int]channelgo.EventBody
	nextTopic  int

	remoteMu sync.Mutex
	remote   map[string]struct{}
}

func (c *Client) IsConnected() bool {
	return c.connected.Load()
}

func (c *Client) IsRunning() bool {
	return c.running.Load()
}

func (c *Client) run() {
	defer close(c.done)
	for c.running.Load() {
		if err := c.session(); err != nil && c.running.Load() {
			// Same retry delay as the WebSocket client
			select {
			case <-c.stop:
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	c.detach(false)
	if c.cleanup != nil {
		c.cleanup()
	}
}

// session dials, sends the hello and reads until the connection ends.
func (c *Client) session() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	go func() {
		select {
		case <-c.stop:
			cancel()
		case <-ctx.Done():
		}
	}()
	conn, err := quicgo.DialAddr(ctx, c.address, c.tls, &quicgo.Config{
		HandshakeIdleTimeout:  5 * time.Second,
		MaxIdleTimeout:        idleTimeout,
		MaxIncomingStreams:    -1,
		MaxIncomingUniStreams: -1,
	})
	if err != nil {
		cancel()
		return err
	}
	stream, err := conn.OpenStreamSync(ctx)
	cancel()
	if err != nil {
		_ = conn.CloseWithError(0, "")
		return err
	}
	writer := newFrameWriter(stream, func(error) { _ = conn.CloseWithError(0, "") })
	hello, _ := json.Marshal(map[string]any{"hello": c.headerValues()})
	writer.write(hello)
	if !c.attach(conn, writer) {
		writer.close()
		_ = conn.CloseWithError(0, "")
		return nil
	}
	if c.OnOpen != nil {
		go c.OnOpen()
	}
	ticker := time.NewTicker(keepAlive)
	defer ticker.Stop()
	go func() {
		for {
			select {
			case <-ticker.C:
				writer.write(nil)
			case <-conn.Context().Done():
				return
			}
		}
	}()

	reader := newFrameReader(stream, c.maxPayload)
	for {
		payload, err := reader.next()
		if err == nil {
			var message any
			if message, err = decodePayload(payload); err == nil {
				if c.OnMessage != nil {
					c.OnMessage(message)
				}
				c.channel.Receive(message, c.controller)
				continue
			}
		}
		c.detach(c.running.Load())
		return err
	}
}

// attach makes conn the current connection and sends what waits for it, oldest first.
func (c *Client) attach(conn *quicgo.Conn, writer *frameWriter) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.running.Load() {
		return false
	}
	c.conn, c.writer = conn, writer
	c.connected.Store(true)
	c.flushLocked()
	return true
}

func (c *Client) flushLocked() {
	for _, id := range c.order {
		entry := c.pending[id]
		if entry == nil || entry.written {
			continue
		}
		c.writer.writeMessage(entry.body)
		entry.written = true
		if entry.requestID < 0 {
			// Nothing answers a message without an id, so it isn't sent again
			delete(c.pending, id)
		}
	}
	c.order = c.order[:0]
}

// detach drops the current connection. With replay, unanswered requests stay to be sent again on the next one;
// otherwise they fail.
func (c *Client) detach(replay bool) {
	c.mu.Lock()
	conn, writer := c.conn, c.writer
	c.conn, c.writer = nil, nil
	var skipped map[int]struct{}
	if replay {
		ids := make([]int, 0, len(c.pending))
		for id, entry := range c.pending {
			entry.written = false
			ids = append(ids, id)
			if entry.requestID >= 0 {
				if skipped == nil {
					skipped = make(map[int]struct{})
				}
				skipped[entry.requestID] = struct{}{}
			}
		}
		sort.Ints(ids)
		c.order = ids
	} else {
		c.pending = make(map[int]*pendingSend)
		c.order = nil
	}
	c.mu.Unlock()
	if conn == nil {
		if !replay {
			// Stopped between connections: the drop already ran the disconnect handlers, so only fail the requests
			c.channel.DisconnectSilent(c.state, c.sender, nil)
		}
		return
	}
	writer.close()
	_ = conn.CloseWithError(0, "")
	c.connected.Store(false)
	c.remoteMu.Lock()
	clear(c.remote)
	c.remoteMu.Unlock()
	if replay {
		c.channel.DisconnectSkipped(c.state, c.sender, skipped)
	} else {
		c.channel.Disconnect(c.state, c.sender)
	}
	if c.OnClose != nil {
		c.OnClose()
	}
}

// Send sends a message now or, while disconnected, once connected; requests are sent again after a reconnect
// until Sent or Cancel.
func (c *Client) Send(body any) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := c.nextID
	c.nextID++
	entry := &pendingSend{body: body, requestID: requestID(body)}
	if c.writer != nil {
		c.writer.writeMessage(body)
		entry.written = true
		if entry.requestID < 0 {
			return id
		}
	} else {
		c.order = append(c.order, id)
	}
	c.pending[id] = entry
	return id
}

// SendClosed fails requests made after Stop instead of queueing them.
func (c *Client) SendClosed() bool {
	return !c.running.Load()
}

func (c *Client) Sent(id int) {
	c.mu.Lock()
	delete(c.pending, id)
	c.mu.Unlock()
}

// Cancel drops a message; true when it never went out, so the server needs no cancel.
func (c *Client) Cancel(id int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.pending[id]
	if entry == nil {
		return false
	}
	delete(c.pending, id)
	return !entry.written
}

// Notify sends a message that isn't kept for a reconnect (responses, cancels, events); dropped while disconnected.
func (c *Client) Notify(body any) {
	c.mu.Lock()
	writer := c.writer
	c.mu.Unlock()
	if writer != nil {
		writer.writeMessage(body)
	}
}

func (c *Client) AddTopic(topic string, event channelgo.EventBody) func() bool {
	c.subsMu.Lock()
	c.nextTopic++
	id := c.nextTopic
	handlers := c.subscribed[topic]
	if handlers == nil {
		handlers = make(map[int]channelgo.EventBody)
		c.subscribed[topic] = handlers
	}
	handlers[id] = event
	c.subsMu.Unlock()

	return func() bool {
		c.subsMu.Lock()
		handlers := c.subscribed[topic]
		if handlers == nil {
			c.subsMu.Unlock()
			return false
		}
		delete(handlers, id)
		if len(handlers) > 0 {
			c.subsMu.Unlock()
			return false
		}
		delete(c.subscribed, topic)
		c.subsMu.Unlock()
		c.Notify(channelgo.Message{Unsub: &topic})
		return true
	}
}

func (c *Client) ReceivedEvent(topic string, event any) {
	c.subsMu.RLock()
	handlers := make([]channelgo.EventBody, 0, len(c.subscribed[topic]))
	for _, handler := range c.subscribed[topic] {
		handlers = append(handlers, handler)
	}
	c.subsMu.RUnlock()
	for _, handler := range handlers {
		handler(event)
	}
}

// Stop closes the connection for good and fails the requests still waiting.
func (c *Client) Stop() {
	c.stopOnce.Do(func() {
		c.mu.Lock()
		c.running.Store(false)
		conn := c.conn
		c.mu.Unlock()
		close(c.stop)
		if conn != nil {
			_ = conn.CloseWithError(0, "")
		}
	})
	<-c.done
}

func (c *Client) addRemoteTopic(topic string) {
	c.remoteMu.Lock()
	c.remote[topic] = struct{}{}
	c.remoteMu.Unlock()
}

func (c *Client) removeRemoteTopic(topic string) {
	c.remoteMu.Lock()
	delete(c.remote, topic)
	c.remoteMu.Unlock()
}

func (c *Client) headerValues() map[string]string {
	out := map[string]string{}
	if c.headers == nil {
		return out
	}
	for key, value := range c.headers() {
		if value != "" {
			out[key] = value
		}
	}
	return out
}

// clientPublisher sends this channel's events to the server when it subscribed to them.
type clientPublisher struct {
	client *Client
}

func (p *clientPublisher) Publish(event channelgo.SubscriptionEvent) {
	p.client.remoteMu.Lock()
	_, ok := p.client.remote[event.Topic]
	p.client.remoteMu.Unlock()
	if ok {
		topic := event.Topic
		p.client.Notify(channelgo.Message{Topic: &topic, Body: event.Body})
	}
}

func resolveClientAddress(address any) (hostPort, host string, err error) {
	switch value := address.(type) {
	case int:
		return net.JoinHostPort("127.0.0.1", strconv.Itoa(value)), "127.0.0.1", nil
	case string:
		if !strings.Contains(value, "://") {
			if !strings.Contains(value, ":") {
				value = "127.0.0.1:" + value
			}
			value = "quic://" + value
		}
		parsed, err := url.Parse(value)
		if err != nil {
			return "", "", err
		}
		if parsed.Scheme != "quic" || parsed.Hostname() == "" || parsed.Port() == "" {
			return "", "", fmt.Errorf("channel quic: expected quic://host:port, got %q", value)
		}
		return net.JoinHostPort(parsed.Hostname(), parsed.Port()), parsed.Hostname(), nil
	default:
		return "", "", fmt.Errorf("channel quic: unsupported address %v", address)
	}
}

func requestID(body any) int {
	switch value := body.(type) {
	case channelgo.Message:
		if value.ID != nil {
			return *value.ID
		}
	case *channelgo.Message:
		if value != nil && value.ID != nil {
			return *value.ID
		}
	}
	return -1
}

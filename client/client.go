package client

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/v57/channel-go/channelgo"
	"github.com/v57/channel-go/internal/jsonx"
	"github.com/v57/channel-go/internal/poller"
	"github.com/v57/channel-go/internal/wsraw"
	"golang.org/x/sys/unix"
)

// Options configures websocket client connection behavior.
type Options struct {
	Headers   func() map[string]string
	OnConnect func(sender *channelgo.Sender)
	OnMessage func(message any)
	// MaxPayload caps an incoming websocket frame payload in bytes; zero means 16 MiB.
	MaxPayload int
}

// ClientSender bundles Sender with its websocket transport.
type ClientSender struct {
	Sender *channelgo.Sender
	WS     *WebSocketClient
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

// Connect creates a websocket connection and binds it to the channel runtime.
func Connect(ch *channelgo.Channel, address any, options Options) *ClientSender {
	resolved := resolveAddress(address)
	wsClient := NewWebSocketClient(resolved, options.Headers)
	wsClient.channel = ch
	wsClient.state = map[string]any{}
	wsClient.OnMessage = options.OnMessage
	wsClient.MaxPayload = options.MaxPayload

	publicConn := &clientConnection{ws: wsClient}
	loopConn := &clientConnection{ws: wsClient, loop: true}
	publicSender := channelgo.MakeSender(ch, publicConn)
	loopSender := channelgo.MakeSender(ch, loopConn)

	wsClient.publicSender = publicSender
	wsClient.loopSender = loopSender
	wsClient.controller = &channelgo.Controller{
		ResponseFn: func(body channelgo.Message) {
			wsClient.loopNotify(body)
		},
		SubscribeFn: func(topic string) {
			wsClient.addRemoteTopic(topic)
		},
		UnsubscribeFn: func(topic string) {
			wsClient.removeRemoteTopic(topic)
		},
		EventFn: func(topic string, body any) {
			wsClient.ReceivedEvent(topic, body)
		},
		SenderValue: loopSender,
		StateValue:  wsClient.state,
	}

	if options.OnConnect != nil {
		wsClient.OnOpen = func() {
			options.OnConnect(loopSender)
		}
	}

	publishers := make([]struct {
		subscription *channelgo.Subscription
		publisher    channelgo.Publisher
	}, 0)
	wsClient.cleanupPublishers = func() {
		for _, item := range publishers {
			item.subscription.RemovePublisher(item.publisher)
		}
	}

	ch.ForEachEvent(func(_ string, subscription *channelgo.Subscription) {
		publisher := &topicPublisher{ws: wsClient}
		subscription.AddPublisher(publisher)
		publishers = append(publishers, struct {
			subscription *channelgo.Subscription
			publisher    channelgo.Publisher
		}{subscription: subscription, publisher: publisher})
	})

	wsClient.start()
	return &ClientSender{Sender: publicSender, WS: wsClient}
}

type topicPublisher struct {
	ws *WebSocketClient
}

func (p *topicPublisher) Publish(event channelgo.SubscriptionEvent) {
	p.ws.PublishEvent(event)
}

type topicSet struct {
	mu sync.RWMutex
	m  map[string]struct{}
}

func (s *topicSet) Add(topic string) {
	s.mu.Lock()
	s.m[topic] = struct{}{}
	s.mu.Unlock()
}

func (s *topicSet) Delete(topic string) {
	s.mu.Lock()
	delete(s.m, topic)
	s.mu.Unlock()
}

func (s *topicSet) Has(topic string) bool {
	s.mu.RLock()
	_, ok := s.m[topic]
	s.mu.RUnlock()
	return ok
}

type pendingSend struct {
	body      any
	requestID int
}

// WebSocketClient is a reconnecting websocket transport driven by a single loop goroutine.
type WebSocketClient struct {
	address string
	headers func() map[string]string

	OnOpen    func()
	OnClose   func()
	OnMessage func(message any)
	// MaxPayload caps an incoming websocket frame payload in bytes; zero means 16 MiB.
	MaxPayload int

	running   atomic.Bool
	connected atomic.Bool
	// loopGoroutine is the id of the goroutine running the event loop, 0 when
	// stopped.
	loopGoroutine atomic.Uint64

	channel      *channelgo.Channel
	state        any
	controller   *channelgo.Controller
	publicSender *channelgo.Sender
	loopSender   *channelgo.Sender

	pendingMu    sync.Mutex
	pendingOrder []int
	pending      map[int]pendingSend
	nextID       atomic.Int64

	notifyMu    sync.Mutex
	notifyQueue []any

	publishMu    sync.Mutex
	publishQueue []channelgo.SubscriptionEvent

	subsMu      sync.RWMutex
	subscribed  map[string]map[int]channelgo.EventBody
	nextTopicID atomic.Int64

	wakeReader *os.File
	wakeWriter *os.File

	loopDone chan struct{}

	loop              *clientLoopState
	cleanupOnce       sync.Once
	cleanupPublishers func()
}

type clientLoopState struct {
	poller        poller.Poller
	wakeFD        int
	conn          net.Conn
	connFD        int
	reader        wsraw.Reader
	readBuf       []byte
	writeQueue    [][]byte
	writeOffset   int
	remoteTopics  map[string]struct{}
	nextReconnect time.Time
}

func NewWebSocketClient(address string, headers func() map[string]string) *WebSocketClient {
	wakeReader, wakeWriter, err := os.Pipe()
	if err != nil {
		panic(err)
	}
	if err := unix.SetNonblock(int(wakeReader.Fd()), true); err != nil {
		panic(err)
	}
	if err := unix.SetNonblock(int(wakeWriter.Fd()), true); err != nil {
		panic(err)
	}
	return &WebSocketClient{
		address:    address,
		headers:    headers,
		pending:    make(map[int]pendingSend),
		subscribed: make(map[string]map[int]channelgo.EventBody),
		wakeReader: wakeReader,
		wakeWriter: wakeWriter,
		loopDone:   make(chan struct{}),
	}
}

func (c *WebSocketClient) IsConnected() bool {
	return c.connected.Load()
}

func (c *WebSocketClient) IsRunning() bool {
	return c.running.Load()
}

func (c *WebSocketClient) start() {
	if c.running.Load() {
		return
	}
	c.running.Store(true)
	go c.run()
}

func (c *WebSocketClient) run() {
	defer close(c.loopDone)
	c.loopGoroutine.Store(goroutineID())
	defer c.loopGoroutine.Store(0)
	defer c.connected.Store(false)
	defer c.running.Store(false)
	defer c.cleanupLoopResources()
	defer c.cleanupOnce.Do(func() {
		if c.cleanupPublishers != nil {
			c.cleanupPublishers()
		}
	})

	p, err := poller.New()
	if err != nil {
		return
	}
	defer p.Close()

	state := &clientLoopState{
		poller:       p,
		wakeFD:       int(c.wakeReader.Fd()),
		readBuf:      make([]byte, 64*1024),
		remoteTopics: make(map[string]struct{}),
	}
	state.reader.MaxPayload = c.MaxPayload
	c.loop = state

	if err := p.Add(state.wakeFD, true, false); err != nil {
		return
	}

	for c.running.Load() {
		if err := c.step(state); err != nil {
			return
		}
	}

	c.disconnect(state, false, false)
}

func (c *WebSocketClient) step(state *clientLoopState) error {
	if !state.isConnected() && time.Now().After(state.nextReconnect) {
		if err := c.connect(state); err != nil {
			state.nextReconnect = time.Now().Add(100 * time.Millisecond)
		}
	}

	c.flushSharedQueues(state)

	// Callbacks above (OnOpen, response handlers) can run a nested step through
	// waitWhileLoop, which drains the wake pipe. Re-check before blocking so a
	// Stop whose wake was consumed there is not missed.
	if !c.running.Load() {
		return nil
	}
	timeout := time.Duration(-1)
	if !state.isConnected() {
		timeout = time.Until(state.nextReconnect)
		if timeout < 0 {
			timeout = 0
		}
	}

	events, err := state.poller.Wait(timeout)
	if err != nil {
		return err
	}
	for _, event := range events {
		switch event.FD {
		case state.wakeFD:
			drainFD(state.wakeFD)
			c.flushSharedQueues(state)
		case state.connFD:
			if event.Readable || event.Error {
				if err := c.handleReadable(state); err != nil {
					c.disconnect(state, true, true)
					state.nextReconnect = time.Now().Add(100 * time.Millisecond)
					continue
				}
			}
			if event.Writable {
				if err := c.flushWrites(state); err != nil {
					c.disconnect(state, true, true)
					state.nextReconnect = time.Now().Add(100 * time.Millisecond)
					continue
				}
			}
		}
	}
	return nil
}

func (c *WebSocketClient) connect(state *clientLoopState) error {
	target, err := parseTarget(c.address)
	if err != nil {
		return err
	}
	if target.Scheme == "wss" {
		return fmt.Errorf("wss is not supported")
	}

	dialer := net.Dialer{Timeout: 2 * time.Second}
	conn, err := dialer.Dial("tcp", target.HostPort)
	if err != nil {
		return err
	}
	fd, err := socketFD(conn)
	if err != nil {
		_ = conn.Close()
		return err
	}
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		_ = conn.Close()
		return err
	}

	keyBytes := make([]byte, 16)
	if _, err := rand.Read(keyBytes); err != nil {
		_ = conn.Close()
		return err
	}
	key := encodeBase64(keyBytes)
	request := buildHandshakeRequest(target, key, c.headerValues())
	if _, err := conn.Write(request); err != nil {
		_ = conn.Close()
		return err
	}

	buffer := make([]byte, 4096)
	total := 0
	expectedAccept := wsraw.AcceptKey(key)
	for {
		if total == len(buffer) {
			next := make([]byte, len(buffer)*2)
			copy(next, buffer)
			buffer = next
		}
		n, err := conn.Read(buffer[total:])
		if err != nil {
			_ = conn.Close()
			return err
		}
		total += n
		consumed, ok, err := wsraw.ParseClientHandshakeResponse(buffer[:total], expectedAccept)
		if err != nil {
			_ = conn.Close()
			return err
		}
		if !ok {
			continue
		}

		if err := conn.SetDeadline(time.Time{}); err != nil {
			_ = conn.Close()
			return err
		}
		if err := unix.SetNonblock(fd, true); err != nil {
			_ = conn.Close()
			return err
		}

		state.conn = conn
		state.connFD = fd
		state.reader.Reset()
		if total > consumed {
			state.reader.Feed(buffer[consumed:total])
		}
		state.writeQueue = nil
		state.writeOffset = 0
		if err := state.poller.Add(fd, true, false); err != nil {
			_ = conn.Close()
			state.conn = nil
			state.connFD = 0
			return err
		}
		c.connected.Store(true)
		if c.OnOpen != nil {
			c.OnOpen()
		}
		if total > consumed {
			if err := c.processFrames(state); err != nil {
				c.disconnect(state, true, true)
				return err
			}
		}
		return nil
	}
}

func (c *WebSocketClient) disconnect(state *clientLoopState, invokeClose bool, preservePending bool) {
	if state.connFD != 0 {
		_ = state.poller.Delete(state.connFD)
	}
	if state.conn != nil {
		_ = state.conn.Close()
	}
	if state.connFD != 0 || state.conn != nil {
		c.connected.Store(false)
		if c.channel != nil && c.loopSender != nil {
			c.channel.Disconnect(c.state, c.loopSender)
		}
		if c.channel != nil && c.publicSender != nil {
			if preservePending {
				c.channel.DisconnectSilent(c.state, c.publicSender, c.pendingRequestIDs())
				c.requeuePending()
			} else {
				c.channel.Disconnect(c.state, c.publicSender)
			}
		}
		if invokeClose && c.OnClose != nil {
			c.OnClose()
		}
	}

	state.conn = nil
	state.connFD = 0
	state.reader.Reset()
	state.writeQueue = nil
	state.writeOffset = 0
	clear(state.remoteTopics)
	c.dropNotifyQueue()
	c.dropPublishQueue()
}

func (c *WebSocketClient) handleReadable(state *clientLoopState) error {
	for {
		n, err := unix.Read(state.connFD, state.readBuf)
		if n > 0 {
			state.reader.Feed(state.readBuf[:n])
			if err := c.processFrames(state); err != nil {
				return err
			}
		}
		if err != nil {
			if err == unix.EAGAIN || err == unix.EWOULDBLOCK {
				return nil
			}
			return err
		}
		if n == 0 {
			return net.ErrClosed
		}
	}
}

func (c *WebSocketClient) processFrames(state *clientLoopState) error {
	for {
		frame, ok, err := state.reader.Next()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		switch frame.OpCode {
		case wsraw.OpText:
			decoded, ok := decodePayload(frame.Payload)
			if !ok {
				return fmt.Errorf("invalid websocket payload")
			}
			if c.OnMessage != nil {
				c.OnMessage(decoded)
			}
			if c.channel != nil && c.controller != nil {
				c.channel.Receive(decoded, c.controller)
			}
		case wsraw.OpPing:
			c.appendFrame(state, wsraw.OpPong, frame.Payload, true)
		case wsraw.OpPong:
		case wsraw.OpClose:
			return net.ErrClosed
		default:
			return fmt.Errorf("unsupported websocket opcode %d", frame.OpCode)
		}
	}
}

func (c *WebSocketClient) flushWrites(state *clientLoopState) error {
	for len(state.writeQueue) > 0 {
		current := state.writeQueue[0]
		if state.writeOffset > 0 {
			current = current[state.writeOffset:]
		}
		n, err := unix.Write(state.connFD, current)
		if n > 0 {
			state.writeOffset += n
			if state.writeOffset >= len(state.writeQueue[0]) {
				state.writeQueue = state.writeQueue[1:]
				state.writeOffset = 0
			}
		}
		if err != nil {
			if err == unix.EAGAIN || err == unix.EWOULDBLOCK {
				break
			}
			return err
		}
		if n == 0 {
			break
		}
	}
	return c.updatePollerInterest(state)
}

func (c *WebSocketClient) flushSharedQueues(state *clientLoopState) {
	if !state.isConnected() {
		c.dropNotifyQueue()
		c.dropPublishQueue()
		return
	}

	pending := c.takePending()
	if len(pending) > 0 {
		c.appendBodies(state, pending, true)
	}

	notifies := c.takeNotifyQueue()
	if len(notifies) > 0 {
		c.appendBodies(state, notifies, true)
	}

	events := c.takePublishQueue()
	if len(events) > 0 {
		bodies := make([]any, 0, len(events))
		for _, event := range events {
			if _, ok := state.remoteTopics[event.Topic]; !ok {
				continue
			}
			topic := event.Topic
			bodies = append(bodies, channelgo.Message{Topic: &topic, Body: event.Body})
		}
		if len(bodies) > 0 {
			c.appendBodies(state, bodies, true)
		}
	}

	_ = c.flushWrites(state)
}

func (c *WebSocketClient) appendBodies(state *clientLoopState, bodies []any, masked bool) {
	payload, err := encodeMessageBatch(bodies)
	if err != nil || len(payload) == 0 {
		return
	}
	c.appendFrame(state, byte(wsraw.OpText), payload, masked)
}

func (c *WebSocketClient) appendFrame(state *clientLoopState, opcode byte, payload []byte, masked bool) {
	state.writeQueue = append(state.writeQueue, wsraw.EncodeFrame(opcode, payload, masked, randomMaskKey()))
	_ = c.updatePollerInterest(state)
}

// randomMaskKey returns an unpredictable masking key (RFC 6455 section 5.3).
func randomMaskKey() uint32 {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return binary.BigEndian.Uint32(b[:])
}

func (c *WebSocketClient) updatePollerInterest(state *clientLoopState) error {
	if !state.isConnected() {
		return nil
	}
	return state.poller.Modify(state.connFD, true, len(state.writeQueue) > 0)
}

func (c *WebSocketClient) Send(body any) int {
	id := int(c.nextID.Add(1) - 1)
	c.pendingMu.Lock()
	c.pending[id] = pendingSend{
		body:      body,
		requestID: requestIDFromBody(body),
	}
	c.pendingOrder = append(c.pendingOrder, id)
	c.pendingMu.Unlock()
	c.wake()
	return id
}

func (c *WebSocketClient) loopSend(body any) int {
	if c.loop == nil || !c.loop.isConnected() {
		return -1
	}
	c.appendBodies(c.loop, []any{body}, true)
	return -1
}

func (c *WebSocketClient) Cancel(id int) bool {
	c.pendingMu.Lock()
	_, ok := c.pending[id]
	if ok {
		delete(c.pending, id)
	}
	c.pendingMu.Unlock()
	if ok {
		c.wake()
	}
	return ok
}

func (c *WebSocketClient) Notify(body any) {
	if !c.running.Load() {
		return
	}
	c.notifyMu.Lock()
	c.notifyQueue = append(c.notifyQueue, body)
	c.notifyMu.Unlock()
	c.wake()
}

func (c *WebSocketClient) loopNotify(body any) {
	if c.loop == nil || !c.loop.isConnected() {
		return
	}
	c.appendBodies(c.loop, []any{body}, true)
}

func (c *WebSocketClient) Sent(id int) {
	c.pendingMu.Lock()
	delete(c.pending, id)
	c.pendingMu.Unlock()
}

func (c *WebSocketClient) Throttle() {
	// kept for API parity; the loop serializes writes.
}

func (c *WebSocketClient) AddTopic(topic string, event channelgo.EventBody) func() bool {
	id := int(c.nextTopicID.Add(1))

	c.subsMu.Lock()
	handlers := c.subscribed[topic]
	if handlers == nil {
		handlers = make(map[int]channelgo.EventBody)
		c.subscribed[topic] = handlers
	}
	handlers[id] = event
	c.subsMu.Unlock()

	return func() bool {
		c.subsMu.Lock()
		defer c.subsMu.Unlock()
		handlers := c.subscribed[topic]
		if handlers == nil {
			return false
		}
		delete(handlers, id)
		if len(handlers) > 0 {
			return false
		}
		delete(c.subscribed, topic)
		c.Notify(channelgo.Message{Unsub: strPtr(topic)})
		return true
	}
}

func (c *WebSocketClient) ReceivedEvent(topic string, event any) {
	c.subsMu.RLock()
	handlers := c.subscribed[topic]
	copies := make([]channelgo.EventBody, 0, len(handlers))
	for _, handler := range handlers {
		copies = append(copies, handler)
	}
	c.subsMu.RUnlock()
	for _, handler := range copies {
		handler(event)
	}
}

func (c *WebSocketClient) PublishEvent(event channelgo.SubscriptionEvent) {
	if !c.running.Load() {
		return
	}
	c.publishMu.Lock()
	c.publishQueue = append(c.publishQueue, event)
	c.publishMu.Unlock()
	c.wake()
}

func (c *WebSocketClient) Stop() {
	if c == nil || c.loopDone == nil {
		return
	}
	if !c.running.CompareAndSwap(true, false) {
		return
	}
	c.wake()
	<-c.loopDone
}

func (c *WebSocketClient) waitWhileLoop(ctx context.Context, ready func() bool) error {
	for !ready() {
		if ctx != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		if !c.running.Load() {
			if ready() {
				return nil
			}
			return errors.New("websocket loop stopped")
		}
		if c.loop == nil {
			time.Sleep(100 * time.Microsecond)
			continue
		}
		if err := c.step(c.loop); err != nil {
			return err
		}
	}
	return nil
}

func (c *WebSocketClient) wake() {
	if c.wakeWriter == nil {
		return
	}
	_, _ = c.wakeWriter.Write([]byte{1})
}

func (c *WebSocketClient) cleanupLoopResources() {
	c.loop = nil
	if c.wakeReader != nil {
		_ = c.wakeReader.Close()
		c.wakeReader = nil
	}
	if c.wakeWriter != nil {
		_ = c.wakeWriter.Close()
		c.wakeWriter = nil
	}
}

func (c *WebSocketClient) takePending() []any {
	c.pendingMu.Lock()
	order := c.pendingOrder
	c.pendingOrder = nil
	out := make([]any, 0, len(order))
	for _, id := range order {
		entry, ok := c.pending[id]
		if !ok {
			continue
		}
		out = append(out, entry.body)
	}
	c.pendingMu.Unlock()
	return out
}

func (c *WebSocketClient) requeuePending() {
	c.pendingMu.Lock()
	order := make([]int, 0, len(c.pending))
	for id := range c.pending {
		order = append(order, id)
	}
	sort.Ints(order)
	c.pendingOrder = order
	c.pendingMu.Unlock()
}

func (c *WebSocketClient) pendingRequestIDs() map[int]struct{} {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	if len(c.pending) == 0 {
		return nil
	}
	out := make(map[int]struct{}, len(c.pending))
	for _, entry := range c.pending {
		if entry.requestID >= 0 {
			out[entry.requestID] = struct{}{}
		}
	}
	return out
}

func (c *WebSocketClient) takeNotifyQueue() []any {
	c.notifyMu.Lock()
	defer c.notifyMu.Unlock()
	out := c.notifyQueue
	c.notifyQueue = nil
	return out
}

func (c *WebSocketClient) dropNotifyQueue() {
	c.notifyMu.Lock()
	c.notifyQueue = nil
	c.notifyMu.Unlock()
}

func (c *WebSocketClient) takePublishQueue() []channelgo.SubscriptionEvent {
	c.publishMu.Lock()
	defer c.publishMu.Unlock()
	out := c.publishQueue
	c.publishQueue = nil
	return out
}

func (c *WebSocketClient) dropPublishQueue() {
	c.publishMu.Lock()
	c.publishQueue = nil
	c.publishMu.Unlock()
}

func (c *WebSocketClient) addRemoteTopic(topic string) {
	if c.loop == nil {
		return
	}
	c.loop.remoteTopics[topic] = struct{}{}
}

func (c *WebSocketClient) removeRemoteTopic(topic string) {
	if c.loop == nil {
		return
	}
	delete(c.loop.remoteTopics, topic)
}

func (c *WebSocketClient) headerValues() map[string]string {
	if c.headers == nil {
		return nil
	}
	values := c.headers()
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]string, len(values))
	for key, value := range values {
		if value != "" {
			out[key] = value
		}
	}
	return out
}

type clientConnection struct {
	ws   *WebSocketClient
	loop bool
}

// onLoop reports whether this loop-bound connection is used from the event-loop
// goroutine. A loop sender kept by a callback and used later from another
// goroutine takes the queued, thread-safe path instead of re-entering the loop.
func (c *clientConnection) onLoop() bool {
	return c.loop && c.ws.loopGoroutine.Load() == goroutineID()
}

func (c *clientConnection) Send(body any) int {
	if c.onLoop() {
		return c.ws.loopSend(body)
	}
	return c.ws.Send(body)
}

func (c *clientConnection) Sent(id int) {
	c.ws.Sent(id)
}

func (c *clientConnection) Cancel(id int) bool {
	if c.onLoop() {
		return false
	}
	return c.ws.Cancel(id)
}

func (c *clientConnection) Notify(body any) {
	if c.onLoop() {
		c.ws.loopNotify(body)
		return
	}
	c.ws.Notify(body)
}

func (c *clientConnection) AddTopic(topic string, event channelgo.EventBody) func() bool {
	return c.ws.AddTopic(topic, event)
}

func (c *clientConnection) Stop() {
	c.ws.Stop()
}

func (c *clientConnection) InLoop() bool {
	return c.onLoop()
}

func (c *clientConnection) Wait(ctx context.Context, ready func() bool) error {
	if !c.onLoop() {
		return nil
	}
	return c.ws.waitWhileLoop(ctx, ready)
}

func resolveAddress(address any) string {
	switch value := address.(type) {
	case int:
		return fmt.Sprintf("ws://127.0.0.1:%d", value)
	case string:
		if strings.HasPrefix(value, "ws://") || strings.HasPrefix(value, "wss://") {
			return value
		}
		if strings.Contains(value, ":") {
			return "ws://" + value
		}
		return "ws://127.0.0.1:" + value
	default:
		return "ws://127.0.0.1:80"
	}
}

func decodePayload(payload []byte) (any, bool) {
	if len(payload) == 0 {
		return nil, false
	}
	if payload[0] == '[' {
		var messages []channelgo.Message
		if err := jsonx.Unmarshal(payload, &messages); err != nil {
			return nil, false
		}
		return messages, true
	}
	var message channelgo.Message
	if err := jsonx.Unmarshal(payload, &message); err != nil {
		return nil, false
	}
	return message, true
}

func encodeMessageBatch(items []any) ([]byte, error) {
	if len(items) == 0 {
		return nil, nil
	}
	if len(items) == 1 {
		return jsonx.Marshal(items[0])
	}
	return jsonx.Marshal(items)
}

type connectTarget struct {
	Scheme   string
	HostPort string
	Host     string
	Port     string
	Path     string
}

func parseTarget(address string) (connectTarget, error) {
	parsed, err := url.Parse(address)
	if err != nil {
		return connectTarget{}, err
	}
	host := parsed.Hostname()
	port := parsed.Port()
	if host == "" {
		host = "127.0.0.1"
	}
	if port == "" {
		if parsed.Scheme == "wss" {
			port = "443"
		} else {
			port = "80"
		}
	}
	path := parsed.RequestURI()
	if path == "" {
		path = "/"
	}
	return connectTarget{
		Scheme:   parsed.Scheme,
		HostPort: net.JoinHostPort(host, port),
		Host:     host,
		Port:     port,
		Path:     path,
	}, nil
}

func buildHandshakeRequest(target connectTarget, key string, headers map[string]string) []byte {
	var builder strings.Builder
	builder.WriteString("GET ")
	builder.WriteString(target.Path)
	builder.WriteString(" HTTP/1.1\r\n")
	builder.WriteString("Host: ")
	builder.WriteString(target.HostPort)
	builder.WriteString("\r\n")
	builder.WriteString("Connection: Upgrade\r\n")
	builder.WriteString("Upgrade: websocket\r\n")
	builder.WriteString("Sec-WebSocket-Version: 13\r\n")
	builder.WriteString("Sec-WebSocket-Key: ")
	builder.WriteString(key)
	builder.WriteString("\r\n")
	for key, value := range headers {
		builder.WriteString(key)
		builder.WriteString(": ")
		builder.WriteString(value)
		builder.WriteString("\r\n")
	}
	builder.WriteString("\r\n")
	return []byte(builder.String())
}

func socketFD(conn net.Conn) (int, error) {
	sysConn, ok := conn.(syscall.Conn)
	if !ok {
		return 0, fmt.Errorf("connection does not expose syscall fd")
	}
	rawConn, err := sysConn.SyscallConn()
	if err != nil {
		return 0, err
	}
	var fd int
	controlErr := rawConn.Control(func(value uintptr) {
		fd = int(value)
	})
	if controlErr != nil {
		return 0, controlErr
	}
	if fd == 0 {
		return 0, fmt.Errorf("missing connection fd")
	}
	return fd, nil
}

func drainFD(fd int) {
	buffer := [256]byte{}
	_, _ = unix.Read(fd, buffer[:])
}

func encodeBase64(value []byte) string {
	const table = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	if len(value) == 0 {
		return ""
	}
	out := make([]byte, ((len(value)+2)/3)*4)
	di := 0
	for i := 0; i < len(value); i += 3 {
		var chunk uint32
		remain := len(value) - i
		chunk |= uint32(value[i]) << 16
		if remain > 1 {
			chunk |= uint32(value[i+1]) << 8
		}
		if remain > 2 {
			chunk |= uint32(value[i+2])
		}
		out[di] = table[(chunk>>18)&0x3f]
		out[di+1] = table[(chunk>>12)&0x3f]
		if remain > 1 {
			out[di+2] = table[(chunk>>6)&0x3f]
		} else {
			out[di+2] = '='
		}
		if remain > 2 {
			out[di+3] = table[chunk&0x3f]
		} else {
			out[di+3] = '='
		}
		di += 4
	}
	return string(out)
}

func requestIDFromBody(body any) int {
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

func strPtr(v string) *string {
	return &v
}

func (s *clientLoopState) isConnected() bool {
	return s != nil && s.conn != nil && s.connFD != 0
}

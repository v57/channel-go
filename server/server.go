package server

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/v57/channel-go/channelgo"
	"github.com/v57/channel-go/internal/jsonx"
	"github.com/v57/channel-go/internal/poller"
	"github.com/v57/channel-go/internal/wsraw"
	"golang.org/x/sys/unix"
)

// BodyContext provides per-connection context for hooks.
type BodyContext struct {
	State         any
	Sender        *channelgo.Sender
	Subscriptions *Subscriptions
}

// Options configures websocket listener behavior.
type Options struct {
	State        func(headers map[string]string) any
	OnConnect    func(connection *BodyContext)
	OnDisconnect func(connection *BodyContext)
	// MaxPayload caps an incoming websocket frame payload in bytes; zero means 16 MiB.
	// Larger frames close that connection.
	MaxPayload int
}

// Server is a running websocket channel server.
type Server struct {
	channel *channelgo.Channel
	options Options

	addr     string
	listener *net.TCPListener
	loopDone chan struct{}

	running  atomic.Bool
	stopOnce sync.Once

	wakeReader *os.File
	wakeWriter *os.File

	publishMu    sync.Mutex
	publishQueue []channelgo.SubscriptionEvent

	cleanup func()
	loop    *serverLoopState
}

type serverLoopState struct {
	poller     poller.Poller
	wakeFD     int
	listenerFD int
	conns      map[int]*serverConn
	broker     map[string]map[int]*serverConn
}

type serverConn struct {
	server *Server
	loop   *serverLoopState

	fd           int
	closed       bool
	handshaken   bool
	handshakeBuf []byte
	reader       wsraw.Reader
	readBuf      []byte
	writeQueue   [][]byte
	writeOffset  int

	sender        *channelgo.Sender
	controller    *channelgo.Controller
	state         any
	subscriptions *Subscriptions
	context       *BodyContext
}

// Listen starts a websocket server on host:port or numeric port.
func Listen(ch *channelgo.Channel, address any, options Options) (*Server, error) {
	host, port := resolveAddress(address)
	listener, err := net.Listen("tcp", net.JoinHostPort(host, port))
	if err != nil {
		return nil, err
	}
	tcpListener, ok := listener.(*net.TCPListener)
	if !ok {
		_ = listener.Close()
		return nil, fmt.Errorf("expected tcp listener")
	}
	fd, err := socketFD(tcpListener)
	if err != nil {
		_ = tcpListener.Close()
		return nil, err
	}
	if err := unix.SetNonblock(fd, true); err != nil {
		_ = tcpListener.Close()
		return nil, err
	}

	wakeReader, wakeWriter, err := os.Pipe()
	if err != nil {
		_ = tcpListener.Close()
		return nil, err
	}
	if err := unix.SetNonblock(int(wakeReader.Fd()), true); err != nil {
		_ = wakeReader.Close()
		_ = wakeWriter.Close()
		_ = tcpListener.Close()
		return nil, err
	}
	if err := unix.SetNonblock(int(wakeWriter.Fd()), true); err != nil {
		_ = wakeReader.Close()
		_ = wakeWriter.Close()
		_ = tcpListener.Close()
		return nil, err
	}

	srv := &Server{
		channel:    ch,
		options:    options,
		addr:       tcpListener.Addr().String(),
		listener:   tcpListener,
		loopDone:   make(chan struct{}),
		wakeReader: wakeReader,
		wakeWriter: wakeWriter,
	}

	publisher := &brokerPublisher{server: srv}
	ch.ForEachEvent(func(_ string, subscription *channelgo.Subscription) {
		subscription.AddPublisher(publisher)
	})
	srv.cleanup = func() {
		ch.ForEachEvent(func(_ string, subscription *channelgo.Subscription) {
			subscription.RemovePublisher(publisher)
		})
	}

	srv.running.Store(true)
	go srv.run(fd)
	return srv, nil
}

func (s *Server) run(listenerFD int) {
	defer close(s.loopDone)
	defer s.running.Store(false)
	defer func() {
		if s.cleanup != nil {
			s.cleanup()
		}
		if s.wakeReader != nil {
			_ = s.wakeReader.Close()
			s.wakeReader = nil
		}
		if s.wakeWriter != nil {
			_ = s.wakeWriter.Close()
			s.wakeWriter = nil
		}
		if s.listener != nil {
			_ = s.listener.Close()
		}
	}()

	p, err := poller.New()
	if err != nil {
		return
	}
	defer p.Close()

	state := &serverLoopState{
		poller:     p,
		wakeFD:     int(s.wakeReader.Fd()),
		listenerFD: listenerFD,
		conns:      make(map[int]*serverConn),
		broker:     make(map[string]map[int]*serverConn),
	}
	s.loop = state

	if err := p.Add(state.wakeFD, true, false); err != nil {
		return
	}
	if err := p.Add(listenerFD, true, false); err != nil {
		return
	}

	for s.running.Load() {
		if err := s.step(state); err != nil {
			return
		}
	}

	s.closeAll(state)
}

func (s *Server) step(state *serverLoopState) error {
	s.flushPublishQueue(state)

	events, err := state.poller.Wait(-1)
	if err != nil {
		return err
	}
	for _, event := range events {
		switch event.FD {
		case state.wakeFD:
			drainFD(state.wakeFD)
			s.flushPublishQueue(state)
		case state.listenerFD:
			if event.Readable || event.Error {
				if err := s.acceptLoop(state); err != nil {
					return err
				}
			}
		default:
			conn := state.conns[event.FD]
			if conn == nil {
				continue
			}
			if event.Readable || event.Error {
				if err := s.handleReadable(conn); err != nil {
					s.closeConn(conn)
					continue
				}
			}
			if event.Writable {
				if err := s.flushWrites(conn); err != nil {
					s.closeConn(conn)
				}
			}
		}
	}
	return nil
}

func (s *Server) acceptLoop(state *serverLoopState) error {
	for {
		fd, _, err := unix.Accept(state.listenerFD)
		if err != nil {
			switch err {
			case unix.EAGAIN, unix.EINTR, unix.ECONNABORTED, unix.EMFILE, unix.ENFILE:
				// Transient or per-connection failures must not stop the whole server.
				return nil
			}
			return err
		}
		if err := unix.SetNonblock(fd, true); err != nil {
			_ = unix.Close(fd)
			continue
		}
		conn := &serverConn{
			server:       s,
			loop:         state,
			fd:           fd,
			handshakeBuf: make([]byte, 0, 4096),
			readBuf:      make([]byte, 64*1024),
		}
		conn.reader.MaxPayload = s.options.MaxPayload
		state.conns[fd] = conn
		if err := state.poller.Add(fd, true, false); err != nil {
			delete(state.conns, fd)
			_ = unix.Close(fd)
			continue
		}
	}
}

func (s *Server) handleReadable(conn *serverConn) error {
	for {
		n, err := unix.Read(conn.fd, conn.readBuf)
		if n > 0 {
			if !conn.handshaken {
				conn.handshakeBuf = append(conn.handshakeBuf, conn.readBuf[:n]...)
				if err := s.processHandshake(conn); err != nil {
					return err
				}
				continue
			}
			conn.reader.Feed(conn.readBuf[:n])
			if err := s.processFrames(conn); err != nil {
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

func (s *Server) processHandshake(conn *serverConn) error {
	handshake, consumed, ok, err := wsraw.ParseServerHandshake(conn.handshakeBuf)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	// Query parameters first, then real HTTP headers, so headers win (TS server.ts parity).
	// Otherwise `?x-user-id=admin` could override a header set by an authenticating proxy.
	headers := make(map[string]string)
	if parsed, err := url.Parse(handshake.Path); err == nil {
		for key, values := range parsed.Query() {
			if len(values) > 0 {
				headers[key] = values[len(values)-1]
			}
		}
	}
	for key, value := range handshake.Headers {
		headers[key] = value
	}

	stateValue := any(map[string]any{})
	if s.options.State != nil {
		stateValue = s.options.State(headers)
	}

	conn.handshaken = true
	conn.state = stateValue
	conn.subscriptions = NewSubscriptions()

	transport := &serverTransport{conn: conn}
	conn.sender = channelgo.MakeSender(s.channel, transport)
	conn.context = &BodyContext{
		State:         stateValue,
		Sender:        conn.sender,
		Subscriptions: conn.subscriptions,
	}
	conn.controller = &channelgo.Controller{
		ResponseFn: func(response channelgo.Message) {
			s.appendBodies(conn, []any{response})
		},
		SubscribeFn: func(topic string) {
			s.subscribeTopic(conn, topic)
		},
		UnsubscribeFn: func(topic string) {
			s.unsubscribeTopic(conn, topic)
		},
		EventFn: func(topic string, event any) {
			conn.subscriptions.ReceivedEvent(topic, event)
		},
		SenderValue: conn.sender,
		StateValue:  stateValue,
	}

	s.appendRaw(conn, wsraw.BuildServerHandshakeResponse(handshake.Key))
	if err := s.flushWrites(conn); err != nil {
		return err
	}

	if s.options.OnConnect != nil {
		s.options.OnConnect(conn.context)
	}

	if len(conn.handshakeBuf) > consumed {
		conn.reader.Feed(conn.handshakeBuf[consumed:])
	}
	conn.handshakeBuf = nil
	return s.processFrames(conn)
}

func (s *Server) processFrames(conn *serverConn) error {
	for {
		frame, ok, err := conn.reader.Next()
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
			s.channel.Receive(decoded, conn.controller)
		case wsraw.OpPing:
			s.appendFrame(conn, wsraw.OpPong, frame.Payload)
		case wsraw.OpPong:
		case wsraw.OpClose:
			return net.ErrClosed
		default:
			return fmt.Errorf("unsupported websocket opcode %d", frame.OpCode)
		}
	}
}

func (s *Server) appendBodies(conn *serverConn, bodies []any) {
	if conn == nil || conn.closed {
		return
	}
	payload, err := encodeMessageBatch(bodies)
	if err != nil || len(payload) == 0 {
		return
	}
	s.appendFrame(conn, wsraw.OpText, payload)
}

func (s *Server) appendFrame(conn *serverConn, opcode byte, payload []byte) {
	if conn == nil || conn.closed {
		return
	}
	conn.writeQueue = append(conn.writeQueue, wsraw.EncodeFrame(opcode, payload, false, 0))
	_ = s.updateConnInterest(conn)
}

func (s *Server) appendRaw(conn *serverConn, payload []byte) {
	if conn == nil || conn.closed {
		return
	}
	if len(payload) == 0 {
		return
	}
	copied := make([]byte, len(payload))
	copy(copied, payload)
	conn.writeQueue = append(conn.writeQueue, copied)
	_ = s.updateConnInterest(conn)
}

func (s *Server) flushWrites(conn *serverConn) error {
	for len(conn.writeQueue) > 0 {
		current := conn.writeQueue[0]
		if conn.writeOffset > 0 {
			current = current[conn.writeOffset:]
		}
		n, err := unix.Write(conn.fd, current)
		if n > 0 {
			conn.writeOffset += n
			if conn.writeOffset >= len(conn.writeQueue[0]) {
				conn.writeQueue = conn.writeQueue[1:]
				conn.writeOffset = 0
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
	return s.updateConnInterest(conn)
}

func (s *Server) updateConnInterest(conn *serverConn) error {
	if conn.closed {
		return nil
	}
	return conn.loop.poller.Modify(conn.fd, true, len(conn.writeQueue) > 0)
}

func (s *Server) subscribeTopic(conn *serverConn, topic string) {
	listeners := conn.loop.broker[topic]
	if listeners == nil {
		listeners = make(map[int]*serverConn)
		conn.loop.broker[topic] = listeners
	}
	listeners[conn.fd] = conn
}

func (s *Server) unsubscribeTopic(conn *serverConn, topic string) {
	listeners := conn.loop.broker[topic]
	if listeners == nil {
		return
	}
	delete(listeners, conn.fd)
	if len(listeners) == 0 {
		delete(conn.loop.broker, topic)
	}
}

func (s *Server) closeConn(conn *serverConn) {
	if conn == nil || conn.closed {
		return
	}
	conn.closed = true
	_ = conn.loop.poller.Delete(conn.fd)
	for topic, listeners := range conn.loop.broker {
		delete(listeners, conn.fd)
		if len(listeners) == 0 {
			delete(conn.loop.broker, topic)
		}
	}
	delete(conn.loop.conns, conn.fd)
	if conn.sender != nil {
		s.channel.Disconnect(conn.state, conn.sender)
	}
	if s.options.OnDisconnect != nil && conn.context != nil {
		s.options.OnDisconnect(conn.context)
	}
	_ = unix.Close(conn.fd)
}

func (s *Server) closeAll(state *serverLoopState) {
	for _, conn := range state.conns {
		s.closeConn(conn)
	}
}

func (s *Server) flushPublishQueue(state *serverLoopState) {
	s.publishMu.Lock()
	queue := s.publishQueue
	s.publishQueue = nil
	s.publishMu.Unlock()
	if len(queue) == 0 {
		return
	}
	for _, event := range queue {
		listeners := state.broker[event.Topic]
		if len(listeners) == 0 {
			continue
		}
		topic := event.Topic
		message := channelgo.Message{Topic: &topic, Body: event.Body}
		for _, conn := range listeners {
			s.appendBodies(conn, []any{message})
		}
	}
}

// Stop closes active connections and stops the listener.
func (s *Server) Stop() error {
	if s == nil || s.loopDone == nil {
		return nil
	}
	s.stopOnce.Do(func() {
		s.running.Store(false)
		s.wake()
		<-s.loopDone
	})
	return nil
}

func (s *Server) Addr() string {
	return s.addr
}

func (s *Server) waitWhileLoop(ctx context.Context, ready func() bool) error {
	for !ready() {
		if ctx != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		if !s.running.Load() {
			return net.ErrClosed
		}
		if s.loop == nil {
			time.Sleep(100 * time.Microsecond)
			continue
		}
		if err := s.step(s.loop); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) PublishEvent(event channelgo.SubscriptionEvent) {
	if !s.running.Load() {
		return
	}
	s.publishMu.Lock()
	s.publishQueue = append(s.publishQueue, event)
	s.publishMu.Unlock()
	s.wake()
}

func (s *Server) wake() {
	if s.wakeWriter == nil {
		return
	}
	_, _ = s.wakeWriter.Write([]byte{1})
}

func resolveAddress(address any) (host, port string) {
	host = "127.0.0.1"
	port = "80"
	switch value := address.(type) {
	case int:
		port = fmt.Sprintf("%d", value)
	case string:
		parts := strings.Split(value, ":")
		if len(parts) == 2 {
			host = parts[0]
			port = parts[1]
		}
	}
	return host, port
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

type serverTransport struct {
	conn *serverConn
}

func (s *serverTransport) Send(body any) int {
	if s.conn == nil || s.conn.closed {
		return -1
	}
	s.conn.server.appendBodies(s.conn, []any{body})
	return -1
}

func (s *serverTransport) Sent(int) {}

func (s *serverTransport) Cancel(int) bool {
	return false
}

func (s *serverTransport) Notify(body any) {
	if s.conn == nil || s.conn.closed {
		return
	}
	s.conn.server.appendBodies(s.conn, []any{body})
}

func (s *serverTransport) AddTopic(topic string, event channelgo.EventBody) func() bool {
	return s.conn.subscriptions.AddTopic(topic, event)
}

func (s *serverTransport) Stop() {
	s.conn.server.closeConn(s.conn)
}

func (s *serverTransport) InLoop() bool {
	return true
}

func (s *serverTransport) Wait(ctx context.Context, ready func() bool) error {
	return s.conn.server.waitWhileLoop(ctx, ready)
}

// Subscriptions tracks transport-local topic listeners.
type Subscriptions struct {
	mu         sync.RWMutex
	nextID     int
	subscribed map[string]map[int]channelgo.EventBody
}

func NewSubscriptions() *Subscriptions {
	return &Subscriptions{subscribed: make(map[string]map[int]channelgo.EventBody)}
}

func (s *Subscriptions) AddTopic(topic string, event channelgo.EventBody) func() bool {
	s.mu.Lock()
	s.nextID++
	id := s.nextID
	handlers := s.subscribed[topic]
	if handlers == nil {
		handlers = make(map[int]channelgo.EventBody)
		s.subscribed[topic] = handlers
	}
	handlers[id] = event
	s.mu.Unlock()

	return func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		handlers := s.subscribed[topic]
		if handlers == nil {
			return false
		}
		delete(handlers, id)
		if len(handlers) > 0 {
			return false
		}
		delete(s.subscribed, topic)
		return true
	}
}

func (s *Subscriptions) ReceivedEvent(topic string, event any) {
	s.mu.RLock()
	handlers := s.subscribed[topic]
	copies := make([]channelgo.EventBody, 0, len(handlers))
	for _, handler := range handlers {
		copies = append(copies, handler)
	}
	s.mu.RUnlock()

	for _, handler := range copies {
		handler(event)
	}
}

type brokerPublisher struct {
	server *Server
}

func (p *brokerPublisher) Publish(event channelgo.SubscriptionEvent) {
	p.server.PublishEvent(event)
}

func socketFD(listener *net.TCPListener) (int, error) {
	rawConn, err := listener.SyscallConn()
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
		return 0, fmt.Errorf("missing listener fd")
	}
	return fd, nil
}

func drainFD(fd int) {
	buffer := [256]byte{}
	_, _ = unix.Read(fd, buffer[:])
}

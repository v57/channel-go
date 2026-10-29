package quic

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"

	quicgo "github.com/quic-go/quic-go"
	"github.com/v57/channel-go/channelgo"
	"github.com/v57/channel-go/internal/jsonx"
	"github.com/v57/channel-go/server"
)

// ListenOptions configures a QUIC listener: the WebSocket server's options plus its certificate.
type ListenOptions struct {
	server.Options
	// Key is the PEM private key.
	Key []byte
	// Cert is the PEM certificate chain.
	Cert []byte
}

// Server is a running QUIC channel server.
type Server struct {
	channel   *channelgo.Channel
	options   ListenOptions
	transport *quicgo.Transport
	listener  *quicgo.Listener
	done      chan struct{}
	stopOnce  sync.Once
	cleanup   func()

	mu     sync.Mutex
	conns  map[*serverConn]struct{}
	topics map[string]map[*serverConn]struct{}
}

type serverConn struct {
	server  *Server
	conn    *quicgo.Conn
	writer  *frameWriter
	sender  *channelgo.Sender
	context *server.BodyContext
}

// Listen serves the channel over QUIC on UDP host:port or a port on every address.
func Listen(ch *channelgo.Channel, address any, options ListenOptions) (*Server, error) {
	tlsConfig, err := serverTLS(options)
	if err != nil {
		return nil, err
	}
	addr, err := resolveListenAddress(address)
	if err != nil {
		return nil, err
	}
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	udp, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}
	// Owning the socket lets Stop free the port before it returns
	transport := &quicgo.Transport{Conn: udp}
	listener, err := transport.Listen(tlsConfig, &quicgo.Config{
		MaxIdleTimeout: idleTimeout,
		// One stream per connection: the client's first; it may not open more or any unidirectional ones
		MaxIncomingStreams:    1,
		MaxIncomingUniStreams: -1,
	})
	if err != nil {
		_ = udp.Close()
		return nil, err
	}
	s := &Server{
		channel:   ch,
		options:   options,
		transport: transport,
		listener:  listener,
		done:      make(chan struct{}),
		conns:     make(map[*serverConn]struct{}),
		topics:    make(map[string]map[*serverConn]struct{}),
	}
	publisher := &serverPublisher{server: s}
	var subscriptions []*channelgo.Subscription
	ch.ForEachEvent(func(_ string, subscription *channelgo.Subscription) {
		subscription.AddPublisher(publisher)
		subscriptions = append(subscriptions, subscription)
	})
	s.cleanup = func() {
		for _, subscription := range subscriptions {
			subscription.RemovePublisher(publisher)
		}
	}
	go s.accept()
	return s, nil
}

// Addr is the UDP address the server listens on.
func (s *Server) Addr() string {
	return s.listener.Addr().String()
}

// Port is the UDP port the server listens on, useful after listening on port 0.
func (s *Server) Port() int {
	return s.listener.Addr().(*net.UDPAddr).Port
}

// Stop closes every connection and the listener.
func (s *Server) Stop() error {
	var err error
	s.stopOnce.Do(func() {
		s.cleanup()
		err = s.listener.Close()
		<-s.done
		s.mu.Lock()
		conns := make([]*serverConn, 0, len(s.conns))
		for conn := range s.conns {
			conns = append(conns, conn)
		}
		s.mu.Unlock()
		for _, conn := range conns {
			_ = conn.conn.CloseWithError(0, "")
		}
		_ = s.transport.Close()
		_ = s.transport.Conn.Close()
	})
	return err
}

func (s *Server) accept() {
	defer close(s.done)
	for {
		conn, err := s.listener.Accept(context.Background())
		if err != nil {
			return
		}
		go s.serve(conn)
	}
}

func (s *Server) serve(conn *quicgo.Conn) {
	defer conn.CloseWithError(0, "")
	stream, err := conn.AcceptStream(conn.Context())
	if err != nil {
		return
	}
	reader := newFrameReader(stream, s.options.MaxPayload)
	headers, err := readHello(reader)
	if err != nil {
		return
	}
	state := any(map[string]any{})
	if s.options.State != nil {
		state = s.options.State(headers)
	}

	c := &serverConn{server: s, conn: conn}
	c.writer = newFrameWriter(stream, func(error) { _ = conn.CloseWithError(0, "") })
	defer c.writer.close()
	subscriptions := server.NewSubscriptions()
	c.sender = channelgo.MakeSender(s.channel, &serverTransport{conn: c, subscriptions: subscriptions})
	c.context = &server.BodyContext{State: state, Sender: c.sender, Subscriptions: subscriptions}
	controller := &channelgo.Controller{
		// Handlers run on their own goroutines, so a slow one or a long stream doesn't hold up the connection
		Async:         true,
		ResponseFn:    func(body channelgo.Message) { c.writer.writeMessage(body) },
		SubscribeFn:   func(topic string) { s.subscribe(c, topic) },
		UnsubscribeFn: func(topic string) { s.unsubscribe(c, topic) },
		EventFn:       subscriptions.ReceivedEvent,
		SenderValue:   c.sender,
		StateValue:    state,
	}

	s.mu.Lock()
	s.conns[c] = struct{}{}
	s.mu.Unlock()
	defer s.drop(c)
	if s.options.OnConnect != nil {
		s.options.OnConnect(c.context)
	}
	for {
		payload, err := reader.next()
		if err != nil {
			return
		}
		message, err := decodePayload(payload)
		if err != nil {
			// Invalid JSON closes the connection, like the WebSocket server
			return
		}
		s.channel.Receive(message, controller)
	}
}

// readHello reads the first frame, {"hello": headers}, with the header names lowercased.
func readHello(reader *frameReader) (map[string]string, error) {
	payload, err := reader.next()
	if err != nil {
		return nil, err
	}
	var first struct {
		Hello map[string]json.RawMessage `json:"hello"`
	}
	if err := json.Unmarshal(payload, &first); err != nil || first.Hello == nil {
		return nil, errNoHello
	}
	headers := make(map[string]string, len(first.Hello))
	for key, raw := range first.Hello {
		var value string
		if json.Unmarshal(raw, &value) == nil {
			headers[strings.ToLower(key)] = value
		}
	}
	return headers, nil
}

func (s *Server) drop(c *serverConn) {
	s.mu.Lock()
	delete(s.conns, c)
	for topic, conns := range s.topics {
		delete(conns, c)
		if len(conns) == 0 {
			delete(s.topics, topic)
		}
	}
	s.mu.Unlock()
	s.channel.Disconnect(c.context.State, c.sender)
	if s.options.OnDisconnect != nil {
		s.options.OnDisconnect(c.context)
	}
}

func (s *Server) subscribe(c *serverConn, topic string) {
	s.mu.Lock()
	conns := s.topics[topic]
	if conns == nil {
		conns = make(map[*serverConn]struct{})
		s.topics[topic] = conns
	}
	conns[c] = struct{}{}
	s.mu.Unlock()
}

func (s *Server) unsubscribe(c *serverConn, topic string) {
	s.mu.Lock()
	if conns := s.topics[topic]; conns != nil {
		delete(conns, c)
		if len(conns) == 0 {
			delete(s.topics, topic)
		}
	}
	s.mu.Unlock()
}

// serverPublisher sends an event, encoded once, to every connection subscribed to its topic.
type serverPublisher struct {
	server *Server
}

func (p *serverPublisher) Publish(event channelgo.SubscriptionEvent) {
	s := p.server
	s.mu.Lock()
	conns := make([]*serverConn, 0, len(s.topics[event.Topic]))
	for conn := range s.topics[event.Topic] {
		conns = append(conns, conn)
	}
	s.mu.Unlock()
	if len(conns) == 0 {
		return
	}
	topic := event.Topic
	payload, err := jsonx.Marshal(channelgo.Message{Topic: &topic, Body: event.Body})
	if err != nil {
		return
	}
	for _, conn := range conns {
		conn.writer.write(payload)
	}
}

type serverTransport struct {
	conn          *serverConn
	subscriptions *server.Subscriptions
}

func (t *serverTransport) Send(body any) int {
	t.conn.writer.writeMessage(body)
	return -1
}

func (t *serverTransport) Sent(int) {}

func (t *serverTransport) Cancel(int) bool {
	return false
}

func (t *serverTransport) Notify(body any) {
	t.conn.writer.writeMessage(body)
}

func (t *serverTransport) AddTopic(topic string, event channelgo.EventBody) func() bool {
	return t.subscriptions.AddTopic(topic, event)
}

func (t *serverTransport) Stop() {
	_ = t.conn.conn.CloseWithError(0, "")
}

func resolveListenAddress(address any) (string, error) {
	switch value := address.(type) {
	case int:
		return net.JoinHostPort("", strconv.Itoa(value)), nil
	case string:
		value = strings.TrimPrefix(value, "quic://")
		if !strings.Contains(value, ":") {
			value = ":" + value
		}
		return value, nil
	default:
		return "", fmt.Errorf("channel quic: unsupported address %v", address)
	}
}

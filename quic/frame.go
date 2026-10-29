// Package quic runs Channel over QUIC (the TypeScript reference's channel/quic, docs/quic.md there): one
// bidirectional stream per connection carrying the same JSON messages as a WebSocket, each one prefixed with its
// length as a QUIC variable-length integer. Only programs that import this package depend on quic-go.
package quic

import (
	"bufio"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/v57/channel-go/channelgo"
	"github.com/v57/channel-go/internal/jsonx"
)

const (
	// ALPN is the QUIC application protocol of a Channel connection.
	ALPN = "channel/1"
	// DefaultMaxPayload is the largest accepted frame, the same as the WebSocket transports'.
	DefaultMaxPayload = 16 << 20
	// idleTimeout closes silent connections; clients send an empty frame every keepAlive to stay open.
	idleTimeout = 30 * time.Second
	keepAlive   = 15 * time.Second
)

var (
	errFrameTooLarge = errors.New("channel quic: frame too large")
	errInvalidJSON   = errors.New("channel quic: invalid message")
	errNoHello       = errors.New("channel quic: the first frame isn't a hello")
)

// appendFrame appends the varint length of payload (RFC 9000 section 16) and payload.
func appendFrame(b []byte, payload []byte) []byte {
	n := uint64(len(payload))
	switch {
	case n < 1<<6:
		b = append(b, byte(n))
	case n < 1<<14:
		b = append(b, byte(n>>8)|0x40, byte(n))
	case n < 1<<30:
		b = append(b, byte(n>>24)|0x80, byte(n>>16), byte(n>>8), byte(n))
	default:
		b = append(b, byte(n>>56)|0xc0, byte(n>>48), byte(n>>40), byte(n>>32), byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
	}
	return append(b, payload...)
}

// frameReader splits a stream back into message payloads, skipping keepalives.
type frameReader struct {
	r   *bufio.Reader
	max int
}

func newFrameReader(r io.Reader, max int) *frameReader {
	if max <= 0 {
		max = DefaultMaxPayload
	}
	return &frameReader{r: bufio.NewReaderSize(r, 64*1024), max: max}
}

func (f *frameReader) next() ([]byte, error) {
	for {
		first, err := f.r.ReadByte()
		if err != nil {
			return nil, err
		}
		size := 1 << (first >> 6)
		length := uint64(first & 0x3f)
		for i := 1; i < size; i++ {
			next, err := f.r.ReadByte()
			if err != nil {
				return nil, unexpected(err)
			}
			length = length<<8 | uint64(next)
		}
		if length > uint64(f.max) {
			return nil, errFrameTooLarge
		}
		if length == 0 {
			continue
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(f.r, payload); err != nil {
			return nil, unexpected(err)
		}
		return payload, nil
	}
}

func unexpected(err error) error {
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}

// frameWriter queues frames and writes them from its own goroutine, so senders never block on flow control and
// frames queued meanwhile go out in one write.
type frameWriter struct {
	mu      sync.Mutex
	queue   []byte
	closed  bool
	wake    chan struct{}
	done    chan struct{}
	onError func(error)
}

func newFrameWriter(w io.Writer, onError func(error)) *frameWriter {
	f := &frameWriter{wake: make(chan struct{}, 1), done: make(chan struct{}), onError: onError}
	go f.run(w)
	return f
}

// write queues one frame; false once the writer is closed.
func (f *frameWriter) write(payload []byte) bool {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return false
	}
	f.queue = appendFrame(f.queue, payload)
	f.mu.Unlock()
	select {
	case f.wake <- struct{}{}:
	default:
	}
	return true
}

func (f *frameWriter) writeMessage(body any) bool {
	payload, err := jsonx.Marshal(body)
	if err != nil {
		return false
	}
	return f.write(payload)
}

func (f *frameWriter) close() {
	f.mu.Lock()
	f.closed = true
	f.mu.Unlock()
	select {
	case f.wake <- struct{}{}:
	default:
	}
}

func (f *frameWriter) run(w io.Writer) {
	defer close(f.done)
	var spare []byte
	for range f.wake {
		f.mu.Lock()
		data, closed := f.queue, f.closed
		f.queue = spare[:0]
		f.mu.Unlock()
		if len(data) > 0 {
			if _, err := w.Write(data); err != nil {
				f.close()
				if f.onError != nil {
					f.onError(err)
				}
				return
			}
		}
		spare = data
		if closed {
			return
		}
	}
}

func decodePayload(payload []byte) (any, error) {
	if len(payload) > 0 && payload[0] == '[' {
		var messages []channelgo.Message
		if err := jsonx.Unmarshal(payload, &messages); err != nil {
			return nil, errInvalidJSON
		}
		return messages, nil
	}
	var message channelgo.Message
	if err := jsonx.Unmarshal(payload, &message); err != nil {
		return nil, errInvalidJSON
	}
	return message, nil
}

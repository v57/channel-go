package client

import (
	"bufio"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/v57/channel-go/channelgo"
	"github.com/v57/channel-go/internal/wsraw"
)

// hostileServer completes the websocket handshake and then writes `frame` to every client.
func hostileServer(t *testing.T, frame []byte) (string, *atomic.Int64) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	accepted := &atomic.Int64{}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			go func(conn net.Conn) {
				defer conn.Close()
				req, err := http.ReadRequest(bufio.NewReader(conn))
				if err != nil {
					return
				}
				accept := wsraw.AcceptKey(req.Header.Get("Sec-WebSocket-Key"))
				_, _ = conn.Write([]byte("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + accept + "\r\n\r\n"))
				_, _ = conn.Write(frame)
				time.Sleep(500 * time.Millisecond)
			}(conn)
		}
	}()
	return listener.Addr().String(), accepted
}

func TestClientSurvivesHugeFrameLength(t *testing.T) {
	for _, frame := range [][]byte{
		{0x81, 0x7f, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, // 2^63-1
		{0x81, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, // 2^64-1
	} {
		addr, accepted := hostileServer(t, frame)
		client := Connect(channelgo.NewChannel(), "ws://"+addr, Options{})
		waitForClient(t, func() bool { return accepted.Load() > 0 })
		time.Sleep(200 * time.Millisecond) // a panic in the loop goroutine would kill the test binary
		client.Stop()
	}
}

func TestMaskKeysAreRandom(t *testing.T) {
	seen := map[uint32]bool{}
	for i := 0; i < 64; i++ {
		seen[randomMaskKey()] = true
	}
	if len(seen) < 60 {
		t.Fatalf("mask keys repeat too often: %d unique of 64", len(seen))
	}
}

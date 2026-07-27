package wsx

import (
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

func TestWriteHelpersWithNilConn(t *testing.T) {
	var mu sync.Mutex
	if err := WriteClientText(nil, &mu, []byte("x")); err == nil {
		t.Fatalf("expected error for nil client conn")
	}
	if err := WriteServerText(nil, &mu, []byte("x")); err == nil {
		t.Fatalf("expected error for nil server conn")
	}
}

func TestReadWriteClientText(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	var mu sync.Mutex
	go func() {
		_ = WriteClientText(c1, &mu, []byte("hello"))
	}()

	payload, err := ReadClientText(c2)
	if err != nil {
		t.Fatalf("read client text failed: %v", err)
	}
	if string(payload) != "hello" {
		t.Fatalf("unexpected payload: %q", payload)
	}
}

func TestReadWriteServerText(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	var mu sync.Mutex
	go func() {
		_ = WriteServerText(c2, &mu, []byte("world"))
	}()

	payload, err := ReadServerText(c1)
	if err != nil {
		t.Fatalf("read server text failed: %v", err)
	}
	if string(payload) != "world" {
		t.Fatalf("unexpected payload: %q", payload)
	}
}

func TestReadSkipsNonTextFrames(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	go func() {
		_ = wsutil.WriteServerMessage(c2, ws.OpBinary, []byte{1, 2, 3})
		_ = wsutil.WriteServerText(c2, []byte("text"))
	}()

	payload, err := ReadServerText(c1)
	if err != nil {
		t.Fatalf("read after binary failed: %v", err)
	}
	if string(payload) != "text" {
		t.Fatalf("unexpected payload: %q", payload)
	}
}

func TestReadReturnsEOF(t *testing.T) {
	c1, c2 := net.Pipe()
	go func() {
		time.Sleep(10 * time.Millisecond)
		_ = c2.Close()
	}()

	_, err := ReadServerText(c1)
	if err == nil || err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestReadClientSkipsNonTextAndEOF(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	go func() {
		_ = wsutil.WriteClientMessage(c1, ws.OpBinary, []byte{9, 9})
		_ = wsutil.WriteClientText(c1, []byte("ok"))
	}()

	payload, err := ReadClientText(c2)
	if err != nil {
		t.Fatalf("read client after binary failed: %v", err)
	}
	if string(payload) != "ok" {
		t.Fatalf("unexpected payload: %q", payload)
	}

	c3, c4 := net.Pipe()
	go func() {
		time.Sleep(10 * time.Millisecond)
		_ = c3.Close()
	}()
	_, err = ReadClientText(c4)
	if err == nil || err != io.EOF {
		t.Fatalf("expected client EOF, got %v", err)
	}
}

func TestReadReturnsNonEOFError(t *testing.T) {
	errConn := &failingConn{err: io.ErrUnexpectedEOF}
	if _, err := ReadServerText(errConn); err == nil || err == io.EOF {
		t.Fatalf("expected non-EOF server read error, got %v", err)
	}
	if _, err := ReadClientText(errConn); err == nil || err == io.EOF {
		t.Fatalf("expected non-EOF client read error, got %v", err)
	}
}

type failingConn struct {
	err error
}

func (f *failingConn) Read([]byte) (int, error)         { return 0, f.err }
func (f *failingConn) Write([]byte) (int, error)        { return 0, f.err }
func (f *failingConn) Close() error                     { return nil }
func (f *failingConn) LocalAddr() net.Addr              { return dummyAddr("local") }
func (f *failingConn) RemoteAddr() net.Addr             { return dummyAddr("remote") }
func (f *failingConn) SetDeadline(time.Time) error      { return nil }
func (f *failingConn) SetReadDeadline(time.Time) error  { return nil }
func (f *failingConn) SetWriteDeadline(time.Time) error { return nil }

type dummyAddr string

func (d dummyAddr) Network() string { return string(d) }
func (d dummyAddr) String() string  { return string(d) }

package wsraw

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func frameHeader64(size uint64, masked bool) []byte {
	second := byte(127)
	if masked {
		second |= 0x80
	}
	out := []byte{0x81, second}
	out = binary.BigEndian.AppendUint64(out, size)
	if masked {
		out = append(out, 1, 2, 3, 4)
	}
	return out
}

func TestReaderRejectsHugeDeclaredLengths(t *testing.T) {
	for _, size := range []uint64{1<<63 - 1, 1<<64 - 1, DefaultMaxPayload + 1, 1 << 30} {
		for _, masked := range []bool{false, true} {
			var r Reader
			r.Feed(frameHeader64(size, masked))
			_, ok, err := r.Next()
			if ok || !errors.Is(err, ErrFrameTooLarge) {
				t.Fatalf("size %d masked %v: expected ErrFrameTooLarge, got ok=%v err=%v", size, masked, ok, err)
			}
		}
	}
}

func TestReaderCustomMaxPayload(t *testing.T) {
	r := Reader{MaxPayload: 4}
	r.Feed(EncodeFrame(OpText, []byte("hello"), true, 0x01020304))
	if _, _, err := r.Next(); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("expected ErrFrameTooLarge, got %v", err)
	}
	r = Reader{MaxPayload: 5}
	r.Feed(EncodeFrame(OpText, []byte("hello"), true, 0x01020304))
	frame, ok, err := r.Next()
	if err != nil || !ok || string(frame.Payload) != "hello" {
		t.Fatalf("expected hello, got %q ok=%v err=%v", frame.Payload, ok, err)
	}
}

func TestReaderAcceptsDefaultMaxPayload(t *testing.T) {
	var r Reader
	payload := bytes.Repeat([]byte("a"), DefaultMaxPayload)
	r.Feed(EncodeFrame(OpText, payload, false, 0))
	frame, ok, err := r.Next()
	if err != nil || !ok || len(frame.Payload) != DefaultMaxPayload {
		t.Fatalf("expected full payload, ok=%v err=%v", ok, err)
	}
}

func TestHandshakeSizeCaps(t *testing.T) {
	endless := append([]byte("GET / HTTP/1.1\r\nX: "), bytes.Repeat([]byte("a"), MaxHandshakeSize)...)
	if _, _, _, err := ParseServerHandshake(endless); !errors.Is(err, ErrHandshakeTooLarge) {
		t.Fatalf("server: expected ErrHandshakeTooLarge, got %v", err)
	}
	reply := append([]byte("HTTP/1.1 101 Switching Protocols\r\nX: "), bytes.Repeat([]byte("a"), MaxHandshakeSize)...)
	if _, _, err := ParseClientHandshakeResponse(reply, "x"); !errors.Is(err, ErrHandshakeTooLarge) {
		t.Fatalf("client: expected ErrHandshakeTooLarge, got %v", err)
	}
	partial := []byte("GET / HTTP/1.1\r\nX: a")
	if _, _, ok, err := ParseServerHandshake(partial); ok || err != nil {
		t.Fatalf("partial handshake should wait, got ok=%v err=%v", ok, err)
	}
}

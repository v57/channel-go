package testutil

import (
	"net"
	"testing"
	"time"
)

func TestWaitForTCP(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer ln.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}()

	if err := WaitForTCP(ln.Addr().String(), time.Second); err != nil {
		t.Fatalf("expected wait success, got %v", err)
	}

	_ = ln.Close()
	<-done
	if err := WaitForTCP(ln.Addr().String(), 100*time.Millisecond); err == nil {
		t.Fatalf("expected wait failure on closed listener")
	}
}

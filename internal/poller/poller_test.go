package poller_test

import (
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/v57/channel-go/internal/poller"
	"github.com/v57/channel-go/internal/sockio"
)

func rawFD(t *testing.T, c syscall.Conn) int {
	t.Helper()
	raw, err := c.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var fd int
	if err := raw.Control(func(v uintptr) { fd = int(v) }); err != nil {
		t.Fatal(err)
	}
	return fd
}

func waitFor(t *testing.T, p poller.Poller, fd int, want func(poller.Event) bool) poller.Event {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		events, err := p.Wait(200 * time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range events {
			if e.FD == fd && want(e) {
				return e
			}
		}
	}
	t.Fatalf("no wanted event for fd %d", fd)
	return poller.Event{}
}

func TestWakeSignalAndDrain(t *testing.T) {
	p, err := poller.New()
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	w, err := sockio.NewWake()
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := p.Add(w.FD(), true, false); err != nil {
		t.Fatal(err)
	}
	events, err := p.Wait(50 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Fatalf("quiet wake reported %v", events)
	}
	go func() {
		time.Sleep(50 * time.Millisecond)
		w.Signal()
		w.Signal()
	}()
	start := time.Now()
	events, err = p.Wait(-1)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].FD != w.FD() || !events[0].Readable {
		t.Fatalf("events = %v", events)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("wait did not wake")
	}
	w.Drain()
	events, err = p.Wait(50 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Fatalf("drained wake still reported %v", events)
	}
}

func TestSocketAcceptReadWrite(t *testing.T) {
	p, err := poller.New()
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	lfd := rawFD(t, listener.(*net.TCPListener))
	if err := sockio.SetNonblock(lfd); err != nil {
		t.Fatal(err)
	}
	if err := p.Add(lfd, true, false); err != nil {
		t.Fatal(err)
	}
	if _, err := sockio.Accept(lfd); err == nil || !sockio.AcceptRetry(err) {
		t.Fatalf("accept with nothing pending: %v", err)
	}

	client, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	waitFor(t, p, lfd, func(e poller.Event) bool { return e.Readable })
	fd, err := sockio.Accept(lfd)
	if err != nil {
		t.Fatal(err)
	}
	defer sockio.Close(fd)
	if err := sockio.SetNonblock(fd); err != nil {
		t.Fatal(err)
	}
	if err := p.Add(fd, true, true); err != nil {
		t.Fatal(err)
	}
	waitFor(t, p, fd, func(e poller.Event) bool { return e.Writable })
	if err := p.Modify(fd, true, false); err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 64)
	if _, err := sockio.Read(fd, buf); !sockio.WouldBlock(err) {
		t.Fatalf("read with nothing sent: %v", err)
	}
	if _, err := client.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, p, fd, func(e poller.Event) bool { return e.Readable })
	n, err := sockio.Read(fd, buf)
	if err != nil || string(buf[:n]) != "ping" {
		t.Fatalf("read %q, %v", buf[:max(n, 0)], err)
	}
	if n, err := sockio.Write(fd, []byte("pong")); err != nil || n != 4 {
		t.Fatalf("write %d, %v", n, err)
	}
	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err = client.Read(buf)
	if err != nil || string(buf[:n]) != "pong" {
		t.Fatalf("client read %q, %v", buf[:n], err)
	}

	_ = client.Close()
	waitFor(t, p, fd, func(e poller.Event) bool { return e.Readable || e.Error })
	n, err = sockio.Read(fd, buf)
	if n > 0 {
		t.Fatalf("read %d bytes after close", n)
	}
	if err := p.Delete(fd); err != nil {
		t.Fatal(err)
	}
}

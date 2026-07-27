package main

import (
	"errors"
	"flag"
	"os"
	"testing"
	"time"

	"github.com/v57/channel-go/channelgo"
	"github.com/v57/channel-go/internal/testutil"
	"github.com/v57/channel-go/server"
)

func TestFormatInt(t *testing.T) {
	if got := formatInt(12); got != "12" {
		t.Fatalf("expected 12, got %q", got)
	}
	if got := formatInt(1234); got != "1,234" {
		t.Fatalf("expected 1,234, got %q", got)
	}
	if got := formatInt(123456789); got != "123,456,789" {
		t.Fatalf("expected 123,456,789, got %q", got)
	}
}

func TestMeasure(t *testing.T) {
	ops := measure(20*time.Millisecond, 4, func() error { return nil })
	if ops <= 0 {
		t.Fatalf("expected positive ops/sec, got %d", ops)
	}
	if got := measure(0, 1, func() error { return nil }); got != 0 {
		t.Fatalf("expected zero ops/sec for zero duration, got %d", got)
	}
}

func TestRunConcurrentBenchmarksAndProcess(t *testing.T) {
	runConcurrentBenchmarks(5*time.Millisecond, func() error { return nil })
	runProcess(5 * time.Millisecond)
}

func TestRunGoAndTSWebSocket(t *testing.T) {
	runGoWebSocket(3 * time.Millisecond)
	if err := testutil.WaitForTCP("127.0.0.1:2049", 200*time.Millisecond); err == nil {
		runTSWebSocket(3 * time.Millisecond)
	}
}

func TestMainTargets(t *testing.T) {
	origArgs := os.Args
	defer func() { os.Args = origArgs }()
	origCommandLine := flag.CommandLine
	defer func() { flag.CommandLine = origCommandLine }()
	origRunGo := mainRunGoWebSocket
	origRunTS := mainRunTSWebSocket
	origRunProcess := mainRunProcess
	defer func() {
		mainRunGoWebSocket = origRunGo
		mainRunTSWebSocket = origRunTS
		mainRunProcess = origRunProcess
	}()

	runMain := func(args ...string) {
		flag.CommandLine = flag.NewFlagSet(args[0], flag.ContinueOnError)
		os.Args = args
		main()
	}

	calls := make([]string, 0, 6)
	mainRunGoWebSocket = func(time.Duration) { calls = append(calls, "go") }
	mainRunTSWebSocket = func(time.Duration) { calls = append(calls, "ts") }
	mainRunProcess = func(time.Duration) { calls = append(calls, "process") }

	runMain("bench", "-target=go", "-duration=1ms")
	runMain("bench", "-target=ts", "-duration=1ms")
	runMain("bench", "-target=process", "-duration=1ms")
	runMain("bench", "-target=all", "-duration=1ms")
	if got, want := len(calls), 6; got != want {
		t.Fatalf("expected %d calls, got %d (%v)", want, got, calls)
	}
	if calls[0] != "go" || calls[1] != "ts" || calls[2] != "process" || calls[3] != "go" || calls[4] != "ts" || calls[5] != "process" {
		t.Fatalf("unexpected call order: %v", calls)
	}

	didPanic := false
	func() {
		defer func() {
			if recover() != nil {
				didPanic = true
			}
		}()
		runMain("bench", "-target=unknown", "-duration=1ms")
	}()
	if !didPanic {
		t.Fatalf("expected panic for unknown target")
	}
}

func TestMainBenchmarkAndServerModes(t *testing.T) {
	origArgs := os.Args
	defer func() { os.Args = origArgs }()
	origCommandLine := flag.CommandLine
	defer func() { flag.CommandLine = origCommandLine }()
	origRunBenchmark := mainRunBenchmark
	origRunBenchServer := mainRunBenchServer
	origRunBenchClient := mainRunBenchClient
	origRunBenchProcess := mainRunBenchProcess
	defer func() {
		mainRunBenchmark = origRunBenchmark
		mainRunBenchServer = origRunBenchServer
		mainRunBenchClient = origRunBenchClient
		mainRunBenchProcess = origRunBenchProcess
	}()

	calledBenchmark := false
	calledServer := false
	calledClient := false
	calledProcess := false
	mainRunBenchmark = func(output string, stageMS int, rpsPerClient int) error {
		calledBenchmark = output == "out.json" && stageMS == 3000 && rpsPerClient == 77
		return nil
	}
	mainRunBenchServer = func() error {
		calledServer = true
		return nil
	}
	mainRunBenchClient = func() error {
		calledClient = true
		return nil
	}
	mainRunBenchProcess = func() error {
		calledProcess = true
		return nil
	}

	runMain := func(args ...string) {
		flag.CommandLine = flag.NewFlagSet(args[0], flag.ContinueOnError)
		os.Args = args
		main()
	}

	runMain("bench", "-mode=benchmark", "-output=out.json", "-stage-ms=3000", "-rps-per-client=77")
	if !calledBenchmark {
		t.Fatalf("expected benchmark mode callback")
	}

	runMain("bench", "-mode=bench-server")
	if !calledServer {
		t.Fatalf("expected server mode callback")
	}
	runMain("bench", "-mode=bench-client")
	if !calledClient {
		t.Fatalf("expected client mode callback")
	}
	runMain("bench", "-mode=bench-process")
	if !calledProcess {
		t.Fatalf("expected process mode callback")
	}

	didPanic := false
	func() {
		defer func() {
			if recover() != nil {
				didPanic = true
			}
		}()
		runMain("bench", "-mode=nope")
	}()
	if !didPanic {
		t.Fatalf("expected panic for unknown mode")
	}
}

func TestEnvOrDefaultInt(t *testing.T) {
	const key = "BENCH_TEST_INT"
	t.Setenv(key, "")
	if got := envOrDefaultInt(key, 7); got != 7 {
		t.Fatalf("expected default value, got %d", got)
	}

	t.Setenv(key, "42")
	if got := envOrDefaultInt(key, 7); got != 42 {
		t.Fatalf("expected env value, got %d", got)
	}

	t.Setenv(key, "bad")
	if got := envOrDefaultInt(key, 7); got != 7 {
		t.Fatalf("expected fallback value on parse error, got %d", got)
	}
}

func TestRunGoWebSocketPanicsOnListenError(t *testing.T) {
	origListen := listenServer
	defer func() { listenServer = origListen }()

	listenServer = func(*channelgo.Channel, any, server.Options) (*server.Server, error) {
		return nil, errors.New("listen failed")
	}

	didPanic := false
	func() {
		defer func() {
			if recover() != nil {
				didPanic = true
			}
		}()
		runGoWebSocket(1 * time.Millisecond)
	}()
	if !didPanic {
		t.Fatalf("expected panic from listen failure")
	}
}

func TestRunTSWebSocketPanicsWhenServerUnavailable(t *testing.T) {
	origWait := waitForTCP
	defer func() { waitForTCP = origWait }()

	waitForTCP = func(string, time.Duration) error {
		return errors.New("unreachable")
	}

	didPanic := false
	func() {
		defer func() {
			if recover() != nil {
				didPanic = true
			}
		}()
		runTSWebSocket(1 * time.Millisecond)
	}()
	if !didPanic {
		t.Fatalf("expected panic when ts server is unavailable")
	}
}

package main

import (
	"context"
	"os"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestParseWorkerMessage(t *testing.T) {
	msg, ok := parseWorkerMessage(`{"type":"result","requests":42}`)
	if !ok {
		t.Fatalf("expected valid message")
	}
	if msg.Type != "result" || msg.Requests != 42 {
		t.Fatalf("unexpected parsed message: %#v", msg)
	}

	if _, ok := parseWorkerMessage(`{"requests":42}`); ok {
		t.Fatalf("expected invalid message without type")
	}
	if _, ok := parseWorkerMessage(`not-json`); ok {
		t.Fatalf("expected invalid message for malformed json")
	}
}

func TestSampleProcessUsage(t *testing.T) {
	usage, err := sampleProcessUsage(os.Getpid())
	if err != nil {
		t.Fatalf("unexpected sample usage error: %v", err)
	}
	if usage.CPU < 0 {
		t.Fatalf("cpu should be non-negative, got %f", usage.CPU)
	}
	if usage.Mem <= 0 {
		t.Fatalf("mem should be positive, got %f", usage.Mem)
	}
}

func TestMapEnv(t *testing.T) {
	items := mapEnv(map[string]string{"A": "1", "B": "2"})
	sort.Strings(items)
	if strings.Join(items, ",") != "A=1,B=2" {
		t.Fatalf("unexpected env mapping: %v", items)
	}
}

func TestRound2(t *testing.T) {
	if got := round2(1.234); got != 1.23 {
		t.Fatalf("expected 1.23, got %f", got)
	}
	if got := round2(1.235); got != 1.24 {
		t.Fatalf("expected 1.24, got %f", got)
	}
}

func TestParsePositiveIntEnv(t *testing.T) {
	t.Setenv("BENCH_X", "10")
	value, err := parsePositiveIntEnv("BENCH_X")
	if err != nil || value != 10 {
		t.Fatalf("unexpected parse result: value=%d err=%v", value, err)
	}

	t.Setenv("BENCH_X", "0")
	if _, err := parsePositiveIntEnv("BENCH_X"); err == nil {
		t.Fatalf("expected parse error for non-positive int")
	}
}

func TestParsePositiveFloatEnv(t *testing.T) {
	t.Setenv("BENCH_Y", "12.5")
	value, err := parsePositiveFloatEnv("BENCH_Y")
	if err != nil || value != 12.5 {
		t.Fatalf("unexpected parse result: value=%f err=%v", value, err)
	}

	t.Setenv("BENCH_Y", "bad")
	if _, err := parsePositiveFloatEnv("BENCH_Y"); err == nil {
		t.Fatalf("expected parse error for invalid float")
	}
}

func TestGetRandomPort(t *testing.T) {
	port, err := getRandomPort()
	if err != nil {
		t.Fatalf("unexpected getRandomPort error: %v", err)
	}
	if port <= 0 {
		t.Fatalf("expected positive port, got %d", port)
	}
}

func TestDeferredResolveRejectWait(t *testing.T) {
	d := newDeferred[int]()
	d.resolve(7)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	value, err := d.wait(ctx)
	if err != nil || value != 7 {
		t.Fatalf("unexpected deferred wait result: value=%d err=%v", value, err)
	}

	dErr := newDeferred[int]()
	dErr.reject(context.Canceled)
	_, err = dErr.wait(ctx)
	if err == nil {
		t.Fatalf("expected deferred reject error")
	}
}

func TestStopWorkerNil(t *testing.T) {
	stopWorker(nil)
}

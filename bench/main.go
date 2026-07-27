package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/v57/channel-go/channelgo"
	"github.com/v57/channel-go/client"
	"github.com/v57/channel-go/internal/testutil"
	"github.com/v57/channel-go/process"
	"github.com/v57/channel-go/server"
)

var (
	textCountPath        = "text/count"
	textCountRequestBody = map[string]string{"text": "hello"}
	mainRunGoWebSocket   = runGoWebSocket
	mainRunTSWebSocket   = runTSWebSocket
	mainRunProcess       = runProcess
	mainRunBenchmark     = runProcessBenchmark
	mainRunBenchServer   = runBenchmarkServerFromEnv
	mainRunBenchClient   = runBenchmarkClientFromEnv
	mainRunBenchProcess  = runProcessWorkerFromEnv

	waitForTCP    = testutil.WaitForTCP
	listenServer  = server.Listen
	connectClient = client.Connect
)

func main() {
	mode := flag.String("mode", "legacy", "mode: legacy|benchmark|bench-server|bench-client|bench-process")
	target := flag.String("target", "all", "benchmark target: go|ts|process|all")
	duration := flag.Duration("duration", 500*time.Millisecond, "benchmark duration per scenario")
	stageMS := flag.Int("stage-ms", envOrDefaultInt("BENCH_STAGE_MS", 5000), "benchmark mode stage duration in milliseconds")
	rpsPerClient := flag.Int("rps-per-client", envOrDefaultInt("BENCH_RPS_PER_CLIENT", 1000), "benchmark mode requests/sec per client")
	output := flag.String("output", "benchmark.json", "benchmark mode output file")
	flag.Parse()

	switch *mode {
	case "legacy":
		switch *target {
		case "go":
			mainRunGoWebSocket(*duration)
		case "ts":
			mainRunTSWebSocket(*duration)
		case "process":
			mainRunProcess(*duration)
		case "all":
			mainRunGoWebSocket(*duration)
			mainRunTSWebSocket(*duration)
			mainRunProcess(*duration)
		default:
			panic("unknown target: " + *target)
		}
	case "benchmark":
		if err := mainRunBenchmark(*output, *stageMS, *rpsPerClient); err != nil {
			panic(err)
		}
	case "bench-server":
		if err := mainRunBenchServer(); err != nil {
			os.Exit(1)
		}
	case "bench-client":
		if err := mainRunBenchClient(); err != nil {
			os.Exit(1)
		}
	case "bench-process":
		if err := mainRunBenchProcess(); err != nil {
			os.Exit(1)
		}
	default:
		panic("unknown mode: " + *mode)
	}
}

func envOrDefaultInt(key string, defaultValue int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return defaultValue
	}
	return parsed
}

func textCountLength(body any) int {
	switch payload := body.(type) {
	case map[string]string:
		return len(payload["text"])
	case map[string]any:
		text, _ := payload["text"].(string)
		return len(text)
	default:
		return 0
	}
}

func runGoWebSocket(duration time.Duration) {
	counter := atomic.Int64{}
	ch := channelgo.NewChannel().Post(textCountPath, func(body channelgo.Body) (any, error) {
		counter.Add(1)
		return textCountLength(body.Body), nil
	})

	srv, err := listenServer(ch, 2048, server.Options{})
	if err != nil {
		panic(err)
	}
	defer srv.Stop()

	cli := connectClient(channelgo.NewChannel(), 2048, client.Options{})
	defer cli.Sender.Stop()
	waitConnected(cli)

	fmt.Println("go websocket")
	runConcurrentBenchmarks(duration, func() error {
		_, err := cli.Sender.Send(textCountPath, textCountRequestBody)
		return err
	})
}

func runTSWebSocket(duration time.Duration) {
	if err := waitForTCP("127.0.0.1:2049", 3*time.Second); err != nil {
		panic(fmt.Sprintf("TypeScript test server is not reachable on 127.0.0.1:2049: %v", err))
	}

	cli := connectClient(channelgo.NewChannel(), 2049, client.Options{})
	defer cli.Sender.Stop()
	waitConnected(cli)

	fmt.Println("ts websocket")
	runConcurrentBenchmarks(duration, func() error {
		_, err := cli.Sender.Send(textCountPath, textCountRequestBody)
		return err
	})
}

func runProcess(duration time.Duration) {
	syncCount := atomic.Int64{}
	syncSender := process.Connect(channelgo.NewChannel().Post(textCountPath, func(channelgo.Body) (any, error) {
		syncCount.Add(1)
		return "world", nil
	}))
	defer syncSender.Stop()

	fmt.Println("process")
	runConcurrentBenchmarks(duration, func() error {
		_, err := syncSender.Send(textCountPath, textCountRequestBody)
		return err
	})
}

func runConcurrentBenchmarks(duration time.Duration, task func() error) {
	concurrency := []int{1, 10, 100, 1000, 10000}
	for _, threads := range concurrency {
		ops := measure(duration, threads, task)
		formatted := formatInt(ops)
		fmt.Printf("%9s ops %d threads\n", formatted, threads)
	}
}

func measure(duration time.Duration, workers int, task func() error) int64 {
	var count atomic.Int64
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				if err := task(); err == nil {
					count.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	seconds := duration.Seconds()
	if seconds == 0 {
		return 0
	}
	return int64(float64(count.Load()) / seconds)
}

func waitConnected(client *client.ClientSender) {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if client.WS.IsConnected() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func formatInt(value int64) string {
	s := strconv.FormatInt(value, 10)
	if len(s) <= 3 {
		return s
	}
	out := make([]byte, 0, len(s)+len(s)/3)
	pre := len(s) % 3
	if pre == 0 {
		pre = 3
	}
	out = append(out, s[:pre]...)
	for i := pre; i < len(s); i += 3 {
		out = append(out, ',')
		out = append(out, s[i:i+3]...)
	}
	return string(out)
}

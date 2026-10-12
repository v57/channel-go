package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/v57/channel-go/channelgo"
	"github.com/v57/channel-go/client"
	"github.com/v57/channel-go/process"
	"github.com/v57/channel-go/server"
)

type benchmarkUsage struct {
	CPU float64 `json:"cpu"`
	Mem float64 `json:"mem"`
}

type benchmarkResult struct {
	RPS     float64        `json:"rps"`
	Clients int            `json:"clients"`
	Client  benchmarkUsage `json:"client"`
	Server  benchmarkUsage `json:"server"`
}

type benchmarkOutput struct {
	Language string            `json:"language"`
	Results  []benchmarkResult `json:"results"`
}

type workerMessage struct {
	Type            string  `json:"type"`
	Message         string  `json:"message,omitempty"`
	Requests        int64   `json:"requests,omitempty"`
	ElapsedSeconds  float64 `json:"elapsedSeconds,omitempty"`
	PID             int     `json:"pid,omitempty"`
	Port            int     `json:"port,omitempty"`
	Clients         int     `json:"clients,omitempty"`
	RequestsSent    int64   `json:"requestsSent,omitempty"`
	ResponsesRecved int64   `json:"responsesReceived,omitempty"`
	Errors          int64   `json:"errors,omitempty"`
}

type deferred[T any] struct {
	valueCh chan T
	errCh   chan error

	settled atomic.Bool
	once    sync.Once
}

func newDeferred[T any]() *deferred[T] {
	return &deferred[T]{
		valueCh: make(chan T, 1),
		errCh:   make(chan error, 1),
	}
}

func (d *deferred[T]) resolve(value T) {
	d.once.Do(func() {
		d.settled.Store(true)
		d.valueCh <- value
	})
}

func (d *deferred[T]) reject(err error) {
	d.once.Do(func() {
		d.settled.Store(true)
		d.errCh <- err
	})
}

func (d *deferred[T]) wait(ctx context.Context) (T, error) {
	var zero T
	select {
	case value := <-d.valueCh:
		return value, nil
	case err := <-d.errCh:
		return zero, err
	case <-ctx.Done():
		return zero, ctx.Err()
	}
}

func (d *deferred[T]) isSettled() bool {
	return d.settled.Load()
}

type worker struct {
	name string
	cmd  *exec.Cmd

	ready  *deferred[struct{}]
	result *deferred[workerMessage]
	exit   chan error
}

var (
	benchmarkStages = []int{1, 10, 100, 1000}
	benchmarkPath   = "text/count"
	benchmarkHost   = "127.0.0.1"
	benchmarkBody   = map[string]string{"text": "hello"}
)

func runProcessBenchmark(outputPath string, stageDurationMS int, rpsPerClient int) error {
	results := make([]benchmarkResult, 0, len(benchmarkStages))

	for _, clientsCount := range benchmarkStages {
		result, err := runBenchmarkStage(clientsCount, stageDurationMS, rpsPerClient)
		if err != nil {
			return err
		}
		results = append(results, result)
	}

	payload := benchmarkOutput{
		Language: "Go",
		Results:  results,
	}
	encoded, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	return os.WriteFile(outputPath, encoded, 0o644)
}

func runBenchmarkStage(clientsCount int, stageDurationMS int, rpsPerClient int) (benchmarkResult, error) {
	port, err := getRandomPort()
	if err != nil {
		return benchmarkResult{}, err
	}

	serverWorker, err := spawnWorker("server", "bench-server", map[string]string{
		"BENCH_PORT": strconv.Itoa(port),
	})
	if err != nil {
		return benchmarkResult{}, err
	}

	clientWorker, err := spawnWorker("client", "bench-client", map[string]string{
		"BENCH_HOST":           benchmarkHost,
		"BENCH_PORT":           strconv.Itoa(port),
		"BENCH_CLIENTS":        strconv.Itoa(clientsCount),
		"BENCH_RPS_PER_CLIENT": strconv.Itoa(rpsPerClient),
	})
	if err != nil {
		stopWorker(serverWorker)
		_, _ = waitWorkerExit(serverWorker, 10*time.Second)
		return benchmarkResult{}, err
	}

	defer func() {
		stopWorker(clientWorker)
		stopWorker(serverWorker)
		_, _ = waitWorkerExit(clientWorker, 10*time.Second)
		_, _ = waitWorkerExit(serverWorker, 10*time.Second)
	}()

	readyCtx, cancelReady := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelReady()
	if _, err := serverWorker.ready.wait(readyCtx); err != nil {
		return benchmarkResult{}, err
	}
	if _, err := clientWorker.ready.wait(readyCtx); err != nil {
		return benchmarkResult{}, err
	}

	time.Sleep(time.Duration(stageDurationMS) * time.Millisecond)

	clientUsage, err := sampleProcessUsage(clientWorker.cmd.Process.Pid)
	if err != nil {
		return benchmarkResult{}, err
	}
	serverUsage, err := sampleProcessUsage(serverWorker.cmd.Process.Pid)
	if err != nil {
		return benchmarkResult{}, err
	}

	stopWorker(clientWorker)
	stopWorker(serverWorker)

	resultCtx, cancelResult := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelResult()
	serverResult, err := serverWorker.result.wait(resultCtx)
	if err != nil {
		return benchmarkResult{}, err
	}

	if _, err := waitWorkerExit(clientWorker, 20*time.Second); err != nil {
		return benchmarkResult{}, err
	}
	if _, err := waitWorkerExit(serverWorker, 20*time.Second); err != nil {
		return benchmarkResult{}, err
	}

	rps := 0.0
	if stageDurationMS > 0 {
		rps = float64(serverResult.Requests) / (float64(stageDurationMS) / 1000.0)
	}

	return benchmarkResult{
		RPS:     round2(rps),
		Clients: clientsCount,
		Client:  clientUsage,
		Server:  serverUsage,
	}, nil
}

func spawnWorker(name string, mode string, env map[string]string) (*worker, error) {
	cmd := exec.Command(os.Args[0], "-mode="+mode)
	cmd.Env = append(os.Environ(), mapEnv(env)...)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}

	if err := cmd.Start(); err != nil {
		return nil, err
	}

	w := &worker{
		name:   name,
		cmd:    cmd,
		ready:  newDeferred[struct{}](),
		result: newDeferred[workerMessage](),
		exit:   make(chan error, 1),
	}

	go consumeWorkerStdout(w, stdout)
	go consumeWorkerStderr(w, stderr)
	go watchWorkerExit(w)

	return w, nil
}

func consumeWorkerStdout(worker *worker, reader io.Reader) {
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		message, ok := parseWorkerMessage(line)
		if !ok {
			continue
		}

		switch message.Type {
		case "ready":
			if !worker.ready.isSettled() {
				worker.ready.resolve(struct{}{})
			}
		case "result":
			if !worker.result.isSettled() {
				worker.result.resolve(message)
			}
		case "error":
			err := fmt.Errorf("%s error: %s", worker.name, message.Message)
			if !worker.ready.isSettled() {
				worker.ready.reject(err)
			}
			if !worker.result.isSettled() {
				worker.result.reject(err)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		if !worker.ready.isSettled() {
			worker.ready.reject(err)
		}
		if !worker.result.isSettled() {
			worker.result.reject(err)
		}
	}
}

func consumeWorkerStderr(worker *worker, reader io.Reader) {
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		fmt.Fprintf(os.Stderr, "[%s] %s\n", worker.name, line)
	}
}

func watchWorkerExit(worker *worker) {
	err := worker.cmd.Wait()
	if err == nil {
		if !worker.ready.isSettled() {
			worker.ready.resolve(struct{}{})
		}
		if !worker.result.isSettled() {
			worker.result.resolve(workerMessage{Type: "result"})
		}
		worker.exit <- nil
		return
	}

	exitErr := fmt.Errorf("%s exited with error: %w", worker.name, err)
	if !worker.ready.isSettled() {
		worker.ready.reject(exitErr)
	}
	if !worker.result.isSettled() {
		worker.result.reject(exitErr)
	}
	worker.exit <- exitErr
}

func waitWorkerExit(worker *worker, timeout time.Duration) (bool, error) {
	if worker == nil {
		return true, nil
	}
	if worker.cmd != nil && worker.cmd.ProcessState != nil && worker.cmd.ProcessState.Exited() {
		return true, nil
	}
	select {
	case err := <-worker.exit:
		if err != nil {
			return true, err
		}
		return true, nil
	case <-time.After(timeout):
		return false, fmt.Errorf("timed out waiting for %s exit", worker.name)
	}
}

func parseWorkerMessage(line string) (workerMessage, bool) {
	var message workerMessage
	if err := json.Unmarshal([]byte(line), &message); err != nil {
		return workerMessage{}, false
	}
	if message.Type == "" {
		return workerMessage{}, false
	}
	return message, true
}

func sampleProcessUsage(pid int) (benchmarkUsage, error) {
	output, err := exec.Command("ps", "-p", strconv.Itoa(pid), "-o", "%cpu=", "-o", "rss=").Output()
	if err != nil {
		return benchmarkUsage{}, err
	}
	fields := strings.Fields(strings.TrimSpace(string(output)))
	if len(fields) < 2 {
		return benchmarkUsage{}, fmt.Errorf("unexpected ps output: %q", strings.TrimSpace(string(output)))
	}

	cpu, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return benchmarkUsage{}, err
	}
	rssKB, err := strconv.ParseFloat(fields[1], 64)
	if err != nil {
		return benchmarkUsage{}, err
	}

	return benchmarkUsage{
		CPU: round2(cpu),
		Mem: round2(rssKB / 1024.0),
	}, nil
}

func stopWorker(worker *worker) {
	if worker == nil || worker.cmd == nil || worker.cmd.Process == nil {
		return
	}
	if worker.cmd.ProcessState != nil && worker.cmd.ProcessState.Exited() {
		return
	}
	_ = worker.cmd.Process.Signal(syscall.SIGTERM)
}

func mapEnv(values map[string]string) []string {
	out := make([]string, 0, len(values))
	for key, value := range values {
		out = append(out, key+"="+value)
	}
	return out
}

func getRandomPort() (int, error) {
	listener, err := net.Listen("tcp", net.JoinHostPort(benchmarkHost, "0"))
	if err != nil {
		return 0, err
	}
	defer listener.Close()

	addr, ok := listener.Addr().(*net.TCPAddr)
	if !ok || addr.Port <= 0 {
		return 0, fmt.Errorf("unable to allocate random port")
	}
	return addr.Port, nil
}

func round2(value float64) float64 {
	return math.Round(value*100) / 100
}

func runBenchmarkServerFromEnv() error {
	port, err := parsePositiveIntEnv("BENCH_PORT")
	if err != nil {
		emitError(err.Error())
		return err
	}

	requests := atomic.Int64{}
	startedAt := time.Now()
	ch := channelgo.NewChannel().Post(benchmarkPath, func(body channelgo.Body) (any, error) {
		requests.Add(1)
		return textCountLength(body.Body), nil
	})

	srv, err := listenServer(ch, port, server.Options{})
	if err != nil {
		emitError(err.Error())
		return err
	}

	emitJSON(workerMessage{Type: "ready", PID: os.Getpid(), Port: port})

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(sig)
	<-sig

	_ = srv.Stop()
	emitJSON(workerMessage{
		Type:           "result",
		PID:            os.Getpid(),
		Requests:       requests.Load(),
		ElapsedSeconds: time.Since(startedAt).Seconds(),
	})
	return nil
}

func runBenchmarkClientFromEnv() error {
	port, err := parsePositiveIntEnv("BENCH_PORT")
	if err != nil {
		emitError("BENCH_PORT must be a valid positive integer")
		return err
	}
	clientsCount, err := parsePositiveIntEnv("BENCH_CLIENTS")
	if err != nil {
		emitError("BENCH_CLIENTS must be a valid positive integer")
		return err
	}
	host := os.Getenv("BENCH_HOST")
	if host == "" {
		host = benchmarkHost
	}

	totalRequests := int64(-1)
	totalRequestsRaw := strings.TrimSpace(os.Getenv("BENCH_TOTAL_REQUESTS"))
	if totalRequestsRaw != "" {
		parsed, parseErr := strconv.ParseInt(totalRequestsRaw, 10, 64)
		if parseErr != nil || parsed < 0 {
			emitError("BENCH_TOTAL_REQUESTS must be a valid non-negative integer")
			return fmt.Errorf("invalid BENCH_TOTAL_REQUESTS")
		}
		totalRequests = parsed
	}

	requestMode := strings.ToLower(strings.TrimSpace(os.Getenv("BENCH_REQUEST_MODE")))
	if requestMode == "" {
		requestMode = "burst"
	}
	if requestMode != "burst" && requestMode != "stream" {
		emitError("BENCH_REQUEST_MODE must be either burst or stream")
		return fmt.Errorf("invalid BENCH_REQUEST_MODE")
	}

	maxInFlight := int64(4096)
	if totalRequests >= 0 {
		maxInFlightRaw := strings.TrimSpace(os.Getenv("BENCH_MAX_IN_FLIGHT"))
		if maxInFlightRaw != "" {
			parsed, parseErr := strconv.ParseInt(maxInFlightRaw, 10, 64)
			if parseErr != nil || parsed <= 0 {
				emitError("BENCH_MAX_IN_FLIGHT must be a valid positive integer")
				return fmt.Errorf("invalid BENCH_MAX_IN_FLIGHT")
			}
			maxInFlight = parsed
		}
	}

	rpsPerClient := 0.0
	if totalRequests < 0 {
		rpsPerClient, err = parsePositiveFloatEnv("BENCH_RPS_PER_CLIENT")
		if err != nil {
			emitError("BENCH_RPS_PER_CLIENT must be a positive number")
			return err
		}
	}

	address := "ws://" + net.JoinHostPort(host, strconv.Itoa(port))
	channel := channelgo.NewChannel()
	clients := make([]*client.ClientSender, clientsCount)
	connected := make([]bool, clientsCount)
	connectedCount := atomic.Int64{}
	connectedReady := make(chan struct{})
	connectedOnce := sync.Once{}

	startedAt := time.Now()
	var requestsSent int64
	var responsesReceived atomic.Int64
	var requestErrors atomic.Int64
	var inFlight atomic.Int64

	running := atomic.Bool{}
	running.Store(false)

	targetRPS := float64(clientsCount) * rpsPerClient
	nextClientIndex := 0
	countResponses := func(message any) int64 {
		switch value := message.(type) {
		case channelgo.Message:
			if value.ID == nil {
				return 0
			}
			return 1
		case []channelgo.Message:
			var count int64
			for i := 0; i < len(value); i++ {
				if value[i].ID != nil {
					count++
				}
			}
			return count
		default:
			return 1
		}
	}

	for i := 0; i < clientsCount; i++ {
		index := i
		clients[i] = connectClient(channel, address, client.Options{
			OnConnect: func(*channelgo.Sender) {
				if connected[index] {
					return
				}
				connected[index] = true
				current := connectedCount.Add(1)
				if int(current) == clientsCount {
					connectedOnce.Do(func() {
						close(connectedReady)
					})
				}
			},
			OnMessage: func(message any) {
				count := countResponses(message)
				if count <= 0 {
					return
				}
				responsesReceived.Add(count)
				if totalRequests >= 0 {
					inFlight.Add(-count)
				}
			},
		})
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(sig)

	setupTimeout := time.NewTimer(10 * time.Second)
	defer setupTimeout.Stop()

	select {
	case <-connectedReady:
		running.Store(true)
		emitJSON(workerMessage{Type: "ready", PID: os.Getpid(), Clients: clientsCount})
	case <-setupTimeout.C:
		emitError(fmt.Sprintf("Timed out waiting for clients to connect (%d/%d)", connectedCount.Load(), clientsCount))
		stopClientConnections(clients)
		return fmt.Errorf("timed out waiting for clients to connect")
	case <-sig:
		stopClientConnections(clients)
		emitJSON(workerMessage{Type: "result", PID: os.Getpid(), ElapsedSeconds: time.Since(startedAt).Seconds()})
		return nil
	}

	tickMS := 100
	carry := 0.0
	ticker := time.NewTicker(time.Duration(tickMS) * time.Millisecond)
	defer ticker.Stop()

	stopAndReport := func() error {
		if running.Load() {
			running.Store(false)
		}

		stopClientConnections(clients)
		emitJSON(workerMessage{
			Type:            "result",
			PID:             os.Getpid(),
			RequestsSent:    requestsSent,
			ResponsesRecved: responsesReceived.Load(),
			Errors:          requestErrors.Load(),
			ElapsedSeconds:  time.Since(startedAt).Seconds(),
		})
		return nil
	}

	sendOne := func() {
		if !running.Load() {
			return
		}
		if totalRequests >= 0 && requestsSent >= totalRequests {
			return
		}

		sender := clients[nextClientIndex]
		nextClientIndex++
		if nextClientIndex >= len(clients) {
			nextClientIndex = 0
		}
		if sender == nil || sender.Sender == nil {
			requestErrors.Add(1)
			return
		}

		requestsSent++
		if totalRequests >= 0 {
			inFlight.Add(1)
		}
		sender.Sender.SendUntracked(benchmarkPath, benchmarkBody)
	}

	isFixedDone := func() bool {
		if totalRequests < 0 {
			return false
		}
		accounted := responsesReceived.Load() + requestErrors.Load()
		if requestsSent < totalRequests {
			return false
		}
		return accounted >= totalRequests && inFlight.Load() <= 0
	}

	if totalRequests == 0 {
		return stopAndReport()
	}

	if totalRequests >= 0 {
		tickMS = 1
		ticker.Reset(time.Duration(tickMS) * time.Millisecond)
	}

	for {
		select {
		case <-ticker.C:
			if !running.Load() {
				continue
			}
			if totalRequests >= 0 {
				if requestMode == "stream" {
					for running.Load() && requestsSent < totalRequests && inFlight.Load() < maxInFlight {
						sendOne()
					}
				} else {
					budget := 4096
					for running.Load() && requestsSent < totalRequests && inFlight.Load() < maxInFlight && budget > 0 {
						sendOne()
						budget--
					}
				}

				if isFixedDone() {
					return stopAndReport()
				}
				continue
			}

			carry += (targetRPS * float64(tickMS)) / 1000.0
			sends := int(math.Floor(carry))
			carry -= float64(sends)

			for i := 0; i < sends; i++ {
				sendOne()
			}
		case <-sig:
			return stopAndReport()
		}
	}
}

func runProcessWorkerFromEnv() error {
	totalRequests := int64(-1)
	totalRequestsRaw := strings.TrimSpace(os.Getenv("BENCH_TOTAL_REQUESTS"))
	if totalRequestsRaw != "" {
		parsed, parseErr := strconv.ParseInt(totalRequestsRaw, 10, 64)
		if parseErr != nil || parsed < 0 {
			emitError("BENCH_TOTAL_REQUESTS must be a valid non-negative integer")
			return fmt.Errorf("invalid BENCH_TOTAL_REQUESTS")
		}
		totalRequests = parsed
	}

	durationMS := 0
	if totalRequests < 0 {
		duration, err := parsePositiveIntEnv("BENCH_DURATION_MS")
		if err != nil {
			emitError("BENCH_DURATION_MS must be a valid positive integer")
			return err
		}
		durationMS = duration
	}

	var stop atomic.Bool
	stop.Store(false)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(sig)

	channel := channelgo.NewChannel().
		Post("sync", func(channelgo.Body) (any, error) { return 1, nil }).
		Post("async", func(channelgo.Body) (any, error) { return 1, nil })
	sender := process.Connect(channel)
	defer sender.Stop()

	emitJSON(workerMessage{Type: "ready", PID: os.Getpid()})

	scenarioByDuration := func(path string) (int64, int64, float64) {
		var completed int64
		var errors int64
		started := time.Now()
		deadline := started.Add(time.Duration(durationMS) * time.Millisecond)
		for !stop.Load() && time.Now().Before(deadline) {
			_, sendErr := sender.Send(path, nil)
			if sendErr != nil {
				errors++
			} else {
				completed++
			}
		}
		return completed, errors, time.Since(started).Seconds()
	}

	scenarioByTotalRequests := func(path string, requests int64) (int64, int64, float64) {
		var completed int64
		var errors int64
		started := time.Now()

		for !stop.Load() && (completed+errors) < requests {
			_, sendErr := sender.Send(path, nil)
			if sendErr != nil {
				errors++
			} else {
				completed++
			}
		}

		return completed, errors, time.Since(started).Seconds()
	}

	go func() {
		<-sig
		stop.Store(true)
	}()

	if totalRequests >= 0 {
		completed, errors, elapsed := scenarioByTotalRequests("sync", totalRequests)
		emitJSON(map[string]any{
			"type":           "result",
			"pid":            os.Getpid(),
			"completed":      completed,
			"errors":         errors,
			"elapsedSeconds": elapsed,
		})
		return nil
	}

	syncCompleted, syncErrors, syncElapsed := scenarioByDuration("sync")
	asyncCompleted, asyncErrors, asyncElapsed := scenarioByDuration("async")

	emitJSON(map[string]any{
		"type":                "result",
		"pid":                 os.Getpid(),
		"syncCompleted":       syncCompleted,
		"syncErrors":          syncErrors,
		"syncElapsedSeconds":  syncElapsed,
		"asyncCompleted":      asyncCompleted,
		"asyncErrors":         asyncErrors,
		"asyncElapsedSeconds": asyncElapsed,
		"asyncFallback":       true,
	})
	return nil
}

func stopClientConnections(clients []*client.ClientSender) {
	for _, sender := range clients {
		if sender != nil && sender.Sender != nil {
			sender.Sender.Stop()
		}
	}
}

func parsePositiveIntEnv(key string) (int, error) {
	value := strings.TrimSpace(os.Getenv(key))
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a valid positive integer", key)
	}
	return parsed, nil
}

func parsePositiveFloatEnv(key string) (float64, error) {
	value := strings.TrimSpace(os.Getenv(key))
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil || parsed <= 0 || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		return 0, fmt.Errorf("%s must be a positive number", key)
	}
	return parsed, nil
}

func emitError(message string) {
	payload := map[string]any{
		"type":    "error",
		"message": message,
	}
	encoded, _ := json.Marshal(payload)
	fmt.Fprintln(os.Stderr, string(encoded))
}

func emitJSON(value any) {
	encoded, _ := json.Marshal(value)
	fmt.Fprintln(os.Stdout, string(encoded))
}

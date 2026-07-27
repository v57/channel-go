# channel-go

Go implementation of Channel with:
- WebSocket client transport
- WebSocket server transport
- In-process transport
- Interop tests against `channel/tests/server.ts` on `127.0.0.1:2049`

Canonical module: `github.com/v57/channel-go`

```bash
go get github.com/v57/channel-go
```

## Supported platforms

Verified build targets (every push, see CI `build` job):

| OS | Arch |
| --- | --- |
| Darwin | `amd64`, `arm64` |
| Linux | `amd64`, `arm64` |

Windows is not supported. The server transport relies on `golang.org/x/sys/unix`
(POSIX) APIs with no Windows equivalent; client and process transports also
target POSIX only. Do not list Windows in release notes.

## Supported toolchain

- Go: `go1.25.0`
- Bun: `1.3.8`
- LCOV: `2.4-0`

## Install
```bash
brew install go bun lcov
go install github.com/jandelgado/gcov2lcov@latest
```

## Package layout
- `channelgo`: core protocol runtime (`Channel`, `Sender`, `Subscription`, `Values`)
- `client`: reconnecting WebSocket client transport
- `server`: WebSocket server transport
- `process`: in-memory transport

## Quick start

### Server
```go
package main

import (
  "log"
  "github.com/v57/channel-go/channelgo"
  "github.com/v57/channel-go/server"
)

func main() {
  ch := channelgo.NewChannel().Post("hello", func(channelgo.Body) (any, error) {
    return "world", nil
  })

  srv, err := server.Listen(ch, 2048, server.Options{})
  if err != nil {
    log.Fatal(err)
  }
  defer srv.Stop()

  select {}
}
```

### Client
```go
package main

import (
  "fmt"
  "log"
  "github.com/v57/channel-go/channelgo"
  "github.com/v57/channel-go/client"
)

func main() {
  conn := client.Connect(channelgo.NewChannel(), 2048, client.Options{})
  defer conn.Stop()

  response, err := conn.Send("hello", nil)
  if err != nil {
    log.Fatal(err)
  }
  fmt.Println(response)
}
```

If you want a typed response instead of `any`:

```go
response, err := channelgo.Send[string](conn, "hello", nil)
```

### Process
```go
package main

import (
  "fmt"
  "github.com/v57/channel-go/channelgo"
  "github.com/v57/channel-go/process"
)

func main() {
  sender := process.Connect(channelgo.NewChannel().Post("hello", func(channelgo.Body) (any, error) {
    return "world", nil
  }))
  defer sender.Stop()

  response, _ := sender.Send("hello", nil)
  fmt.Println(response)
}
```

## Scripts
- `scripts/test`
  - Runs all tests.
  - Integration tests assume TS server is already running on `127.0.0.1:2049`.
- `scripts/benchmark`
  - Runs Go benchmark tests (`go test -bench`) and scenario benchmark executable (`bench/main.go`).
  - Includes Go↔Go websocket, Go↔TS websocket, and process benchmarks.
- `scripts/coverage`
  - Produces `coverage/coverage.out` and `coverage/lcov.info`.

## JSON codec policy

`encoding/json` is the default codec on every platform. This keeps the dependency
surface and agent binary size predictable. Sonic remains available as an explicit
opt-in on Darwin and Linux for `amd64` and `arm64`:

```bash
go build -tags channelgo_sonic ./...
```

Reference stripped-binary measurement for `./bench` on Darwin/arm64 with Go
1.26.5:

| Codec | Build command | Bytes |
| --- | --- | ---: |
| `encoding/json` | `go build -trimpath -ldflags='-s -w' ./bench` | 3,254,898 |
| Sonic | `go build -trimpath -tags channelgo_sonic -ldflags='-s -w' ./bench` | 4,043,058 |

Sonic adds 788,160 bytes (24.2%) and 28 codec-specific packages to this
reference binary. Re-run both commands with the pinned toolchain when updating
the codec policy or release measurements.

## Notes

- WebSocket transport uses Gobwas (`github.com/gobwas/ws`).

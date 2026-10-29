<h1>
  <img alt="Containerization logo" src="./icon.png" width="70" valign="middle">
  &nbsp;channel
</h1>

Powerful and lightweight communication built for modern language. Go implementation of Channel: run a websocket server, a websocket client, or communicate inside your process. Typed responses and streaming values are supported out of the box.

# Installation
```bash
go get github.com/v57/channel-go
```

# Usage

```go
// Server
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

```go
// Client
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

For a typed response instead of `any`:

```go
response, err := channelgo.Send[string](conn, "hello", nil)
```

# Process transport (in-memory)

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

# QUIC transport (experimental)

Channel over QUIC, as in the TypeScript reference (`channel/docs/quic.md`): the same messages over one QUIC stream,
TLS always on, WebSocket stays the default. It is its own module, so only programs that import it depend on
[quic-go](https://github.com/quic-go/quic-go) (and Go 1.26); the WebSocket transports don't.

```bash
go get github.com/v57/channel-go/quic
```

```go
import (
  "github.com/v57/channel-go/channelgo"
  "github.com/v57/channel-go/quic"
  "github.com/v57/channel-go/server"
)

// Server: PEM key and certificate, plus the WebSocket server's options
srv, err := quic.Listen(ch, 4433, quic.ListenOptions{Key: key, Cert: cert, Options: server.Options{State: state}})

// Client: quic://host:port; trust a self-signed certificate with CA, or pin its SHA-256 with Fingerprint
conn, err := quic.Connect(channelgo.NewChannel(), "quic://127.0.0.1:4433", quic.Options{CA: cert})
response, err := conn.Send("hello", nil)
```

`quic.Options` embeds `client.Options` (headers, `OnConnect`, `OnMessage`, `MaxPayload`) and adds `CA`,
`Fingerprint` and `ServerName`. Without `CA` or `Fingerprint` the server certificate must be trusted by the system.
The client reconnects every 100 ms like the WebSocket client and sends unanswered requests again.

# Package layout
- `channelgo`: core protocol runtime (`Channel`, `Sender`, `Subscription`, `Values`)
- `client`: reconnecting WebSocket client transport
- `server`: WebSocket server transport
- `process`: in-memory transport
- `quic`: QUIC client and server transport, a separate module (`github.com/v57/channel-go/quic`)

# Supported platforms

Verified build targets (every push, see CI `build` job):

| OS | Arch |
| --- | --- |
| Darwin | `amd64`, `arm64` |
| Linux | `amd64`, `arm64` |

Windows is not supported. The server transport relies on `golang.org/x/sys/unix`
(POSIX) APIs with no Windows equivalent; client and process transports also
target POSIX only. Do not list Windows in release notes.

# Supported toolchain

- Go: `go1.25.0`
- Bun: `1.3.8`
- LCOV: `2.4-0`

# Install
```bash
brew install go bun lcov
go install github.com/jandelgado/gcov2lcov@latest
```

# Scripts
- `scripts/test`
  - Runs all tests, the `quic` module's too.
  - Integration tests assume TS server is already running on `127.0.0.1:2049` (WebSocket on TCP, QUIC on UDP);
    the QUIC tests also run the TypeScript QUIC client against the Go server when `bun` is installed.
- `scripts/benchmark`
  - Runs Go benchmark tests (`go test -bench`) and scenario benchmark executable (`bench/main.go`).
  - Includes Go↔Go websocket, Go↔TS websocket, and process benchmarks.
- `scripts/coverage`
  - Produces `coverage/coverage.out` and `coverage/lcov.info`.

# JSON codec policy

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

# Notes

- The WebSocket transports use their own RFC 6455 code (`internal/wsraw`), so they need no WebSocket library;
  Gobwas (`github.com/gobwas/ws`) is used only by tests.

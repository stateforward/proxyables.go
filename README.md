# Proxyables (Go)

A high-performance, peer-to-peer RPC library that makes remote objects feel local. Built on top of **Yamux** multiplexing and Go's reflection, it enables seamless bi-directional interaction between processes with support for callbacks, distributed garbage collection, and stream pooling.

## Features

- **Peer-to-Peer Architecture**: No strict client/server distinction — both sides can import and export objects, enabling true bi-directional communication.
- **Cursor-Based API**: Chain property accesses and method calls using `ProxyCursor` with `.Get()` and `.Apply()`, then execute synchronously or asynchronously.
- **Distributed Garbage Collection**: Automatically manages remote object lifecycles using Go's `runtime.SetFinalizer` and a reference counting protocol.
- **Bi-Directional Callbacks**: Pass functions as arguments — they are automatically registered and invoked remotely.
- **Stream Pooling**: Reuses Yamux substreams to eliminate handshake overhead for high-frequency calls.
- **Context Support**: Full `context.Context` integration for cancellation and timeouts.

## Installation

```bash
go get github.com/stateforward/proxyables.go
```

## Usage

### Basic Example

**Server (Exporting an object):**
```go
import "github.com/stateforward/proxyables.go"

type API struct{}

func (a *API) Echo(msg string) string {
    return "echo " + msg
}

func (a *API) Compute(a1, b int) int {
    return a1 + b
}

// conn is any net.Conn or io.ReadWriteCloser
exported, _ := proxyables.Export(conn, &API{}, nil)
```

**Client (Importing the object):**
```go
import "github.com/stateforward/proxyables.go"

proxy, _, _ := proxyables.ImportFrom(conn, nil)

// Chain instructions and execute
result, _ := proxy.Get("Echo").Apply("hello").Exec(ctx)
// result: "echo hello"

result, _ = proxy.Get("Compute").Apply(10, 20).Exec(ctx)
// result: 30
```

## Architecture

1. **Proxy Layer**: `ProxyCursor` builds instruction chains for remote execution.
2. **Instruction Protocol**: Operations (get, apply, etc.) are serialized into `ProxyInstruction` messages using MessagePack.
3. **Transport**: Uses `hashicorp/yamux` to multiplex concurrent operations over a single connection.
4. **Reference Management**: An `ObjectRegistry` tracks local objects with reference counting and deduplication.
5. **Stream Pool**: Maintains idle Yamux streams for reuse, reducing allocation overhead.

## License

MIT

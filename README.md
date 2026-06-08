# snowflake

A Go package for distributed unique ID generation using Twitter's Snowflake algorithm, optimized for high throughput and low latency.

## How It Works

Each generated ID is a 64-bit integer composed of three parts:

```
| 41 bits: timestamp | 10 bits: worker ID | 12 bits: sequence |
```

| Component | Bits | Capacity |
|---|---|---|
| Timestamp | 41 | Millisecond precision, ~69 years of unique IDs |
| Worker ID | 10 | Up to 1,024 unique nodes |
| Sequence | 12 | 4,096 IDs per millisecond per node |

This gives a theoretical maximum of **4,096,000 IDs/second per node**.

## Installation

```bash
go get github.com/hekimapro/snowflake
```

## Quick Start

```go
package main

import (
    "fmt"
    "github.com/hekimapro/snowflake"
)

func main() {
    gen, err := snowflake.NewGenerator(snowflake.Configuration{
        WorkerIdentifier: 1,
    })
    if err != nil {
        panic(err)
    }

    id := gen.NextID()
    fmt.Println(id)
}
```

## Configuration

```go
cfg := snowflake.Configuration{
    // Required. Must be unique per node in your cluster. Range: 0–1023.
    WorkerIdentifier: 42,

    // Optional. Unix timestamp in milliseconds.
    // Defaults to 2025-01-01 00:00:00 UTC if zero or omitted.
    // Set this to a date before your earliest production deployment
    // to maximize the usable ID lifetime.
    CustomEpoch: 1735689600000,
}

gen, err := snowflake.NewGenerator(cfg)
```

## Generating IDs

```go
id := gen.NextID() // returns int64
```

`NextID` is safe for concurrent use. Under normal conditions it runs in 100–200 ns. If the sequence counter is exhausted within a single millisecond, it blocks for up to ~50 µs while waiting for the clock to advance.

## Inspecting IDs

Three helper functions let you decompose any Snowflake ID without a database lookup:

```go
epoch := int64(1735689600000) // must match the epoch used at generation time

ts  := snowflake.ExtractTimestamp(id, epoch)        // Unix ms when the ID was created
wid := snowflake.ExtractWorkerIdentifier(id)        // which node generated it (0–1023)
seq := snowflake.ExtractSequence(id)                // sequence counter (0–4095)
```

These are useful for sorting, debugging, and forensic analysis of ID generation patterns.

## Error Handling

| Error | Cause |
|---|---|
| `ErrInvalidWorkerIdentifier` | `WorkerIdentifier` is outside the range 0–1023 |
| `ErrTimeRollback` | System clock drifted backwards |

Clock rollback is handled automatically for small drifts (≤5 seconds) by sleeping until the clock catches up. For larger rollbacks, the generator panics with `ErrTimeRollback`.

```go
gen, err := snowflake.NewGenerator(cfg)
if err != nil {
    if errors.Is(err, snowflake.ErrInvalidWorkerIdentifier) {
        // handle configuration error
    }
}
```

## Deploying Multiple Nodes

Each node in your cluster **must** have a unique `WorkerIdentifier` in the range 0–1023. Assigning the same ID to two nodes will produce duplicate snowflake IDs.

Common strategies for distributing worker IDs:

- Assign them statically via environment variable or config file per host
- Use a coordination service (e.g. Redis, ZooKeeper, Consul) to lease IDs at startup
- Derive them from a hash of the machine's hostname or IP address

## Performance

| Condition | Latency |
|---|---|
| Normal operation | 100–200 ns/ID |
| Sequence exhausted (waiting for next ms) | up to ~50 µs |

## License

See [LICENSE](LICENSE).

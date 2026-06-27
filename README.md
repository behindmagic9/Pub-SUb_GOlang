# Pub/Sub Event Broker

A concurrent, in-memory, topic-based Pub/Sub broker written in Go to explore event-driven system design, worker pools, queue sharding, retries, and channel-based concurrency.

The broker follows a producer-consumer architecture internally. Published events are routed to sharded queues using FNV hashing, processed by worker pools, and delivered concurrently to subscribers. Failed deliveries are retried before being moved to a Dead Letter Queue (DLQ).

> **Project Status**
>
> This version is intentionally frozen. It represents a learning implementation focused on concurrency, routing, retries, worker coordination, and event delivery.

---

# Features

- Topic-based Publish/Subscribe
- Producer-Consumer architecture
- Concurrent publishers
- Queue sharding using FNV hashing
- Worker pool per shard
- Retry mechanism (up to 5 retries)
- Dead Letter Queue (DLQ)
- Dead Letter logging to the local file system
- Delivery tracking
- Atomic delivery metrics
- `sync.Pool` for `DeliveryTracker` reuse
- Race tested (`go test -race`)

---

# Running

```bash
go run .
```

---

# Benchmarks

### Machine

- CPU: AMD Ryzen 5 5500U
- OS: Linux (amd64)

## Microbenchmarks

Microbenchmarks measure individual broker operations.

Executed using:

```bash
go test ./broker -bench=. -count=5
```

Representative results:

| Benchmark | Measures | Result |
|-----------|----------|--------|
| BenchmarkNotify | Publish path for a single event | **~376 ns/op** |
| BenchmarkParallelPublish | Concurrent publishing from multiple goroutines | **~260 ns/op** |
| BenchmarkThroughput | Continuous publish workload | **~690 ns/op** |
| BenchmarkFanout100Subscribers | Deliver one event to 100 subscribers | **~44 µs/op** |
| BenchmarkMultipleTopics | Publishing across multiple topics | **~635 ns/op** |
| BenchmarkQueuePressure | Queue behaviour under sustained load | **~385 ns/op** |

Run benchmarks:

```bash
go test ./broker -bench=. -benchmem
```

---

# End-to-End Throughput

The end-to-end benchmark measures the complete broker execution rather than individual operations.

A fixed workload of **1,200,000 events** is published.

Throughput is calculated as:

```text
Rate = Published Events / Total Execution Time
```

## Benchmark Configuration

- Published events: **1,200,000**
- Worker count: `runtime.NumCPU()`
- Shard count: `runtime.NumCPU()`
- Retry limit: **5**
- Event structure:
    - `ID int64`
    - `Topic string`
    - `Message string`
    - `Price int`
- Every published event is encapsulated in a `DeliveryTracker`, which maintains delivery state, retry count, and subscriber context throughout the delivery pipeline.
- Failure injection:
    - **0%** (normal workload)
    - **~33%** (failure workload)

### Normal Operation

```text
Published   : 1,200,000
Delivered   : 1,200,000
Retried     : 0
Dead Letter : 0
Dropped     : 0

Execution Time : ~1.01 s
Rate           : ~1.19M messages/sec
```

### Failure Injection (~33%)

Subscriber failures are randomly injected.

Failed deliveries are:

- retried up to 5 times
- tracked through the delivery pipeline
- moved to the Dead Letter Queue after exhausting retries
- logged to the local file system

```text
Published   : 1,200,000
Delivered   : 1,194,981
Retried     : 597,311
Dead Letter : 5,014
Dropped     : 0

Execution Time : ~1.97 s
Rate           : ~608K messages/sec
```

> **Benchmark Note**
>
> During benchmarking, Dead Letter file logging was temporarily disabled to avoid measuring filesystem I/O. The retry pipeline, Dead Letter Queue, delivery tracking, and metrics remained enabled. This isolates broker performance from disk latency.

---

# Race Detection

```bash
go test ./... -race
```

---

# Architecture

Each topic is hashed using FNV and mapped to one of several shards.

Every shard owns:

- an independent event queue
- a dedicated worker pool

Workers consume events from their shard, invoke subscribers, and update delivery state. Failed deliveries enter the retry pipeline until the retry limit is reached. Events that continue to fail are moved to the Dead Letter Queue, logged, and persisted to the local file system.

```
             +-------------+
             |  Publisher  |
             +------+------+
                    |
                    v
             +-------------+
             |   Notify()  |
             +------+------+
                    |
                    v
          +-------------------+
          | Topic Subscribers |
          +-------------------+
                    |
                    v
            Hash Topic (FNV)
                    |
                    v
      +----------------------------+
      | Sharded Queues (CPU Count) |
      +----------------------------+
             |      |      |
             v      v      v

      +--------------------------+
      | Worker Goroutines        |
      | (per shard workers)      |
      +------------+-------------+
                   |
                   v
            Subscriber.Update()
                   |
         +---------+---------+
         |                   |
      Success             Failure
         |                   |
         v                   v
 Delivered++          Retry Count++
                             |
                 +-----------+-----------+
                 |                       |
          Retry < Max             Retry >= Max
                 |                       |
                 v                       v
            Retry Queue           Dead Letter Queue
                 |                       |
                 |                  Log to File
                 |                       |
                 +-----------+-----------+
                             |
                             v
                      Back to Shard Queue
```

---

# Future Improvements

Possible future directions include:

- Configurable shutdown strategies
- Adaptive buffering and backpressure handling for burst workloads
- Persistent Dead Letter storage backend
- Storage abstraction for durable events
- Configurable retry policies (exponential backoff, scheduled retries, etc.)
- Latency benchmarking (P50 / P95 / P99)
- CPU and memory profiling
- Configuration-driven broker initialization
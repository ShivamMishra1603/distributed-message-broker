# Distributed Message Broker

A high-performance, single-node, multi-partition distributed message broker written in Go.

---

## Features

- **Multi-Partition Logging**: Partition streams managed through rolled segment files and sparse index mapping.
- **Durable Storage**: Synchronous (`fsync`) and asynchronous (page-cache buffered) log flush modes.
- **Client APIs**: Complete gRPC implementation for message publishing (producing), fetching (consuming), and group offsets tracking.
- **Prometheus Telemetry**: Integrated HTTP operations server exposing partition-level counters, sizes, latencies, and pprof endpoints.
- **Durable Consumer Offsets**: persistent append-only offset log ensuring commit durability.

---

## Quick Start

### Prerequisites
- Go 1.21 or later
- Docker (optional)

### Build
To compile the broker, producer/consumer clients, administrative command, and benchmarking tools:
```bash
make build
```
Executables are placed in the `bin/` directory.

### Run Tests
To run the full suite of unit and integration tests:
```bash
make test
```

### Start the Broker
To start the broker using default configurations:
```bash
./bin/broker -config config/config.yaml
```

---

## Benchmarking Suite

The broker includes concurrent, high-throughput benchmark clients in `cmd/bench_producer` and `cmd/bench_consumer`.

### Run Producer Benchmark
```bash
./bin/bench_producer -broker localhost:9092 -topic bench-topic -count 100000 -concurrency 4 -batch-size 100
```

### Run Consumer Benchmark
```bash
./bin/bench_consumer -broker localhost:9092 -topic bench-topic -count 100000 -concurrency 4 -max-fetch-bytes 1048576
```

### Automated Benchmark Scenarios Matrix
To automatically build, execute, and profile the full suite of performance scenarios (S1–S7) and capture JSON results:
```bash
make bench
```

---

## Documentation Index

Detailed specifications are available in the [docs/](docs/) directory:

- 🏛️ **[System Architecture](docs/architecture.md)**: multi-component diagrams, produce and fetch operation data paths.
- 🔌 **[gRPC API Specification](docs/api.md)**: protobuf interfaces, schemas, and gRPC handler error maps.
- ⚙️ **[Configuration Reference](docs/configuration.md)**: environment variables, YAML properties, and defaults.
- 💾 **[Storage & Layout Formats](docs/storage_format.md)**: exact record header byte layouts, index files, and consumer offset entries.
- 🤝 **[Guarantees & Scope Limits](docs/guarantees_limitations.md)**: message ordering, durability types, at-least-once delivery, and single-node constraints.
- 📊 **[Performance Benchmark Report](docs/benchmark_report.md)**: test matrices, methodology, and execution metrics.
- 🛠️ **[Operations Guide](docs/operations.md)**: health endpoints, docker volumes, retention, and index rebuild procedures.
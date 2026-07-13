# Operations Guide

This guide details operational guidelines for monitoring, profiling, maintaining, and deploying the Distributed Message Broker.

---

## 1. Monitoring & Observability

The broker runs an auxiliary HTTP administration server on port `9093` for health and metrics.

### Health Endpoints
- **Liveness probe**: `GET http://localhost:9093/healthz`
  - Returns `200 OK` (plain text `OK`) if the HTTP server is running.
- **Readiness probe**: `GET http://localhost:9093/readyz`
  - Returns `200 OK` (plain text `OK`) when the broker listeners are bound and ready to accept traffic.
  - Returns `503 Service Unavailable` during startup errors or gracefully draining shutdown windows.

### Prometheus Metrics
- **Metrics endpoint**: `GET http://localhost:9093/metrics`
- Exposes partition-specific message counters, write/read payload volumes, active segments counts, and gRPC execution duration distributions.
- Go runtime garbage collection and CPU usage stats are collected via Go's built-in collector registries.

### Pprof Diagnostics
- **Endpoint**: `http://localhost:9093/debug/pprof/`
- Enabled by setting `pprof_enabled: true` in the configuration.
- Allows live profiling of CPU utilization, lock contention, memory allocations, and heap allocations.

---

## 2. Docker Compose Deployment

The broker is packaged as a secure multi-stage Docker image running under a non-root `broker` user (UID 10001).

### Mounting Data Directory
Durable logs must be persisted in a mounted volume. The container writes logs to `/var/lib/broker`.
```yaml
services:
  broker:
    image: distributed-message-broker:latest
    ports:
      - "9092:9092"
      - "9093:9093"
    volumes:
      - broker-data:/var/lib/broker
    environment:
      - BROKER_DATA_DIRECTORY=/var/lib/broker
```

---

## 3. Maintenance Tasks

### Index Verification and Rebuild Procedure
If the broker is terminated abruptly, index files (`.index`) could become stale or corrupted. The broker implements automatic index recovery on startup:
1. When loading segments, the broker checks for the presence of the matching `.index` file.
2. If the index file is missing, the partition log manager performs a sequential recovery scan of the segment log file, recalculating record offsets and writing a fresh, valid `.index` file.
3. To trigger a manual index rebuild:
   - Shut down the broker.
   - Delete the `.index` files from the partition directory:
     ```bash
     find /var/lib/broker -name "*.index" -type f -delete
     ```
   - Restart the broker. The startup logs will show index reconstruction executing for all rolled log segments.

### Log Segment Retention behavior
- **Age-based retention**: Periodically deletes log segments whose maximum record timestamp is older than `retention.max_age`.
- **Size-based retention**: If the aggregate size of all segments in a partition exceeds `retention.max_partition_bytes`, the broker deletes segments starting from the earliest rolled segment until the partition size drops below the limit.
- **Active Segment Exemption**: The active log segment (currently accepting appends) is never deleted by retention policies, even if it exceeds the configured age or size thresholds.

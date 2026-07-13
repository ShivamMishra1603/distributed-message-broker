# Performance Benchmark Report

This document reports performance benchmarks, throughput limits, latency distributions, and pprof runtime analysis for the Distributed Message Broker.

---

## 1. Benchmark Methodology

We measure performance under controlled execution scenarios to observe system behaviors under varying workloads.
- **Warmup Phase**: Every trial is preceded by a warmup phase of 10,000 requests (excluded from results) to eliminate JIT compilation, GC setup, and connection establishment overheads.
- **Latency Percentiles**: Latency metric percentiles are reported **per RPC** (Produce/Fetch request duration) rather than per-record, using deterministic reservoir sampling (max size 1,000,000, fixed random seed `12345`).
- **Filesystem Cache Disclaimer**: Unless otherwise noted, broker restarts do not clear OS-level page cache buffers, representing a **warm-cache recovery** state.
- **Lock Contention profiling**: mutexp/block profiles are diagnostic runs configured via `diagnostic.yaml` to prevent baseline benchmark skewing.
- **Throughput calculation**:
  - Producer throughput uses absolute successfully acknowledged messages.
  - Consumer throughput uses exact counted target records, reporting overshoot separately.

---

## 2. Environment Specification

| Parameter | Measured Baseline Values |
|---|---|
| **Commit SHA** | `8b95a690` (dirty: `true`) |
| **Go Version** | `go1.25.1` |
| **Operating System** | `Darwin` |
| **CPU Specs** | `Apple M4` |
| **RAM Size** | `24 GB` |
| **Disk Type** | Apple SSD (APFS) |
| **Broker Location** | Local (Client and Broker run on same host loopback interface) |

---

## 3. Performance Results Matrix

*All values represent median figures obtained over 3 repeated trials.*

### S1: Flush Mode Comparison
- Workload: 100,000 records, concurrency 4, batch size 100, record size 100 bytes.
- Durability: Sync (fsync on each write) vs Async (page-cache buffered writes).

| Flush Mode | Messages/sec | Payload (MiB/s) | Min Latency | P50 (Median) | P95 | P99 | Max Latency |
|---|---|---|---|---|---|---|---|
| **Sync** | 18,975.44 | 1.81 | 10.84ms | 19.99ms | 29.88ms | 34.67ms | 40.12ms |
| **Async** | 2,366,044.89 | 225.64 | 0.06ms | 0.13ms | 0.30ms | 0.41ms | 0.57ms |

### S2: Batching Impact (Sync Mode)
- Workload: scaled based on batch size to bound request execution time (10k records for batch 1, 50k records for batch 10, 100k records for batches 100 & 500). Record size 100 bytes, concurrency 4.

| Batch Size | Messages/sec | Payload (MiB/s) | P50 RPC Latency | P95 RPC Latency | P99 RPC Latency |
|---|---|---|---|---|---|
| **1** (No batching) | 300.45 | 0.03 | 13.06ms | 17.84ms | 25.91ms |
| **10** | 2,613.96 | 0.25 | 14.02ms | 26.03ms | 28.11ms |
| **100** | 18,920.06 | 1.80 | 20.01ms | 29.96ms | 34.91ms |
| **500** | 85,024.47 | 8.11 | 23.68ms | 30.43ms | 32.67ms |

### S3: Record Payload Size Impact
- Workload: 50,000 records, concurrency 4, batch size 100.

| Message Size | Messages/sec | Payload (MiB/s) | P50 RPC Latency | P95 RPC Latency | P99 RPC Latency |
|---|---|---|---|---|---|
| **100 B** | 18,921.10 | 1.80 | 20.08ms | 29.94ms | 32.99ms |
| **1 KiB** | 16,467.75 | 16.08 | 25.00ms | 30.19ms | 34.90ms |
| **10 KiB** | 16,566.54 | 161.78 | 24.03ms | 32.94ms | 38.02ms |

### S4: Concurrency & Partition Scaling
- Workload: 100,000 records, batch size 100, record size 100 bytes.

| Concurrency | Partition Mode | Messages/sec | Payload (MiB/s) | P50 RPC Latency | P95 RPC Latency |
|---|---|---|---|---|---|
| **1** | Single Partition | 12,916.14 | 1.23 | 7.91ms | 8.72ms |
| **4** | Single Partition | 13,442.71 | 1.28 | 29.96ms | 32.04ms |
| **16** | Single Partition | 13,119.39 | 1.25 | 126.03ms | 128.13ms |
| **32** | Single Partition | 13,182.41 | 1.26 | 245.98ms | 256.09ms |
| **1** | Round-Robin (4 Partitions) | 12,966.39 | 1.24 | 7.91ms | 8.96ms |
| **4** | Round-Robin (4 Partitions) | 18,720.87 | 1.79 | 21.05ms | 30.01ms |
| **16** | Round-Robin (4 Partitions) | 19,206.66 | 1.83 | 82.07ms | 152.45ms |
| **32** | Round-Robin (4 Partitions) | 19,122.69 | 1.82 | 163.33ms | 252.07ms |

### S5: Consumer Fetch size Impact
- Workload: 100,000 records, concurrency 4.

| Max Fetch Bytes | Messages/sec | Payload (MiB/s) | Observed Count | Overshoot Count | P50 RPC Latency |
|---|---|---|---|---|---|
| **64 KiB** | 5,263,423.56 | 501.96 | 100,000 | 0 | 0.36ms |
| **1 MiB** | 6,565,431.65 | 626.13 | 100,000 | 0 | 5.56ms |
| **5 MiB** | 6,962,939.75 | 664.04 | 100,000 | 0 | 14.12ms |

---

## 4. Recovery & Retention Operational Performance

### S6: Process Restart & Index Recovery Duration
- Dataset: 100,000 pre-produced records.

| Recovery Scenario | Process Boot-to-Readiness Duration |
|---|---|
| **Restart recovery (Valid Indexes)** | 143 ms |
| **Restart recovery (Index Reconstruction)** | 130 ms |

### S7: Retention Cleanup
- Deletion of oldest segment files completed successfully: **PASS**
- Active segment correctly retained: **PASS**
- Cleanup execution wall-clock time: ~2 ms

---

## 5. Diagnostic Profiling Analysis (Diagnostic Run only)

*The profiling run was executed using benchmarks/configs/diagnostic.yaml with MutexProfileFraction: 10 and BlockProfileRate: 10000.*

### CPU Utilization Bottlenecks
- Top CPU consumers (extracted via `go tool pprof -top`):
  ```
     flat  flat%   sum%        cum   cum%
    100ms 21.74% 21.74%      100ms 21.74%  runtime.fcntl
     80ms 17.39% 39.13%       80ms 17.39%  runtime.usleep
     80ms 17.39% 56.52%       80ms 17.39%  syscall.syscall6
     50ms 10.87% 67.39%       50ms 10.87%  syscall.syscall
     30ms  6.52% 73.91%       30ms  6.52%  runtime.kevent
  ```
  *Analysis: Filesystem `fcntl` and kernel `syscalls` (mostly `fsync` blockages) take up >50% of the active execution time during synchronous operations.*

### Memory Allocation Analysis
- Heap allocation hotspots (extracted via `go tool pprof -top`):
  ```
     flat  flat%   sum%        cum   cum%
   2565kB 37.05% 37.05%     2565kB 37.05%  runtime.allocm
1762.94kB 25.47% 62.52%  1762.94kB 25.47%  runtime/pprof.StartCPUProfile
 528.17kB  7.63% 70.15%   528.17kB  7.63%  google.golang.org/grpc/internal/mem.(*SimpleBufferPool).Get
 518.65kB  7.49% 85.21%   518.65kB  7.49%  github.com/ShivamMishra1603/distributed-message-broker/internal/storage.EncodeBatch
  ```
  *Analysis: Dynamic record serialization allocations in `EncodeBatch` remain under 7.5% of inuse memory, highlighting the effectiveness of the pre-allocated slice buffers.*

# Performance Benchmark Report

This document reports performance benchmarks, throughput limits, latency distributions, and pprof runtime analysis for the Distributed Message Broker.

> [!NOTE]
> All results in this report were produced from a **clean commit** (`fcb910cc`) on a single-host loopback setup. Numbers represent local-machine performance and should not be compared directly with production or distributed deployments.

---

## 1. Benchmark Methodology

- **Duration-based trials**: Each trial runs for a fixed **10 seconds** to ensure steady-state measurement. Workers continuously produce or consume records until the deadline expires.
- **Three independent trials**: Every scenario is executed 3 times; tables report the **median** of the 3 trials.
- **Warmup phase**: A dedicated warmup run (excluded from results) precedes the measured trials to eliminate JIT compilation, GC setup, and connection establishment overheads.
- **Latency percentiles**: Reported **per RPC** (Produce/Fetch request duration) using reservoir sampling (max 1,000,000 samples, fixed random seed `12345`). Exact retained samples may vary with concurrent completion order.
- **Filesystem cache disclaimer**: Broker restarts do not clear OS-level page cache buffers, representing a **warm-cache** state.
- **Lock-contention profiling**: Mutex and block profiles are collected in a separate diagnostic run (`diagnostic.yaml`) to avoid skewing baseline throughput measurements.
- **Throughput calculation**:
  - Producer throughput = acknowledged records ÷ wall-clock duration.
  - Consumer throughput = counted target records ÷ wall-clock duration (overshoot reported separately).

---

## 2. Environment Specification

| Parameter | Value |
|---|---|
| **Commit SHA (Commit A)** | `fcb910cc` (dirty: `false`) |
| **Go Version** | `go1.25.1` |
| **Operating System** | `Darwin (macOS)` |
| **CPU** | `Apple M4` |
| **RAM** | `24 GB` |
| **Disk** | Apple SSD (APFS) |
| **Broker Location** | Local (client and broker on same host via loopback) |
| **gRPC Max Message Size** | 16 MiB (client-side send/receive) |

---

## 3. Performance Results Matrix

*All values represent median figures from 3 repeated 10-second trials.*

### S1: Flush Mode Comparison

Workload: duration-based (10s), concurrency 4, batch size 100, record size 100 bytes, round-robin across 4 partitions.

| Flush Mode | Messages/sec | Payload (MiB/s) | P50 Latency | P95 | P99 | Max |
|---|---|---|---|---|---|---|
| **Sync** | 19,152 | 1.83 | 19.99ms | 29.03ms | 33.99ms | 41.01ms |
| **Async** | 3,131,401 | 277.98 | 0.10ms | 0.26ms | 0.31ms | 31.35ms |

> **Key insight**: Async mode achieves ~164× higher throughput than sync mode. Sync latency is dominated by `fsync` system calls (see §5). The async max latency spike (31ms) reflects occasional GC or scheduling pauses.

### S2: Batching Impact (Sync Mode)

Workload: duration-based (10s), record size 100 bytes, concurrency 4, sync flush.

| Batch Size | Messages/sec | Payload (MiB/s) | P50 RPC Latency | P95 | P99 |
|---|---|---|---|---|---|
| **1** (no batching) | 319 | 0.03 | 11.94ms | 17.33ms | 23.94ms |
| **10** | 2,755 | 0.26 | 14.00ms | 24.08ms | 27.98ms |
| **100** | 18,895 | 1.80 | 20.11ms | 29.03ms | 34.01ms |
| **500** | 84,276 | 8.04 | 23.98ms | 29.99ms | 31.97ms |

> **Key insight**: Batching amortizes the per-`fsync` cost. Going from batch=1 to batch=500 yields a ~264× throughput increase while RPC latency only doubles (12ms → 24ms).

### S3: Record Payload Size Impact

Workload: duration-based (10s), concurrency 4, batch size 100, sync flush.

| Message Size | Messages/sec | Payload (MiB/s) | P50 RPC Latency | P95 | P99 |
|---|---|---|---|---|---|
| **100 B** | 19,496 | 1.86 | 19.97ms | 29.03ms | 33.96ms |
| **1 KiB** | 15,202 | 14.85 | 26.00ms | 30.03ms | 34.00ms |
| **10 KiB** | 14,643 | 142.99 | 28.02ms | 30.08ms | 34.03ms |

> **Key insight**: Throughput in MiB/s scales nearly linearly with message size (1.86 → 143 MiB/s), while msg/s drops only ~25%. The broker's write path is I/O-bound, not serialization-bound.

### S4: Concurrency & Partition Scaling

Workload: duration-based (10s), batch size 100, record size 100 bytes, sync flush.

| Concurrency | Partition Mode | Messages/sec | P50 RPC Latency | P95 |
|---|---|---|---|---|
| **1** | Single Partition | 12,105 | 8.00ms | 8.96ms |
| **4** | Single Partition | 12,274 | 31.98ms | 32.00ms |
| **16** | Single Partition | 12,445 | 127.99ms | 128.01ms |
| **32** | Single Partition | 12,450 | 256.05ms | 256.08ms |
| **1** | Round-Robin (4 Partitions) | 12,052 | 8.01ms | 8.98ms |
| **4** | Round-Robin (4 Partitions) | 17,537 | 23.86ms | 29.98ms |
| **16** | Round-Robin (4 Partitions) | 17,605 | 91.98ms | 152.00ms |
| **32** | Round-Robin (4 Partitions) | 17,591 | 190.45ms | 248.01ms |

> **Key insight**: Single-partition throughput plateaus at ~12.4K msgs/s regardless of concurrency — this is the serialization bottleneck of the per-partition write lock + fsync. Multi-partition mode achieves ~17.6K msgs/s by distributing writes across 4 independent partition locks, a ~46% improvement.

### S5: Consumer Fetch Size Impact

Workload: duration-based (10s), concurrency 4, all-partitions mode, replaying from offset 0.

| Max Fetch Bytes | RPCs Completed | P50 RPC Latency | P99 | Max |
|---|---|---|---|---|
| **64 KiB** | ~92,000 | 0.40ms | 1.01ms | 7.68ms |
| **1 MiB** | ~8,500 | 4.69ms | 7.01ms | 13.94ms |
| **5 MiB** | ~1,936 | 21.31ms | 30.27ms | 44.06ms |

> **Key insight**: Larger fetch sizes reduce RPC count by ~47× (92K → 1.9K) while latency scales sub-linearly. The 5 MiB fetch effectively saturates the loopback gRPC channel.

---

## 4. Recovery & Retention Operational Performance

### S6: Process Restart & Index Recovery Duration

Dataset: pre-produced records on 1 partition. 5 cloned-directory trials per scenario to eliminate filesystem cache variance.

| Recovery Scenario | Trial 1 | Trial 2 | Trial 3 | Trial 4 | Trial 5 | **Median** |
|---|---|---|---|---|---|---|
| **Valid Indexes** | 76ms | 90ms | 89ms | 89ms | 87ms | **89ms** |
| **Index Reconstruction** | 89ms | 90ms | 89ms | 86ms | 89ms | **89ms** |

> **Key insight**: Index reconstruction adds negligible overhead in this dataset size because the partition log is small enough to scan quickly. Larger datasets would show a more pronounced difference.

### S7: Retention Cleanup

Configuration: 1 MiB segment max, 3 MiB partition limit, 1s check interval.

| Metric | Result |
|---|---|
| **Segments deleted** | ✅ Yes (files before: 8, active segment advanced to offset 42000) |
| **Active segment preserved** | ✅ `00000000000000042000.log` |
| **Detection + cleanup latency** | **258ms** |

> **Key insight**: The retention cleaner correctly identified and deleted old segments while preserving the active segment. The 258ms cleanup time includes the filesystem `unlink` calls for both `.log` and `.index` files.

---

## 5. Diagnostic Profiling Analysis

*Collected during a separate diagnostic run with `MutexProfileFraction: 10` and `BlockProfileRate: 10000`. These numbers should not be compared with baseline throughput runs.*

### CPU Utilization Bottlenecks

Top CPU consumers (via `go tool pprof -top`):

```
     flat  flat%   sum%        cum   cum%
    100ms 25.00% 25.00%      100ms 25.00%  runtime.fcntl
     70ms 17.50% 42.50%       70ms 17.50%  runtime.usleep
     60ms 15.00% 57.50%       60ms 15.00%  syscall.syscall6
     50ms 12.50% 70.00%       50ms 12.50%  runtime.pthread_cond_timedwait_relative_np
     30ms  7.50% 90.00%       30ms  7.50%  syscall.syscall
     20ms  5.00% 95.00%       20ms  5.00%  runtime.kevent
```

**Analysis**: `fcntl` (used by `fsync`) and kernel syscalls dominate active CPU time during synchronous operations, accounting for >57% of samples. This confirms that sync-mode throughput is fundamentally I/O-bound rather than compute-bound.

### Memory Allocation Analysis

Heap allocation hotspots (via `go tool pprof -top`):

```
     flat  flat%   sum%        cum   cum%
 1184.27kB 31.60% 31.60%  1184.27kB 31.60%  runtime/pprof.StartCPUProfile
    1026kB 27.38% 58.98%  1538.22kB 41.05%  runtime.allocm
  512.69kB 13.68% 72.67%   512.69kB 13.68%  regexp/syntax.(*compiler).inst
  512.22kB 13.67% 86.34%   512.22kB 13.67%  runtime.malg
  512.05kB 13.66%   100%   512.05kB 13.66%  internal/broker.(*Broker).monitorServe
```

**Analysis**: Total in-use heap is only **3.7 MiB** during active load, with no application-level allocation hotspots. The top consumers are runtime internals (`allocm`, `malg`) and profiling infrastructure. The broker's record serialization path (`EncodeBatch`) does not appear in the top allocators, confirming effective use of pre-allocated buffers.

---

## 6. Reproducibility

To reproduce these results:

```bash
# From repository root on commit fcb910cc
./benchmarks/scripts/run-benchmarks.sh
```

Results are saved to `benchmarks/results/<timestamp>-<os>-<arch>-<sha>/` with machine-readable JSON files per trial and human-readable pprof text summaries.

| Artifact | Path |
|---|---|
| **Published results** | `benchmarks/published/2026-07-13T161557-darwin-arm64-fcb910cc/` |
| **Benchmark script** | `benchmarks/scripts/run-benchmarks.sh` |
| **Configurations** | `benchmarks/configs/{sync,async,diagnostic,retention}.yaml` |
| **Commit A (framework)** | `fcb910cc` |

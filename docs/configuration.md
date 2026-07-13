# Configuration Reference

The broker can be configured using a YAML configuration file or by overriding values with environment variables.

---

## Configuration Variables Map

| YAML Key | Environment Variable | Type | Default | Description & Durability Implications |
|---|---|---|---|---|
| **`broker.grpc_address`** | `BROKER_GRPC_ADDRESS` | `string` | `localhost:50051` | Hostname and port bound by the gRPC Server (e.g. `0.0.0.0:9092`). |
| **`broker.http_address`** | `BROKER_HTTP_ADDRESS` | `string` | `localhost:9093` | Hostname and port bound by the HTTP operations server. |
| **`broker.graceful_shutdown_timeout`** | `BROKER_SHUTDOWN_TIMEOUT` | `string` | `15s` | Timeout duration limit for server shutdown. |
| **`storage.data_directory`** | `BROKER_DATA_DIRECTORY` | `string` | `./data` | File system location for storing partition logs and offsets. |
| **`storage.max_record_bytes`** | `BROKER_MAX_RECORD_BYTES` | `int` | `1048576` | Size limit in bytes for a single record payload (1 MiB). |
| **`storage.max_batch_bytes`** | `BROKER_MAX_BATCH_BYTES` | `int` | `5242880` | Size limit in bytes for a single Produce request batch (5 MiB). |
| **`storage.segment_max_bytes`** | `BROKER_SEGMENT_MAX_BYTES` | `int64` | `134217728` | Threshold size to roll an active log segment file (128 MiB). |
| **`storage.index_interval_bytes`** | `BROKER_INDEX_INTERVAL_BYTES` | `int` | `4096` | Interval of data bytes written before mapping a sparse index entry (4 KiB). |
| **`storage.flush_mode`** | `BROKER_FLUSH_MODE` | `string` | `sync` | Durability mode: `sync` triggers fsync immediately upon write acknowledgment; `async` relies on the OS page cache flush. |
| **`retention.max_age`** | `BROKER_RETENTION_MAX_AGE` | `string` | `168h` | Time duration threshold for segment age retention (168 hours). |
| **`retention.max_partition_bytes`** | `BROKER_RETENTION_MAX_PARTITION_BYTES` | `uint64` | `10737418240` | Total log size threshold limit per partition before deleting old segments (10 GiB). |
| **`retention.check_interval`** | `BROKER_RETENTION_CHECK_INTERVAL` | `string` | `5m` | Frequency interval of background retention runs (5 minutes). |
| **`observability.log_level`** | `BROKER_LOG_LEVEL` | `string` | `info` | Logging severity filter: `debug`, `info`, `warn`, `error`. |
| **`observability.log_format`** | `BROKER_LOG_FORMAT` | `string` | `json` | Logging output structure: `json` or `text`. |
| **`observability.metrics_enabled`** | `BROKER_METRICS_ENABLED` | `bool` | `true` | Exposes Prometheus metrics on `/metrics`. |
| **`observability.pprof_enabled`** | `BROKER_PPROF_ENABLED` | `bool` | `false` | Enables pprof diagnostics on `/debug/pprof/*`. |
| **`observability.mutex_profile_fraction`** | `BROKER_MUTEX_PROFILE_FRACTION` | `int` | `0` | Process-wide lock contention sampling rate (0 to disable, >0 to sample 1/N). |
| **`observability.block_profile_rate`** | `BROKER_BLOCK_PROFILE_RATE` | `int` | `0` | Process-wide block profile rate in nanoseconds (0 to disable). |

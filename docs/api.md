# gRPC API Specification

The broker exposes all controls (producing, consuming, offsets tracking, and topic administration) via standard protocol buffer APIs on port `9092`.

---

## 1. AdminService

Provides administrative functions for creating and inspecting topic partition topologies.

### CreateTopic
Creates a new topic with the specified partition count and retention constraints.
*   **Request**: `CreateTopicRequest`
    *   `name`: string (valid name formats match `^[a-zA-Z0-9._-]+$`)
    *   `partition_count`: uint32 (must be greater than 0)
    *   `retention`: `RetentionPolicy` (`max_age_seconds` and `max_bytes`)
*   **Response**: `CreateTopicResponse` containing the created `TopicMetadata`.

### ListTopics
Returns metadata for all existing topics.
*   **Request**: `ListTopicsRequest`
*   **Response**: `ListTopicsResponse` containing list of `TopicMetadata`.

### DescribeTopic
Describes the topology and segment partition metrics for a given topic.
*   **Request**: `DescribeTopicRequest`
    *   `name`: string
*   **Response**: `DescribeTopicResponse` containing `TopicMetadata`.

---

## 2. BrokerService

Main data plane service for high-throughput message publishing and consumption.

### Produce
Appends a batch of records to a partition log.
*   **Request**: `ProduceRequest`
    *   `topic`: string
    *   `partition`: uint32
    *   `records`: list of `Record` (contains `key`, `value`, `headers`, and `timestamp`)
*   **Response**: `ProduceResponse`
    *   `base_offset`: uint64 (first offset assigned to this batch)
    *   `last_offset`: uint64 (last offset assigned to this batch)
    *   `record_count`: uint32 (number of records successfully written)

### Fetch
Fetches record batches from a partition starting at a specific offset.
*   **Request**: `FetchRequest`
    *   `topic`: string
    *   `partition`: uint32
    *   `offset`: uint64
    *   `max_bytes`: uint32 (upper limit of data payload to return)
*   **Response**: `FetchResponse`
    *   `records`: list of `StoredRecord` (includes absolute log `offset`)
    *   `earliest_offset`: uint64 (earliest available offset in this partition)
    *   `log_end_offset`: uint64 (next offset to write to)

### CommitOffset
Commits a consumer group offset for a given partition.
*   **Request**: `CommitOffsetRequest`
    *   `consumer_group`: string
    *   `topic`: string
    *   `partition`: uint32
    *   `next_offset`: uint64 (the next offset to consume)
*   **Response**: `CommitOffsetResponse`

### FetchCommittedOffset
Queries the committed offset of a consumer group.
*   **Request**: `FetchCommittedOffsetRequest`
    *   `consumer_group`: string
    *   `topic`: string
    *   `partition`: uint32
*   **Response**: `FetchCommittedOffsetResponse`
    *   `found`: bool (whether a committed offset exists)
    *   `next_offset`: uint64

---

## 3. gRPC Error Mapping Invariants

Handlers map service failures to standard gRPC error status codes:

| gRPC Status Code | Root Cause Condition |
|---|---|
| **`InvalidArgument`** | Request contains malformed parameters (empty topic name, invalid characters, negative partition IDs). |
| **`NotFound`** | Request targets a topic or partition that does not exist on this broker. |
| **`OutOfRange`** | Fetch request specifies a starting offset larger than the current partition log-end-offset. |
| **`ResourceExhausted`** | Produce request size or single record payload exceeds configuration limits (`max_record_bytes`, `max_batch_bytes`). |
| **`Unavailable`** | Request is rejected because the broker is shutting down or offset store database is closed. |
| **`Internal`** | File system disk space exhaustion, index write errors, or CRC checksum validation failure. |

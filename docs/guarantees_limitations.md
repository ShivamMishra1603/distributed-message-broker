# Delivery Guarantees and Design Limitations

This document specifies the exact delivery, durability, and availability guarantees of the Distributed Message Broker, alongside current version limitations.

---

## 1. Guarantees

### Message Ordering
- **Partition Order**: Message order is guaranteed strictly **within a single partition**. Messages written to a partition are assigned sequential offsets and delivered to consumers in the exact order they were appended.
- **No Global Ordering**: There is no ordering guarantee across different partitions of the same topic or across different topics.

### Message Delivery
- **At-Least-Once Processing**: The broker supports at-least-once message processing. Consumers manually fetch records, process them, and then commit the next offset back to the broker.
- **Duplicate Delivery**: If a consumer successfully fetches and processes messages but crashes before committing the offsets, the next consumer instance starting up will fetch the same records again, resulting in duplicate delivery. Consumers must implement idempotent processing where necessary.

### Durability
- **Sync Durability**: In `sync` flush mode, the broker triggers a physical `fsync` on the segment file descriptor before returning the gRPC Produce acknowledgment. Once acknowledged, the records are guaranteed to survive host crash and power failure.
- **Async Durability**: In `async` flush mode, the broker acknowledges the write as soon as it is written to the file descriptor (OS page cache). If the broker process crashes, the data is preserved by the OS. However, if the machine suffers a power loss or kernel panic before the OS flushes the page cache to disk, recently acknowledged records may be lost.

### Consumer Offset Storage
- **Durable Offsets**: Consumer committed offsets are persisted in a dedicated, broker-managed append-only consumer-offset log. Committed offsets survive broker restarts.

---

## 2. Limitations (Version 1 Scope)

### Single-Node Availability
- **No Clustering/Replication**: Version 1 runs as a single-node process. There is no replica synchronization, partition leader election, or automatic failover. If the host machine fails, the broker becomes unavailable.

### Manual Partition Assignment
- **No Consumer Groups Coordinator**: There is no active group coordinator service. Partition assignment among consumers is manual. The broker does not execute automatic partition rebalancing, consumer heartbeats, or dynamic group membership protocols.

### Security
- **No Transport Security**: Version 1 communicates via unencrypted TCP/gRPC streams. It does not natively support TLS, client SASL/SCRAM authentication, or Access Control List (ACL) authorization. Deployments requiring security must use secure private networks or VPN overlays.

# Storage Format Specification

This document details the binary representation, serialization framing, index files, directory structure, and checksum boundaries for both partition log records and the consumer offset store.

---

## 1. Directory Structure & Naming Conventions

All data resides in the configured `storage.data_directory`.

### Topics and Partitions
Each topic partition is represented as a dedicated directory named:
```
<topic_name>-<partition_id>/
```
Example filesystem layout for a topic `orders` with 2 partitions:
```
/var/lib/broker/
  ├── orders-0/
  │    ├── 00000000000000000000.log      # Segment log file
  │    └── 00000000000000000000.index    # Sparse index file
  ├── orders-1/
  │    ├── 00000000000000000000.log
  │    └── 00000000000000000000.index
  └── consumer-offsets.log               # Broker-managed consumer-offset log
```

### Segment Filenames
Segment log and index files are named with the **base offset** left-padded with zeroes to 20 digits (e.g. `00000000000000010000.log` for base offset 10000).

---

## 2. Partition Log Record Batch Format

Log segments store records in binary serialized batches. A single batch contains a 33-byte header followed by contiguous record payloads.

### Batch Header (33 bytes)

| Byte Range | Field Name | Type | Description |
|---|---|---|---|
| **0 – 3** | Magic Bytes | `[4]byte` | Static framing bytes: `0x50, 0x47, 0x4C, 0x47` ("PGLG"). |
| **4** | Format Version | `uint8` | Version number (`1`). |
| **5 – 8** | Batch Length | `uint32` | Size in bytes of everything after this field (Total size - 9). |
| **9 – 12** | CRC32 Checksum | `uint32` | IEEE CRC32 checksum covering bytes 13 to end. Excludes magic, version, and length fields. |
| **13 – 20** | Base Offset | `uint64` | The absolute offset assigned to the first record in the batch. |
| **21 – 24** | Record Count | `uint32` | Total number of records packed in this batch. |
| **25 – 32** | Max Timestamp | `int64` | Maximum Unix epoch timestamp (milliseconds) across all records in the batch. |

### Record Encoding (Variable Length)
Immediately following the 33-byte batch header are the encoded records:

| Field Name | Type | Size | Description |
|---|---|---|---|
| **Offset Delta** | `uint32` | 4 bytes | Relative offset difference from the batch Base Offset (`Offset - BaseOffset`). |
| **Timestamp Delta** | `uint64` | 8 bytes | Timestamp difference relative to Max Timestamp (`MaxTimestamp - RecordTimestamp`). |
| **Key Length** | `uint32` | 4 bytes | Byte size of the key payload. Represented as `0xFFFFFFFF` (-1) if key is `nil`. |
| **Key Payload** | `[]byte` | Variable | Raw key data bytes (omitted if key is `nil` or empty). |
| **Value Length** | `uint32` | 4 bytes | Byte size of the value payload. Represented as `0xFFFFFFFF` (-1) if value is `nil`. |
| **Value Payload** | `[]byte` | Variable | Raw value data bytes (omitted if value is `nil` or empty). |
| **Header Count** | `uint32` | 4 bytes | Number of key-value header tags. |
| **Headers** | List | Variable | Sequential list of headers, each serialized as: <br>• Key Length (`uint32`, 4 bytes)<br>• Key String (`[]byte`, variable)<br>• Value Length (`uint32`, 4 bytes)<br>• Value String (`[]byte`, variable) |

### Key & Value Null vs Empty Semantics
- **Length = `-1` (encoded as `0xFFFFFFFF`)**: Represents a `nil` field.
- **Length = `0`**: Represents a non-nil empty field (`[]byte{}`).
- **Length > `0`**: Length bytes are read, and payload is extracted.

---

## 3. Sparse Index Entry Layout

Index files (`.index`) consist of fixed-size 12-byte entries. A new entry is appended every time segment appends cross the configured `index_interval_bytes`.

| Byte Range | Field Name | Type | Description |
|---|---|---|---|
| **0 – 3** | Relative Offset | `uint32` | Offset delta relative to the segment's base offset (`Offset - BaseOffset`). |
| **4 – 11** | Physical Position | `uint64` | Physical byte position where the batch starts inside the `.log` file. |

---

## 4. Consumer Offset Log Format

The broker-managed append-only consumer-offset log is stored in a single file named `consumer-offsets.log` at the root of the data directory. It uses a custom binary framing layout.

### Offset Entry Format

| Byte Range | Field Name | Type | Description |
|---|---|---|---|
| **0 – 3** | Magic Bytes | `[4]byte` | Static framing bytes: `OFFS`. |
| **4** | Version | `uint8` | Version byte (`0x01`). |
| **5 – 8** | Entry Length | `uint32` | Length in bytes of the payload portion (28 + gl + tl). |
| **9 – 12** | CRC32 Checksum | `uint32` | IEEE CRC32 checksum covering the payload portion only (from byte 13 to end). |
| **13 – 16** | Group Length (`gl`) | `uint32` | Byte size of the consumer group string. |
| **17 – (17+gl-1)**| Consumer Group | `string` | Consumer group identifier. |
| **(17+gl) – (17+gl+3)**| Topic Length (`tl`) | `uint32` | Byte size of the topic name string. |
| **(21+gl) – (21+gl+tl-1)**| Topic Name | `string` | Topic identifier. |
| **(21+gl+tl) – (21+gl+tl+3)**| Partition ID | `uint32` | Bounded partition number. |
| **(25+gl+tl) – (25+gl+tl+7)**| Committed Offset | `uint64` | Next offset to be read by the consumer. |
| **(33+gl+tl) – (33+gl+tl+7)**| Commit Timestamp | `uint64` | Unix epoch millisecond timestamp of the commit event. |

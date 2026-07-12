package model

import "time"

// Header is a key-value metadata pair.
type Header struct {
	Key   string
	Value []byte
}

// Record represents a record produced by a client.
type Record struct {
	Key       []byte
	Value     []byte
	Headers   []Header
	Timestamp int64
}

// StoredRecord represents a record stored on the broker with assigned offset and timestamp.
type StoredRecord struct {
	Offset    uint64
	Timestamp int64
	Key       []byte
	Value     []byte
	Headers   []Header
}

// Clock defines a function interface for retrieving current time (used for testing).
type Clock func() time.Time

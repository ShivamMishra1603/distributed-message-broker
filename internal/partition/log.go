package partition

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var ErrOffsetOutOfRange = errors.New("offset out of range")

// Header represents a key-value pair.
type Header struct {
	Key   string
	Value []byte
}

// Record is the input record structure.
type Record struct {
	Key       []byte
	Value     []byte
	Headers   []Header
	Timestamp int64
}

// StoredRecord is the stored record structure with assigned offset and timestamp.
type StoredRecord struct {
	Offset    uint64
	Timestamp int64
	Key       []byte
	Value     []byte
	Headers   []Header
}

// Clock provides timestamp injection.
type Clock func() time.Time

// Log is an in-memory append-only partition log.
type Log struct {
	mu             sync.RWMutex
	records        []StoredRecord
	earliestOffset uint64
	logEndOffset   uint64
	clock          Clock
	topic          string
	partitionID    uint32
}

// NewLog constructs a new in-memory Partition Log.
func NewLog(topic string, partitionID uint32, clock Clock) *Log {
	if clock == nil {
		clock = func() time.Time { return time.Now().UTC() }
	}
	return &Log{
		clock:          clock,
		topic:          topic,
		partitionID:    partitionID,
		earliestOffset: 0,
		logEndOffset:   0,
		records:        make([]StoredRecord, 0),
	}
}

// RecordSize calculates the size of a StoredRecord in bytes.
func RecordSize(record StoredRecord) int {
	size := len(record.Key) + len(record.Value)
	for _, h := range record.Headers {
		size += len(h.Key) + len(h.Value)
	}
	// Fixed overhead: 8 bytes for offset, 8 bytes for timestamp
	size += 16
	return size
}

// Append assigns sequential offsets and broker timestamps to records,
// deep-copies all slices, and appends them to the in-memory log.
func (l *Log) Append(records []Record) (baseOffset, lastOffset uint64, err error) {
	if len(records) == 0 {
		return 0, 0, fmt.Errorf("cannot append empty batch")
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	base := l.logEndOffset
	timestamp := l.clock().UTC().UnixMilli()

	for i, rec := range records {
		offset := base + uint64(i)

		// Perform deep copies
		keyCopy := make([]byte, len(rec.Key))
		copy(keyCopy, rec.Key)

		valueCopy := make([]byte, len(rec.Value))
		copy(valueCopy, rec.Value)

		headersCopy := make([]Header, len(rec.Headers))
		for j, h := range rec.Headers {
			valCopy := make([]byte, len(h.Value))
			copy(valCopy, h.Value)
			headersCopy[j] = Header{
				Key:   h.Key,
				Value: valCopy,
			}
		}

		stored := StoredRecord{
			Offset:    offset,
			Timestamp: timestamp,
			Key:       keyCopy,
			Value:     valueCopy,
			Headers:   headersCopy,
		}

		l.records = append(l.records, stored)
	}

	l.logEndOffset = base + uint64(len(records))
	return base, l.logEndOffset - 1, nil
}

// Read returns records starting from the requested offset.
// It stops returning records before exceeding maxBytes, unless the first record
// itself is larger than maxBytes, in which case it returns just that record to ensure progress.
func (l *Log) Read(offset uint64, maxBytes int) ([]StoredRecord, error) {
	if maxBytes <= 0 {
		return nil, fmt.Errorf("maxBytes must be greater than zero, got %d", maxBytes)
	}

	l.mu.RLock()
	defer l.mu.RUnlock()

	if offset < l.earliestOffset || offset > l.logEndOffset {
		return nil, ErrOffsetOutOfRange
	}

	if offset == l.logEndOffset {
		return []StoredRecord{}, nil
	}

	var result []StoredRecord
	accumulatedBytes := 0

	startIndex := int(offset - l.earliestOffset)
	for i := startIndex; i < len(l.records); i++ {
		rec := l.records[i]
		size := RecordSize(rec)

		// Stop before exceeding maxBytes, unless it is the very first record.
		if len(result) > 0 && accumulatedBytes+size > maxBytes {
			break
		}

		result = append(result, l.deepCopyStoredRecord(rec))
		accumulatedBytes += size
	}

	return result, nil
}

// LogEndOffset returns the current next offset to assign.
func (l *Log) LogEndOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.logEndOffset
}

// EarliestOffset returns the current earliest offset.
func (l *Log) EarliestOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.earliestOffset
}

func (l *Log) deepCopyStoredRecord(rec StoredRecord) StoredRecord {
	keyCopy := make([]byte, len(rec.Key))
	copy(keyCopy, rec.Key)

	valueCopy := make([]byte, len(rec.Value))
	copy(valueCopy, rec.Value)

	headersCopy := make([]Header, len(rec.Headers))
	for j, h := range rec.Headers {
		valCopy := make([]byte, len(h.Value))
		copy(valCopy, h.Value)
		headersCopy[j] = Header{
			Key:   h.Key,
			Value: valCopy,
		}
	}

	return StoredRecord{
		Offset:    rec.Offset,
		Timestamp: rec.Timestamp,
		Key:       keyCopy,
		Value:     valueCopy,
		Headers:   headersCopy,
	}
}

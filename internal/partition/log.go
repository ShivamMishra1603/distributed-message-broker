package partition

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/ShivamMishra1603/distributed-message-broker/internal/model"
	"github.com/ShivamMishra1603/distributed-message-broker/internal/storage"
)

// Clock is a re-export of model.Clock for convenience.
type Clock = model.Clock

// Log wraps a durable segmented partition store.
type Log struct {
	store *storage.Store
}

// NewLog constructs a partition log that delegates all operations to storage.Store.
func NewLog(topic string, partitionID uint32, dataDir string, segmentMaxBytes int64, maxBatchBytes int64, indexIntervalBytes int, flushMode string, clock Clock, observer storage.Observer) (*Log, error) {
	dir := filepath.Join(dataDir, "topics", topic, fmt.Sprintf("partition-%d", partitionID))
	store, err := storage.OpenStore(dir, segmentMaxBytes, maxBatchBytes, indexIntervalBytes, flushMode, clock, observer)
	if err != nil {
		return nil, fmt.Errorf("failed to open partition store for topic %q partition %d: %w", topic, partitionID, err)
	}

	return &Log{
		store: store,
	}, nil
}

// Append forwards record list to underlying store.
func (l *Log) Append(records []model.Record) (baseOffset, lastOffset uint64, err error) {
	return l.store.Append(records)
}

// Read reads matching records under byte limits from store.
func (l *Log) Read(offset uint64, maxBytes int) ([]model.StoredRecord, error) {
	recs, err := l.store.Read(offset, maxBytes)
	if err != nil {
		if err == storage.ErrOffsetOutOfRange {
			return nil, storage.ErrOffsetOutOfRange
		}
		return nil, err
	}
	return recs, nil
}

// LogEndOffset returns next offset to assign.
func (l *Log) LogEndOffset() uint64 {
	return l.store.LogEndOffset()
}

// EarliestOffset returns earliest available offset.
func (l *Log) EarliestOffset() uint64 {
	return l.store.EarliestOffset()
}

// Close gracefully closes the underlying store files.
func (l *Log) Close() error {
	return l.store.Close()
}

// ApplyRetention deletes expired log segments based on age or size configuration limits.
func (l *Log) ApplyRetention(maxAge time.Duration, maxPartitionBytes uint64) error {
	return l.store.ApplyRetention(maxAge, maxPartitionBytes)
}

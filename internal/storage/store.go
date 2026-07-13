package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ShivamMishra1603/distributed-message-broker/internal/model"
)

var (
	ErrStoreClosed         = errors.New("store is closed")
	ErrOffsetOutOfRange    = errors.New("offset out of range")
	ErrBatchTooLarge       = errors.New("batch size exceeds maximum configured limit")
	ErrStoreCorrupt        = errors.New("store is in a corrupted state due to a previous I/O failure")
	ErrIndexOffsetOverflow = errors.New("index relative offset overflows uint32 limit")
)

type BatchLocation struct {
	BaseOffset uint64
	LastOffset uint64
	Position   int64
	Length     uint32
	SegmentRef *Segment
}

type Store struct {
	dir                string
	segmentMaxBytes    int64
	maxBatchBytes      int64
	indexIntervalBytes int
	flushMode          string
	clock              model.Clock

	mu             sync.RWMutex
	closed         bool
	writeFailed    bool
	segments       []*Segment
	activeSegment  *Segment
	logEndOffset   uint64
	earliestOffset uint64
}

// OpenStore opens a partition store directory, scans and validates all log files,
// reconstructs offset states, validates/rebuilds sparse indexes, and opens the active segment.
func OpenStore(dir string, segmentMaxBytes int64, maxBatchBytes int64, indexIntervalBytes int, flushMode string, clock model.Clock) (*Store, error) {
	if clock == nil {
		clock = func() time.Time { return time.Now().UTC() }
	}

	// Create directory if not exists
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create partition directory %q: %w", dir, err)
	}

	// Remove any abandoned index temp files
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read partition directory %q: %w", dir, err)
	}
	for _, f := range files {
		if !f.IsDir() && strings.HasSuffix(f.Name(), ".index.tmp") {
			_ = os.Remove(filepath.Join(dir, f.Name()))
		}
	}

	var baseOffsets []uint64
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		name := f.Name()
		if strings.HasSuffix(name, ".log") {
			prefix := strings.TrimSuffix(name, ".log")
			if len(prefix) != 20 {
				continue // ignore non-matching formats
			}
			val, err := strconv.ParseUint(prefix, 10, 64)
			if err != nil {
				continue // ignore parse errors
			}
			baseOffsets = append(baseOffsets, val)
		}
	}

	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	store := &Store{
		dir:                dir,
		segmentMaxBytes:    segmentMaxBytes,
		maxBatchBytes:      maxBatchBytes,
		indexIntervalBytes: indexIntervalBytes,
		flushMode:          strings.ToLower(flushMode),
		clock:              clock,
		segments:           make([]*Segment, 0),
		earliestOffset:     0,
		logEndOffset:       0,
	}

	// Load and validate segments
	expectedNextOffset := uint64(0)
	if len(baseOffsets) > 0 {
		expectedNextOffset = baseOffsets[0]
		store.earliestOffset = baseOffsets[0]
	}

	for i, base := range baseOffsets {
		if base != expectedNextOffset {
			store.closeAllOpenSegments()
			return nil, fmt.Errorf("offset discontinuity: expected segment base %d, got %d", expectedNextOffset, base)
		}

		isActive := (i == len(baseOffsets)-1)
		seg, err := NewSegment(dir, base, isActive)
		if err != nil {
			store.closeAllOpenSegments()
			return nil, err
		}
		store.segments = append(store.segments, seg)

		// Recover segment (truncating active tail if needed) and check index
		nextOffset, err := store.recoverSegment(seg, isActive)
		if err != nil {
			store.closeAllOpenSegments()
			return nil, err
		}
		seg.nextOffset = nextOffset
		expectedNextOffset = nextOffset
	}

	store.logEndOffset = expectedNextOffset

	// Ensure there is at least one active segment
	if len(store.segments) == 0 {
		seg, err := NewSegment(dir, 0, true)
		if err != nil {
			return nil, err
		}
		store.segments = append(store.segments, seg)
		store.activeSegment = seg
		store.logEndOffset = 0
		store.earliestOffset = 0
	} else {
		store.activeSegment = store.segments[len(store.segments)-1]
	}

	return store, nil
}

// Append writes a batch of records. Implements sync/async flush, rollover, and offset assignments.
func (s *Store) Append(records []model.Record) (baseOffset, lastOffset uint64, err error) {
	if len(records) == 0 {
		return 0, 0, fmt.Errorf("cannot append empty batch")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return 0, 0, ErrStoreClosed
	}

	if s.writeFailed {
		return 0, 0, ErrStoreCorrupt
	}

	timestamp := s.clock().UTC().UnixMilli()
	base := s.logEndOffset

	// 1. Convert client records to stored records with offsets
	storedRecs := make([]model.StoredRecord, len(records))
	for i, r := range records {
		storedRecs[i] = model.StoredRecord{
			Offset:    base + uint64(i),
			Timestamp: timestamp,
			Key:       r.Key,
			Value:     r.Value,
			Headers:   r.Headers,
		}
	}

	// 2. Encode batch
	encoded, err := EncodeBatch(storedRecs)
	if err != nil {
		return 0, 0, err
	}

	batchSize := int64(len(encoded))
	if batchSize > s.maxBatchBytes {
		return 0, 0, ErrBatchTooLarge
	}

	// 3. Roll segment if boundary exceeded
	if s.activeSegment.Size() > 0 && s.activeSegment.Size()+batchSize > s.segmentMaxBytes {
		if err := s.activeSegment.Flush(); err != nil {
			s.writeFailed = true
			return 0, 0, err
		}
		s.activeSegment.active = false

		newSeg, err := NewSegment(s.dir, s.logEndOffset, true)
		if err != nil {
			s.writeFailed = true
			return 0, 0, err
		}
		s.segments = append(s.segments, newSeg)
		s.activeSegment = newSeg
	}

	// Verify Relative Offset limit (recomputed after rollover)
	relative := base - s.activeSegment.baseOffset
	if relative > math.MaxUint32 {
		s.writeFailed = true
		return 0, 0, ErrIndexOffsetOverflow
	}

	// 4. Append to active segment
	pos, err := s.activeSegment.Append(encoded)
	if err != nil {
		s.writeFailed = true
		return 0, 0, err
	}

	for _, r := range records {
		if r.Timestamp > s.activeSegment.maxTimestamp {
			s.activeSegment.maxTimestamp = r.Timestamp
		}
	}

	last := base + uint64(len(records)) - 1
	s.logEndOffset = last + 1
	s.activeSegment.nextOffset = s.logEndOffset

	// 5. Append index entry if required
	shouldIndex := len(s.activeSegment.indexEntries) == 0 ||
		pos-s.activeSegment.lastIndexedPosition >= int64(s.indexIntervalBytes)

	if shouldIndex {
		entry := IndexEntry{
			RelativeOffset: uint32(relative),
			Position:       uint64(pos),
		}
		_ = s.activeSegment.AppendIndexEntry(entry)
		// Derived index write failures do not fail the produce or reuse offsets.
		// Segment.AppendIndexEntry already updates s.indexEntries and s.indexDirty.
	}

	// 6. Apply Flush Policy
	if s.flushMode == "sync" {
		if err := s.activeSegment.Flush(); err != nil {
			s.writeFailed = true
			return 0, 0, err
		}
	}

	return base, last, nil
}

// Read reads records starting from the requested offset up to maxBytes.
func (s *Store) Read(offset uint64, maxBytes int) ([]model.StoredRecord, error) {
	if maxBytes <= 0 {
		return nil, fmt.Errorf("maxBytes must be greater than zero, got %d", maxBytes)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, ErrStoreClosed
	}

	if s.writeFailed {
		return nil, ErrStoreCorrupt
	}

	if offset < s.earliestOffset || offset > s.logEndOffset {
		return nil, ErrOffsetOutOfRange
	}

	if offset == s.logEndOffset {
		return []model.StoredRecord{}, nil
	}

	// 1. Locate starting segment
	segIdx := sort.Search(len(s.segments), func(i int) bool {
		return s.segments[i].nextOffset > offset
	})
	if segIdx >= len(s.segments) {
		return nil, ErrOffsetOutOfRange
	}

	var result []model.StoredRecord
	accumulatedBytes := 0
	currOffset := offset

	// 2. Traverse segments sequentially (cross-segment reads)
	for i := segIdx; i < len(s.segments); i++ {
		seg := s.segments[i]

		// Binary search inside segment index to locate closest starting position
		idx := sort.Search(len(seg.indexEntries), func(j int) bool {
			return seg.baseOffset+uint64(seg.indexEntries[j].RelativeOffset) > currOffset
		})

		startPos := int64(0)
		if idx > 0 {
			startPos = int64(seg.indexEntries[idx-1].Position)
		}

		// Scan forward from indexed file position
		for startPos < seg.Size() {
			if startPos+9 > seg.Size() {
				return nil, fmt.Errorf("truncated batch header at position %d inside segment %q", startPos, seg.path)
			}

			prefixBuf := make([]byte, 9)
			if _, err := seg.ReadAt(prefixBuf, startPos); err != nil {
				return nil, err
			}

			batchLength := binary.BigEndian.Uint32(prefixBuf[5:9])
			totalBatchSize := int64(9) + int64(batchLength)

			if startPos+totalBatchSize > seg.Size() {
				return nil, fmt.Errorf("truncated batch payload at position %d inside segment %q", startPos, seg.path)
			}

			batchBuf := make([]byte, totalBatchSize)
			if _, err := seg.ReadAt(batchBuf, startPos); err != nil {
				return nil, err
			}

			recs, err := DecodeBatch(batchBuf)
			if err != nil {
				return nil, fmt.Errorf("failed to decode batch at file position %d inside segment %q: %w", startPos, seg.path, err)
			}

			for _, r := range recs {
				if r.Offset < currOffset {
					continue
				}

				size := recordSize(r)

				// Check limit boundaries (Milestone 2 rule: return first record anyway to guarantee progress)
				if len(result) > 0 && accumulatedBytes+size > maxBytes {
					return result, nil
				}

				result = append(result, r)
				accumulatedBytes += size
				currOffset = r.Offset + 1
			}

			startPos += totalBatchSize
		}
	}

	return result, nil
}

// Close gracefully flushes and closes all descriptors.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true

	var errs []string
	if s.activeSegment != nil {
		if err := s.activeSegment.Flush(); err != nil {
			errs = append(errs, err.Error())
		}
	}

	for _, seg := range s.segments {
		if err := seg.Close(); err != nil {
			errs = append(errs, err.Error())
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing store: %s", strings.Join(errs, "; "))
	}
	return nil
}

// LogEndOffset returns next offset to assign.
func (s *Store) LogEndOffset() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.logEndOffset
}

// EarliestOffset returns earliest available offset.
func (s *Store) EarliestOffset() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.earliestOffset
}

func (s *Store) recoverSegment(seg *Segment, active bool) (uint64, error) {
	var currentPos int64 = 0
	expectedOffset := seg.baseOffset
	var validSize int64 = 0

	for currentPos < seg.size {
		// EOF check inside prefix
		if currentPos+9 > seg.size {
			if active {
				// Incomplete prefix tail truncation
				if err := s.truncateLog(seg, validSize); err != nil {
					return 0, err
				}
				break
			}
			return 0, fmt.Errorf("incomplete batch header in closed segment %q", seg.path)
		}

		prefixBuf := make([]byte, 9)
		if _, err := seg.ReadAt(prefixBuf, currentPos); err != nil {
			return 0, err
		}

		// Verify Magic
		var magic [4]byte
		copy(magic[:], prefixBuf[0:4])
		if magic != MagicBytes {
			return 0, fmt.Errorf("magic byte mismatch inside %q: %w", seg.path, ErrMalformedMagic)
		}

		// Verify Version
		version := prefixBuf[4]
		if version != FormatVersion1 {
			return 0, fmt.Errorf("unsupported format version inside %q: %w", seg.path, ErrUnsupportedVersion)
		}

		batchLength := binary.BigEndian.Uint32(prefixBuf[5:9])
		if batchLength < MinimumBatchRemainder {
			return 0, fmt.Errorf("%w: batch length %d too small in segment %q", ErrCorruptBatch, batchLength, seg.path)
		}

		totalBatchSize := int64(MagicSize+VersionSize+BatchLengthSize) + int64(batchLength)
		if totalBatchSize > s.maxBatchBytes {
			return 0, fmt.Errorf("batch size %d exceeds config limits %d: %w in %q", totalBatchSize, s.maxBatchBytes, ErrBatchTooLarge, seg.path)
		}

		// Incomplete batch payload truncation at EOF
		if currentPos+totalBatchSize > seg.size {
			if active {
				if err := s.truncateLog(seg, validSize); err != nil {
					return 0, err
				}
				break
			}
			return 0, fmt.Errorf("incomplete batch payload inside closed segment %q", seg.path)
		}

		// Read complete batch payload
		batchBuf := make([]byte, totalBatchSize)
		if _, err := seg.ReadAt(batchBuf, currentPos); err != nil {
			return 0, err
		}

		// Decode batch to validate CRC and details
		recs, err := DecodeBatch(batchBuf)
		if err != nil {
			return 0, fmt.Errorf("corrupt batch decode failed inside %q: %w", seg.path, err)
		}

		if len(recs) == 0 {
			return 0, fmt.Errorf("segment %q contains empty record batch", seg.path)
		}

		// Verify offset continuity
		if recs[0].Offset != expectedOffset {
			return 0, fmt.Errorf("offset discontinuity in %q: expected offset %d, got %d", seg.path, expectedOffset, recs[0].Offset)
		}

		for j, r := range recs {
			expected := expectedOffset + uint64(j)
			if r.Offset != expected {
				return 0, fmt.Errorf("offset discontinuity inside batch in %q: expected %d, got %d", seg.path, expected, r.Offset)
			}
			if r.Timestamp > seg.maxTimestamp {
				seg.maxTimestamp = r.Timestamp
			}
		}

		expectedOffset = expectedOffset + uint64(len(recs))
		currentPos += totalBatchSize
		validSize = currentPos
	}

	// Verify loaded index file validity
	indexValid := false
	entries, err := seg.ReadIndexEntries()
	if err == nil {
		indexValid = s.validateIndexEntries(seg, entries, validSize)
	}

	if indexValid {
		seg.indexEntries = entries
		if len(entries) > 0 {
			seg.lastIndexedPosition = int64(entries[len(entries)-1].Position)
		}
	} else {
		// Rebuild index file atomically
		if err := s.rebuildIndex(seg, validSize); err != nil {
			return 0, err
		}
	}

	return expectedOffset, nil
}

func (s *Store) validateIndexEntries(seg *Segment, entries []IndexEntry, validLogSize int64) bool {
	if len(entries) == 0 {
		return validLogSize == 0 // empty segment has empty index
	}

	// First entry must be {0, 0}
	if entries[0].RelativeOffset != 0 || entries[0].Position != 0 {
		return false
	}

	var lastRel uint32 = 0
	var lastPos uint64 = 0

	for i, entry := range entries {
		if i > 0 {
			if entry.RelativeOffset <= lastRel {
				return false
			}
			if entry.Position <= lastPos {
				return false
			}
		}

		if int64(entry.Position) >= validLogSize {
			return false // points past log file size
		}

		// Read batch prefix at position to verify it matches absolute offset
		prefixBuf := make([]byte, 9)
		if _, err := seg.ReadAt(prefixBuf, int64(entry.Position)); err != nil {
			return false
		}

		// Verify Magic
		var magic [4]byte
		copy(magic[:], prefixBuf[0:4])
		if magic != MagicBytes {
			return false
		}

		// Verify expected absolute offset
		expectedAbsOffset := seg.baseOffset + uint64(entry.RelativeOffset)
		// Fetch batch details from log position to verify expectedAbsOffset matches actual batch Base Offset
		if int64(entry.Position)+33 > validLogSize {
			return false
		}
		headerBuf := make([]byte, 33)
		if _, err := seg.ReadAt(headerBuf, int64(entry.Position)); err != nil {
			return false
		}
		batchBaseOffset := binary.BigEndian.Uint64(headerBuf[13:21])
		if batchBaseOffset != expectedAbsOffset {
			return false
		}

		lastRel = entry.RelativeOffset
		lastPos = entry.Position
	}

	return true
}

func (s *Store) rebuildIndex(seg *Segment, validLogSize int64) error {
	tmpPath := seg.indexPath + ".tmp"
	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	var indexEntries []IndexEntry
	var lastIndexedPosition int64 = -1
	var currentPos int64 = 0

	for currentPos < validLogSize {
		// Read batch header details to extract offsets
		headerBuf := make([]byte, 33)
		if _, err := seg.ReadAt(headerBuf, currentPos); err != nil {
			tmpFile.Close()
			os.Remove(tmpPath)
			return err
		}

		batchBaseOffset := binary.BigEndian.Uint64(headerBuf[13:21])
		batchLength := binary.BigEndian.Uint32(headerBuf[5:9])
		totalBatchSize := int64(9) + int64(batchLength)

		batchMaxTimestamp := int64(binary.BigEndian.Uint64(headerBuf[25:33]))
		if batchMaxTimestamp > seg.maxTimestamp {
			seg.maxTimestamp = batchMaxTimestamp
		}

		relative := batchBaseOffset - seg.baseOffset
		if relative > math.MaxUint32 {
			tmpFile.Close()
			os.Remove(tmpPath)
			return ErrIndexOffsetOverflow
		}

		shouldIndex := len(indexEntries) == 0 ||
			currentPos-lastIndexedPosition >= int64(s.indexIntervalBytes)

		if shouldIndex {
			entry := IndexEntry{
				RelativeOffset: uint32(relative),
				Position:       uint64(currentPos),
			}

			buf := make([]byte, 12)
			binary.BigEndian.PutUint32(buf[0:4], entry.RelativeOffset)
			binary.BigEndian.PutUint64(buf[4:12], entry.Position)

			n, err := tmpFile.Write(buf)
			if err != nil {
				tmpFile.Close()
				os.Remove(tmpPath)
				return err
			}
			if n != 12 {
				tmpFile.Close()
				os.Remove(tmpPath)
				return io.ErrShortWrite
			}

			indexEntries = append(indexEntries, entry)
			lastIndexedPosition = currentPos
		}

		currentPos += totalBatchSize
	}

	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return err
	}

	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}

	// Close old index descriptor and replace it
	seg.indexFile.Close()

	if err := os.Rename(tmpPath, seg.indexPath); err != nil {
		os.Remove(tmpPath)
		return err
	}

	// Open new index file handle
	newIdxFile, err := os.OpenFile(seg.indexPath, os.O_RDWR, 0640)
	if err != nil {
		return err
	}

	seg.indexFile = newIdxFile
	seg.indexEntries = indexEntries
	seg.lastIndexedPosition = lastIndexedPosition
	seg.indexDirty = false
	seg.indexSize = int64(len(indexEntries) * 12)

	// Sync partition directory to commit updates
	return syncDirectory(s.dir)
}

func (s *Store) truncateLog(seg *Segment, validSize int64) error {
	if err := seg.file.Truncate(validSize); err != nil {
		return fmt.Errorf("failed to truncate active log file %q to size %d: %w", seg.path, validSize, err)
	}
	seg.size = validSize
	return nil
}

func (s *Store) closeAllOpenSegments() {
	for _, seg := range s.segments {
		seg.Close()
	}
	s.segments = nil
}

func syncDirectory(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func recordSize(record model.StoredRecord) int {
	size := len(record.Key) + len(record.Value)
	for _, h := range record.Headers {
		size += len(h.Key) + len(h.Value)
	}
	size += 16 // 8 byte offset + 8 byte timestamp
	return size
}

// ApplyRetention deletes expired closed segments by age first, then by size.
// It executes under s.mu.Lock().
func (s *Store) ApplyRetention(maxAge time.Duration, maxPartitionBytes uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed || s.writeFailed {
		return nil
	}

	var cleanupErrors []error

	// 1. Time-Based retention: delete all expired closed segments by age, oldest first
	cutoff := s.clock().UTC().Add(-maxAge).UnixMilli()

	for {
		if len(s.segments) <= 1 {
			break // only active segment remains
		}
		oldest := s.segments[0]
		if oldest == s.activeSegment {
			break
		}

		if oldest.MaxTimestamp() <= cutoff {
			if err := s.deleteSegmentAtIndex(0); err != nil {
				cleanupErrors = append(cleanupErrors, err)
				break
			}
		} else {
			break
		}
	}

	// 2. Size-Based retention: delete additional oldest closed segments
	for {
		if len(s.segments) <= 1 {
			break
		}
		totalBytes, err := s.calculateTotalBytes()
		if err != nil {
			cleanupErrors = append(cleanupErrors, err)
			break
		}

		if totalBytes <= maxPartitionBytes {
			break
		}

		oldest := s.segments[0]
		if oldest == s.activeSegment {
			break // never delete the active segment
		}

		if err := s.deleteSegmentAtIndex(0); err != nil {
			cleanupErrors = append(cleanupErrors, err)
			break
		}
	}

	if len(cleanupErrors) > 0 {
		return fmt.Errorf("errors during retention cleanup: %w", errors.Join(cleanupErrors...))
	}
	return nil
}

func (s *Store) calculateTotalBytes() (uint64, error) {
	var total uint64
	for _, seg := range s.segments {
		logInfo, err := os.Stat(seg.path)
		if err != nil {
			return 0, fmt.Errorf("failed to stat segment log %q: %w", seg.path, err)
		}
		indexInfo, err := os.Stat(seg.indexPath)
		if err != nil {
			return 0, fmt.Errorf("failed to stat segment index %q: %w", seg.indexPath, err)
		}
		total += uint64(logInfo.Size() + indexInfo.Size())
	}
	return total, nil
}

func (s *Store) deleteSegmentAtIndex(idx int) error {
	seg := s.segments[idx]

	// 1. Attempt to remove the authoritative log file first
	if err := os.Remove(seg.path); err != nil {
		return fmt.Errorf("failed to delete authoritative log file %q: %w", seg.path, err)
	}

	// 2. Immediately remove the segment from the in-memory view and update earliest offset
	copy(s.segments[idx:], s.segments[idx+1:])
	s.segments[len(s.segments)-1] = nil
	s.segments = s.segments[:len(s.segments)-1]

	if len(s.segments) > 0 {
		s.earliestOffset = s.segments[0].baseOffset
	}

	// 3. Cleanup index file and handles (non-authoritative)
	var errs []error
	if err := os.Remove(seg.indexPath); err != nil && !os.IsNotExist(err) {
		errs = append(errs, fmt.Errorf("failed to delete index file %q: %w", seg.indexPath, err))
	}

	if err := seg.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close segment handles: %w", err))
	}

	if err := syncDirectory(filepath.Dir(seg.path)); err != nil {
		errs = append(errs, fmt.Errorf("failed to sync partition directory: %w", err))
	}

	return errors.Join(errs...)
}

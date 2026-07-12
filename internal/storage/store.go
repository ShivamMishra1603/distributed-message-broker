package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ShivamMishra1603/distributed-message-broker/internal/model"
)

var (
	ErrStoreClosed      = errors.New("store is closed")
	ErrOffsetOutOfRange = errors.New("offset out of range")
	ErrBatchTooLarge    = errors.New("batch size exceeds maximum configured limit")
)

type BatchLocation struct {
	BaseOffset uint64
	LastOffset uint64
	Position   int64
	Length     uint32
	SegmentRef *Segment
}

type Store struct {
	dir             string
	segmentMaxBytes int64
	maxBatchBytes   int64
	flushMode       string
	clock           model.Clock

	mu             sync.RWMutex
	closed         bool
	segments       []*Segment
	activeSegment  *Segment
	scanTable      []BatchLocation
	logEndOffset   uint64
	earliestOffset uint64
}

// OpenStore opens a partition store directory, scans and validates all log files,
// reconstructs offset states and the in-memory scan table, and opens the active segment.
func OpenStore(dir string, segmentMaxBytes int64, maxBatchBytes int64, flushMode string, clock model.Clock) (*Store, error) {
	if clock == nil {
		clock = func() time.Time { return time.Now().UTC() }
	}

	// Create directory if not exists
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create partition directory %q: %w", dir, err)
	}

	// 1. Scan for log files
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read partition directory %q: %w", dir, err)
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
		dir:             dir,
		segmentMaxBytes: segmentMaxBytes,
		maxBatchBytes:   maxBatchBytes,
		flushMode:       strings.ToLower(flushMode),
		clock:           clock,
		segments:        make([]*Segment, 0),
		scanTable:       make([]BatchLocation, 0),
		earliestOffset:  0,
		logEndOffset:    0,
	}

	// 2. Load and validate segments
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

		// Validate all batches in segment and construct scan table
		nextOffset, err := store.scanAndValidateSegment(seg)
		if err != nil {
			store.closeAllOpenSegments()
			return nil, fmt.Errorf("segment %q validation failed: %w", seg.path, err)
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
			return 0, 0, err
		}
		s.activeSegment.active = false

		newSeg, err := NewSegment(s.dir, s.logEndOffset, true)
		if err != nil {
			return 0, 0, err
		}
		s.segments = append(s.segments, newSeg)
		s.activeSegment = newSeg
	}

	// 4. Append to active segment
	pos, err := s.activeSegment.Append(encoded)
	if err != nil {
		return 0, 0, err
	}

	// 5. Apply Flush Policy
	if s.flushMode == "sync" {
		if err := s.activeSegment.Flush(); err != nil {
			return 0, 0, err
		}
	}

	// 6. Record Batch Location
	last := base + uint64(len(records)) - 1
	loc := BatchLocation{
		BaseOffset: base,
		LastOffset: last,
		Position:   pos,
		Length:     uint32(batchSize),
		SegmentRef: s.activeSegment,
	}
	s.scanTable = append(s.scanTable, loc)

	s.logEndOffset = last + 1
	s.activeSegment.nextOffset = s.logEndOffset

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

	if offset < s.earliestOffset || offset > s.logEndOffset {
		return nil, ErrOffsetOutOfRange
	}

	if offset == s.logEndOffset {
		return []model.StoredRecord{}, nil
	}

	// 1. Locate starting batch using binary search on scanTable
	startIdx := sort.Search(len(s.scanTable), func(i int) bool {
		return s.scanTable[i].LastOffset >= offset
	})

	if startIdx >= len(s.scanTable) {
		return nil, ErrOffsetOutOfRange
	}

	var result []model.StoredRecord
	accumulatedBytes := 0

	// 2. Read matching records sequentially across batches/segments
	for i := startIdx; i < len(s.scanTable); i++ {
		loc := s.scanTable[i]

		buf := make([]byte, loc.Length)
		_, err := loc.SegmentRef.ReadAt(buf, loc.Position)
		if err != nil && err != io.EOF {
			return nil, err
		}

		recs, err := DecodeBatch(buf)
		if err != nil {
			return nil, fmt.Errorf("failed to decode batch at offset %d: %w", loc.BaseOffset, err)
		}

		for _, r := range recs {
			if r.Offset < offset {
				continue
			}

			// Size includes key, value, headers, and 16 bytes overhead
			size := recordSize(r)

			if len(result) > 0 && accumulatedBytes+size > maxBytes {
				return result, nil
			}

			result = append(result, r)
			accumulatedBytes += size
		}
	}

	return result, nil
}

// Close gracefully flushes the active segment, closes all segment files, and sets store state.
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

func (s *Store) scanAndValidateSegment(seg *Segment) (uint64, error) {
	var currentPos int64 = 0
	expectedOffset := seg.baseOffset

	for currentPos < seg.size {
		// Minimum size to read magic, version, and batch length prefix is 9 bytes
		if currentPos+9 > seg.size {
			return 0, fmt.Errorf("truncated batch header in %q", seg.path)
		}

		prefixBuf := make([]byte, 9)
		if _, err := seg.ReadAt(prefixBuf, currentPos); err != nil {
			return 0, err
		}

		// Verify Magic
		var magic [4]byte
		copy(magic[:], prefixBuf[0:4])
		if magic != MagicBytes {
			return 0, ErrMalformedMagic
		}

		// Verify Version
		version := prefixBuf[4]
		if version != FormatVersion1 {
			return 0, ErrUnsupportedVersion
		}

		batchLength := binary.BigEndian.Uint32(prefixBuf[5:9])
		if batchLength < MinimumBatchRemainder {
			return 0, fmt.Errorf("%w: batch length %d too small", ErrCorruptBatch, batchLength)
		}

		totalBatchSize := int64(MagicSize+VersionSize+BatchLengthSize) + int64(batchLength)
		if currentPos+totalBatchSize > seg.size {
			// Milestone 3: Return error on incomplete tails (Milestone 4 will truncate)
			return 0, fmt.Errorf("truncated batch payload in %q", seg.path)
		}

		// Read complete batch payload
		batchBuf := make([]byte, totalBatchSize)
		if _, err := seg.ReadAt(batchBuf, currentPos); err != nil {
			return 0, err
		}

		// Decode batch to validate CRC and record details
		recs, err := DecodeBatch(batchBuf)
		if err != nil {
			return 0, fmt.Errorf("corrupt batch decode failure: %w", err)
		}

		if len(recs) == 0 {
			return 0, fmt.Errorf("segment contains empty record batch")
		}

		// Assert first record offset matches expected
		if recs[0].Offset != expectedOffset {
			return 0, fmt.Errorf("offset discontinuity: expected offset %d, got %d", expectedOffset, recs[0].Offset)
		}

		// Verify contiguous offsets inside the batch
		for j, r := range recs {
			expected := expectedOffset + uint64(j)
			if r.Offset != expected {
				return 0, fmt.Errorf("offset discontinuity inside batch: expected %d, got %d", expected, r.Offset)
			}
		}

		lastOffset := expectedOffset + uint64(len(recs)) - 1
		loc := BatchLocation{
			BaseOffset: expectedOffset,
			LastOffset: lastOffset,
			Position:   currentPos,
			Length:     uint32(totalBatchSize),
			SegmentRef: seg,
		}
		s.scanTable = append(s.scanTable, loc)

		expectedOffset = lastOffset + 1
		currentPos += totalBatchSize
	}

	return expectedOffset, nil
}

func (s *Store) closeAllOpenSegments() {
	for _, seg := range s.segments {
		seg.Close()
	}
	s.segments = nil
}

func recordSize(record model.StoredRecord) int {
	size := len(record.Key) + len(record.Value)
	for _, h := range record.Headers {
		size += len(h.Key) + len(h.Value)
	}
	size += 16 // 8 byte offset + 8 byte timestamp
	return size
}

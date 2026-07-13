package offsets

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
	"unicode/utf8"
)

// Framing constants
const (
	OffsetMagicSize    = 4
	OffsetVersionSize  = 1
	OffsetLengthSize   = 4
	OffsetCRCSize      = 4
	OffsetPrefixSize   = 9  // Magic + Version + Length
	OffsetFixedPayload = 28 // GroupLen (4) + TopicLen (4) + Partition (4) + NextOffset (8) + Timestamp (8)
	OffsetMinRemainder = 32 // CRC (4) + FixedPayload (28)
)

// Decoder safety limits
const (
	MaxConsumerGroupBytes = 255
	MaxTopicNameBytes     = 255
	MaxOffsetEntryBytes   = 1024
)

var (
	ErrOffsetStoreClosed      = errors.New("offset store closed")
	ErrOffsetStoreUnavailable = errors.New("offset store unavailable")
	ErrCorruptOffsetLog       = errors.New("offset log is corrupt")
	ErrInvalidArgument        = errors.New("invalid offset commit arguments")
)

// Key uniquely identifies an offset position
type Key struct {
	Group     string
	Topic     string
	Partition uint32
}

// Store manages persistent offset records
type Store struct {
	mu          sync.RWMutex
	filePath    string
	file        *os.File
	offsets     map[Key]uint64
	size        int64
	closed      bool
	writeFailed bool
	clock       func() time.Time
}

// OpenStore opens (or creates) the offset log file, recovers state, and truncates trailing incomplete writes
func OpenStore(path string, clock func() time.Time) (*Store, error) {
	if clock == nil {
		clock = time.Now
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create offsets directory %q: %w", dir, err)
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		return nil, fmt.Errorf("failed to open offsets log %q: %w", path, err)
	}

	store := &Store{
		filePath: path,
		file:     file,
		offsets:  make(map[Key]uint64),
		clock:    clock,
	}

	if err := store.recover(); err != nil {
		_ = file.Close()
		return nil, err
	}

	return store, nil
}

func (s *Store) recover() error {
	stat, err := s.file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat offset log: %w", err)
	}
	fileSize := stat.Size()

	var pos int64
	var validSize int64

	prefixBuf := make([]byte, OffsetPrefixSize)

	for pos < fileSize {
		// 1. Check for incomplete prefix
		if fileSize-pos < int64(OffsetPrefixSize) {
			if err := s.truncateAndSync(pos); err != nil {
				return err
			}
			fileSize = pos
			break
		}

		if _, err := s.file.ReadAt(prefixBuf, pos); err != nil {
			return fmt.Errorf("failed to read prefix at position %d: %w", pos, err)
		}

		magic := prefixBuf[0:4]
		version := prefixBuf[4]
		entryLength := binary.BigEndian.Uint32(prefixBuf[5:9])

		if string(magic) != "OFFS" || version != 0x01 {
			return fmt.Errorf("%w: invalid magic or version at position %d", ErrCorruptOffsetLog, pos)
		}

		if entryLength < uint32(OffsetMinRemainder) {
			return fmt.Errorf("%w: entry length %d smaller than min remainder %d at position %d", ErrCorruptOffsetLog, entryLength, OffsetMinRemainder, pos)
		}

		if entryLength > uint32(MaxOffsetEntryBytes) {
			return fmt.Errorf("%w: entry length %d exceeds max offset entry limit %d at position %d", ErrCorruptOffsetLog, entryLength, MaxOffsetEntryBytes, pos)
		}

		entryEnd := pos + int64(OffsetPrefixSize) + int64(entryLength)

		// 2. Check if the declared entry extends past file size
		if entryEnd > fileSize {
			if err := s.truncateAndSync(pos); err != nil {
				return err
			}
			fileSize = pos
			break
		}

		// 3. Read the entry body
		frame := make([]byte, entryLength)
		if _, err := s.file.ReadAt(frame, pos+int64(OffsetPrefixSize)); err != nil {
			return fmt.Errorf("failed to read entry body at position %d: %w", pos, err)
		}

		expectedCRC := binary.BigEndian.Uint32(frame[0:4])
		payload := frame[4:]
		actualCRC := crc32.ChecksumIEEE(payload)

		// 4. If CRC is invalid, it is a fatal corruption (do not truncate complete record)
		if expectedCRC != actualCRC {
			return fmt.Errorf("%w: crc32 checksum mismatch in complete record at position %d", ErrCorruptOffsetLog, pos)
		}

		cursor := uint32(0)
		if uint32(len(payload))-cursor < 4 {
			return fmt.Errorf("%w: payload too short for group length field at position %d", ErrCorruptOffsetLog, pos)
		}
		groupLen := binary.BigEndian.Uint32(payload[cursor : cursor+4])
		cursor += 4

		if groupLen == 0 || groupLen > uint32(MaxConsumerGroupBytes) {
			return fmt.Errorf("%w: invalid group name length %d at position %d", ErrCorruptOffsetLog, groupLen, pos)
		}
		if groupLen > uint32(len(payload))-cursor {
			return fmt.Errorf("%w: group name extends past payload at position %d", ErrCorruptOffsetLog, pos)
		}
		group := string(payload[cursor : cursor+groupLen])
		cursor += groupLen

		if !utf8.ValidString(group) {
			return fmt.Errorf("%w: group name at position %d is not valid UTF-8", ErrCorruptOffsetLog, pos)
		}

		if uint32(len(payload))-cursor < 4 {
			return fmt.Errorf("%w: payload too short for topic length field at position %d", ErrCorruptOffsetLog, pos)
		}
		topicLen := binary.BigEndian.Uint32(payload[cursor : cursor+4])
		cursor += 4

		if topicLen == 0 || topicLen > uint32(MaxTopicNameBytes) {
			return fmt.Errorf("%w: invalid topic name length %d at position %d", ErrCorruptOffsetLog, topicLen, pos)
		}
		if topicLen > uint32(len(payload))-cursor {
			return fmt.Errorf("%w: topic name extends past payload at position %d", ErrCorruptOffsetLog, pos)
		}
		topic := string(payload[cursor : cursor+topicLen])
		cursor += topicLen

		if !utf8.ValidString(topic) {
			return fmt.Errorf("%w: topic name at position %d is not valid UTF-8", ErrCorruptOffsetLog, pos)
		}

		expectedEntryLen := uint32(OffsetMinRemainder) + groupLen + topicLen
		if entryLength != expectedEntryLen {
			return fmt.Errorf("%w: entry length %d does not match expected size %d based on payloads at position %d", ErrCorruptOffsetLog, entryLength, expectedEntryLen, pos)
		}

		if uint32(len(payload))-cursor != 20 {
			return fmt.Errorf("%w: payload remainder does not match expected size at position %d", ErrCorruptOffsetLog, pos)
		}

		partition := binary.BigEndian.Uint32(payload[cursor : cursor+4])
		nextOffset := binary.BigEndian.Uint64(payload[cursor+4 : cursor+12])

		key := Key{Group: group, Topic: topic, Partition: partition}
		s.offsets[key] = nextOffset

		pos = entryEnd
		validSize = entryEnd
	}

	s.size = validSize
	return nil
}

func (s *Store) truncateAndSync(size int64) error {
	if err := s.file.Truncate(size); err != nil {
		return fmt.Errorf("failed to truncate corrupt tail of offset log: %w", err)
	}
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync offset log after truncation: %w", err)
	}
	return nil
}

// Commit records a consumer offset commit to disk and updates the in-memory cache
func (s *Store) Commit(group string, topic string, partition uint32, nextOffset uint64) error {
	// Intrinsic validations
	if len(group) == 0 || len(group) > MaxConsumerGroupBytes || !utf8.ValidString(group) {
		return fmt.Errorf("%w: group name is invalid", ErrInvalidArgument)
	}
	if len(topic) == 0 || len(topic) > MaxTopicNameBytes || !utf8.ValidString(topic) {
		return fmt.Errorf("%w: topic name is invalid", ErrInvalidArgument)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrOffsetStoreClosed
	}
	if s.writeFailed {
		return ErrOffsetStoreUnavailable
	}

	// Encode record
	gl := len(group)
	tl := len(topic)
	entryLength := uint32(OffsetMinRemainder + gl + tl)
	totalSize := int(OffsetPrefixSize + entryLength)

	encoded := make([]byte, totalSize)

	// Magic + Version + EntryLength
	copy(encoded[0:4], []byte("OFFS"))
	encoded[4] = 0x01
	binary.BigEndian.PutUint32(encoded[5:9], entryLength)

	// Payload
	payload := make([]byte, 28+gl+tl)
	binary.BigEndian.PutUint32(payload[0:4], uint32(gl))
	copy(payload[4:4+gl], group)

	off := 4 + gl
	binary.BigEndian.PutUint32(payload[off:off+4], uint32(tl))
	copy(payload[off+4:off+4+tl], topic)

	off = off + 4 + tl
	binary.BigEndian.PutUint32(payload[off:off+4], partition)
	binary.BigEndian.PutUint64(payload[off+4:off+12], nextOffset)
	binary.BigEndian.PutUint64(payload[off+12:off+20], uint64(s.clock().UnixMilli()))

	// CRC
	crc := crc32.ChecksumIEEE(payload)

	// Put CRC in encoded
	binary.BigEndian.PutUint32(encoded[9:13], crc)
	copy(encoded[13:], payload)

	// Append to file
	pos := s.size
	n, writeErr := s.file.WriteAt(encoded, pos)
	if writeErr != nil {
		s.writeFailed = true
		return fmt.Errorf("failed to append to offset log: %w", writeErr)
	}
	if n != totalSize {
		s.writeFailed = true
		return fmt.Errorf("short write appending to offset log: wrote %d of %d: %w", n, totalSize, io.ErrShortWrite)
	}

	if syncErr := s.file.Sync(); syncErr != nil {
		s.writeFailed = true
		return fmt.Errorf("failed to sync offset log changes: %w", syncErr)
	}

	s.size += int64(totalSize)
	key := Key{Group: group, Topic: topic, Partition: partition}
	s.offsets[key] = nextOffset

	return nil
}

// Get retrieves the next offset to read for the group, topic, and partition
func (s *Store) Get(group string, topic string, partition uint32) (uint64, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return 0, false, ErrOffsetStoreClosed
	}
	if s.writeFailed {
		return 0, false, ErrOffsetStoreUnavailable
	}

	key := Key{Group: group, Topic: topic, Partition: partition}
	val, found := s.offsets[key]
	return val, found, nil
}

// Close gracefully flushes and closes the offset log file handles
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true

	var syncErr error
	if !s.writeFailed {
		syncErr = s.file.Sync()
	}
	closeErr := s.file.Close()

	return errors.Join(syncErr, closeErr)
}

package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
)

type IndexEntry struct {
	RelativeOffset uint32
	Position       uint64
}

type Segment struct {
	baseOffset          uint64
	nextOffset          uint64
	size                int64
	path                string
	indexPath           string
	file                *os.File
	indexFile           *os.File
	indexEntries        []IndexEntry
	lastIndexedPosition int64
	indexSize           int64
	indexDirty          bool
	active              bool
	maxTimestamp        int64
	handlesClosed       bool
}

// NewSegment constructs or opens a segment log file and its corresponding sparse index file.
func NewSegment(dir string, baseOffset uint64, active bool) (*Segment, error) {
	filename := fmt.Sprintf("%020d.log", baseOffset)
	path := filepath.Join(dir, filename)

	indexFilename := fmt.Sprintf("%020d.index", baseOffset)
	indexPath := filepath.Join(dir, indexFilename)

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o640)
	if err != nil {
		return nil, fmt.Errorf("failed to open segment file %q: %w", path, err)
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat segment file %q: %w", path, err)
	}

	indexFile, err := os.OpenFile(indexPath, os.O_CREATE|os.O_RDWR, 0o640)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to open index file %q: %w", indexPath, err)
	}

	idxInfo, err := indexFile.Stat()
	if err != nil {
		file.Close()
		indexFile.Close()
		return nil, fmt.Errorf("failed to stat index file %q: %w", indexPath, err)
	}

	return &Segment{
		baseOffset:          baseOffset,
		nextOffset:          baseOffset,
		size:                info.Size(),
		path:                path,
		indexPath:           indexPath,
		file:                file,
		indexFile:           indexFile,
		indexSize:           idxInfo.Size(),
		lastIndexedPosition: -1,
		active:              active,
	}, nil
}

// ReadAt reads length bytes starting from the given position in the log file.
// It is position-independent and safe for concurrent reads.
func (s *Segment) ReadAt(buf []byte, position int64) (int, error) {
	n, err := s.file.ReadAt(buf, position)
	if err != nil && err != io.EOF {
		return n, fmt.Errorf("failed to read from segment %q at position %d: %w", s.path, position, err)
	}
	return n, err
}

// Append writes data at the end of the segment file using WriteAt.
// Store.mu must protect this operation and s.size.
func (s *Segment) Append(data []byte) (position int64, err error) {
	startPos := s.size

	n, err := s.file.WriteAt(data, startPos)
	if err != nil {
		return 0, fmt.Errorf("failed to append to segment %q: %w", s.path, err)
	}
	if n != len(data) {
		return 0, fmt.Errorf("short write to segment %q: wrote %d of %d bytes: %w", s.path, n, len(data), io.ErrShortWrite)
	}

	s.size += int64(n)
	return startPos, nil
}

// ReadIndexEntries reads and decodes the 12-byte entries from the index file.
// It performs validation on offset/position ordering.
func (s *Segment) ReadIndexEntries() ([]IndexEntry, error) {
	if s.indexSize == 0 {
		return []IndexEntry{}, nil
	}

	if s.indexSize%12 != 0 {
		return nil, fmt.Errorf("index size %d is not a multiple of 12 bytes", s.indexSize)
	}

	if s.indexSize > 64*1024*1024 {
		return nil, fmt.Errorf("index file size %d exceeds maximum allowable index size (64 MiB)", s.indexSize)
	}

	numEntries := s.indexSize / 12
	entries := make([]IndexEntry, numEntries)
	buf := make([]byte, s.indexSize)

	if _, err := s.indexFile.ReadAt(buf, 0); err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read index entries from %q: %w", s.indexPath, err)
	}

	var lastRel uint32 = 0
	var lastPos uint64 = 0

	for i := int64(0); i < numEntries; i++ {
		offset := i * 12
		rel := binary.BigEndian.Uint32(buf[offset : offset+4])
		pos := binary.BigEndian.Uint64(buf[offset+4 : offset+12])

		// Basic ordering checks
		if i > 0 {
			if rel <= lastRel {
				return nil, fmt.Errorf("non-increasing relative offset %d <= %d in index %q", rel, lastRel, s.indexPath)
			}
			if pos <= lastPos {
				return nil, fmt.Errorf("non-increasing position %d <= %d in index %q", pos, lastPos, s.indexPath)
			}
		} else {
			// First entry validation for non-empty index
			if rel != 0 || pos != 0 {
				return nil, fmt.Errorf("first index entry must be {0, 0}, got {%d, %d} in %q", rel, pos, s.indexPath)
			}
		}

		// Pos sanity check against math limits
		if pos > math.MaxInt64 {
			return nil, fmt.Errorf("position %d overflows max int64 in index %q", pos, s.indexPath)
		}

		entries[i] = IndexEntry{
			RelativeOffset: rel,
			Position:       pos,
		}

		lastRel = rel
		lastPos = pos
	}

	return entries, nil
}

// AppendIndexEntry appends a 12-byte index entry to the index file.
// Store.mu must protect this operation and s.indexSize.
func (s *Segment) AppendIndexEntry(entry IndexEntry) error {
	s.indexEntries = append(s.indexEntries, entry)
	s.lastIndexedPosition = int64(entry.Position)
	s.indexDirty = true

	buf := make([]byte, 12)
	binary.BigEndian.PutUint32(buf[0:4], entry.RelativeOffset)
	binary.BigEndian.PutUint64(buf[4:12], entry.Position)

	n, writeErr := s.indexFile.WriteAt(buf, s.indexSize)
	if writeErr == nil {
		if n == 12 {
			s.indexSize += 12
		} else {
			writeErr = io.ErrShortWrite
		}
	}
	return writeErr
}

// Flush forces writes to disk.
func (s *Segment) Flush() error {
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync segment file %q: %w", s.path, err)
	}
	if err := s.FlushIndex(); err != nil {
		return err
	}
	return nil
}

// FlushIndex syncs index file changes to disk.
func (s *Segment) FlushIndex() error {
	if s.indexDirty {
		if err := s.indexFile.Sync(); err != nil {
			return fmt.Errorf("failed to sync index file %q: %w", s.indexPath, err)
		}
		s.indexDirty = false
	}
	return nil
}

// Close closes both file handles.
func (s *Segment) Close() error {
	if s.handlesClosed {
		return nil
	}
	s.handlesClosed = true
	return errors.Join(s.file.Close(), s.indexFile.Close())
}

// Size returns the tracked file size.
func (s *Segment) Size() int64 {
	return s.size
}

// BaseOffset returns the segment's base offset.
func (s *Segment) BaseOffset() uint64 {
	return s.baseOffset
}

// MaxTimestamp returns the segment's max record timestamp.
func (s *Segment) MaxTimestamp() int64 {
	return s.maxTimestamp
}

// SetMaxTimestamp sets the segment's max record timestamp.
func (s *Segment) SetMaxTimestamp(ts int64) {
	s.maxTimestamp = ts
}

package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type Segment struct {
	baseOffset uint64
	nextOffset uint64 // Track offset after last record in this segment
	size       int64
	path       string
	file       *os.File
	active     bool
}

// NewSegment constructs or opens a segment file on disk.
func NewSegment(dir string, baseOffset uint64, active bool) (*Segment, error) {
	filename := fmt.Sprintf("%020d.log", baseOffset)
	path := filepath.Join(dir, filename)

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o640)
	if err != nil {
		return nil, fmt.Errorf("failed to open segment file %q: %w", path, err)
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat segment file %q: %w", path, err)
	}

	return &Segment{
		baseOffset: baseOffset,
		nextOffset: baseOffset,
		size:       info.Size(),
		path:       path,
		file:       file,
		active:     active,
	}, nil
}

// ReadAt reads length bytes starting from the given position.
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

// Flush forces writes to disk.
func (s *Segment) Flush() error {
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync segment file %q: %w", s.path, err)
	}
	return nil
}

// Close closes the file handle.
func (s *Segment) Close() error {
	if err := s.file.Close(); err != nil {
		return fmt.Errorf("failed to close segment file %q: %w", s.path, err)
	}
	return nil
}

// Size returns the tracked file size.
func (s *Segment) Size() int64 {
	return s.size
}

// BaseOffset returns the segment's base offset.
func (s *Segment) BaseOffset() uint64 {
	return s.baseOffset
}

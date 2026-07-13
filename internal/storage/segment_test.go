package storage

import (
	"bytes"
	"io"
	"sync"
	"testing"
)

func TestSegment_AppendAndReadAt(t *testing.T) {
	dir := t.TempDir()

	seg, err := NewSegment(dir, 100, true)
	if err != nil {
		t.Fatalf("failed to create segment: %v", err)
	}
	defer seg.Close()

	if seg.BaseOffset() != 100 {
		t.Errorf("expected base offset 100, got %d", seg.BaseOffset())
	}

	data1 := []byte("first batch data")
	pos1, err := seg.Append(data1)
	if err != nil {
		t.Fatalf("failed to append first block: %v", err)
	}
	if pos1 != 0 {
		t.Errorf("expected position 0, got %d", pos1)
	}
	if seg.Size() != int64(len(data1)) {
		t.Errorf("expected size %d, got %d", len(data1), seg.Size())
	}

	data2 := []byte("second batch")
	pos2, err := seg.Append(data2)
	if err != nil {
		t.Fatalf("failed to append second block: %v", err)
	}
	if pos2 != int64(len(data1)) {
		t.Errorf("expected position %d, got %d", len(data1), pos2)
	}

	expectedTotalSize := int64(len(data1) + len(data2))
	if seg.Size() != expectedTotalSize {
		t.Errorf("expected total size %d, got %d", expectedTotalSize, seg.Size())
	}

	// Read block 1
	buf1 := make([]byte, len(data1))
	n, err := seg.ReadAt(buf1, 0)
	if err != nil && err != io.EOF {
		t.Fatalf("failed to read block 1: %v", err)
	}
	if n != len(data1) {
		t.Errorf("expected to read %d bytes, got %d", len(data1), n)
	}
	if !bytes.Equal(buf1, data1) {
		t.Errorf("expected %q, got %q", string(data1), string(buf1))
	}

	// Read block 2
	buf2 := make([]byte, len(data2))
	n, err = seg.ReadAt(buf2, pos2)
	if err != nil && err != io.EOF {
		t.Fatalf("failed to read block 2: %v", err)
	}
	if n != len(data2) {
		t.Errorf("expected to read %d bytes, got %d", len(data2), n)
	}
	if !bytes.Equal(buf2, data2) {
		t.Errorf("expected %q, got %q", string(data2), string(buf2))
	}
}

func TestSegment_IndexAppendingAndValidation(t *testing.T) {
	dir := t.TempDir()

	seg, err := NewSegment(dir, 1000, true)
	if err != nil {
		t.Fatalf("failed to create segment: %v", err)
	}
	defer seg.Close()

	// 1. First entry must be {0, 0}
	err = seg.AppendIndexEntry(IndexEntry{RelativeOffset: 0, Position: 0})
	if err != nil {
		t.Fatalf("failed to append first index: %v", err)
	}

	// 2. Append normal entries
	err = seg.AppendIndexEntry(IndexEntry{RelativeOffset: 10, Position: 100})
	if err != nil {
		t.Fatalf("failed to append index: %v", err)
	}

	err = seg.AppendIndexEntry(IndexEntry{RelativeOffset: 20, Position: 300})
	if err != nil {
		t.Fatalf("failed to append index: %v", err)
	}

	// 3. Read back and check ordering validation
	entries, err := seg.ReadIndexEntries()
	if err != nil {
		t.Fatalf("failed to read index entries: %v", err)
	}

	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}

	if entries[1].RelativeOffset != 10 || entries[1].Position != 100 {
		t.Errorf("unexpected index entry: %+v", entries[1])
	}

	if entries[2].RelativeOffset != 20 || entries[2].Position != 300 {
		t.Errorf("unexpected index entry: %+v", entries[2])
	}
}

func TestSegment_ConcurrentReaders(t *testing.T) {
	dir := t.TempDir()

	seg, err := NewSegment(dir, 0, true)
	if err != nil {
		t.Fatalf("failed to create segment: %v", err)
	}
	defer seg.Close()

	payload1 := []byte("payloadA-pos0")
	payload2 := []byte("payloadB-pos13")

	_, _ = seg.Append(payload1)
	_, _ = seg.Append(payload2)

	var wg sync.WaitGroup
	numReaders := 20
	iterations := 100

	wg.Add(numReaders)
	for i := 0; i < numReaders; i++ {
		go func(readerID int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				if (readerID+j)%2 == 0 {
					buf := make([]byte, len(payload1))
					_, err := seg.ReadAt(buf, 0)
					if err != nil && err != io.EOF {
						t.Errorf("concurrent read 1 failed: %v", err)
						return
					}
					if !bytes.Equal(buf, payload1) {
						t.Errorf("expected %q, got %q", string(payload1), string(buf))
						return
					}
				} else {
					buf := make([]byte, len(payload2))
					_, err := seg.ReadAt(buf, int64(len(payload1)))
					if err != nil && err != io.EOF {
						t.Errorf("concurrent read 2 failed: %v", err)
						return
					}
					if !bytes.Equal(buf, payload2) {
						t.Errorf("expected %q, got %q", string(payload2), string(buf))
						return
					}
				}
			}
		}(i)
	}

	wg.Wait()
}

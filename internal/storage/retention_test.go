package storage

import (
	"os"
	"testing"
	"time"

	"github.com/ShivamMishra1603/distributed-message-broker/internal/model"
)

func TestStore_TimeRetentionBasic(t *testing.T) {
	dir := t.TempDir()

	var now time.Time
	clock := func() time.Time {
		return now
	}

	// Create store with 10-byte index interval
	store, err := OpenStore(dir, 1024*1024, 10000, 10, "sync", clock)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// 1. Append records in Segment 1 (Base offset 0)
	now = time.Unix(1000, 0) // long ago
	_, _, err = store.Append([]model.Record{{Key: []byte("k1"), Value: []byte("v1"), Timestamp: now.UnixMilli()}})
	if err != nil {
		t.Fatal(err)
	}

	// Rollover to Segment 2
	store.mu.Lock()
	store.activeSegment.active = false
	newSeg, err := NewSegment(store.dir, store.logEndOffset, true)
	if err != nil {
		store.mu.Unlock()
		t.Fatal(err)
	}
	store.segments = append(store.segments, newSeg)
	store.activeSegment = newSeg
	store.mu.Unlock()

	// 2. Append records in Segment 2 (Active segment, Base offset 1)
	now = time.Unix(5000, 0) // recent
	_, _, err = store.Append([]model.Record{{Key: []byte("k2"), Value: []byte("v2"), Timestamp: now.UnixMilli()}})
	if err != nil {
		t.Fatal(err)
	}

	// Verify segment state
	if len(store.segments) != 2 {
		t.Fatalf("expected 2 segments, got %d", len(store.segments))
	}
	if store.segments[0].MaxTimestamp() != 1000000 {
		t.Errorf("expected segment 0 maxTimestamp to be 1000000, got %d", store.segments[0].MaxTimestamp())
	}

	// Call time-based retention: age cutoff 2 hours (Segment 1 is older, Segment 2 is active/recent)
	// cutoff = 5000 - 3600 = 1400. Segment 1 maxTimestamp 1000 <= 1400 (eligible).
	err = store.ApplyRetention(1*time.Hour, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	// Verify Segment 1 is deleted
	if len(store.segments) != 1 {
		t.Errorf("expected 1 segment remaining, got %d", len(store.segments))
	}
	if store.segments[0] != store.activeSegment {
		t.Errorf("expected remaining segment to be active segment")
	}
	if store.EarliestOffset() != 1 {
		t.Errorf("expected earliestOffset to advance to 1, got %d", store.EarliestOffset())
	}

	// Fetching at offset 0 must fail with ErrOffsetOutOfRange
	_, err = store.Read(0, 100)
	if err != ErrOffsetOutOfRange {
		t.Errorf("expected ErrOffsetOutOfRange, got %v", err)
	}
}

func TestStore_SizeRetentionBasic(t *testing.T) {
	dir := t.TempDir()

	store, err := OpenStore(dir, 1024*1024, 10000, 10, "sync", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	now := time.Now()

	// Write record to Segment 1 (base 0)
	_, _, _ = store.Append([]model.Record{{Key: []byte("k1"), Value: []byte("v1"), Timestamp: now.UnixMilli()}})

	// Roll 1
	store.mu.Lock()
	store.activeSegment.active = false
	newSeg1, _ := NewSegment(store.dir, store.logEndOffset, true)
	store.segments = append(store.segments, newSeg1)
	store.activeSegment = newSeg1
	store.mu.Unlock()

	// Write record to Segment 2 (base 1)
	_, _, _ = store.Append([]model.Record{{Key: []byte("k2"), Value: []byte("v2"), Timestamp: now.UnixMilli()}})

	// Roll 2
	store.mu.Lock()
	store.activeSegment.active = false
	newSeg2, _ := NewSegment(store.dir, store.logEndOffset, true)
	store.segments = append(store.segments, newSeg2)
	store.activeSegment = newSeg2
	store.mu.Unlock()

	// Write record to Segment 3 (active segment, base 2)
	_, _, _ = store.Append([]model.Record{{Key: []byte("k3"), Value: []byte("v3"), Timestamp: now.UnixMilli()}})

	if len(store.segments) != 3 {
		t.Fatalf("expected 3 segments, got %d", len(store.segments))
	}

	// Calculate total partition size
	totalSize, err := store.calculateTotalBytes()
	if err != nil {
		t.Fatal(err)
	}

	for i, seg := range store.segments {
		lStat, _ := os.Stat(seg.path)
		iStat, _ := os.Stat(seg.indexPath)
		t.Logf("Seg %d: log=%d, index=%d", i, lStat.Size(), iStat.Size())
	}
	t.Logf("Total size: %d", totalSize)

	// Set partition limit smaller than total size but enough to keep segment 2 & 3
	limit := totalSize - 10 // will force deletion of oldest segment (Segment 1)
	t.Logf("Applying retention limit: %d", limit)
	err = store.ApplyRetention(100*time.Hour, limit)
	if err != nil {
		t.Fatal(err)
	}

	if len(store.segments) != 2 {
		t.Errorf("expected 2 segments remaining, got %d", len(store.segments))
	}
	if store.EarliestOffset() != 1 {
		t.Errorf("expected earliestOffset to advance to 1, got %d", store.EarliestOffset())
	}
}

func TestStore_RetentionReadLockConcurrency(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir, 1024*1024, 10000, 10, "sync", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Create segment 1 & roll
	_, _, _ = store.Append([]model.Record{{Key: []byte("k1"), Value: []byte("v1")}})
	store.mu.Lock()
	store.activeSegment.active = false
	newSeg, _ := NewSegment(store.dir, store.logEndOffset, true)
	store.segments = append(store.segments, newSeg)
	store.activeSegment = newSeg
	store.mu.Unlock()

	// Hold read lock
	store.mu.RLock()

	retentionDone := make(chan struct{})
	go func() {
		// This will block until RLock is released
		_ = store.ApplyRetention(0, 0) // immediate deletion of all closed segments
		close(retentionDone)
	}()

	select {
	case <-retentionDone:
		t.Error("ApplyRetention completed while read lock is held!")
	case <-time.After(100 * time.Millisecond):
		// Success: blocked
	}

	// Release read lock and verify retention completes
	store.mu.RUnlock()

	select {
	case <-retentionDone:
		// Success
	case <-time.After(1 * time.Second):
		t.Error("ApplyRetention did not resume after read lock release")
	}

	if len(store.segments) != 1 {
		t.Errorf("expected 1 segment remaining after retention run, got %d", len(store.segments))
	}
}

func TestStore_LogRemovalFailureBehavior(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir, 1024*1024, 10000, 10, "sync", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Create segment 1 & roll
	_, _, _ = store.Append([]model.Record{{Key: []byte("k1"), Value: []byte("v1")}})
	store.mu.Lock()
	store.activeSegment.active = false
	newSeg, _ := NewSegment(store.dir, store.logEndOffset, true)
	store.segments = append(store.segments, newSeg)
	store.activeSegment = newSeg
	store.mu.Unlock()

	oldestSegPath := store.segments[0].path

	// Change folder permissions to read-only to force os.Remove of log file to fail
	if err := os.Chmod(dir, 0555); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(dir, 0755) // restore permissions for cleanup

	// Attempt retention (should fail during os.Remove)
	err = store.ApplyRetention(0, 0)
	if err == nil {
		t.Error("expected error during ApplyRetention when log file cannot be deleted, got nil")
	}

	// Verify segment remains open, handles are valid, and still in-memory view
	if len(store.segments) != 2 {
		t.Errorf("expected segment count to remain 2, got %d", len(store.segments))
	}

	// Verify log file still exists on disk
	if _, err := os.Stat(oldestSegPath); os.IsNotExist(err) {
		t.Error("authoritative log file was deleted despite permission failure")
	}
}

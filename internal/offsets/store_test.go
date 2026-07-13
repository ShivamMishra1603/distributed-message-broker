package offsets

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestStore_BasicCommitAndGet(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "consumer-offsets.log")

	store, err := OpenStore(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// 1. Two groups on the same topic/partition remain independent
	err = store.Commit("groupA", "topic1", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	err = store.Commit("groupB", "topic1", 0, 200)
	if err != nil {
		t.Fatal(err)
	}

	valA, foundA, err := store.Get("groupA", "topic1", 0)
	if err != nil || !foundA || valA != 100 {
		t.Errorf("groupA failed: val=%d, found=%v, err=%v", valA, foundA, err)
	}

	valB, foundB, err := store.Get("groupB", "topic1", 0)
	if err != nil || !foundB || valB != 200 {
		t.Errorf("groupB failed: val=%d, found=%v, err=%v", valB, foundB, err)
	}

	// 2. Same group across two partitions remains independent
	err = store.Commit("groupA", "topic1", 1, 150)
	if err != nil {
		t.Fatal(err)
	}
	valA1, foundA1, err := store.Get("groupA", "topic1", 1)
	if err != nil || !foundA1 || valA1 != 150 {
		t.Errorf("groupA partition 1 failed: val=%d, found=%v", valA1, foundA1)
	}

	// 3. Backward commit is accepted
	err = store.Commit("groupA", "topic1", 0, 50)
	if err != nil {
		t.Fatal(err)
	}
	valABack, foundABack, err := store.Get("groupA", "topic1", 0)
	if err != nil || !foundABack || valABack != 50 {
		t.Errorf("groupA backward commit failed: val=%d, found=%v", valABack, foundABack)
	}
}

func TestStore_CloseAndErrors(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "consumer-offsets.log")

	store, err := OpenStore(path, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Commit after close
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	err = store.Commit("groupA", "topic1", 0, 100)
	if !errors.Is(err, ErrOffsetStoreClosed) {
		t.Errorf("expected ErrOffsetStoreClosed, got %v", err)
	}

	_, _, err = store.Get("groupA", "topic1", 0)
	if !errors.Is(err, ErrOffsetStoreClosed) {
		t.Errorf("expected ErrOffsetStoreClosed, got %v", err)
	}

	// Double close should be idempotent
	if err := store.Close(); err != nil {
		t.Errorf("expected no error on double close, got %v", err)
	}
}

func TestStore_ValidationConstraints(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "consumer-offsets.log")

	store, err := OpenStore(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Empty group/topic names
	if err := store.Commit("", "topic1", 0, 100); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("expected ErrInvalidArgument, got %v", err)
	}
	if err := store.Commit("groupA", "", 0, 100); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("expected ErrInvalidArgument, got %v", err)
	}

	// Oversized group/topic names
	longGroup := make([]byte, 256)
	for i := range longGroup {
		longGroup[i] = 'a'
	}
	if err := store.Commit(string(longGroup), "topic1", 0, 100); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("expected ErrInvalidArgument for long group, got %v", err)
	}

	// Invalid UTF-8
	invalidUTF8 := string([]byte{0xff, 0xfe, 0xfd})
	if err := store.Commit(invalidUTF8, "topic1", 0, 100); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("expected ErrInvalidArgument for invalid UTF-8 group, got %v", err)
	}
}

func TestStore_ConcurrentAccess(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "consumer-offsets.log")

	store, err := OpenStore(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	var wg sync.WaitGroup
	workers := 10
	commitsPerWorker := 100

	// Concurrent commits to same and different keys
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < commitsPerWorker; j++ {
				topic := fmt.Sprintf("topic-%d", workerID)
				_ = store.Commit("group", topic, 0, uint64(j))
				_ = store.Commit("group", "shared-topic", 0, uint64(j))
			}
		}(i)
	}

	wg.Wait()

	// Verify lookups work correctly
	val, found, err := store.Get("group", "shared-topic", 0)
	if err != nil || !found {
		t.Errorf("failed to get shared topic offset: %v", err)
	}
	if val >= uint64(commitsPerWorker) {
		t.Errorf("unexpected large committed value: %d", val)
	}
}

func TestStore_RecoveryTruncationAndCorruption(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "consumer-offsets.log")

	// 1. Write some valid offset entries
	store, err := OpenStore(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Commit("group", "topic", 0, 10); err != nil {
		t.Fatal(err)
	}
	if err := store.Commit("group", "topic", 1, 20); err != nil {
		t.Fatal(err)
	}
	store.Close()

	// Verify we can recover them
	store2, err := OpenStore(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	val0, found0, _ := store2.Get("group", "topic", 0)
	val1, found1, _ := store2.Get("group", "topic", 1)
	if !found0 || val0 != 10 || !found1 || val1 != 20 {
		t.Errorf("failed simple recovery: val0=%d, val1=%d", val0, val1)
	}
	store2.Close()

	// Get file size
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	origSize := info.Size()

	// 2. Append incomplete trailing bytes (fewer than prefix size)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0640)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write([]byte{0x01, 0x02})
	f.Close()

	// Recovery should succeed by truncating the extra 2 bytes
	store3, err := OpenStore(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	val0, found0, _ = store3.Get("group", "topic", 0)
	if !found0 || val0 != 10 {
		t.Errorf("failed recovery after minor truncate: val=%d", val0)
	}
	store3.Close()

	// Verify file is truncated back to original size
	info, _ = os.Stat(path)
	if info.Size() != origSize {
		t.Errorf("expected size %d after recovery truncate, got %d", origSize, info.Size())
	}

	// 3. Append declared entry that extends past file size
	f, err = os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0640)
	if err != nil {
		t.Fatal(err)
	}
	// Write magic "OFFS", version 1, entryLength = 100 bytes (which is not fully written)
	_, _ = f.Write([]byte("OFFS"))
	_, _ = f.Write([]byte{0x01})
	lenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBytes, 100)
	_, _ = f.Write(lenBytes)
	_, _ = f.Write([]byte{0xaa, 0xbb}) // incomplete entry body
	f.Close()

	// Recovery should succeed by truncating back to origSize
	store4, err := OpenStore(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	val0, found0, _ = store4.Get("group", "topic", 0)
	if !found0 || val0 != 10 {
		t.Errorf("failed recovery after major tail truncate: val=%d", val0)
	}
	store4.Close()

	info, _ = os.Stat(path)
	if info.Size() != origSize {
		t.Errorf("expected size %d after major recovery truncate, got %d", origSize, info.Size())
	}

	// 4. Complete final entry with bad CRC must FAIL startup
	// We will append a complete entry, but corrupt the CRC
	f, err = os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0640)
	if err != nil {
		t.Fatal(err)
	}
	// Frame size: 32 + 5 (group) + 5 (topic) = 42
	// Write magic, version, entryLength
	_, _ = f.Write([]byte("OFFS"))
	_, _ = f.Write([]byte{0x01})
	binary.BigEndian.PutUint32(lenBytes, 42)
	_, _ = f.Write(lenBytes)
	// Write corrupt CRC
	_, _ = f.Write([]byte{0x00, 0x00, 0x00, 0x00})
	// Write group len and name
	binary.BigEndian.PutUint32(lenBytes, 5)
	_, _ = f.Write(lenBytes)
	_, _ = f.Write([]byte("group"))
	// Write topic len and name
	binary.BigEndian.PutUint32(lenBytes, 5)
	_, _ = f.Write(lenBytes)
	_, _ = f.Write([]byte("topic"))
	// Partition, offset, timestamp
	binary.BigEndian.PutUint32(lenBytes, 2)
	_, _ = f.Write(lenBytes)
	offsetBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(offsetBytes, 999)
	_, _ = f.Write(offsetBytes)
	_, _ = f.Write(offsetBytes) // timestamp
	f.Close()

	// Reopen must fail with corruption error
	_, err = OpenStore(path, nil)
	if !errors.Is(err, ErrCorruptOffsetLog) {
		t.Errorf("expected ErrCorruptOffsetLog for complete record with bad CRC, got: %v", err)
	}
}

func TestStore_PhysicalOrderWinsNotTimestamp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "consumer-offsets.log")

	// We use a mock clock that moves backward
	var now time.Time
	clockFunc := func() time.Time {
		return now
	}

	store, err := OpenStore(path, clockFunc)
	if err != nil {
		t.Fatal(err)
	}

	// 1. Commit with newer logical timestamp
	now = time.Unix(2000, 0)
	if err := store.Commit("group", "topic", 0, 100); err != nil {
		t.Fatal(err)
	}

	// 2. Commit with older logical timestamp physically AFTER the first one
	now = time.Unix(1000, 0)
	if err := store.Commit("group", "topic", 0, 50); err != nil {
		t.Fatal(err)
	}
	store.Close()

	// Recovery should load the physically last entry (50), not the one with the higher timestamp (100)
	store2, err := OpenStore(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()

	val, found, _ := store2.Get("group", "topic", 0)
	if !found || val != 50 {
		t.Errorf("expected physical order winner (50), got %d (found=%v)", val, found)
	}
}

func TestStore_MalformedPayloadPanicSafety(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "malformed-offsets.log")

	// We construct an entry of 36 bytes entryLength.
	// Prefix = 9 bytes: Magic (4) + Version (1) + entryLength (4) = "OFFS" + 0x01 + 36
	// Frame size: 36 bytes.
	// Payload of 32 bytes. The first 4 bytes of frame is CRC.
	// CRC covers the 32 bytes of payload.
	// In the payload:
	// Bytes 0-3: Group length = 25 (declaring 25 bytes, but we only provide 4 bytes group name to cause index panic if unchecked)
	payload := make([]byte, 32)
	binary.BigEndian.PutUint32(payload[0:4], 25) // groupLen = 25 (out of bounds)
	copy(payload[4:8], []byte("g123"))

	// Fill the rest with some topic length (e.g. 1) and details
	binary.BigEndian.PutUint32(payload[8:12], 1)
	binary.BigEndian.PutUint32(payload[12:16], 0) // partition
	binary.BigEndian.PutUint64(payload[16:24], 100) // next offset
	binary.BigEndian.PutUint64(payload[24:32], 0) // timestamp

	crc := crc32.ChecksumIEEE(payload)

	// Build final entry
	entry := make([]byte, 9+36)
	copy(entry[0:4], []byte("OFFS"))
	entry[4] = 0x01
	binary.BigEndian.PutUint32(entry[5:9], 36)
	binary.BigEndian.PutUint32(entry[9:13], crc)
	copy(entry[13:], payload)

	err := os.WriteFile(path, entry, 0640)
	if err != nil {
		t.Fatal(err)
	}

	// Try to open. Must not panic and must return ErrCorruptOffsetLog.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("OpenStore panicked on malformed payload: %v", r)
		}
	}()

	_, err = OpenStore(path, nil)
	if err == nil || !errors.Is(err, ErrCorruptOffsetLog) {
		t.Errorf("expected ErrCorruptOffsetLog, got: %v", err)
	}
}

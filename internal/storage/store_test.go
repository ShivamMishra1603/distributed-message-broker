package storage

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/ShivamMishra1603/distributed-message-broker/internal/model"
)

func TestStore_AppendAndReadRoundTrip(t *testing.T) {
	dir := t.TempDir()

	store, err := OpenStore(dir, 1024*1024, 10000, "sync", nil)
	if err != nil {
		t.Fatalf("failed to open store: %v", err)
	}

	// 1. Append records
	recs1 := []model.Record{
		{Key: []byte("k1"), Value: []byte("val1")},
		{Key: []byte("k2"), Value: []byte("val2")},
	}
	base, last, err := store.Append(recs1)
	if err != nil {
		t.Fatalf("failed to append: %v", err)
	}
	if base != 0 || last != 1 {
		t.Errorf("expected base=0, last=1; got base=%d, last=%d", base, last)
	}

	recs2 := []model.Record{
		{Key: []byte("k3"), Value: []byte("val3")},
	}
	base2, last2, err := store.Append(recs2)
	if err != nil {
		t.Fatalf("failed to append again: %v", err)
	}
	if base2 != 2 || last2 != 2 {
		t.Errorf("expected base=2, last=2; got base=%d, last=%d", base2, last2)
	}

	// 2. Read back
	readRecs, err := store.Read(0, 1000)
	if err != nil {
		t.Fatalf("failed to read offset 0: %v", err)
	}
	if len(readRecs) != 3 {
		t.Fatalf("expected 3 records, got %d", len(readRecs))
	}
	if string(readRecs[0].Key) != "k1" || string(readRecs[2].Key) != "k3" {
		t.Errorf("unexpected read results: %+v", readRecs)
	}

	// 3. Graceful Close and Reopen recovery check
	if err := store.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	recoveredStore, err := OpenStore(dir, 1024*1024, 10000, "sync", nil)
	if err != nil {
		t.Fatalf("failed to reopen store: %v", err)
	}
	defer recoveredStore.Close()

	if recoveredStore.LogEndOffset() != 3 {
		t.Errorf("expected logEndOffset 3, got %d", recoveredStore.LogEndOffset())
	}

	recoveredRecs, err := recoveredStore.Read(1, 1000)
	if err != nil {
		t.Fatalf("failed to read reopened store: %v", err)
	}
	if len(recoveredRecs) != 2 {
		t.Fatalf("expected 2 records starting at offset 1, got %d", len(recoveredRecs))
	}
	if string(recoveredRecs[0].Key) != "k2" || string(recoveredRecs[1].Key) != "k3" {
		t.Errorf("unexpected read results on reload: %+v", recoveredRecs)
	}
}

func TestStore_Rollover(t *testing.T) {
	dir := t.TempDir()

	// Set segment max size very small (e.g. 80 bytes) so each batch triggers rollover
	store, err := OpenStore(dir, 80, 1000, "sync", nil)
	if err != nil {
		t.Fatalf("failed to open store: %v", err)
	}
	defer store.Close()

	recs := []model.Record{
		{Key: []byte("largekey"), Value: []byte("largeval")}, // size of encoded batch will be > 50 bytes
	}

	base1, last1, _ := store.Append(recs)
	if base1 != 0 || last1 != 0 {
		t.Errorf("expected offset 0, got %d", base1)
	}

	base2, last2, _ := store.Append(recs)
	if base2 != 1 || last2 != 1 {
		t.Errorf("expected offset 1, got %d", base2)
	}

	base3, last3, _ := store.Append(recs)
	if base3 != 2 || last3 != 2 {
		t.Errorf("expected offset 2, got %d", base3)
	}

	// Verify that multiple files are created
	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	logFilesCount := 0
	for _, f := range files {
		if !f.IsDir() && filepath.Ext(f.Name()) == ".log" {
			logFilesCount++
		}
	}

	if logFilesCount != 3 {
		t.Errorf("expected 3 log files, got %d", logFilesCount)
	}

	// Read spanning multiple segments
	readRecs, err := store.Read(0, 1000)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if len(readRecs) != 3 {
		t.Errorf("expected 3 records, got %d", len(readRecs))
	}
	if readRecs[0].Offset != 0 || readRecs[1].Offset != 1 || readRecs[2].Offset != 2 {
		t.Errorf("offsets returned out of order or discontinuous: %+v", readRecs)
	}
}

func TestStore_IdempotentCloseAndErrors(t *testing.T) {
	dir := t.TempDir()

	store, err := OpenStore(dir, 1024, 1000, "sync", nil)
	if err != nil {
		t.Fatalf("failed to open store: %v", err)
	}

	// 1. Double Close is safe
	if err := store.Close(); err != nil {
		t.Errorf("first Close failed: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Errorf("second Close failed: %v", err)
	}

	// 2. Append after close fails with ErrStoreClosed
	_, _, err = store.Append([]model.Record{{Key: []byte("k"), Value: []byte("v")}})
	if !errors.Is(err, ErrStoreClosed) {
		t.Errorf("expected ErrStoreClosed, got %v", err)
	}

	// 3. Read after close fails with ErrStoreClosed
	_, err = store.Read(0, 100)
	if !errors.Is(err, ErrStoreClosed) {
		t.Errorf("expected ErrStoreClosed, got %v", err)
	}
}

func TestStore_OffsetDiscontinuityValidation(t *testing.T) {
	dir := t.TempDir()

	store1, err := OpenStore(dir, 1024, 1000, "sync", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _, _ = store1.Append([]model.Record{{Key: []byte("k"), Value: []byte("v")}})
	store1.Close()

	// Rename first segment file to simulate gap
	oldFile := filepath.Join(dir, "00000000000000000000.log")
	newFile := filepath.Join(dir, "00000000000000000010.log")
	if err := os.Rename(oldFile, newFile); err != nil {
		t.Fatal(err)
	}

	// Open store should fail due to offset discontinuity (base 0 missing, starting with 10 but expected 0)
	_, err = OpenStore(dir, 1024, 1000, "sync", nil)
	if err == nil {
		t.Error("expected error opening discontinuous store, got nil")
	}
}

func TestStore_BatchSizeLimits(t *testing.T) {
	dir := t.TempDir()

	// Let's determine the exact encoded size of a single record:
	// batchHeader = 33 bytes.
	// record = Key: "k" (1), Value: "v" (1). keyLen (4) + valLen (4) + offsetDelta (4) + timestampDelta (8) + headerCount (4) = 24.
	// Total record size = 24 + 1 + 1 = 26 bytes.
	// Total encoded batch = 33 + 26 = 59 bytes.

	// Open store with maxBatchBytes exactly at 59 bytes.
	store, err := OpenStore(dir, 1024, 59, "sync", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// 1. Encoded batch exactly at max_batch_bytes succeeds
	recs := []model.Record{
		{Key: []byte("k"), Value: []byte("v")},
	}
	_, _, err = store.Append(recs)
	if err != nil {
		t.Errorf("expected append of 59 bytes to succeed, got: %v", err)
	}

	// 2. Payload is under individual limits but fully encoded batch goes over max_batch_bytes (e.g. 2 records = ~85 bytes)
	recsOver := []model.Record{
		{Key: []byte("k"), Value: []byte("v")},
		{Key: []byte("a"), Value: []byte("b")},
	}
	_, _, err = store.Append(recsOver)
	if !errors.Is(err, ErrBatchTooLarge) {
		t.Errorf("expected ErrBatchTooLarge, got: %v", err)
	}
}

func TestStore_ConcurrentReaders(t *testing.T) {
	dir := t.TempDir()

	store, err := OpenStore(dir, 1024*1024, 10000, "sync", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Write two distinct batches
	_, _, _ = store.Append([]model.Record{{Key: []byte("keyA"), Value: []byte("valA")}})
	_, _, _ = store.Append([]model.Record{{Key: []byte("keyB"), Value: []byte("valB")}})

	var wg sync.WaitGroup
	numReaders := 20
	iterations := 100

	wg.Add(numReaders)
	for i := 0; i < numReaders; i++ {
		go func(readerID int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				if (readerID+j)%2 == 0 {
					recs, err := store.Read(0, 1000)
					if err != nil {
						t.Errorf("read at offset 0 failed: %v", err)
						return
					}
					if len(recs) < 1 || string(recs[0].Key) != "keyA" {
						t.Errorf("unexpected record read: %+v", recs)
						return
					}
				} else {
					recs, err := store.Read(1, 1000)
					if err != nil {
						t.Errorf("read at offset 1 failed: %v", err)
						return
					}
					if len(recs) < 1 || string(recs[0].Key) != "keyB" {
						t.Errorf("unexpected record read: %+v", recs)
						return
					}
				}
			}
		}(i)
	}

	wg.Wait()
}

package partition

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func mockClock(t time.Time) Clock {
	return func() time.Time { return t }
}

func TestLog_AppendAndReadBasic(t *testing.T) {
	now := time.Now().UTC()
	clock := mockClock(now)
	log := NewLog("test-topic", 0, clock)

	// 1. Single append returns offset 0
	records := []Record{
		{Key: []byte("k1"), Value: []byte("v1")},
	}
	base, last, err := log.Append(records)
	if err != nil {
		t.Fatalf("failed to append: %v", err)
	}
	if base != 0 || last != 0 {
		t.Errorf("expected base=0, last=0; got base=%d, last=%d", base, last)
	}

	// 2. Sequential appends produce gap-free monotonic offsets
	records2 := []Record{
		{Key: []byte("k2"), Value: []byte("v2")},
		{Key: []byte("k3"), Value: []byte("v3")},
	}
	base2, last2, err := log.Append(records2)
	if err != nil {
		t.Fatalf("failed to append: %v", err)
	}
	if base2 != 1 || last2 != 2 {
		t.Errorf("expected base=1, last=2; got base=%d, last=%d", base2, last2)
	}

	if log.LogEndOffset() != 3 {
		t.Errorf("expected LogEndOffset to be 3, got %d", log.LogEndOffset())
	}

	// 3. Read at offset 0 returns all records
	readRecs, err := log.Read(0, 1000)
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}
	if len(readRecs) != 3 {
		t.Fatalf("expected 3 records, got %d", len(readRecs))
	}
	for i, r := range readRecs {
		if r.Offset != uint64(i) {
			t.Errorf("expected offset %d, got %d", i, r.Offset)
		}
		if r.Timestamp != now.UnixMilli() {
			t.Errorf("expected timestamp %d, got %d", now.UnixMilli(), r.Timestamp)
		}
	}

	// 4. Read at logEndOffset returns empty slice, no error
	readRecsEmpty, err := log.Read(3, 1000)
	if err != nil {
		t.Fatalf("expected no error at logEndOffset, got %v", err)
	}
	if len(readRecsEmpty) != 0 {
		t.Errorf("expected empty slice, got %d elements", len(readRecsEmpty))
	}

	// 5. Read beyond logEndOffset returns ErrOffsetOutOfRange
	_, err = log.Read(4, 1000)
	if !errors.Is(err, ErrOffsetOutOfRange) {
		t.Errorf("expected ErrOffsetOutOfRange, got %v", err)
	}
}

func TestLog_DeepCopySafety(t *testing.T) {
	log := NewLog("test-topic", 0, nil)

	mutableKey := []byte("original-key")
	mutableVal := []byte("original-val")
	mutableHeaderVal := []byte("original-hval")

	records := []Record{
		{
			Key:   mutableKey,
			Value: mutableVal,
			Headers: []Header{
				{Key: "hk", Value: mutableHeaderVal},
			},
		},
	}

	_, _, err := log.Append(records)
	if err != nil {
		t.Fatalf("failed to append: %v", err)
	}

	// Mutate the original slices
	mutableKey[0] = 'X'
	mutableVal[0] = 'X'
	mutableHeaderVal[0] = 'X'

	readRecs, err := log.Read(0, 1000)
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}
	if len(readRecs) != 1 {
		t.Fatalf("expected 1 record, got %d", len(readRecs))
	}

	r := readRecs[0]
	if string(r.Key) != "original-key" {
		t.Errorf("log returned mutated key: %q", string(r.Key))
	}
	if string(r.Value) != "original-val" {
		t.Errorf("log returned mutated value: %q", string(r.Value))
	}
	if string(r.Headers[0].Value) != "original-hval" {
		t.Errorf("log returned mutated header value: %q", string(r.Headers[0].Value))
	}

	// Mutate the read slice
	r.Key[0] = 'Z'
	r.Value[0] = 'Z'
	r.Headers[0].Value[0] = 'Z'

	// Read again and verify it is still untouched
	readRecs2, _ := log.Read(0, 1000)
	r2 := readRecs2[0]
	if string(r2.Key) != "original-key" {
		t.Errorf("log returned mutated key after mutating read output: %q", string(r2.Key))
	}
}

func TestLog_ReadByteLimits(t *testing.T) {
	log := NewLog("test-topic", 0, nil)

	r1 := Record{Key: []byte("k"), Value: []byte("val1")} // stored record size: 1 + 4 + 16 = 21 bytes
	r2 := Record{Key: []byte("k"), Value: []byte("val2")} // stored record size: 21 bytes
	r3 := Record{Key: []byte("k"), Value: []byte("val3")} // stored record size: 21 bytes

	_, _, err := log.Append([]Record{r1, r2, r3})
	if err != nil {
		t.Fatalf("failed to append: %v", err)
	}

	// Case 1: MaxBytes is less than size of 1 record (21 bytes),
	// but it is the first record so it should be returned to ensure progress.
	recs, err := log.Read(0, 10)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if len(recs) != 1 {
		t.Errorf("expected exactly 1 record to ensure progress, got %d", len(recs))
	}

	// Case 2: MaxBytes accommodates exactly 2 records (42 bytes).
	recs, err = log.Read(0, 42)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if len(recs) != 2 {
		t.Errorf("expected exactly 2 records, got %d", len(recs))
	}

	// Case 3: Invalid MaxBytes returns error.
	_, err = log.Read(0, 0)
	if err == nil {
		t.Error("expected error for maxBytes = 0, got nil")
	}
}

func TestLog_ConcurrentAppends(t *testing.T) {
	log := NewLog("test-topic", 0, nil)
	var wg sync.WaitGroup

	numGoroutines := 10
	recordsPerGoroutine := 1000

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < recordsPerGoroutine; j++ {
				_, _, err := log.Append([]Record{
					{Key: []byte("key"), Value: []byte("val")},
				})
				if err != nil {
					return
				}
			}
		}()
	}

	wg.Wait()

	totalExpected := numGoroutines * recordsPerGoroutine
	if int(log.LogEndOffset()) != totalExpected {
		t.Errorf("expected LogEndOffset to be %d, got %d", totalExpected, log.LogEndOffset())
	}

	readRecs, err := log.Read(0, 1000*totalExpected) // high limit to read all
	if err != nil {
		t.Fatalf("failed to read records: %v", err)
	}

	if len(readRecs) != totalExpected {
		t.Fatalf("expected to read %d records, got %d", totalExpected, len(readRecs))
	}

	// Validate offset monotonicity, no gaps, no duplicates
	seen := make(map[uint64]bool)
	for i, r := range readRecs {
		if r.Offset != uint64(i) {
			t.Errorf("gap detected in offsets: index %d has offset %d", i, r.Offset)
		}
		if seen[r.Offset] {
			t.Errorf("duplicate offset detected: %d", r.Offset)
		}
		seen[r.Offset] = true
	}
}

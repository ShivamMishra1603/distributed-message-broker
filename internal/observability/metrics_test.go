package observability

import (
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMetrics_MultipleInstancesAllowed(t *testing.T) {
	// Constructing two separate Metrics instances should not panic because they use private registries.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("constructing multiple Metrics instances panicked: %v", r)
		}
	}()

	m1 := NewMetrics()
	m2 := NewMetrics()

	if m1 == nil || m2 == nil {
		t.Fatal("Metrics instances should not be nil")
	}

	if m1.Registry == m2.Registry {
		t.Fatal("Metrics instances should have separate registries")
	}
}

func TestPartitionObserver_Telemetry(t *testing.T) {
	m := NewMetrics()
	obs := m.NewPartitionObserver("test-topic", 2)

	// 1. Telemetry for Append
	obs.ObserveAppend(10*time.Millisecond, 5, 250, nil)

	// Validate count & bytes
	valRecords := testutil.ToFloat64(m.RecordsAppended)
	if valRecords != 5 {
		t.Errorf("expected 5 records appended, got %f", valRecords)
	}

	valBytes := testutil.ToFloat64(m.BytesWritten)
	if valBytes != 250 {
		t.Errorf("expected 250 bytes written, got %f", valBytes)
	}

	// 2. Append error
	obs.ObserveAppend(5*time.Millisecond, 0, 0, errors.New("write failed"))
	errCount := testutil.ToFloat64(m.StorageErrors.WithLabelValues("append"))
	if errCount != 1 {
		t.Errorf("expected 1 append storage error, got %f", errCount)
	}

	// 3. Telemetry for Read
	obs.ObserveRead(20*time.Millisecond, 2, 100, nil)
	valFetched := testutil.ToFloat64(m.RecordsFetched)
	if valFetched != 2 {
		t.Errorf("expected 2 records fetched, got %f", valFetched)
	}
	valReadBytes := testutil.ToFloat64(m.BytesRead)
	if valReadBytes != 100 {
		t.Errorf("expected 100 bytes read, got %f", valReadBytes)
	}

	// 4. Index rebuild & segment deletion
	obs.IndexRebuilt()
	rebuildCount := testutil.ToFloat64(m.IndexRebuilds)
	if rebuildCount != 1 {
		t.Errorf("expected 1 index rebuild, got %f", rebuildCount)
	}

	obs.SegmentDeleted()
	delCount := testutil.ToFloat64(m.RetentionSegmentsDel)
	if delCount != 1 {
		t.Errorf("expected 1 segment deleted, got %f", delCount)
	}

	// 5. State update
	obs.SetPartitionState(1000, 3)
	valSize := testutil.ToFloat64(m.PartitionLogSize.WithLabelValues("test-topic", "2"))
	if valSize != 1000 {
		t.Errorf("expected partition log size 1000, got %f", valSize)
	}

	valSegs := testutil.ToFloat64(m.PartitionSegmentCount.WithLabelValues("test-topic", "2"))
	if valSegs != 3 {
		t.Errorf("expected partition segment count 3, got %f", valSegs)
	}
}

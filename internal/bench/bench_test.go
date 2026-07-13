package bench

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestLatencySampler_Basic(t *testing.T) {
	sampler := NewSampler(10, 12345)

	// Add 5 samples (less than max size)
	for i := 1; i <= 5; i++ {
		sampler.Add(time.Duration(i) * time.Millisecond)
	}

	report := sampler.GetReport()
	if report.MinMs != 1.0 {
		t.Errorf("expected MinMs to be 1.0, got %f", report.MinMs)
	}
	if report.MaxMs != 5.0 {
		t.Errorf("expected MaxMs to be 5.0, got %f", report.MaxMs)
	}
	if report.MeanMs != 3.0 {
		t.Errorf("expected MeanMs to be 3.0, got %f", report.MeanMs)
	}
	// Nearest-rank P50 of 5 items: ceil(0.50 * 5) = 3 -> index 2 (val: 3ms)
	if report.P50Ms != 3.0 {
		t.Errorf("expected P50Ms to be 3.0, got %f", report.P50Ms)
	}
}

func TestLatencySampler_ReservoirSampling(t *testing.T) {
	// Add 100 samples to a size 10 sampler
	sampler := NewSampler(10, 42)
	for i := 1; i <= 100; i++ {
		sampler.Add(time.Duration(i) * time.Millisecond)
	}

	report := sampler.GetReport()
	if len(sampler.samples) != 10 {
		t.Errorf("expected sampler to limit size to 10, got %d", len(sampler.samples))
	}
	// Check that we got random values that are in bounds
	if report.MinMs < 1.0 || report.MaxMs > 100.0 {
		t.Errorf("samples out of bound: Min=%f, Max=%f", report.MinMs, report.MaxMs)
	}
}

func TestExactCounts_ClaimConsumed(t *testing.T) {
	bench := &ConsumerBench{
		target: 100,
	}

	// Claim 40
	res1 := bench.claimConsumed(40)
	if res1 != 40 {
		t.Errorf("expected accepted to be 40, got %d", res1)
	}

	// Claim 80 (overshoot by 20)
	res2 := bench.claimConsumed(80)
	if res2 != 60 {
		t.Errorf("expected accepted to be 60, got %d", res2)
	}

	// Claim 10 when target already reached
	res3 := bench.claimConsumed(10)
	if res3 != 0 {
		t.Errorf("expected accepted to be 0, got %d", res3)
	}
}

func TestSortRanges(t *testing.T) {
	ranges := []AcknowledgedRange{
		{Partition: 0, Base: 10, Last: 19},
		{Partition: 0, Base: 0, Last: 9},
		{Partition: 0, Base: 30, Last: 39},
		{Partition: 0, Base: 20, Last: 29},
	}

	sortRanges(ranges)

	for i := 0; i < len(ranges); i++ {
		expectedBase := uint64(i * 10)
		if ranges[i].Base != expectedBase {
			t.Errorf("index %d: expected Base %d, got %d", i, expectedBase, ranges[i].Base)
		}
	}
}

func Test64BitRandomNoPanic(t *testing.T) {
	sampler := NewSampler(10, 999)
	sampler.seen = 1000000000000 // Large uint64 that doesn't fit in 32-bit int

	// Random addition should not panic
	for i := 0; i < 100; i++ {
		sampler.Add(time.Duration(rand.Intn(100)) * time.Millisecond)
	}
}

func TestLatencySampler_EmptyReport(t *testing.T) {
	sampler := NewSampler(10, 1)
	report := sampler.GetReport()
	if fmt.Sprintf("%v", report) != "{0 0 0 0 0 0}" {
		t.Errorf("expected empty report to be all zero values, got %v", report)
	}
}

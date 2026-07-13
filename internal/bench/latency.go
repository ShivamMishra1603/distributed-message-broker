package bench

import (
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"
)

type LatencyReport struct {
	MinMs  float64 `json:"min_ms"`
	MeanMs float64 `json:"mean_ms"`
	P50Ms  float64 `json:"p50_ms"`
	P95Ms  float64 `json:"p95_ms"`
	P99Ms  float64 `json:"p99_ms"`
	MaxMs  float64 `json:"max_ms"`
}

type Sampler struct {
	mu      sync.Mutex
	seen    uint64
	samples []time.Duration
	maxSize int
	rng     *rand.Rand
}

func NewSampler(maxSize int, seed int64) *Sampler {
	return &Sampler{
		samples: make([]time.Duration, 0, maxSize),
		maxSize: maxSize,
		rng:     rand.New(rand.NewSource(seed)),
	}
}

func (s *Sampler) Add(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seen++
	if len(s.samples) < s.maxSize {
		s.samples = append(s.samples, d)
	} else {
		val := s.rng.Uint64() % s.seen
		if val < uint64(s.maxSize) {
			s.samples[val] = d
		}
	}
}

func (s *Sampler) GetReport() LatencyReport {
	s.mu.Lock()
	n := len(s.samples)
	if n == 0 {
		s.mu.Unlock()
		return LatencyReport{}
	}
	sorted := make([]time.Duration, n)
	copy(sorted, s.samples)
	s.mu.Unlock()

	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})

	minVal := sorted[0]
	maxVal := sorted[n-1]
	var sum time.Duration
	for _, d := range sorted {
		sum += d
	}
	meanVal := time.Duration(int64(sum) / int64(n))

	p50 := getPercentile(sorted, 0.50)
	p95 := getPercentile(sorted, 0.95)
	p99 := getPercentile(sorted, 0.99)

	return LatencyReport{
		MinMs:  durationToMs(minVal),
		MeanMs: durationToMs(meanVal),
		P50Ms:  durationToMs(p50),
		P95Ms:  durationToMs(p95),
		P99Ms:  durationToMs(p99),
		MaxMs:  durationToMs(maxVal),
	}
}

func getPercentile(sorted []time.Duration, p float64) time.Duration {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	idx := int(math.Ceil(p*float64(n))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= n {
		idx = n - 1
	}
	return sorted[idx]
}

func durationToMs(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

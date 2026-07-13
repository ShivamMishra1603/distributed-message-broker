package bench

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	brokerpb "github.com/ShivamMishra1603/distributed-message-broker/gen/proto/broker/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

type AcknowledgedRange struct {
	Partition uint32
	Base      uint64
	Last      uint64
}

type ProducerBench struct {
	conn           *grpc.ClientConn
	topic          string
	msgSize        int
	total          int64
	duration       time.Duration
	concurrency    int
	batchSize      int
	partition      int // -1 for round-robin
	failFast       bool
	requestTimeout time.Duration
	flushMode      string
	runID          string
	runNumber      int
	warmup         bool
	commitSHA      string
	dirty          bool
	buildTime      string
	goVersion      string
	osName         string
	archName       string
	brokerVersion  string

	// Latency sampler
	sampler *Sampler

	// State tracking
	nextRecord       int64
	acknowledged     int64
	failed           int64
	rpcCount         int64
	canceledRequests int64

	mu       sync.Mutex
	ranges   []AcknowledgedRange
	errMap   map[string]int64
	crashed  bool
	firstErr error
}

func NewProducerBench(
	conn *grpc.ClientConn,
	topic string,
	msgSize int,
	total int64,
	duration time.Duration,
	concurrency int,
	batchSize int,
	partition int,
	failFast bool,
	requestTimeout time.Duration,
	flushMode string,
	runID string,
	runNumber int,
	warmup bool,
	commitSHA string,
	dirty bool,
	buildTime string,
	goVersion string,
	osName string,
	archName string,
	brokerVersion string,
) *ProducerBench {
	return &ProducerBench{
		conn:           conn,
		topic:          topic,
		msgSize:        msgSize,
		total:          total,
		duration:       duration,
		concurrency:    concurrency,
		batchSize:      batchSize,
		partition:      partition,
		failFast:       failFast,
		requestTimeout: requestTimeout,
		flushMode:      flushMode,
		runID:          runID,
		runNumber:      runNumber,
		warmup:         warmup,
		commitSHA:      commitSHA,
		dirty:          dirty,
		buildTime:      buildTime,
		goVersion:      goVersion,
		osName:         osName,
		archName:       archName,
		brokerVersion:  brokerVersion,
		sampler:        NewSampler(1000000, 12345),
		errMap:         make(map[string]int64),
	}
}

func (p *ProducerBench) Run(ctx context.Context) (*ProducerResult, error) {
	adminClient := brokerpb.NewAdminServiceClient(p.conn)
	brokerClient := brokerpb.NewBrokerServiceClient(p.conn)

	// 1. Discover partition count and baseline log end offsets
	descResp, err := adminClient.DescribeTopic(ctx, &brokerpb.DescribeTopicRequest{Name: p.topic})
	if err != nil {
		return nil, fmt.Errorf("failed to describe topic: %w", err)
	}

	partitionCount := descResp.GetTopic().GetPartitionCount()
	if partitionCount == 0 {
		return nil, fmt.Errorf("topic has 0 partitions configured")
	}

	initialOffsets := make(map[uint32]uint64)
	for _, pm := range descResp.GetTopic().GetPartitions() {
		initialOffsets[pm.GetPartitionId()] = pm.GetLogEndOffset()
	}

	// Validate explicit partition target
	if p.partition != -1 {
		if p.partition < 0 || uint32(p.partition) >= partitionCount {
			return nil, fmt.Errorf("explicit partition %d out of bounds [0, %d)", p.partition, partitionCount)
		}
	}

	// 2. Pre-allocate immutable static payload
	payloadBytes := make([]byte, p.msgSize)
	for i := range payloadBytes {
		payloadBytes[i] = 'x'
	}

	// 3. Initialize worker cancellation context & start barrier
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	var startTime time.Time
	startCh := make(chan struct{})
	var wg sync.WaitGroup

	var partitionCounter uint64

	// Spawn workers
	for i := 0; i < p.concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startCh

			for {
				select {
				case <-runCtx.Done():
					return
				default:
				}

				currentBatchSize := int(p.batchSize)
				if p.duration > 0 {
					if time.Since(startTime) >= p.duration {
						return
					}
					atomic.AddInt64(&p.nextRecord, int64(currentBatchSize))
				} else {
					startIdx := atomic.AddInt64(&p.nextRecord, int64(p.batchSize)) - int64(p.batchSize)
					if startIdx >= p.total {
						return
					}
					remaining := p.total - startIdx
					if remaining < int64(p.batchSize) {
						currentBatchSize = int(remaining)
					}
				}

				// Resolve target partition for this request batch
				var targetPartition uint32
				if p.partition != -1 {
					targetPartition = uint32(p.partition)
				} else {
					nextVal := atomic.AddUint64(&partitionCounter, 1) - 1
					targetPartition = uint32(nextVal % uint64(partitionCount))
				}

				// Build records using same immutable payload
				records := make([]*brokerpb.Record, currentBatchSize)
				for j := 0; j < currentBatchSize; j++ {
					records[j] = &brokerpb.Record{
						Value:     payloadBytes,
						Timestamp: time.Now().UnixMilli(),
					}
				}

				req := &brokerpb.ProduceRequest{
					Topic:     p.topic,
					Partition: targetPartition,
					Records:   records,
				}

				reqStart := time.Now()
				reqCtx, reqCancel := context.WithTimeout(runCtx, p.requestTimeout)

				resp, rpcErr := brokerClient.Produce(reqCtx, req)
				reqCancel()

				atomic.AddInt64(&p.rpcCount, 1)

				if rpcErr != nil {
					// Check if this was a cancel caused by another worker's fail-fast
					if errors.Is(rpcErr, context.Canceled) && runCtx.Err() != nil {
						atomic.AddInt64(&p.canceledRequests, 1)
						continue
					}

					// Real causal error
					atomic.AddInt64(&p.failed, int64(currentBatchSize))

					st, _ := status.FromError(rpcErr)
					codeStr := st.Code().String()

					p.mu.Lock()
					p.errMap[codeStr]++
					if !p.crashed {
						p.crashed = true
						p.firstErr = rpcErr
					}
					p.mu.Unlock()

					if p.failFast {
						cancelRun()
						return
					}
				} else {
					// Success
					elapsed := time.Since(reqStart)
					p.sampler.Add(elapsed)

					atomic.AddInt64(&p.acknowledged, int64(currentBatchSize))

					p.mu.Lock()
					p.ranges = append(p.ranges, AcknowledgedRange{
						Partition: targetPartition,
						Base:      resp.GetBaseOffset(),
						Last:      resp.GetLastOffset(),
					})
					p.mu.Unlock()
				}
			}
		}()
	}

	// Open the start barrier
	startTime = time.Now()
	close(startCh)

	// Wait for workers
	wg.Wait()
	duration := time.Since(startTime)

	// If we crashed with fail-fast, return the causal error
	p.mu.Lock()
	crashed := p.crashed
	firstErr := p.firstErr
	p.mu.Unlock()

	if crashed && p.failFast {
		return nil, fmt.Errorf("fail-fast triggered: %w", firstErr)
	}

	// 4. Verification Check: DescribeTopic to fetch final offsets
	finalDesc, err := adminClient.DescribeTopic(ctx, &brokerpb.DescribeTopicRequest{Name: p.topic})
	if err != nil {
		return nil, fmt.Errorf("failed to describe topic for post-run verification: %w", err)
	}

	finalOffsets := make(map[uint32]uint64)
	for _, pm := range finalDesc.GetTopic().GetPartitions() {
		finalOffsets[pm.GetPartitionId()] = pm.GetLogEndOffset()
	}

	// Verify per-partition offset advancement
	recordsPerPartition := make(map[uint32]int64)
	p.mu.Lock()
	for _, r := range p.ranges {
		recordsPerPartition[r.Partition] += int64(r.Last - r.Base + 1)
	}
	p.mu.Unlock()

	for partID, expectedCount := range recordsPerPartition {
		initial := initialOffsets[partID]
		final := finalOffsets[partID]
		if final-initial != uint64(expectedCount) {
			return nil, fmt.Errorf("offset verification failed on partition %d: expected end-offset delta %d, got %d (initial: %d, final: %d)",
				partID, expectedCount, final-initial, initial, final)
		}
	}

	// Verify offset continuity within partition ranges
	p.mu.Lock()
	partitionRanges := make(map[uint32][]AcknowledgedRange)
	for _, r := range p.ranges {
		partitionRanges[r.Partition] = append(partitionRanges[r.Partition], r)
	}
	p.mu.Unlock()

	// Validate range base/last offset matching sequentially
	for _, ranges := range partitionRanges {
		if len(ranges) <= 1 {
			continue
		}
		// Sort by base offset
		sortRanges(ranges)
		for i := 0; i < len(ranges)-1; i++ {
			if ranges[i].Last+1 != ranges[i+1].Base {
				return nil, fmt.Errorf("offset discontinuity detected: range %d ended at offset %d, next range starts at offset %d",
					i, ranges[i].Last, ranges[i+1].Base)
			}
		}
	}

	// 5. Generate final report
	latReport := p.sampler.GetReport()

	pMode := "round-robin"
	if p.partition != -1 {
		pMode = "explicit"
	}

	acked := atomic.LoadInt64(&p.acknowledged)
	failed := atomic.LoadInt64(&p.failed)
	rpcs := atomic.LoadInt64(&p.rpcCount)

	durationSecs := duration.Seconds()
	var msgsPerSec float64
	var mibPerSec float64
	if durationSecs > 0 {
		msgsPerSec = float64(acked) / durationSecs
		mibPerSec = (float64(acked) * float64(p.msgSize)) / (1024 * 1024) / durationSecs
	}

	p.mu.Lock()
	errsCopy := make(map[string]int64)
	for k, v := range p.errMap {
		errsCopy[k] = v
	}
	p.mu.Unlock()

	if atomic.LoadInt64(&p.canceledRequests) > 0 {
		errsCopy["Canceled"] = atomic.LoadInt64(&p.canceledRequests)
	}

	sysMeta := SystemMetadata{
		RunID:         p.runID,
		RunNumber:     p.runNumber,
		Warmup:        p.warmup,
		Commit:        p.commitSHA,
		Dirty:         p.dirty,
		BuildTime:     p.buildTime,
		GoVersion:     p.goVersion,
		OS:            p.osName,
		Arch:          p.archName,
		BrokerVersion: p.brokerVersion,
	}

	return &ProducerResult{
		Metadata:            sysMeta,
		Topic:               p.topic,
		Partitions:          partitionCount,
		PartitionMode:       pMode,
		FlushMode:           p.flushMode,
		RequestedRecords:    p.total,
		AcknowledgedRecords: acked,
		FailedRecords:       failed,
		RPCCount:            rpcs,
		RecordSizeBytes:     p.msgSize,
		BatchSize:           p.batchSize,
		Concurrency:         p.concurrency,
		DurationSeconds:     durationSecs,
		MessagesPerSecond:   msgsPerSec,
		PayloadMiBPerSecond: mibPerSec,
		Latency:             latReport,
		Errors:              errsCopy,
	}, nil
}

func sortRanges(ranges []AcknowledgedRange) {
	// Simple manual insertion sort or standard sort is fine since range count is bounded
	for i := 1; i < len(ranges); i++ {
		j := i
		for j > 0 && ranges[j-1].Base > ranges[j].Base {
			ranges[j-1], ranges[j] = ranges[j], ranges[j-1]
			j--
		}
	}
}

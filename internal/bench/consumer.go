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

type ConsumerBench struct {
	conn           *grpc.ClientConn
	topic          string
	partition      int // -1 for all partitions
	target         int64
	duration       time.Duration
	maxFetchBytes  int
	concurrency    int
	offset         uint64
	pollInterval   time.Duration
	idleTimeout    time.Duration
	requestTimeout time.Duration
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
	observedRecords  int64
	countedRecords   int64
	overshootRecords int64
	countedBytes     int64
	rpcCount         int64
	failedRequests   int64
	canceledRequests int64

	mu       sync.Mutex
	errMap   map[string]int64
	crashed  bool
	firstErr error
}

func NewConsumerBench(
	conn *grpc.ClientConn,
	topic string,
	partition int,
	target int64,
	duration time.Duration,
	maxFetchBytes int,
	concurrency int,
	offset uint64,
	pollInterval time.Duration,
	idleTimeout time.Duration,
	requestTimeout time.Duration,
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
) *ConsumerBench {
	return &ConsumerBench{
		conn:           conn,
		topic:          topic,
		partition:      partition,
		target:         target,
		duration:       duration,
		maxFetchBytes:  maxFetchBytes,
		concurrency:    concurrency,
		offset:         offset,
		pollInterval:   pollInterval,
		idleTimeout:    idleTimeout,
		requestTimeout: requestTimeout,
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

func (c *ConsumerBench) claimConsumed(recordsReturned int64) int64 {
	for {
		current := atomic.LoadInt64(&c.countedRecords)
		if current >= c.target {
			return 0
		}
		accepted := recordsReturned
		if current+recordsReturned > c.target {
			accepted = c.target - current
		}
		if atomic.CompareAndSwapInt64(&c.countedRecords, current, current+accepted) {
			return accepted
		}
	}
}

func (c *ConsumerBench) Run(ctx context.Context) (*ConsumerResult, error) {
	adminClient := brokerpb.NewAdminServiceClient(c.conn)
	brokerClient := brokerpb.NewBrokerServiceClient(c.conn)

	// 1. Discover target partitions
	descResp, err := adminClient.DescribeTopic(ctx, &brokerpb.DescribeTopicRequest{Name: c.topic})
	if err != nil {
		return nil, fmt.Errorf("failed to describe topic: %w", err)
	}

	partitionCount := descResp.GetTopic().GetPartitionCount()
	if partitionCount == 0 {
		return nil, fmt.Errorf("topic has 0 partitions configured")
	}

	var partitionsToConsume []uint32
	if c.partition != -1 {
		if c.partition < 0 || uint32(c.partition) >= partitionCount {
			return nil, fmt.Errorf("explicit partition %d out of bounds [0, %d)", c.partition, partitionCount)
		}
		partitionsToConsume = append(partitionsToConsume, uint32(c.partition))
	} else {
		for i := uint32(0); i < partitionCount; i++ {
			partitionsToConsume = append(partitionsToConsume, i)
		}
	}

	// 2. Setup workers & synchronization
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	var startTime time.Time
	startCh := make(chan struct{})
	var wg sync.WaitGroup

	partitionChan := make(chan uint32, len(partitionsToConsume))
	for _, p := range partitionsToConsume {
		partitionChan <- p
	}
	close(partitionChan)

	numWorkers := c.concurrency
	if numWorkers > len(partitionsToConsume) {
		numWorkers = len(partitionsToConsume)
	}
	if numWorkers <= 0 {
		numWorkers = 1
	}

	for i := 0; i < numWorkers; i++ {
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

				var partID uint32
				var ok bool
				select {
				case partID, ok = <-partitionChan:
					if !ok {
						return
					}
				case <-runCtx.Done():
					return
				}

				currentOffset := c.offset
				lastActivity := time.Now()

				for {
					if c.duration > 0 {
						if time.Since(startTime) >= c.duration {
							return
						}
					} else {
						if atomic.LoadInt64(&c.countedRecords) >= c.target {
							return
						}
					}

					if time.Since(lastActivity) > c.idleTimeout {
						c.mu.Lock()
						if !c.crashed {
							c.crashed = true
							c.firstErr = fmt.Errorf("partition %d idle timeout exceeded %v without reaching target count", partID, c.idleTimeout)
						}
						c.mu.Unlock()
						cancelRun()
						return
					}

					reqCtx, reqCancel := context.WithTimeout(runCtx, c.requestTimeout)
					reqStart := time.Now()

					resp, rpcErr := brokerClient.Fetch(reqCtx, &brokerpb.FetchRequest{
						Topic:     c.topic,
						Partition: partID,
						Offset:    currentOffset,
						MaxBytes:  uint32(c.maxFetchBytes),
					})
					reqCancel()

					atomic.AddInt64(&c.rpcCount, 1)

					if rpcErr != nil {
						if errors.Is(rpcErr, context.Canceled) && runCtx.Err() != nil {
							atomic.AddInt64(&c.canceledRequests, 1)
							return
						}

						atomic.AddInt64(&c.failedRequests, 1)
						st, _ := status.FromError(rpcErr)
						codeStr := st.Code().String()

						c.mu.Lock()
						c.errMap[codeStr]++
						if !c.crashed {
							c.crashed = true
							c.firstErr = rpcErr
						}
						c.mu.Unlock()

						cancelRun()
						return
					}

					elapsed := time.Since(reqStart)
					c.sampler.Add(elapsed)

					records := resp.GetRecords()
					numRecords := int64(len(records))

					if numRecords > 0 {
						lastActivity = time.Now()

						for idx, rec := range records {
							expected := currentOffset + uint64(idx)
							if rec.GetOffset() != expected {
								c.mu.Lock()
								if !c.crashed {
									c.crashed = true
									c.firstErr = fmt.Errorf("offset continuity failure on partition %d: expected offset %d, got %d",
										partID, expected, rec.GetOffset())
								}
								c.mu.Unlock()
								cancelRun()
								return
							}
						}

						accepted := c.claimConsumed(numRecords)
						overshoot := numRecords - accepted

						atomic.AddInt64(&c.observedRecords, numRecords)
						atomic.AddInt64(&c.overshootRecords, overshoot)

						// Calculate payload bytes of counted records
						countedBytesInBatch := int64(0)
						for j := 0; j < int(accepted); j++ {
							countedBytesInBatch += int64(len(records[j].GetKey()) + len(records[j].GetValue()))
						}
						atomic.AddInt64(&c.countedBytes, countedBytesInBatch)

						currentOffset = records[numRecords-1].GetOffset() + 1
					}

					// Break if we have reached the log end offset of this partition
					if currentOffset >= resp.GetLogEndOffset() {
						if c.duration > 0 {
							// Replay from start offset
							currentOffset = c.offset
							if numRecords == 0 {
								select {
								case <-time.After(c.pollInterval):
								case <-runCtx.Done():
									return
								}
							}
						} else {
							break
						}
					}

					if numRecords == 0 {
						select {
						case <-time.After(c.pollInterval):
						case <-runCtx.Done():
							return
						}
					}
				}
			}
		}()
	}

	// Open the start barrier
	startTime = time.Now()
	close(startCh)

	wg.Wait()
	duration := time.Since(startTime)

	if c.duration == 0 {
		counted := atomic.LoadInt64(&c.countedRecords)
		if counted < c.target {
			return nil, fmt.Errorf("consumer benchmark exhausted all partitions after %d records; target was %d", counted, c.target)
		}
	}

	c.mu.Lock()
	crashed := c.crashed
	firstErr := c.firstErr
	c.mu.Unlock()

	if crashed {
		return nil, fmt.Errorf("consumer benchmark failed: %w", firstErr)
	}

	latReport := c.sampler.GetReport()

	pMode := "all-partitions"
	if c.partition != -1 {
		pMode = "single"
	}

	observed := atomic.LoadInt64(&c.observedRecords)
	counted := atomic.LoadInt64(&c.countedRecords)
	overshoot := atomic.LoadInt64(&c.overshootRecords)
	bytesCounted := atomic.LoadInt64(&c.countedBytes)
	rpcs := atomic.LoadInt64(&c.rpcCount)

	durationSecs := duration.Seconds()
	var msgsPerSec float64
	var mibPerSec float64
	if durationSecs > 0 {
		msgsPerSec = float64(counted) / durationSecs
		mibPerSec = float64(bytesCounted) / (1024 * 1024) / durationSecs
	}

	c.mu.Lock()
	errsCopy := make(map[string]int64)
	for k, v := range c.errMap {
		errsCopy[k] = v
	}
	c.mu.Unlock()

	if atomic.LoadInt64(&c.canceledRequests) > 0 {
		errsCopy["Canceled"] = atomic.LoadInt64(&c.canceledRequests)
	}

	sysMeta := SystemMetadata{
		RunID:         c.runID,
		RunNumber:     c.runNumber,
		Warmup:        c.warmup,
		Commit:        c.commitSHA,
		Dirty:         c.dirty,
		BuildTime:     c.buildTime,
		GoVersion:     c.goVersion,
		OS:            c.osName,
		Arch:          c.archName,
		BrokerVersion: c.brokerVersion,
	}

	return &ConsumerResult{
		Metadata:            sysMeta,
		Topic:               c.topic,
		PartitionMode:       pMode,
		TargetRecords:       c.target,
		ObservedRecords:     observed,
		CountedRecords:      counted,
		OvershootRecords:    overshoot,
		RPCCount:            rpcs,
		MaxFetchBytes:       c.maxFetchBytes,
		Concurrency:         c.concurrency,
		DurationSeconds:     durationSecs,
		MessagesPerSecond:   msgsPerSec,
		PayloadMiBPerSecond: mibPerSec,
		Latency:             latReport,
		Errors:              errsCopy,
	}, nil
}

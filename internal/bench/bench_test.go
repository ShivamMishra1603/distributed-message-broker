package bench

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"testing"
	"time"

	brokerpb "github.com/ShivamMishra1603/distributed-message-broker/gen/proto/broker/v1"
	"github.com/ShivamMishra1603/distributed-message-broker/internal/broker"
	"github.com/ShivamMishra1603/distributed-message-broker/internal/config"
	"github.com/ShivamMishra1603/distributed-message-broker/internal/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

func setupTestBroker(t *testing.T) (*broker.Broker, *grpc.ClientConn, string) {
	dir := t.TempDir()
	log, _ := logger.New("error", "json", io.Discard)

	cfg := config.DefaultConfig()
	cfg.Storage.DataDirectory = dir
	cfg.Broker.GRPCAddress = "127.0.0.1:0"
	cfg.Broker.HTTPAddress = "127.0.0.1:0"

	b, err := broker.New(cfg, log)
	if err != nil {
		t.Fatalf("failed to create broker: %v", err)
	}

	if err := b.Start(); err != nil {
		t.Fatalf("failed to start broker: %v", err)
	}

	conn, err := grpc.Dial(b.GRPCAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		b.Shutdown(context.Background())
		t.Fatalf("failed to connect to broker: %v", err)
	}

	return b, conn, dir
}

func TestConsumer_IntegrationScenarios(t *testing.T) {
	b, conn, _ := setupTestBroker(t)
	defer b.Shutdown(context.Background())
	defer conn.Close()

	adminClient := brokerpb.NewAdminServiceClient(conn)
	brokerClient := brokerpb.NewBrokerServiceClient(conn)

	ctx := context.Background()

	// 1. Create topics
	_, err := adminClient.CreateTopic(ctx, &brokerpb.CreateTopicRequest{
		Name:           "empty-topic",
		PartitionCount: 1,
	})
	if err != nil {
		t.Fatalf("failed to create empty-topic: %v", err)
	}

	_, err = adminClient.CreateTopic(ctx, &brokerpb.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 2,
	})
	if err != nil {
		t.Fatalf("failed to create test-topic: %v", err)
	}

	// 2. Produce 5 records to partition 0 and 5 records to partition 1 on test-topic
	for p := uint32(0); p < 2; p++ {
		var records []*brokerpb.Record
		for i := 0; i < 5; i++ {
			records = append(records, &brokerpb.Record{
				Key:   []byte(fmt.Sprintf("key-%d-%d", p, i)),
				Value: []byte(fmt.Sprintf("value-%d-%d", p, i)),
			})
		}
		_, err = brokerClient.Produce(ctx, &brokerpb.ProduceRequest{
			Topic:     "test-topic",
			Partition: p,
			Records:   records,
		})
		if err != nil {
			t.Fatalf("failed to produce to partition %d: %v", p, err)
		}
	}

	// Sub-Test A: Empty topic (target = 10, should fail because 0 < 10)
	{
		benchmarker := NewConsumerBench(
			conn,
			"empty-topic",
			-1, // all partitions
			10, // target
			0,  // duration = 0
			1024*1024,
			1, // concurrency
			0, // offset
			10*time.Millisecond,
			500*time.Millisecond,
			1*time.Second,
			"test-run", 1, false, "commit", false, "now", "go", "os", "arch", "1.0",
		)
		_, err := benchmarker.Run(ctx)
		if err == nil {
			t.Errorf("expected error for empty topic fetch when target > 0, got nil")
		}
	}

	// Sub-Test B: Target exceeds available (target = 20, topic only has 10, should fail)
	{
		benchmarker := NewConsumerBench(
			conn,
			"test-topic",
			-1,
			20,
			0,
			1024*1024,
			2,
			0,
			10*time.Millisecond,
			500*time.Millisecond,
			1*time.Second,
			"test-run", 1, false, "commit", false, "now", "go", "os", "arch", "1.0",
		)
		_, err := benchmarker.Run(ctx)
		if err == nil {
			t.Errorf("expected error when target exceeds available, got nil")
		}
	}

	// Sub-Test C: Exact target reached (target = 10, topic has 10, should succeed)
	{
		benchmarker := NewConsumerBench(
			conn,
			"test-topic",
			-1,
			10,
			0,
			1024*1024,
			2,
			0,
			10*time.Millisecond,
			500*time.Millisecond,
			1*time.Second,
			"test-run", 1, false, "commit", false, "now", "go", "os", "arch", "1.0",
		)
		res, err := benchmarker.Run(ctx)
		if err != nil {
			t.Errorf("expected success for exact target, got error: %v", err)
		}
		if res == nil || res.CountedRecords != 10 {
			t.Errorf("expected 10 counted records, got %v", res)
		}
	}

	// Sub-Test D: One partition exhausted while another still contains data (target = 7)
	{
		benchmarker := NewConsumerBench(
			conn,
			"test-topic",
			-1,
			7,
			0,
			1024*1024,
			2,
			0,
			10*time.Millisecond,
			500*time.Millisecond,
			1*time.Second,
			"test-run", 1, false, "commit", false, "now", "go", "os", "arch", "1.0",
		)
		res, err := benchmarker.Run(ctx)
		if err != nil {
			t.Errorf("expected success when target is partially met by partitions, got error: %v", err)
		}
		if res == nil || res.CountedRecords != 7 {
			t.Errorf("expected 7 counted records, got %v", res)
		}
	}
}

package broker

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"

	brokerpb "github.com/ShivamMishra1603/distributed-message-broker/gen/proto/broker/v1"
	"github.com/ShivamMishra1603/distributed-message-broker/internal/config"
	"github.com/ShivamMishra1603/distributed-message-broker/internal/logger"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestBroker_E2ERestartAndTelemetry(t *testing.T) {
	dir := t.TempDir()
	log, _ := logger.New("error", "json", io.Discard)

	cfg := config.DefaultConfig()
	cfg.Storage.DataDirectory = dir
	cfg.Broker.GRPCAddress = "127.0.0.1:0"
	cfg.Broker.HTTPAddress = "127.0.0.1:0"
	cfg.Observability.MetricsEnabled = true

	// 1. Start Broker Instance 1
	b1, err := New(cfg, log)
	if err != nil {
		t.Fatalf("failed to create broker 1: %v", err)
	}

	if err := b1.Start(); err != nil {
		t.Fatalf("failed to start broker 1: %v", err)
	}

	// Dynamic addresses
	grpcAddr1 := b1.grpcServer.Address()
	httpAddr1 := b1.httpServer.Address()

	// Verify Readiness on Instance 1
	resp, err := http.Get(fmt.Sprintf("http://%s/readyz", httpAddr1))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected broker 1 ready (200), got %d", resp.StatusCode)
	}

	// 2. Connect via gRPC and create topic
	conn, err := grpc.Dial(grpcAddr1, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	adminClient := brokerpb.NewAdminServiceClient(conn)
	brokerClient := brokerpb.NewBrokerServiceClient(conn)

	_, err = adminClient.CreateTopic(context.Background(), &brokerpb.CreateTopicRequest{
		Name:           "e2e-orders",
		PartitionCount: 2,
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// 3. Produce records
	produceResp, err := brokerClient.Produce(context.Background(), &brokerpb.ProduceRequest{
		Topic:     "e2e-orders",
		Partition: 0,
		Records: []*brokerpb.Record{
			{Key: []byte("key-1"), Value: []byte("value-1")},
			{Key: []byte("key-2"), Value: []byte("value-2")},
			{Key: []byte("key-3"), Value: []byte("value-3")},
		},
	})
	if err != nil {
		t.Fatalf("failed to produce records: %v", err)
	}
	if produceResp.RecordCount != 3 {
		t.Fatalf("expected 3 records produced, got %d", produceResp.RecordCount)
	}

	// 4. Commit consumer offset
	_, err = brokerClient.CommitOffset(context.Background(), &brokerpb.CommitOffsetRequest{
		ConsumerGroup: "order-processor",
		Topic:         "e2e-orders",
		Partition:     0,
		NextOffset:    3,
	})
	if err != nil {
		t.Fatalf("failed to commit offset: %v", err)
	}

	// 5. Assert metrics telemetry on Instance 1 using testutil
	recordsAppended := testutil.ToFloat64(b1.metrics.RecordsAppended)
	if recordsAppended != 3 {
		t.Errorf("expected metric broker_records_appended_total = 3, got %f", recordsAppended)
	}

	produceRequests := testutil.ToFloat64(b1.metrics.ProduceRequests.WithLabelValues("produce", "ok"))
	if produceRequests != 1 {
		t.Errorf("expected metric broker_produce_requests_total = 1, got %f", produceRequests)
	}

	commitsCount := testutil.ToFloat64(b1.metrics.OffsetCommits.WithLabelValues("commit_offset", "ok"))
	if commitsCount != 1 {
		t.Errorf("expected metric broker_offset_commits_total = 1, got %f", commitsCount)
	}

	// Log size gauge check
	logSize := testutil.ToFloat64(b1.metrics.PartitionLogSize.WithLabelValues("e2e-orders", "0"))
	if logSize <= 0 {
		t.Errorf("expected partition log size to be positive, got %f", logSize)
	}

	// Shutdown Instance 1
	if err := b1.Shutdown(context.Background()); err != nil {
		t.Fatalf("failed to shut down broker 1: %v", err)
	}

	// 6. Start Broker Instance 2 (Restart/Recovery E2E)
	b2, err := New(cfg, log)
	if err != nil {
		t.Fatalf("failed to create broker 2: %v", err)
	}

	if err := b2.Start(); err != nil {
		t.Fatalf("failed to start broker 2: %v", err)
	}
	defer b2.Shutdown(context.Background())

	grpcAddr2 := b2.grpcServer.Address()
	httpAddr2 := b2.httpServer.Address()

	// Verify Readiness on restarted Instance 2
	resp2, err := http.Get(fmt.Sprintf("http://%s/readyz", httpAddr2))
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("expected broker 2 ready (200) after restart, got %d", resp2.StatusCode)
	}

	// Connect to Instance 2
	conn2, err := grpc.Dial(grpcAddr2, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()

	brokerClient2 := brokerpb.NewBrokerServiceClient(conn2)

	// 7. Verify committed offset was recovered and is read correctly
	offsetResp, err := brokerClient2.FetchCommittedOffset(context.Background(), &brokerpb.FetchCommittedOffsetRequest{
		ConsumerGroup: "order-processor",
		Topic:         "e2e-orders",
		Partition:     0,
	})
	if err != nil {
		t.Fatalf("failed to fetch committed offset: %v", err)
	}
	if !offsetResp.Found {
		t.Fatal("expected committed offset to be found after restart")
	}
	if offsetResp.NextOffset != 3 {
		t.Fatalf("expected next offset to be 3, got %d", offsetResp.NextOffset)
	}

	// 8. Verify partition log records are recovered and fetched correctly
	fetchResp, err := brokerClient2.Fetch(context.Background(), &brokerpb.FetchRequest{
		Topic:     "e2e-orders",
		Partition: 0,
		Offset:    0,
		MaxBytes:  65536,
	})
	if err != nil {
		t.Fatalf("failed to fetch records from restarted broker: %v", err)
	}
	if len(fetchResp.Records) != 3 {
		t.Fatalf("expected 3 records fetched, got %d", len(fetchResp.Records))
	}
	if string(fetchResp.Records[0].Value) != "value-1" || string(fetchResp.Records[2].Value) != "value-3" {
		t.Errorf("recovered records mismatch")
	}
}

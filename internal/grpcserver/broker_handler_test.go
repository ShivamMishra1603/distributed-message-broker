package grpcserver

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	brokerpb "github.com/ShivamMishra1603/distributed-message-broker/gen/proto/broker/v1"
	"github.com/ShivamMishra1603/distributed-message-broker/internal/config"
	"github.com/ShivamMishra1603/distributed-message-broker/internal/topic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func startBrokerBufServer(t *testing.T, mgr *topic.Manager, storageCfg config.StorageConfig) (brokerpb.BrokerServiceClient, func()) {
	lis := bufconn.Listen(1024 * 1024)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	brokerSrv := NewBrokerServer(logger, mgr, storageCfg)

	s := grpc.NewServer()
	brokerpb.RegisterBrokerServiceServer(s, brokerSrv)

	go func() {
		if err := s.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			t.Errorf("bufconn server serve failed: %v", err)
		}
	}()

	dialer := func(ctx context.Context, addr string) (net.Conn, error) {
		return lis.Dial()
	}

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial bufnet: %v", err)
	}

	client := brokerpb.NewBrokerServiceClient(conn)

	cleanup := func() {
		conn.Close()
		s.Stop()
		lis.Close()
	}

	return client, cleanup
}

func TestBrokerServer_ProduceAndFetch(t *testing.T) {
	dir := t.TempDir()

	storageCfg := config.StorageConfig{
		DataDirectory:      dir,
		MaxRecordBytes:     1000,
		MaxBatchBytes:      5000,
		SegmentMaxBytes:    10000,
		IndexIntervalBytes: 4096,
		FlushMode:          "sync",
	}

	mgr, err := topic.NewManager(dir, storageCfg.SegmentMaxBytes, int64(storageCfg.MaxBatchBytes), storageCfg.IndexIntervalBytes, storageCfg.FlushMode, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	_, err = mgr.CreateTopic("orders", 2, topic.RetentionPolicy{})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	client, cleanup := startBrokerBufServer(t, mgr, storageCfg)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// 1. Produce to valid topic/partition returns base and last offset
	prodResp, err := client.Produce(ctx, &brokerpb.ProduceRequest{
		Topic:     "orders",
		Partition: 0,
		Records: []*brokerpb.Record{
			{Key: []byte("k1"), Value: []byte("v1")},
			{Key: []byte("k2"), Value: []byte("v2")},
		},
	})
	if err != nil {
		t.Fatalf("Produce failed: %v", err)
	}
	if prodResp.GetBaseOffset() != 0 || prodResp.GetLastOffset() != 1 || prodResp.GetRecordCount() != 2 {
		t.Errorf("expected base=0, last=1, count=2; got base=%d, last=%d, count=%d",
			prodResp.GetBaseOffset(), prodResp.GetLastOffset(), prodResp.GetRecordCount())
	}

	// 2. Fetch after produce returns matching records in order
	fetchResp, err := client.Fetch(ctx, &brokerpb.FetchRequest{
		Topic:     "orders",
		Partition: 0,
		Offset:    0,
		MaxBytes:  1000,
	})
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}
	if len(fetchResp.GetRecords()) != 2 {
		t.Fatalf("expected 2 records, got %d", len(fetchResp.GetRecords()))
	}
	if string(fetchResp.GetRecords()[0].GetKey()) != "k1" || string(fetchResp.GetRecords()[1].GetKey()) != "k2" {
		t.Errorf("records returned out of order or invalid: %v", fetchResp.GetRecords())
	}
	if fetchResp.GetLogEndOffset() != 2 {
		t.Errorf("expected log end offset 2, got %d", fetchResp.GetLogEndOffset())
	}

	// 3. Fetch at log-end offset returns empty records
	fetchRespEnd, err := client.Fetch(ctx, &brokerpb.FetchRequest{
		Topic:     "orders",
		Partition: 0,
		Offset:    2,
		MaxBytes:  1000,
	})
	if err != nil {
		t.Fatalf("Fetch at end failed: %v", err)
	}
	if len(fetchRespEnd.GetRecords()) != 0 {
		t.Errorf("expected 0 records at log end, got %d", len(fetchRespEnd.GetRecords()))
	}

	// 4. Fetch beyond log-end returns OutOfRange
	_, err = client.Fetch(ctx, &brokerpb.FetchRequest{
		Topic:     "orders",
		Partition: 0,
		Offset:    3,
		MaxBytes:  1000,
	})
	if err == nil {
		t.Fatal("expected error on out of range fetch, got nil")
	}
	if status.Code(err) != codes.OutOfRange {
		t.Errorf("expected OutOfRange status code, got %v", status.Code(err))
	}
}

func TestBrokerServer_LimitsAndFailures(t *testing.T) {
	dir := t.TempDir()

	storageCfg := config.StorageConfig{
		DataDirectory:      dir,
		MaxRecordBytes:     10,
		MaxBatchBytes:      70, // single encoded batch for 1 record is ~60 bytes
		SegmentMaxBytes:    1000,
		IndexIntervalBytes: 4096,
		FlushMode:          "sync",
	}

	mgr, err := topic.NewManager(dir, storageCfg.SegmentMaxBytes, int64(storageCfg.MaxBatchBytes), storageCfg.IndexIntervalBytes, storageCfg.FlushMode, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	_, _ = mgr.CreateTopic("orders", 1, topic.RetentionPolicy{})

	client, cleanup := startBrokerBufServer(t, mgr, storageCfg)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// 1. Produce to non-existent topic returns NotFound
	_, err = client.Produce(ctx, &brokerpb.ProduceRequest{
		Topic:     "missing",
		Partition: 0,
		Records:   []*brokerpb.Record{{Key: []byte("k"), Value: []byte("v")}},
	})
	if err == nil {
		t.Fatal("expected error on missing topic produce, got nil")
	}
	if status.Code(err) != codes.NotFound {
		t.Errorf("expected NotFound status code, got %v", status.Code(err))
	}

	// 2. Produce to invalid partition returns NotFound
	_, err = client.Produce(ctx, &brokerpb.ProduceRequest{
		Topic:     "orders",
		Partition: 1,
		Records:   []*brokerpb.Record{{Key: []byte("k"), Value: []byte("v")}},
	})
	if err == nil {
		t.Fatal("expected error on invalid partition produce, got nil")
	}
	if status.Code(err) != codes.NotFound {
		t.Errorf("expected NotFound status code, got %v", status.Code(err))
	}

	// 3. Produce empty records returns InvalidArgument
	_, err = client.Produce(ctx, &brokerpb.ProduceRequest{
		Topic:     "orders",
		Partition: 0,
		Records:   []*brokerpb.Record{},
	})
	if err == nil {
		t.Fatal("expected error on empty produce batch, got nil")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument status code, got %v", status.Code(err))
	}

	// 4. Record payload exceeds max_record_bytes
	_, err = client.Produce(ctx, &brokerpb.ProduceRequest{
		Topic:     "orders",
		Partition: 0,
		Records: []*brokerpb.Record{
			{Key: []byte("longerkey"), Value: []byte("longerval")}, // payload total = 18 > 10
		},
	})
	if err == nil {
		t.Fatal("expected error on record exceeding limit, got nil")
	}
	if status.Code(err) != codes.ResourceExhausted {
		t.Errorf("expected ResourceExhausted, got %v", status.Code(err))
	}

	// 5. Batch size exceeds max_batch_bytes (payload under but fully encoded over 70 bytes)
	// Single encoded batch for 2 records takes ~85 bytes.
	_, err = client.Produce(ctx, &brokerpb.ProduceRequest{
		Topic:     "orders",
		Partition: 0,
		Records: []*brokerpb.Record{
			{Key: []byte("k"), Value: []byte("v")},
			{Key: []byte("a"), Value: []byte("b")},
		},
	})
	if err == nil {
		t.Fatal("expected error on batch size exceeding max_batch_bytes, got nil")
	}
	if status.Code(err) != codes.ResourceExhausted {
		t.Errorf("expected ResourceExhausted status code, got %v", status.Code(err))
	}
}

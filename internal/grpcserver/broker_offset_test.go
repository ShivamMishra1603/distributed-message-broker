package grpcserver

import (
	"context"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"testing"
	"time"

	brokerpb "github.com/ShivamMishra1603/distributed-message-broker/gen/proto/broker/v1"
	"github.com/ShivamMishra1603/distributed-message-broker/internal/config"
	"github.com/ShivamMishra1603/distributed-message-broker/internal/offsets"
	"github.com/ShivamMishra1603/distributed-message-broker/internal/topic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func startBrokerBufServerWithStore(t *testing.T, mgr *topic.Manager, ostore *offsets.Store, storageCfg config.StorageConfig) (brokerpb.BrokerServiceClient, func()) {
	lis := bufconn.Listen(1024 * 1024)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	brokerSrv := NewBrokerServer(logger, mgr, ostore, storageCfg)

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

func TestBrokerServer_OffsetsAPI(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr, err := topic.NewManager(dir, 1024*1024, 512*1024, 4096, "sync", logger)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	topicName := "orders"
	_, err = mgr.CreateTopic(topicName, 2, topic.RetentionPolicy{MaxAgeSeconds: 3600, MaxBytes: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	ostore, err := offsets.OpenStore(filepath.Join(dir, "offsets.log"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ostore.Close()

	storageCfg := config.StorageConfig{
		DataDirectory:   dir,
		MaxRecordBytes:  1024,
		MaxBatchBytes:   4096,
		SegmentMaxBytes: 1024 * 1024,
		FlushMode:       "sync",
	}

	client, cleanup := startBrokerBufServerWithStore(t, mgr, ostore, storageCfg)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Fetch committed offset for non-existent group/topic/partition
	fetchRes, err := client.FetchCommittedOffset(ctx, &brokerpb.FetchCommittedOffsetRequest{
		ConsumerGroup: "groupA",
		Topic:         topicName,
		Partition:     0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if fetchRes.GetFound() {
		t.Errorf("expected found=false for new group, got true")
	}

	// 2. Commit offset at logEndOffset = 0 (empty partition)
	_, err = client.CommitOffset(ctx, &brokerpb.CommitOffsetRequest{
		ConsumerGroup: "groupA",
		Topic:         topicName,
		Partition:     0,
		NextOffset:    0,
	})
	if err != nil {
		t.Fatalf("commit offset failed: %v", err)
	}

	// Verify committed
	fetchRes, err = client.FetchCommittedOffset(ctx, &brokerpb.FetchCommittedOffsetRequest{
		ConsumerGroup: "groupA",
		Topic:         topicName,
		Partition:     0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !fetchRes.GetFound() || fetchRes.GetNextOffset() != 0 {
		t.Errorf("expected found=true and nextOffset=0, got found=%v, nextOffset=%d", fetchRes.GetFound(), fetchRes.GetNextOffset())
	}

	// 3. Produce records to advance logEndOffset
	prodRes, err := client.Produce(ctx, &brokerpb.ProduceRequest{
		Topic:     topicName,
		Partition: 0,
		Records: []*brokerpb.Record{
			{Key: []byte("k1"), Value: []byte("v1")},
			{Key: []byte("k2"), Value: []byte("v2")},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = prodRes
	// logEndOffset is now prodRes.LastOffset + 1 = 2

	// 4. Commit offset out of bounds (above logEndOffset)
	_, err = client.CommitOffset(ctx, &brokerpb.CommitOffsetRequest{
		ConsumerGroup: "groupA",
		Topic:         topicName,
		Partition:     0,
		NextOffset:    3,
	})
	if err == nil {
		t.Error("expected error committing out of bounds offset, got nil")
	} else if status.Code(err) != codes.OutOfRange {
		t.Errorf("expected OutOfRange, got status code %v", status.Code(err))
	}

	// 5. Commit valid offset (nextOffset = 2)
	_, err = client.CommitOffset(ctx, &brokerpb.CommitOffsetRequest{
		ConsumerGroup: "groupA",
		Topic:         topicName,
		Partition:     0,
		NextOffset:    2,
	})
	if err != nil {
		t.Fatalf("failed valid commit: %v", err)
	}

	// Fetch again
	fetchRes, err = client.FetchCommittedOffset(ctx, &brokerpb.FetchCommittedOffsetRequest{
		ConsumerGroup: "groupA",
		Topic:         topicName,
		Partition:     0,
	})
	if err != nil || !fetchRes.GetFound() || fetchRes.GetNextOffset() != 2 {
		t.Errorf("failed fetching latest committed offset: err=%v, found=%v, val=%d", err, fetchRes.GetFound(), fetchRes.GetNextOffset())
	}

	// 6. Commit invalid arguments (empty consumer group)
	_, err = client.CommitOffset(ctx, &brokerpb.CommitOffsetRequest{
		ConsumerGroup: "",
		Topic:         topicName,
		Partition:     0,
		NextOffset:    2,
	})
	if err == nil || status.Code(err) != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument for empty group, got %v", err)
	}

	// 7. Commit non-existent topic
	_, err = client.CommitOffset(ctx, &brokerpb.CommitOffsetRequest{
		ConsumerGroup: "groupA",
		Topic:         "missing-topic",
		Partition:     0,
		NextOffset:    2,
	})
	if err == nil || status.Code(err) != codes.NotFound {
		t.Errorf("expected NotFound for missing topic, got %v", err)
	}
}

func TestBrokerServer_OffsetsAPI_UnavailableStore(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr, err := topic.NewManager(dir, 1024*1024, 512*1024, 4096, "sync", logger)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	topicName := "orders"
	_, err = mgr.CreateTopic(topicName, 1, topic.RetentionPolicy{MaxAgeSeconds: 3600, MaxBytes: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	ostore, err := offsets.OpenStore(filepath.Join(dir, "offsets.log"), nil)
	if err != nil {
		t.Fatal(err)
	}

	storageCfg := config.StorageConfig{
		DataDirectory:   dir,
		MaxRecordBytes:  1024,
		MaxBatchBytes:   4096,
		SegmentMaxBytes: 1024 * 1024,
		FlushMode:       "sync",
	}

	client, cleanup := startBrokerBufServerWithStore(t, mgr, ostore, storageCfg)
	defer cleanup()

	// Close the store directly to simulate unavailability
	_ = ostore.Close()

	ctx := context.Background()
	_, err = client.CommitOffset(ctx, &brokerpb.CommitOffsetRequest{
		ConsumerGroup: "groupA",
		Topic:         topicName,
		Partition:     0,
		NextOffset:    0,
	})
	if err == nil || status.Code(err) != codes.Unavailable {
		t.Errorf("expected Unavailable error code for closed store, got status: %v", err)
	}
}

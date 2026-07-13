package grpcserver

import (
	"context"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	brokerpb "github.com/ShivamMishra1603/distributed-message-broker/gen/proto/broker/v1"
	"github.com/ShivamMishra1603/distributed-message-broker/internal/topic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func startAdminBufServer(t *testing.T, mgr *topic.Manager) (brokerpb.AdminServiceClient, func()) {
	lis := bufconn.Listen(1024 * 1024)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	adminSrv := NewAdminServer(logger, mgr)

	s := grpc.NewServer()
	brokerpb.RegisterAdminServiceServer(s, adminSrv)

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

	client := brokerpb.NewAdminServiceClient(conn)

	cleanup := func() {
		conn.Close()
		s.Stop()
		lis.Close()
	}

	return client, cleanup
}

func TestAdminServer_Lifecycle(t *testing.T) {
	dir := t.TempDir()
	mgr, err := topic.NewManager(dir, 1024*1024, 512*1024, 4096, "sync", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	client, cleanup := startAdminBufServer(t, mgr)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// 1. Create topic
	createResp, err := client.CreateTopic(ctx, &brokerpb.CreateTopicRequest{
		Name:           "events",
		PartitionCount: 3,
		Retention: &brokerpb.RetentionPolicy{
			MaxAgeSeconds: 3600,
			MaxBytes:      500000,
		},
	})
	if err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}

	if createResp.GetTopic().GetName() != "events" {
		t.Errorf("expected topic 'events', got %q", createResp.GetTopic().GetName())
	}
	if createResp.GetTopic().GetPartitionCount() != 3 {
		t.Errorf("expected 3 partitions, got %d", createResp.GetTopic().GetPartitionCount())
	}

	// 2. Duplicate CreateTopic returns AlreadyExists
	_, err = client.CreateTopic(ctx, &brokerpb.CreateTopicRequest{
		Name:           "events",
		PartitionCount: 1,
	})
	if err == nil {
		t.Fatal("expected error on duplicate CreateTopic, got nil")
	}
	if status.Code(err) != codes.AlreadyExists {
		t.Errorf("expected AlreadyExists status, got %v", status.Code(err))
	}

	// 3. DescribeTopic for non-existent topic returns NotFound
	_, err = client.DescribeTopic(ctx, &brokerpb.DescribeTopicRequest{Name: "missing"})
	if err == nil {
		t.Fatal("expected error on missing topic description, got nil")
	}
	if status.Code(err) != codes.NotFound {
		t.Errorf("expected NotFound status, got %v", status.Code(err))
	}

	// 4. DescribeTopic for valid topic returns topic details
	descResp, err := client.DescribeTopic(ctx, &brokerpb.DescribeTopicRequest{Name: "events"})
	if err != nil {
		t.Fatalf("DescribeTopic failed: %v", err)
	}
	if descResp.GetTopic().GetName() != "events" {
		t.Errorf("expected name events, got %s", descResp.GetTopic().GetName())
	}
	if len(descResp.GetTopic().GetPartitions()) != 3 {
		t.Errorf("expected 3 partition items, got %d", len(descResp.GetTopic().GetPartitions()))
	}

	// 5. ListTopics returns list of topics sorted
	_, _ = client.CreateTopic(ctx, &brokerpb.CreateTopicRequest{Name: "alerts", PartitionCount: 1})

	listResp, err := client.ListTopics(ctx, &brokerpb.ListTopicsRequest{})
	if err != nil {
		t.Fatalf("ListTopics failed: %v", err)
	}
	if len(listResp.GetTopics()) != 2 {
		t.Fatalf("expected 2 topics, got %d", len(listResp.GetTopics()))
	}
	// Check alphabetical ordering
	if listResp.GetTopics()[0].GetName() != "alerts" || listResp.GetTopics()[1].GetName() != "events" {
		t.Errorf("expected ordering: alerts, events. Got %s, %s",
			listResp.GetTopics()[0].GetName(), listResp.GetTopics()[1].GetName())
	}
}

func TestAdminServer_Validation(t *testing.T) {
	dir := t.TempDir()
	mgr, err := topic.NewManager(dir, 1024*1024, 512*1024, 4096, "sync", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	client, cleanup := startAdminBufServer(t, mgr)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	tests := []struct {
		name    string
		req     *brokerpb.CreateTopicRequest
		wantErr codes.Code
		errSub  string
	}{
		{
			name:    "empty name",
			req:     &brokerpb.CreateTopicRequest{Name: "", PartitionCount: 1},
			wantErr: codes.InvalidArgument,
			errSub:  "topic name cannot be empty",
		},
		{
			name:    "zero partitions",
			req:     &brokerpb.CreateTopicRequest{Name: "topic", PartitionCount: 0},
			wantErr: codes.InvalidArgument,
			errSub:  "partition count must be greater than zero",
		},
		{
			name:    "invalid name format",
			req:     &brokerpb.CreateTopicRequest{Name: "-invalid", PartitionCount: 1},
			wantErr: codes.InvalidArgument,
			errSub:  "invalid topic name",
		},
		{
			name:    "too many partitions",
			req:     &brokerpb.CreateTopicRequest{Name: "valid", PartitionCount: topic.MaxPartitionsPerTopic + 1},
			wantErr: codes.InvalidArgument,
			errSub:  "too many partitions",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := client.CreateTopic(ctx, tt.req)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if status.Code(err) != tt.wantErr {
				t.Errorf("expected code %v, got %v", tt.wantErr, status.Code(err))
			}
			if !strings.Contains(err.Error(), tt.errSub) {
				t.Errorf("expected message containing %q, got %q", tt.errSub, err.Error())
			}
		})
	}
}

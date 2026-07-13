package grpcserver

import (
	"context"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/ShivamMishra1603/distributed-message-broker/internal/config"
	"github.com/ShivamMishra1603/distributed-message-broker/internal/offsets"
	"github.com/ShivamMishra1603/distributed-message-broker/internal/topic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestServer_LifecycleAndHealth(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dir := t.TempDir()
	mgr, err := topic.NewManager(dir, 1024*1024, 512*1024, 4096, "sync", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	ostore, err := offsets.OpenStore(filepath.Join(dir, "offsets.log"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ostore.Close()

	admin := NewAdminServer(logger, mgr)
	broker := NewBrokerServer(logger, mgr, ostore, config.StorageConfig{
		DataDirectory:   dir,
		MaxRecordBytes:  100,
		MaxBatchBytes:   500,
		SegmentMaxBytes: 1000,
		FlushMode:       "sync",
	}, nil)

	srv := New("127.0.0.1:0", logger, admin, broker, 1024*1024, nil)

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Start()
	}()

	// Wait for listener to bind
	var addr string
	for i := 0; i < 50; i++ {
		addr = srv.GetAddress()
		if addr != "127.0.0.1:0" && addr != "" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if addr == "127.0.0.1:0" || addr == "" {
		t.Fatal("failed to get ephemeral bind address")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create gRPC client: %v", err)
	}
	defer conn.Close()

	client := healthpb.NewHealthClient(conn)

	// Query overall health (empty service name)
	resp, err := client.Check(ctx, &healthpb.HealthCheckRequest{Service: ""})
	if err != nil {
		t.Fatalf("health check failed: %v", err)
	}
	if resp.Status != healthpb.HealthCheckResponse_SERVING {
		t.Errorf("expected general status to be SERVING, got %v", resp.Status)
	}

	// Query AdminService health
	resp, err = client.Check(ctx, &healthpb.HealthCheckRequest{Service: "broker.v1.AdminService"})
	if err != nil {
		t.Fatalf("health check for AdminService failed: %v", err)
	}
	if resp.Status != healthpb.HealthCheckResponse_SERVING {
		t.Errorf("expected AdminService status to be SERVING, got %v", resp.Status)
	}

	// Initiate graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer shutdownCancel()

	err = srv.Shutdown(shutdownCtx)
	if err != nil {
		t.Fatalf("shutdown returned error: %v", err)
	}

	// Wait for Start goroutine to finish
	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("Start returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("Start goroutine did not exit after shutdown")
	}

	// Verify health status on internal healthServer is NOT_SERVING
	status, err := srv.healthServer.Check(ctx, &healthpb.HealthCheckRequest{Service: ""})
	if err != nil {
		t.Fatalf("checking health server directly failed: %v", err)
	}
	if status.Status != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Errorf("expected health status to be NOT_SERVING after shutdown, got %v", status.Status)
	}
}

func TestServer_ShutdownTimeoutReturnsError(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dir := t.TempDir()
	mgr, err := topic.NewManager(dir, 1024*1024, 512*1024, 4096, "sync", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	ostore1, err := offsets.OpenStore(filepath.Join(dir, "offsets1.log"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ostore1.Close()

	admin := NewAdminServer(logger, mgr)
	broker := NewBrokerServer(logger, mgr, ostore1, config.StorageConfig{
		DataDirectory:   dir,
		MaxRecordBytes:  100,
		MaxBatchBytes:   500,
		SegmentMaxBytes: 1000,
		FlushMode:       "sync",
	}, nil)

	srv := New("127.0.0.1:0", logger, admin, broker, 1024*1024, nil)

	go func() {
		srv.Start()
	}()

	// Wait for listener to bind
	for i := 0; i < 50; i++ {
		addr := srv.GetAddress()
		if addr != "127.0.0.1:0" && addr != "" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Use an already-cancelled context to force immediate timeout
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = srv.Shutdown(ctx)
	if err == nil {
		t.Error("expected error from timed-out shutdown, got nil")
	}
}

func TestServer_BindError(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dir := t.TempDir()
	mgr, err := topic.NewManager(dir, 1024*1024, 512*1024, 4096, "sync", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	ostore2, err := offsets.OpenStore(filepath.Join(dir, "offsets2.log"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ostore2.Close()

	admin := NewAdminServer(logger, mgr)
	broker := NewBrokerServer(logger, mgr, ostore2, config.StorageConfig{
		DataDirectory:   dir,
		MaxRecordBytes:  100,
		MaxBatchBytes:   500,
		SegmentMaxBytes: 1000,
		FlushMode:       "sync",
	}, nil)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to bind test listener: %v", err)
	}
	defer lis.Close()

	srv := New(lis.Addr().String(), logger, admin, broker, 1024*1024, nil)
	err = srv.Start()
	if err == nil {
		t.Error("expected start to return bind error, got nil")
	}
}

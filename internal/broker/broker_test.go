package broker

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/ShivamMishra1603/distributed-message-broker/internal/config"
)

func TestBroker_Lifecycle(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Broker.GRPCAddress = "127.0.0.1:0"
	cfg.Storage.DataDirectory = t.TempDir()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	b, err := New(cfg, logger)
	if err != nil {
		t.Fatalf("failed to create broker: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- b.Start()
	}()

	// Allow server to start
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err = b.Shutdown(ctx)
	if err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("broker Start returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("broker Start did not exit cleanly")
	}
}

func TestBroker_FailedStartRollback(t *testing.T) {
	addr := "127.0.0.1:48293"

	// 1. Create and start broker 1 on addr
	cfg1 := config.DefaultConfig()
	cfg1.Broker.GRPCAddress = addr
	cfg1.Storage.DataDirectory = t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	b1, err := New(cfg1, logger)
	if err != nil {
		t.Fatalf("failed to create broker 1: %v", err)
	}

	b1ErrCh := make(chan error, 1)
	go func() {
		b1ErrCh <- b1.Start()
	}()

	// Let it bind
	time.Sleep(100 * time.Millisecond)

	// 2. Create broker 2 on the same address
	cfg2 := config.DefaultConfig()
	cfg2.Broker.GRPCAddress = addr
	cfg2.Storage.DataDirectory = t.TempDir()

	b2, err := New(cfg2, logger)
	if err != nil {
		t.Fatalf("failed to create broker 2: %v", err)
	}

	// 3. Starting broker 2 must fail (port in use)
	err = b2.Start()
	if err == nil {
		b2.Shutdown(context.Background())
		b1.Shutdown(context.Background())
		t.Fatal("expected b2.Start() to fail with bind error, got nil")
	}

	// 4. Shutdown broker 1 to free the port
	b1.Shutdown(context.Background())
	<-b1ErrCh

	// 5. Try starting broker 2 again; it should succeed because of the rollback
	b2ErrCh := make(chan error, 1)
	go func() {
		b2ErrCh <- b2.Start()
	}()

	// Let it bind
	time.Sleep(100 * time.Millisecond)

	// 6. Graceful shutdown broker 2
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = b2.Shutdown(ctx)
	if err != nil {
		t.Errorf("b2 shutdown failed: %v", err)
	}

	select {
	case err := <-b2ErrCh:
		if err != nil {
			t.Errorf("b2 Start returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("b2 Start did not exit cleanly")
	}
}

func TestBroker_UnexpectedServerCrash(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Broker.GRPCAddress = "127.0.0.1:0"
	cfg.Broker.HTTPAddress = "127.0.0.1:0"
	cfg.Storage.DataDirectory = t.TempDir()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	b, err := New(cfg, logger)
	if err != nil {
		t.Fatalf("failed to create broker: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- b.Start()
	}()

	// Allow servers to start
	time.Sleep(100 * time.Millisecond)

	// Simulate unexpected crash of gRPC server by closing its listener directly
	err = b.grpcServer.CloseListener()
	if err != nil {
		t.Fatalf("failed to close listener: %v", err)
	}

	// The broker should detect this crash, cancel maintenance worker, mark ready=false, and shutdown
	// We wait up to 1 second for the broker to transition to stopped = true
	success := false
	for i := 0; i < 50; i++ {
		b.mu.Lock()
		stopped := b.stopped
		b.mu.Unlock()
		if stopped {
			success = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !success {
		t.Error("broker did not auto-shutdown after unexpected server crash")
	}

	// Start should have returned
	select {
	case <-errCh:
		// expected to exit successfully since monitorServe called Shutdown
	case <-time.After(2 * time.Second):
		t.Error("broker Start did not exit after crash")
	}
}

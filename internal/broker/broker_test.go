package broker

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/ShivamMishra1603/distributed-message-broker/internal/config"
)

func TestBroker_Lifecycle(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Broker.GRPCAddress = "127.0.0.1:0"

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

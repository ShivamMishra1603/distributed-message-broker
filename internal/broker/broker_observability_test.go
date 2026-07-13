package broker

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"

	"github.com/ShivamMishra1603/distributed-message-broker/internal/config"
	"github.com/ShivamMishra1603/distributed-message-broker/internal/logger"
)

func TestBroker_TransactionalStartupRollback(t *testing.T) {
	log, _ := logger.New("error", "json", io.Discard)

	t.Run("HTTP port collision", func(t *testing.T) {
		dir := t.TempDir()

		// Pre-bind HTTP port
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer lis.Close()

		cfg := config.DefaultConfig()
		cfg.Storage.DataDirectory = dir
		cfg.Broker.HTTPAddress = lis.Addr().String()
		cfg.Broker.GRPCAddress = "127.0.0.1:0" // dynamic

		b, err := New(cfg, log)
		if err != nil {
			t.Fatal(err)
		}

		err = b.Start()
		if err == nil {
			t.Fatal("expected start to fail due to HTTP port collision")
		}

		// Verify readiness remains false
		if b.httpServer.IsReady() {
			t.Error("readiness should remain false on failed startup")
		}
	})

	t.Run("gRPC port collision", func(t *testing.T) {
		dir := t.TempDir()

		// Pre-bind gRPC port
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer lis.Close()

		cfg := config.DefaultConfig()
		cfg.Storage.DataDirectory = dir
		cfg.Broker.HTTPAddress = "127.0.0.1:0" // dynamic
		cfg.Broker.GRPCAddress = lis.Addr().String()

		b, err := New(cfg, log)
		if err != nil {
			t.Fatal(err)
		}

		err = b.Start()
		if err == nil {
			t.Fatal("expected start to fail due to gRPC port collision")
		}

		// Verify readiness remains false
		if b.httpServer.IsReady() {
			t.Error("readiness should remain false on failed startup")
		}

		// Verify HTTP listener was closed
		if b.httpServer.Address() == cfg.Broker.HTTPAddress {
			t.Error("expected HTTP server to be closed and not bound to the address")
		}
	})
}

func TestBroker_ObservabilityEndpoints(t *testing.T) {
	log, _ := logger.New("error", "json", io.Discard)

	t.Run("Endpoints Enabled", func(t *testing.T) {
		dir := t.TempDir()
		cfg := config.DefaultConfig()
		cfg.Storage.DataDirectory = dir
		cfg.Broker.HTTPAddress = "127.0.0.1:0"
		cfg.Broker.GRPCAddress = "127.0.0.1:0"
		cfg.Observability.MetricsEnabled = true
		cfg.Observability.PprofEnabled = true

		b, err := New(cfg, log)
		if err != nil {
			t.Fatal(err)
		}

		if err := b.Start(); err != nil {
			t.Fatal(err)
		}
		defer b.Shutdown(context.Background())

		httpAddr := b.httpServer.Address()

		// Check healthz
		resp, err := http.Get(fmt.Sprintf("http://%s/healthz", httpAddr))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected healthz 200, got %d", resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		if string(body) != `{"status":"ok"}` {
			t.Errorf("unexpected healthz body: %q", string(body))
		}

		// Check readyz
		respReady, err := http.Get(fmt.Sprintf("http://%s/readyz", httpAddr))
		if err != nil {
			t.Fatal(err)
		}
		defer respReady.Body.Close()
		if respReady.StatusCode != http.StatusOK {
			t.Errorf("expected readyz 200, got %d", respReady.StatusCode)
		}
		bodyReady, _ := io.ReadAll(respReady.Body)
		if string(bodyReady) != `{"status":"ready"}` {
			t.Errorf("unexpected readyz body: %q", string(bodyReady))
		}

		// Check metrics
		respMetrics, err := http.Get(fmt.Sprintf("http://%s/metrics", httpAddr))
		if err != nil {
			t.Fatal(err)
		}
		defer respMetrics.Body.Close()
		if respMetrics.StatusCode != http.StatusOK {
			t.Errorf("expected metrics 200, got %d", respMetrics.StatusCode)
		}

		// Check pprof enabled
		respPprof, err := http.Get(fmt.Sprintf("http://%s/debug/pprof/", httpAddr))
		if err != nil {
			t.Fatal(err)
		}
		defer respPprof.Body.Close()
		if respPprof.StatusCode != http.StatusOK {
			t.Errorf("expected pprof 200, got %d", respPprof.StatusCode)
		}
	})

	t.Run("Endpoints Disabled", func(t *testing.T) {
		dir := t.TempDir()
		cfg := config.DefaultConfig()
		cfg.Storage.DataDirectory = dir
		cfg.Broker.HTTPAddress = "127.0.0.1:0"
		cfg.Broker.GRPCAddress = "127.0.0.1:0"
		cfg.Observability.MetricsEnabled = false
		cfg.Observability.PprofEnabled = false

		b, err := New(cfg, log)
		if err != nil {
			t.Fatal(err)
		}

		if err := b.Start(); err != nil {
			t.Fatal(err)
		}
		defer b.Shutdown(context.Background())

		httpAddr := b.httpServer.Address()

		// Check metrics returning 404
		respMetrics, err := http.Get(fmt.Sprintf("http://%s/metrics", httpAddr))
		if err != nil {
			t.Fatal(err)
		}
		defer respMetrics.Body.Close()
		if respMetrics.StatusCode != http.StatusNotFound {
			t.Errorf("expected metrics 404, got %d", respMetrics.StatusCode)
		}

		// Check pprof returning 404
		respPprof, err := http.Get(fmt.Sprintf("http://%s/debug/pprof/", httpAddr))
		if err != nil {
			t.Fatal(err)
		}
		defer respPprof.Body.Close()
		if respPprof.StatusCode != http.StatusNotFound {
			t.Errorf("expected pprof 404, got %d", respPprof.StatusCode)
		}
	})
}

package httpserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"sync/atomic"
	"time"

	"github.com/ShivamMishra1603/distributed-message-broker/internal/observability"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Server struct {
	logger   *slog.Logger
	address  string
	server   *http.Server
	listener net.Listener
	ready    atomic.Bool
}

// New constructs an HTTP operations Server wrapper.
func New(address string, logger *slog.Logger, metrics *observability.Metrics, metricsEnabled bool, pprofEnabled bool) *Server {
	mux := http.NewServeMux()

	s := &Server{
		logger:  logger,
		address: address,
	}

	// 1. Register /healthz
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	// 2. Register /readyz
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if s.ready.Load() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ready"}`))
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"not_ready"}`))
		}
	})

	// 3. Register /metrics if enabled
	if metricsEnabled && metrics != nil {
		mux.Handle("/metrics", promhttp.HandlerFor(metrics.Registry, promhttp.HandlerOpts{}))
	} else {
		mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "Metrics endpoint disabled", http.StatusNotFound)
		})
	}

	// 4. Register pprof if enabled
	if pprofEnabled {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	} else {
		// Explicitly return 404 for any /debug/pprof/ route
		mux.HandleFunc("/debug/pprof/", func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "Profiling endpoint disabled", http.StatusNotFound)
		})
	}

	s.server = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	return s
}

// Bind binds the TCP listener on the configured address.
func (s *Server) Bind() error {
	lis, err := net.Listen("tcp", s.address)
	if err != nil {
		return fmt.Errorf("failed to bind HTTP server on %q: %w", s.address, err)
	}
	s.listener = lis
	s.server.Addr = lis.Addr().String()
	return nil
}

// Serve starts the HTTP serving loop on the bound listener.
func (s *Server) Serve() error {
	if s.listener == nil {
		return errors.New("cannot serve without a bound listener, call Bind() first")
	}
	s.logger.Info("HTTP server serving", "address", s.server.Addr)
	return s.server.Serve(s.listener)
}

// Shutdown gracefully stops the HTTP server.
func (s *Server) Shutdown(ctx context.Context) error {
	s.logger.Info("shutting down HTTP server")
	return s.server.Shutdown(ctx)
}

// CloseListener closes the TCP listener manually (useful for transactional rollbacks).
func (s *Server) CloseListener() error {
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

// SetReady sets the atomic readiness flag.
func (s *Server) SetReady(ready bool) {
	s.ready.Store(ready)
}

// IsReady reports current readiness.
func (s *Server) IsReady() bool {
	return s.ready.Load()
}

// Address returns the bound listener address.
func (s *Server) Address() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.address
}

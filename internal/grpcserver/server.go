package grpcserver

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type Server struct {
	logger       *slog.Logger
	address      string
	grpcServer   *grpc.Server
	healthServer *health.Server
	listener     net.Listener
	mu           sync.Mutex
}

// New constructs a Server wrapper and registers the standard gRPC Health service.
func New(address string, logger *slog.Logger) *Server {
	grpcServer := grpc.NewServer()
	healthServer := health.NewServer()

	healthpb.RegisterHealthServer(grpcServer, healthServer)

	// Initially, set service state to NOT_SERVING during setup
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	healthServer.SetServingStatus("broker", healthpb.HealthCheckResponse_NOT_SERVING)

	return &Server{
		logger:       logger,
		address:      address,
		grpcServer:   grpcServer,
		healthServer: healthServer,
	}
}

// Start binds to the TCP port and blocks on grpc.Server.Serve.
func (s *Server) Start() error {
	s.mu.Lock()
	lis, err := net.Listen("tcp", s.address)
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("failed to bind tcp listener on %q: %w", s.address, err)
	}
	s.listener = lis
	s.mu.Unlock()

	s.logger.Info("gRPC server listening", "address", s.address)

	// Mark status as SERVING now that the listener is active and we are about to serve
	s.healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	s.healthServer.SetServingStatus("broker", healthpb.HealthCheckResponse_SERVING)

	err = s.grpcServer.Serve(lis)

	// Ensure cleanup occurs if Serve returns unexpectedly
	s.cleanup()

	if err != nil && err != grpc.ErrServerStopped {
		return fmt.Errorf("gRPC server Serve returned error: %w", err)
	}

	return nil
}

// Shutdown stops the server wrapper. It sets the health status to NOT_SERVING,
// calls GracefulStop, and falls back to Stop() if the context times out.
func (s *Server) Shutdown(ctx context.Context) error {
	s.logger.Info("gRPC server shutdown initiated")

	// 1. Immediately mark health status as NOT_SERVING
	s.healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	s.healthServer.SetServingStatus("broker", healthpb.HealthCheckResponse_NOT_SERVING)

	// 2. Channel to monitor GracefulStop completion
	done := make(chan struct{})
	go func() {
		s.grpcServer.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
		s.logger.Info("gRPC server stopped gracefully")
	case <-ctx.Done():
		s.logger.Warn("graceful shutdown timed out; forcing stop")
		s.grpcServer.Stop()
	}

	s.cleanup()
	return nil
}

// GetAddress returns the actual address the server is listening to (useful for testing on dynamic ports like :0).
func (s *Server) GetAddress() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.address
}

func (s *Server) cleanup() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		s.listener.Close()
		s.listener = nil
	}
}

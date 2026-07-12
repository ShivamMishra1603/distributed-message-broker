package grpcserver

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"

	brokerpb "github.com/ShivamMishra1603/distributed-message-broker/gen/proto/broker/v1"
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

// New constructs a Server wrapper and registers the Admin, Broker, and Health services.
func New(
	address string,
	logger *slog.Logger,
	admin brokerpb.AdminServiceServer,
	broker brokerpb.BrokerServiceServer,
	maxMsgSize int,
) *Server {
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxMsgSize),
		grpc.MaxSendMsgSize(maxMsgSize),
	)
	healthServer := health.NewServer()

	healthpb.RegisterHealthServer(grpcServer, healthServer)
	brokerpb.RegisterAdminServiceServer(grpcServer, admin)
	brokerpb.RegisterBrokerServiceServer(grpcServer, broker)

	// Initially, set overall and service health to NOT_SERVING during setup
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	healthServer.SetServingStatus("broker.v1.AdminService", healthpb.HealthCheckResponse_NOT_SERVING)
	healthServer.SetServingStatus("broker.v1.BrokerService", healthpb.HealthCheckResponse_NOT_SERVING)

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
	s.healthServer.SetServingStatus("broker.v1.AdminService", healthpb.HealthCheckResponse_SERVING)
	s.healthServer.SetServingStatus("broker.v1.BrokerService", healthpb.HealthCheckResponse_SERVING)

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
	s.healthServer.SetServingStatus("broker.v1.AdminService", healthpb.HealthCheckResponse_NOT_SERVING)
	s.healthServer.SetServingStatus("broker.v1.BrokerService", healthpb.HealthCheckResponse_NOT_SERVING)

	// 2. Channel to monitor GracefulStop completion
	done := make(chan struct{})
	go func() {
		s.grpcServer.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
		s.logger.Info("gRPC server stopped gracefully")
		s.cleanup()
		return nil
	case <-ctx.Done():
		s.logger.Warn("graceful shutdown timed out; forcing stop")
		s.grpcServer.Stop()
		s.cleanup()
		return ctx.Err()
	}
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

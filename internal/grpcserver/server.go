package grpcserver

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"

	brokerpb "github.com/ShivamMishra1603/distributed-message-broker/gen/proto/broker/v1"
	"github.com/ShivamMishra1603/distributed-message-broker/internal/observability"
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
	metrics *observability.Metrics,
) *Server {
	var opts []grpc.ServerOption
	opts = append(opts, grpc.MaxRecvMsgSize(maxMsgSize))
	opts = append(opts, grpc.MaxSendMsgSize(maxMsgSize))
	if metrics != nil {
		opts = append(opts, grpc.StatsHandler(NewConnStatsHandler(metrics)))
	}

	grpcServer := grpc.NewServer(opts...)
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

// Bind binds the TCP listener on the configured address.
func (s *Server) Bind() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	lis, err := net.Listen("tcp", s.address)
	if err != nil {
		return fmt.Errorf("failed to bind tcp listener on %q: %w", s.address, err)
	}
	s.listener = lis
	s.address = lis.Addr().String()
	return nil
}

// Serve starts the gRPC serving loop on the bound listener.
func (s *Server) Serve() error {
	s.mu.Lock()
	lis := s.listener
	s.mu.Unlock()
	if lis == nil {
		return fmt.Errorf("cannot serve without a bound listener, call Bind() first")
	}

	s.logger.Info("gRPC server listening", "address", s.address)

	// Mark status as SERVING now that the listener is active and we are about to serve
	s.healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	s.healthServer.SetServingStatus("broker.v1.AdminService", healthpb.HealthCheckResponse_SERVING)
	s.healthServer.SetServingStatus("broker.v1.BrokerService", healthpb.HealthCheckResponse_SERVING)

	err := s.grpcServer.Serve(lis)

	// Ensure cleanup occurs if Serve returns unexpectedly
	s.cleanup()

	if err != nil && err != grpc.ErrServerStopped {
		return fmt.Errorf("gRPC server Serve returned error: %w", err)
	}

	return nil
}

// Start binds to the TCP port and blocks on grpc.Server.Serve.
func (s *Server) Start() error {
	if err := s.Bind(); err != nil {
		return err
	}
	return s.Serve()
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
	return s.address
}

// Address returns the actual address the server is listening to.
func (s *Server) Address() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.address
}

// CloseListener closes the TCP listener manually (useful for transactional rollbacks).
func (s *Server) CloseListener() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		err := s.listener.Close()
		s.listener = nil
		return err
	}
	return nil
}

func (s *Server) cleanup() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		s.listener.Close()
		s.listener = nil
	}
}

package broker

import (
	"context"
	"log/slog"

	"github.com/ShivamMishra1603/distributed-message-broker/internal/config"
	"github.com/ShivamMishra1603/distributed-message-broker/internal/grpcserver"
	"github.com/ShivamMishra1603/distributed-message-broker/internal/topic"
)

type Broker struct {
	cfg          config.Config
	logger       *slog.Logger
	topicManager *topic.Manager
	grpcServer   *grpcserver.Server
}

// New instantiates the Broker orchestrator wiring all packages.
func New(cfg config.Config, logger *slog.Logger) (*Broker, error) {
	topicManager := topic.NewManager()

	admin := grpcserver.NewAdminServer(logger, topicManager)
	brokerSrv := grpcserver.NewBrokerServer(logger, topicManager, cfg.Storage)

	// Set max gRPC message size to MaxBatchBytes + 64 KiB allowance
	maxMsgSize := cfg.Storage.MaxBatchBytes + 65536

	grpcSrv := grpcserver.New(cfg.Broker.GRPCAddress, logger, admin, brokerSrv, maxMsgSize)

	return &Broker{
		cfg:          cfg,
		logger:       logger,
		topicManager: topicManager,
		grpcServer:   grpcSrv,
	}, nil
}

// Start launches the gRPC server.
func (b *Broker) Start() error {
	return b.grpcServer.Start()
}

// Shutdown gracefully stops the broker server.
func (b *Broker) Shutdown(ctx context.Context) error {
	return b.grpcServer.Shutdown(ctx)
}

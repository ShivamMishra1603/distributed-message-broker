package broker

import (
	"context"
	"errors"
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
	topicManager, err := topic.NewManager(
		cfg.Storage.DataDirectory,
		cfg.Storage.SegmentMaxBytes,
		int64(cfg.Storage.MaxBatchBytes),
		cfg.Storage.FlushMode,
		logger,
	)
	if err != nil {
		return nil, err
	}

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

// Shutdown gracefully stops the broker server, then flushes and closes storage.
func (b *Broker) Shutdown(ctx context.Context) error {
	grpcErr := b.grpcServer.Shutdown(ctx)
	storageErr := b.topicManager.Close()
	return errors.Join(grpcErr, storageErr)
}

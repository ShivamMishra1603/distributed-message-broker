package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"github.com/ShivamMishra1603/distributed-message-broker/internal/config"
	"github.com/ShivamMishra1603/distributed-message-broker/internal/grpcserver"
	"github.com/ShivamMishra1603/distributed-message-broker/internal/offsets"
	"github.com/ShivamMishra1603/distributed-message-broker/internal/topic"
)

type Broker struct {
	cfg               config.Config
	logger            *slog.Logger
	topicManager      *topic.Manager
	offsetStore       *offsets.Store
	grpcServer        *grpcserver.Server
	maintenanceCtx    context.Context
	maintenanceCancel context.CancelFunc
	maintenanceWG     sync.WaitGroup
	mu                sync.Mutex
	started           bool
	stopped           bool
}

// New instantiates the Broker orchestrator wiring all packages.
func New(cfg config.Config, logger *slog.Logger) (*Broker, error) {
	topicManager, err := topic.NewManager(
		cfg.Storage.DataDirectory,
		cfg.Storage.SegmentMaxBytes,
		int64(cfg.Storage.MaxBatchBytes),
		cfg.Storage.IndexIntervalBytes,
		cfg.Storage.FlushMode,
		logger,
	)
	if err != nil {
		return nil, err
	}

	topicManager.SetRetentionDefaults(
		uint64(cfg.Retention.DefaultMaxAge.Seconds()),
		cfg.Retention.DefaultMaxPartitionBytes,
	)

	offsetStorePath := filepath.Join(cfg.Storage.DataDirectory, "offsets", "consumer-offsets.log")
	offsetStore, err := offsets.OpenStore(offsetStorePath, nil)
	if err != nil {
		_ = topicManager.Close()
		return nil, fmt.Errorf("failed to open offset store: %w", err)
	}

	admin := grpcserver.NewAdminServer(logger, topicManager)
	brokerSrv := grpcserver.NewBrokerServer(logger, topicManager, offsetStore, cfg.Storage)

	// Set max gRPC message size to MaxBatchBytes + 64 KiB allowance
	maxMsgSize := cfg.Storage.MaxBatchBytes + 65536

	grpcSrv := grpcserver.New(cfg.Broker.GRPCAddress, logger, admin, brokerSrv, maxMsgSize)

	mCtx, mCancel := context.WithCancel(context.Background())

	return &Broker{
		cfg:               cfg,
		logger:            logger,
		topicManager:      topicManager,
		offsetStore:       offsetStore,
		grpcServer:        grpcSrv,
		maintenanceCtx:    mCtx,
		maintenanceCancel: mCancel,
	}, nil
}

// Start launches the gRPC server and starts background maintenance tasks.
func (b *Broker) Start() error {
	b.mu.Lock()
	if b.started {
		b.mu.Unlock()
		return nil
	}
	b.started = true
	b.mu.Unlock()

	b.maintenanceWG.Add(1)
	go b.runRetentionWorker()

	err := b.grpcServer.Start()
	if err != nil {
		b.maintenanceCancel()
		b.maintenanceWG.Wait()

		b.mu.Lock()
		b.started = false
		mCtx, mCancel := context.WithCancel(context.Background())
		b.maintenanceCtx = mCtx
		b.maintenanceCancel = mCancel
		b.mu.Unlock()

		return err
	}
	return nil
}

// Shutdown gracefully stops the broker server, then flushes and closes storage.
func (b *Broker) Shutdown(ctx context.Context) error {
	b.mu.Lock()
	if b.stopped {
		b.mu.Unlock()
		return nil
	}
	b.stopped = true
	b.mu.Unlock()

	b.logger.Info("initiating broker shutdown")

	// 1. Cancel maintenance context
	b.maintenanceCancel()

	// 2. Wait for background workers to exit
	b.maintenanceWG.Wait()

	// 3. Gracefully stop/drain the gRPC server
	grpcErr := b.grpcServer.Shutdown(ctx)

	// 4. Close the offset store
	offsetErr := b.offsetStore.Close()

	// 5. Close topic manager and partition stores
	storageErr := b.topicManager.Close()

	return errors.Join(grpcErr, offsetErr, storageErr)
}

func (b *Broker) runRetentionWorker() {
	defer b.maintenanceWG.Done()

	b.logger.Info("starting background retention worker", "interval", b.cfg.Retention.CheckInterval)

	// Run retention once at startup after recovery
	b.topicManager.ApplyRetention(b.maintenanceCtx)

	ticker := time.NewTicker(b.cfg.Retention.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-b.maintenanceCtx.Done():
			b.logger.Info("stopping background retention worker")
			return
		case <-ticker.C:
			b.topicManager.ApplyRetention(b.maintenanceCtx)
		}
	}
}

package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"github.com/ShivamMishra1603/distributed-message-broker/internal/config"
	"github.com/ShivamMishra1603/distributed-message-broker/internal/grpcserver"
	"github.com/ShivamMishra1603/distributed-message-broker/internal/httpserver"
	"github.com/ShivamMishra1603/distributed-message-broker/internal/observability"
	"github.com/ShivamMishra1603/distributed-message-broker/internal/offsets"
	"github.com/ShivamMishra1603/distributed-message-broker/internal/storage"
	"github.com/ShivamMishra1603/distributed-message-broker/internal/topic"
)

type Broker struct {
	cfg               config.Config
	logger            *slog.Logger
	topicManager      *topic.Manager
	offsetStore       *offsets.Store
	grpcServer        *grpcserver.Server
	httpServer        *httpserver.Server
	metrics           *observability.Metrics
	maintenanceCtx    context.Context
	maintenanceCancel context.CancelFunc
	maintenanceWG     sync.WaitGroup
	mu                sync.Mutex
	started           bool
	stopped           bool
}

// New instantiates the Broker orchestrator wiring all packages.
func New(cfg config.Config, logger *slog.Logger) (*Broker, error) {
	metrics := observability.NewMetrics()

	observerFactory := func(topic string, partitionID uint32) storage.Observer {
		return metrics.NewPartitionObserver(topic, partitionID)
	}

	topicManager, err := topic.NewManager(
		cfg.Storage.DataDirectory,
		cfg.Storage.SegmentMaxBytes,
		int64(cfg.Storage.MaxBatchBytes),
		cfg.Storage.IndexIntervalBytes,
		cfg.Storage.FlushMode,
		logger,
		observerFactory,
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
	brokerSrv := grpcserver.NewBrokerServer(logger, topicManager, offsetStore, cfg.Storage, metrics)

	// Set max gRPC message size to MaxBatchBytes + 64 KiB allowance
	maxMsgSize := cfg.Storage.MaxBatchBytes + 65536

	grpcSrv := grpcserver.New(cfg.Broker.GRPCAddress, logger, admin, brokerSrv, maxMsgSize, metrics)
	httpSrv := httpserver.New(cfg.Broker.HTTPAddress, logger, metrics, cfg.Observability.MetricsEnabled, cfg.Observability.PprofEnabled)

	mCtx, mCancel := context.WithCancel(context.Background())

	return &Broker{
		cfg:               cfg,
		logger:            logger,
		topicManager:      topicManager,
		offsetStore:       offsetStore,
		grpcServer:        grpcSrv,
		httpServer:        httpSrv,
		metrics:           metrics,
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

	// 1. Bind HTTP server listener
	if err := b.httpServer.Bind(); err != nil {
		b.mu.Lock()
		b.started = false
		b.mu.Unlock()
		return err
	}

	// 2. Bind gRPC server listener
	if err := b.grpcServer.Bind(); err != nil {
		_ = b.httpServer.CloseListener()
		b.mu.Lock()
		b.started = false
		b.mu.Unlock()
		return err
	}

	// 3. Start HTTP serving loop
	httpErrCh := make(chan error, 1)
	go func() {
		err := b.httpServer.Serve()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			b.logger.Error("HTTP server stopped unexpectedly", "err", err)
			b.httpServer.SetReady(false)
			httpErrCh <- err
		}
	}()

	// 4. Start gRPC serving loop
	grpcErrCh := make(chan error, 1)
	go func() {
		err := b.grpcServer.Serve()
		if err != nil {
			b.logger.Error("gRPC server stopped unexpectedly", "err", err)
			b.httpServer.SetReady(false)
			grpcErrCh <- err
		}
	}()

	// 5. Start background maintenance tasks
	b.maintenanceWG.Add(1)
	go b.runRetentionWorker()

	// 6. Set ready status
	b.httpServer.SetReady(true)

	// Briefly check if servers failed immediately
	select {
	case err := <-httpErrCh:
		_ = b.Shutdown(context.Background())
		return fmt.Errorf("HTTP server failed immediately: %w", err)
	case err := <-grpcErrCh:
		_ = b.Shutdown(context.Background())
		return fmt.Errorf("gRPC server failed immediately: %w", err)
	case <-time.After(50 * time.Millisecond):
		// started successfully
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

	// 1. Mark readiness to false immediately
	b.httpServer.SetReady(false)

	// 2. Cancel maintenance context
	b.maintenanceCancel()

	// 3. Wait for background workers to exit
	b.maintenanceWG.Wait()

	// 4. Gracefully stop/drain the gRPC server
	grpcErr := b.grpcServer.Shutdown(ctx)

	// 5. Shutdown HTTP server
	httpErr := b.httpServer.Shutdown(ctx)

	// 6. Close the offset store
	offsetErr := b.offsetStore.Close()

	// 7. Close topic manager and partition stores
	storageErr := b.topicManager.Close()

	return errors.Join(grpcErr, httpErr, offsetErr, storageErr)
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

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
	serveWG           sync.WaitGroup
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

	serveErrCh := make(chan error, 2)

	// 3. Start HTTP serving loop
	b.serveWG.Add(1)
	go func() {
		defer b.serveWG.Done()
		err := b.httpServer.Serve()

		b.mu.Lock()
		stopped := b.stopped
		b.mu.Unlock()

		if !stopped {
			b.logger.Error("HTTP server stopped unexpectedly", "err", err)
			b.httpServer.SetReady(false)
			if err == nil {
				err = errors.New("server exited without error")
			}
			select {
			case serveErrCh <- fmt.Errorf("HTTP server crashed: %w", err):
			default:
			}
		}
	}()

	// 4. Start gRPC serving loop
	b.serveWG.Add(1)
	go func() {
		defer b.serveWG.Done()
		err := b.grpcServer.Serve()

		b.mu.Lock()
		stopped := b.stopped
		b.mu.Unlock()

		if !stopped {
			b.logger.Error("gRPC server stopped unexpectedly", "err", err)
			b.httpServer.SetReady(false)
			if err == nil {
				err = errors.New("server exited without error")
			}
			select {
			case serveErrCh <- fmt.Errorf("gRPC server crashed: %w", err):
			default:
			}
		}
	}()

	// 5. Start background maintenance tasks
	b.maintenanceWG.Add(1)
	go b.runRetentionWorker()

	// Briefly check if servers failed immediately during 50ms startup window
	select {
	case err := <-serveErrCh:
		_ = b.Shutdown(context.Background())
		return fmt.Errorf("server failed immediately during startup: %w", err)
	case <-time.After(50 * time.Millisecond):
		// Mark ready now that start has succeeded
		b.httpServer.SetReady(true)
		go b.monitorServe(serveErrCh)
	}

	return nil
}

func (b *Broker) monitorServe(serveErrCh <-chan error) {
	select {
	case err := <-serveErrCh:
		b.mu.Lock()
		alreadyStopping := b.stopped
		b.mu.Unlock()
		if !alreadyStopping {
			b.logger.Error("Fatal serve loop crash detected, shutting down broker", "err", err)
			_ = b.Shutdown(context.Background())
		}
	case <-b.maintenanceCtx.Done():
		// Graceful exit of the monitor loop when maintenance context is cancelled
	}
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

	// 6. Wait for serving loops to complete
	b.serveWG.Wait()

	// 7. Close the offset store
	offsetErr := b.offsetStore.Close()

	// 8. Close topic manager and partition stores
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

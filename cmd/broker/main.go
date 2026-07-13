package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/ShivamMishra1603/distributed-message-broker/internal/broker"
	"github.com/ShivamMishra1603/distributed-message-broker/internal/config"
	"github.com/ShivamMishra1603/distributed-message-broker/internal/logger"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// 1. Parse command line flags
	configPath := flag.String("config", "./config/config.yaml", "path to YAML configuration file")
	flag.Parse()

	// 2. Load Configuration
	cfg, err := config.Load(*configPath)
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	// Apply process-global profiling rates if configured
	if cfg.Observability.MutexProfileFraction > 0 {
		runtime.SetMutexProfileFraction(cfg.Observability.MutexProfileFraction)
	}
	if cfg.Observability.BlockProfileRate > 0 {
		runtime.SetBlockProfileRate(cfg.Observability.BlockProfileRate)
	}

	// 3. Construct Logger
	log, err := logger.New(cfg.Observability.LogLevel, cfg.Observability.LogFormat, os.Stdout)
	if err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}

	log.Info("initializing broker", "config_file", *configPath)

	// 4. Validate Storage Directory
	if err := validateStorageDir(cfg.Storage.DataDirectory); err != nil {
		return fmt.Errorf("storage directory validation failed: %w", err)
	}
	log.Info("storage directory validated successfully", "directory", cfg.Storage.DataDirectory)

	// 5. Initialize Broker Orchestrator
	b, err := broker.New(cfg, log)
	if err != nil {
		return fmt.Errorf("failed to construct broker orchestrator: %w", err)
	}

	// 6. Start Broker Orchestrator
	if err := b.Start(); err != nil {
		return fmt.Errorf("server startup failed: %w", err)
	}

	// 7. Listen for OS Signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	// Block on either a signal or server crash
	select {
	case <-b.Done():
		log.Error("broker stopped unexpectedly")
		return fmt.Errorf("broker stopped unexpectedly")

	case sig := <-sigCh:
		log.Info("os signal received, starting shutdown", "signal", sig.String())

		// Set a shutdown timeout bound context
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.Broker.GracefulShutdownTimeout)
		defer shutdownCancel()

		if err := b.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("error during server shutdown: %w", err)
		}

		log.Info("broker shutdown completed cleanly")
		return nil
	}
}

func validateStorageDir(dir string) error {
	// Create directory if it does not exist
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	// Verify that it is indeed a directory
	info, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("failed to stat data directory: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("configured path is a file, not a directory: %s", dir)
	}

	// Verify that it is writable by creating a temp file
	tmpFile, err := os.CreateTemp(dir, "broker-write-check-*.tmp")
	if err != nil {
		return fmt.Errorf("directory is not writable: %w", err)
	}
	tmpPath := tmpFile.Name()

	// Clean up file if we return early
	defer func() {
		tmpFile.Close()
		os.Remove(tmpPath)
	}()

	testBytes := []byte("write-check")
	if _, err := tmpFile.Write(testBytes); err != nil {
		return fmt.Errorf("failed to write to temp file in data directory: %w", err)
	}
	if err := tmpFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync temp file: %w", err)
	}

	// Verify we can read it back
	readBytes, err := os.ReadFile(tmpPath)
	if err != nil {
		return fmt.Errorf("failed to read back temp file: %w", err)
	}
	if string(readBytes) != string(testBytes) {
		return fmt.Errorf("read bytes mismatch, expected %q, got %q", string(testBytes), string(readBytes))
	}

	return nil
}

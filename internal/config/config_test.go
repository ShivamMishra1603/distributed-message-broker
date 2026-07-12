package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Broker.GRPCAddress != "localhost:50051" {
		t.Errorf("expected default address localhost:50051, got %q", cfg.Broker.GRPCAddress)
	}
	if cfg.Broker.ShutdownTimeoutStr != "15s" {
		t.Errorf("expected default timeout 15s, got %q", cfg.Broker.ShutdownTimeoutStr)
	}
	if cfg.Storage.DataDirectory != "./data" {
		t.Errorf("expected default data dir ./data, got %q", cfg.Storage.DataDirectory)
	}
	if cfg.Storage.MaxRecordBytes != 1048576 {
		t.Errorf("expected default max record bytes 1048576, got %d", cfg.Storage.MaxRecordBytes)
	}
	if cfg.Storage.MaxBatchBytes != 5242880 {
		t.Errorf("expected default max batch bytes 5242880, got %d", cfg.Storage.MaxBatchBytes)
	}
	if cfg.Storage.SegmentMaxBytes != 134217728 {
		t.Errorf("expected default segment max bytes 134217728, got %d", cfg.Storage.SegmentMaxBytes)
	}
	if cfg.Storage.FlushMode != "sync" {
		t.Errorf("expected default flush mode sync, got %q", cfg.Storage.FlushMode)
	}
	if cfg.Observability.LogLevel != "info" {
		t.Errorf("expected default log level info, got %q", cfg.Observability.LogLevel)
	}
	if cfg.Observability.LogFormat != "json" {
		t.Errorf("expected default log format json, got %q", cfg.Observability.LogFormat)
	}
}

func TestLoad_ValidYAML(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")

	yamlContent := `
broker:
  grpc_address: "127.0.0.1:8080"
  graceful_shutdown_timeout: "30s"
storage:
  data_directory: "/tmp/broker-data"
  max_record_bytes: 500000
  max_batch_bytes: 2000000
  segment_max_bytes: 10000000
  flush_mode: "async"
observability:
  log_level: "debug"
  log_format: "text"
`
	if err := os.WriteFile(configPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("failed to write temp config file: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load returned unexpected error: %v", err)
	}

	if cfg.Broker.GRPCAddress != "127.0.0.1:8080" {
		t.Errorf("expected grpc_address to be '127.0.0.1:8080', got %q", cfg.Broker.GRPCAddress)
	}
	if cfg.Broker.GracefulShutdownTimeout != 30*time.Second {
		t.Errorf("expected graceful_shutdown_timeout to be 30s, got %v", cfg.Broker.GracefulShutdownTimeout)
	}
	if cfg.Storage.DataDirectory != "/tmp/broker-data" {
		t.Errorf("expected data_directory to be '/tmp/broker-data', got %q", cfg.Storage.DataDirectory)
	}
	if cfg.Storage.MaxRecordBytes != 500000 {
		t.Errorf("expected max_record_bytes to be 500000, got %d", cfg.Storage.MaxRecordBytes)
	}
	if cfg.Storage.MaxBatchBytes != 2000000 {
		t.Errorf("expected max_batch_bytes to be 2000000, got %d", cfg.Storage.MaxBatchBytes)
	}
	if cfg.Storage.SegmentMaxBytes != 10000000 {
		t.Errorf("expected segment_max_bytes to be 10000000, got %d", cfg.Storage.SegmentMaxBytes)
	}
	if cfg.Storage.FlushMode != "async" {
		t.Errorf("expected flush_mode to be 'async', got %q", cfg.Storage.FlushMode)
	}
}

func TestLoad_EnvOverrides(t *testing.T) {
	t.Setenv("BROKER_GRPC_ADDRESS", "0.0.0.0:9092")
	t.Setenv("BROKER_SHUTDOWN_TIMEOUT", "5s")
	t.Setenv("BROKER_DATA_DIRECTORY", "/var/lib/broker")
	t.Setenv("BROKER_MAX_RECORD_BYTES", "65536")
	t.Setenv("BROKER_MAX_BATCH_BYTES", "262144")
	t.Setenv("BROKER_SEGMENT_MAX_BYTES", "1048576")
	t.Setenv("BROKER_FLUSH_MODE", "async")
	t.Setenv("BROKER_LOG_LEVEL", "warn")
	t.Setenv("BROKER_LOG_FORMAT", "json")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("failed to load config with env variables: %v", err)
	}

	if cfg.Broker.GRPCAddress != "0.0.0.0:9092" {
		t.Errorf("expected address override '0.0.0.0:9092', got %q", cfg.Broker.GRPCAddress)
	}
	if cfg.Storage.MaxRecordBytes != 65536 {
		t.Errorf("expected max record bytes override 65536, got %d", cfg.Storage.MaxRecordBytes)
	}
	if cfg.Storage.MaxBatchBytes != 262144 {
		t.Errorf("expected max batch bytes override 262144, got %d", cfg.Storage.MaxBatchBytes)
	}
	if cfg.Storage.SegmentMaxBytes != 1048576 {
		t.Errorf("expected segment max bytes override 1048576, got %d", cfg.Storage.SegmentMaxBytes)
	}
	if cfg.Storage.FlushMode != "async" {
		t.Errorf("expected flush mode override 'async', got %q", cfg.Storage.FlushMode)
	}
}

func TestLoad_ValidationErrors(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name    string
		setup   func(configPath string)
		wantErr string
	}{
		{
			name: "zero segment max bytes",
			setup: func(path string) {
				t.Setenv("BROKER_SEGMENT_MAX_BYTES", "0")
			},
			wantErr: "storage.segment_max_bytes must be positive",
		},
		{
			name: "negative segment max bytes",
			setup: func(path string) {
				t.Setenv("BROKER_SEGMENT_MAX_BYTES", "-500")
			},
			wantErr: "storage.segment_max_bytes must be positive",
		},
		{
			name: "segment max bytes smaller than max batch bytes",
			setup: func(path string) {
				t.Setenv("BROKER_MAX_RECORD_BYTES", "500")
				t.Setenv("BROKER_MAX_BATCH_BYTES", "2000")
				t.Setenv("BROKER_SEGMENT_MAX_BYTES", "1000")
			},
			wantErr: "storage.segment_max_bytes",
		},
		{
			name: "invalid flush mode",
			setup: func(path string) {
				t.Setenv("BROKER_FLUSH_MODE", "invalid_mode")
			},
			wantErr: "invalid storage.flush_mode",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configPath := filepath.Join(tmpDir, tt.name+".yaml")
			os.Clearenv()

			tt.setup(configPath)

			pathArg := ""
			if _, err := os.Stat(configPath); err == nil {
				pathArg = configPath
			}

			_, err := Load(pathArg)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("expected error containing %q, got %q", tt.wantErr, err.Error())
			}
		})
	}
}

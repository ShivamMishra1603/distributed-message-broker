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
	if cfg.Observability.LogLevel != "debug" {
		t.Errorf("expected log_level to be 'debug', got %q", cfg.Observability.LogLevel)
	}
	if cfg.Observability.LogFormat != "text" {
		t.Errorf("expected log_format to be 'text', got %q", cfg.Observability.LogFormat)
	}
}

func TestLoad_MissingFileReturnsError(t *testing.T) {
	_, err := Load("non_existent_file_path_1234.yaml")
	if err == nil {
		t.Fatal("expected error for non-existent config file, got nil")
	}
	if !strings.Contains(err.Error(), "failed to open config file") {
		t.Errorf("expected open error, got: %v", err)
	}
}

func TestLoad_EmptyPathUsesDefaults(t *testing.T) {
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load with empty path returned error: %v", err)
	}
	if cfg.Broker.GRPCAddress != "localhost:50051" {
		t.Errorf("expected default address localhost:50051, got %q", cfg.Broker.GRPCAddress)
	}
}

func TestLoad_EnvOverrides(t *testing.T) {
	t.Setenv("BROKER_GRPC_ADDRESS", "0.0.0.0:9092")
	t.Setenv("BROKER_SHUTDOWN_TIMEOUT", "5s")
	t.Setenv("BROKER_DATA_DIRECTORY", "/var/lib/broker")
	t.Setenv("BROKER_MAX_RECORD_BYTES", "65536")
	t.Setenv("BROKER_MAX_BATCH_BYTES", "262144")
	t.Setenv("BROKER_LOG_LEVEL", "warn")
	t.Setenv("BROKER_LOG_FORMAT", "json")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("failed to load config with env variables: %v", err)
	}

	if cfg.Broker.GRPCAddress != "0.0.0.0:9092" {
		t.Errorf("expected address override '0.0.0.0:9092', got %q", cfg.Broker.GRPCAddress)
	}
	if cfg.Broker.GracefulShutdownTimeout != 5*time.Second {
		t.Errorf("expected timeout override 5s, got %v", cfg.Broker.GracefulShutdownTimeout)
	}
	if cfg.Storage.DataDirectory != "/var/lib/broker" {
		t.Errorf("expected data directory override '/var/lib/broker', got %q", cfg.Storage.DataDirectory)
	}
	if cfg.Storage.MaxRecordBytes != 65536 {
		t.Errorf("expected max record bytes override 65536, got %d", cfg.Storage.MaxRecordBytes)
	}
	if cfg.Storage.MaxBatchBytes != 262144 {
		t.Errorf("expected max batch bytes override 262144, got %d", cfg.Storage.MaxBatchBytes)
	}
	if cfg.Observability.LogLevel != "warn" {
		t.Errorf("expected log level override 'warn', got %q", cfg.Observability.LogLevel)
	}
	if cfg.Observability.LogFormat != "json" {
		t.Errorf("expected log format override 'json', got %q", cfg.Observability.LogFormat)
	}
}

func TestLoad_UnknownFieldsRejected(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "invalid_config.yaml")

	yamlContent := `
broker:
  grpc_address: "127.0.0.1:8080"
unknown_root_field: "trigger_error"
`
	if err := os.WriteFile(configPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("failed to write temp config file: %v", err)
	}

	_, err := Load(configPath)
	if err == nil {
		t.Fatal("expected error due to unknown fields, got nil")
	}
	if !strings.Contains(err.Error(), "field unknown_root_field not found") && !strings.Contains(err.Error(), "not found in type config.Config") {
		t.Errorf("expected unknown field error, got: %v", err)
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
			name: "empty address",
			setup: func(path string) {
				yamlContent := `
broker:
  grpc_address: ""
`
				os.WriteFile(path, []byte(yamlContent), 0644)
			},
			wantErr: "broker.grpc_address cannot be empty",
		},
		{
			name: "invalid timeout format",
			setup: func(path string) {
				t.Setenv("BROKER_SHUTDOWN_TIMEOUT", "invalid_duration")
			},
			wantErr: "invalid broker.graceful_shutdown_timeout",
		},
		{
			name: "negative timeout duration",
			setup: func(path string) {
				t.Setenv("BROKER_SHUTDOWN_TIMEOUT", "-5s")
			},
			wantErr: "broker.graceful_shutdown_timeout must be positive",
		},
		{
			name: "zero timeout duration",
			setup: func(path string) {
				t.Setenv("BROKER_SHUTDOWN_TIMEOUT", "0s")
			},
			wantErr: "broker.graceful_shutdown_timeout must be positive",
		},
		{
			name: "empty data directory",
			setup: func(path string) {
				yamlContent := `
storage:
  data_directory: ""
`
				os.WriteFile(path, []byte(yamlContent), 0644)
			},
			wantErr: "storage.data_directory cannot be empty",
		},
		{
			name: "zero max record bytes",
			setup: func(path string) {
				t.Setenv("BROKER_MAX_RECORD_BYTES", "0")
			},
			wantErr: "storage.max_record_bytes must be positive",
		},
		{
			name: "negative max record bytes",
			setup: func(path string) {
				t.Setenv("BROKER_MAX_RECORD_BYTES", "-100")
			},
			wantErr: "storage.max_record_bytes must be positive",
		},
		{
			name: "max batch bytes smaller than max record bytes",
			setup: func(path string) {
				t.Setenv("BROKER_MAX_RECORD_BYTES", "200")
				t.Setenv("BROKER_MAX_BATCH_BYTES", "100")
			},
			wantErr: "cannot be less than storage.max_record_bytes",
		},
		{
			name: "invalid log level",
			setup: func(path string) {
				t.Setenv("BROKER_LOG_LEVEL", "fatal")
			},
			wantErr: "invalid observability.log_level",
		},
		{
			name: "invalid log format",
			setup: func(path string) {
				t.Setenv("BROKER_LOG_FORMAT", "xml")
			},
			wantErr: "invalid observability.log_format",
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

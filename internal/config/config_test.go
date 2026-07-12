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
	if !strings.Contains(err.Error(), "failed to read config file") {
		t.Errorf("expected read error, got: %v", err)
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
	if cfg.Observability.LogLevel != "warn" {
		t.Errorf("expected log level override 'warn', got %q", cfg.Observability.LogLevel)
	}
	if cfg.Observability.LogFormat != "json" {
		t.Errorf("expected log format override 'json', got %q", cfg.Observability.LogFormat)
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
			// Clear env variables that could interfere
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

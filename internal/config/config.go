package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type BrokerConfig struct {
	GRPCAddress             string        `yaml:"grpc_address"`
	GracefulShutdownTimeout time.Duration `yaml:"-"`
	ShutdownTimeoutStr      string        `yaml:"graceful_shutdown_timeout"`
}

type StorageConfig struct {
	DataDirectory string `yaml:"data_directory"`
}

type ObservabilityConfig struct {
	LogLevel  string `yaml:"log_level"`
	LogFormat string `yaml:"log_format"`
}

type Config struct {
	Broker        BrokerConfig        `yaml:"broker"`
	Storage       StorageConfig       `yaml:"storage"`
	Observability ObservabilityConfig `yaml:"observability"`
}

// DefaultConfig returns a configuration with built-in default values.
func DefaultConfig() Config {
	return Config{
		Broker: BrokerConfig{
			GRPCAddress:        "localhost:50051",
			ShutdownTimeoutStr: "15s",
		},
		Storage: StorageConfig{
			DataDirectory: "./data",
		},
		Observability: ObservabilityConfig{
			LogLevel:  "info",
			LogFormat: "json",
		},
	}
}

// Load loads config from a YAML file (if provided) and overrides with environment variables.
func Load(path string) (Config, error) {
	cfg := DefaultConfig()

	// 1. Read YAML file if path is specified
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return Config{}, fmt.Errorf("failed to read config file %q: %w", path, err)
		}
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			return Config{}, fmt.Errorf("failed to parse yaml config: %w", err)
		}
	}

	// 2. Load from Env Variables
	if val := os.Getenv("BROKER_GRPC_ADDRESS"); val != "" {
		cfg.Broker.GRPCAddress = val
	}
	if val := os.Getenv("BROKER_SHUTDOWN_TIMEOUT"); val != "" {
		cfg.Broker.ShutdownTimeoutStr = val
	}
	if val := os.Getenv("BROKER_DATA_DIRECTORY"); val != "" {
		cfg.Storage.DataDirectory = val
	}
	if val := os.Getenv("BROKER_LOG_LEVEL"); val != "" {
		cfg.Observability.LogLevel = val
	}
	if val := os.Getenv("BROKER_LOG_FORMAT"); val != "" {
		cfg.Observability.LogFormat = val
	}

	// 3. Validation and parsing of Durations
	if cfg.Broker.GRPCAddress == "" {
		return Config{}, fmt.Errorf("broker.grpc_address cannot be empty")
	}

	timeout, err := time.ParseDuration(cfg.Broker.ShutdownTimeoutStr)
	if err != nil {
		return Config{}, fmt.Errorf("invalid broker.graceful_shutdown_timeout %q: %w", cfg.Broker.ShutdownTimeoutStr, err)
	}
	if timeout <= 0 {
		return Config{}, fmt.Errorf("broker.graceful_shutdown_timeout must be positive, got %v", timeout)
	}
	cfg.Broker.GracefulShutdownTimeout = timeout

	if cfg.Storage.DataDirectory == "" {
		return Config{}, fmt.Errorf("storage.data_directory cannot be empty")
	}

	logLevel := strings.ToLower(cfg.Observability.LogLevel)
	switch logLevel {
	case "debug", "info", "warn", "error":
		cfg.Observability.LogLevel = logLevel
	default:
		return Config{}, fmt.Errorf("invalid observability.log_level %q (must be debug, info, warn, error)", cfg.Observability.LogLevel)
	}

	logFormat := strings.ToLower(cfg.Observability.LogFormat)
	switch logFormat {
	case "json", "text":
		cfg.Observability.LogFormat = logFormat
	default:
		return Config{}, fmt.Errorf("invalid observability.log_format %q (must be json, text)", cfg.Observability.LogFormat)
	}

	return cfg, nil
}

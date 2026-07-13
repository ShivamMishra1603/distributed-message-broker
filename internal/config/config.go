package config

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type BrokerConfig struct {
	GRPCAddress             string        `yaml:"grpc_address"`
	HTTPAddress             string        `yaml:"http_address"`
	GracefulShutdownTimeout time.Duration `yaml:"-"`
	ShutdownTimeoutStr      string        `yaml:"graceful_shutdown_timeout"`
}

type StorageConfig struct {
	DataDirectory      string `yaml:"data_directory"`
	MaxRecordBytes     int    `yaml:"max_record_bytes"`
	MaxBatchBytes      int    `yaml:"max_batch_bytes"`
	SegmentMaxBytes    int64  `yaml:"segment_max_bytes"`
	IndexIntervalBytes int    `yaml:"index_interval_bytes"`
	FlushMode          string `yaml:"flush_mode"`
}

type ObservabilityConfig struct {
	LogLevel             string `yaml:"log_level"`
	LogFormat            string `yaml:"log_format"`
	MetricsEnabled       bool   `yaml:"metrics_enabled"`
	PprofEnabled         bool   `yaml:"pprof_enabled"`
	MutexProfileFraction int    `yaml:"mutex_profile_fraction"`
	BlockProfileRate     int    `yaml:"block_profile_rate"`
}

type RetentionConfig struct {
	DefaultMaxAgeStr         string        `yaml:"max_age"`
	DefaultMaxPartitionBytes uint64        `yaml:"max_partition_bytes"`
	CheckIntervalStr         string        `yaml:"check_interval"`
	DefaultMaxAge            time.Duration `yaml:"-"`
	CheckInterval            time.Duration `yaml:"-"`
}

type Config struct {
	Broker        BrokerConfig        `yaml:"broker"`
	Storage       StorageConfig       `yaml:"storage"`
	Retention     RetentionConfig     `yaml:"retention"`
	Observability ObservabilityConfig `yaml:"observability"`
}

// DefaultConfig returns a configuration with built-in default values.
func DefaultConfig() Config {
	return Config{
		Broker: BrokerConfig{
			GRPCAddress:        "localhost:50051",
			HTTPAddress:        "localhost:9093",
			ShutdownTimeoutStr: "15s",
		},
		Storage: StorageConfig{
			DataDirectory:      "./data",
			MaxRecordBytes:     1048576,   // 1 MiB
			MaxBatchBytes:      5242880,   // 5 MiB
			SegmentMaxBytes:    134217728, // 128 MiB
			IndexIntervalBytes: 4096,      // 4 KiB
			FlushMode:          "sync",
		},
		Retention: RetentionConfig{
			DefaultMaxAgeStr:         "168h",
			DefaultMaxPartitionBytes: 10737418240, // 10 GiB
			CheckIntervalStr:         "5m",
			DefaultMaxAge:            168 * time.Hour,
			CheckInterval:            5 * time.Minute,
		},
		Observability: ObservabilityConfig{
			LogLevel:             "info",
			LogFormat:            "json",
			MetricsEnabled:       true,
			PprofEnabled:         false,
			MutexProfileFraction: 0,
			BlockProfileRate:     0,
		},
	}
}

// Load loads config from a YAML file (if provided) and overrides with environment variables.
func Load(path string) (Config, error) {
	cfg := DefaultConfig()

	// 1. Read YAML file if path is specified
	if path != "" {
		file, err := os.Open(path)
		if err != nil {
			if !os.IsNotExist(err) || path != "./config/config.yaml" {
				return Config{}, fmt.Errorf("failed to open config file %q: %w", path, err)
			}
		} else {
			defer file.Close()
			dec := yaml.NewDecoder(file)
			dec.KnownFields(true)
			if err := dec.Decode(&cfg); err != nil {
				return Config{}, fmt.Errorf("failed to parse yaml config: %w", err)
			}
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
	if val := os.Getenv("BROKER_MAX_RECORD_BYTES"); val != "" {
		var bytesVal int
		if _, err := fmt.Sscanf(val, "%d", &bytesVal); err == nil {
			cfg.Storage.MaxRecordBytes = bytesVal
		}
	}
	if val := os.Getenv("BROKER_MAX_BATCH_BYTES"); val != "" {
		var bytesVal int
		if _, err := fmt.Sscanf(val, "%d", &bytesVal); err == nil {
			cfg.Storage.MaxBatchBytes = bytesVal
		}
	}
	if val := os.Getenv("BROKER_SEGMENT_MAX_BYTES"); val != "" {
		var bytesVal int64
		if _, err := fmt.Sscanf(val, "%d", &bytesVal); err == nil {
			cfg.Storage.SegmentMaxBytes = bytesVal
		}
	}
	if val := os.Getenv("BROKER_INDEX_INTERVAL_BYTES"); val != "" {
		var bytesVal int
		if _, err := fmt.Sscanf(val, "%d", &bytesVal); err == nil {
			cfg.Storage.IndexIntervalBytes = bytesVal
		}
	}
	if val := os.Getenv("BROKER_FLUSH_MODE"); val != "" {
		cfg.Storage.FlushMode = val
	}
	if val := os.Getenv("BROKER_LOG_LEVEL"); val != "" {
		cfg.Observability.LogLevel = val
	}
	if val := os.Getenv("BROKER_LOG_FORMAT"); val != "" {
		cfg.Observability.LogFormat = val
	}
	if val := os.Getenv("BROKER_RETENTION_MAX_AGE"); val != "" {
		cfg.Retention.DefaultMaxAgeStr = val
	}
	if val := os.Getenv("BROKER_RETENTION_MAX_PARTITION_BYTES"); val != "" {
		var bytesVal uint64
		if _, err := fmt.Sscanf(val, "%d", &bytesVal); err == nil {
			cfg.Retention.DefaultMaxPartitionBytes = bytesVal
		}
	}
	if val := os.Getenv("BROKER_RETENTION_CHECK_INTERVAL"); val != "" {
		cfg.Retention.CheckIntervalStr = val
	}

	if val := os.Getenv("BROKER_HTTP_ADDRESS"); val != "" {
		cfg.Broker.HTTPAddress = val
	}
	if val := os.Getenv("BROKER_METRICS_ENABLED"); val != "" {
		bVal, err := strconv.ParseBool(val)
		if err != nil {
			return Config{}, fmt.Errorf("invalid BROKER_METRICS_ENABLED %q: %w", val, err)
		}
		cfg.Observability.MetricsEnabled = bVal
	}
	if val := os.Getenv("BROKER_PPROF_ENABLED"); val != "" {
		bVal, err := strconv.ParseBool(val)
		if err != nil {
			return Config{}, fmt.Errorf("invalid BROKER_PPROF_ENABLED %q: %w", val, err)
		}
		cfg.Observability.PprofEnabled = bVal
	}
	if val := os.Getenv("BROKER_MUTEX_PROFILE_FRACTION"); val != "" {
		var intVal int
		if _, err := fmt.Sscanf(val, "%d", &intVal); err == nil {
			cfg.Observability.MutexProfileFraction = intVal
		}
	}
	if val := os.Getenv("BROKER_BLOCK_PROFILE_RATE"); val != "" {
		var intVal int
		if _, err := fmt.Sscanf(val, "%d", &intVal); err == nil {
			cfg.Observability.BlockProfileRate = intVal
		}
	}

	// 3. Validation and parsing of Durations
	if err := validateAddr(cfg.Broker.GRPCAddress, "broker.grpc_address"); err != nil {
		return Config{}, err
	}
	if err := validateAddr(cfg.Broker.HTTPAddress, "broker.http_address"); err != nil {
		return Config{}, err
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

	if cfg.Storage.MaxRecordBytes <= 0 {
		return Config{}, fmt.Errorf("storage.max_record_bytes must be positive, got %d", cfg.Storage.MaxRecordBytes)
	}
	if cfg.Storage.MaxBatchBytes < cfg.Storage.MaxRecordBytes {
		return Config{}, fmt.Errorf("storage.max_batch_bytes (%d) cannot be less than storage.max_record_bytes (%d)", cfg.Storage.MaxBatchBytes, cfg.Storage.MaxRecordBytes)
	}

	if cfg.Storage.SegmentMaxBytes <= 0 {
		return Config{}, fmt.Errorf("storage.segment_max_bytes must be positive, got %d", cfg.Storage.SegmentMaxBytes)
	}
	if cfg.Storage.SegmentMaxBytes < int64(cfg.Storage.MaxBatchBytes) {
		return Config{}, fmt.Errorf("storage.segment_max_bytes (%d) cannot be less than storage.max_batch_bytes (%d)", cfg.Storage.SegmentMaxBytes, cfg.Storage.MaxBatchBytes)
	}

	if cfg.Storage.IndexIntervalBytes <= 0 {
		return Config{}, fmt.Errorf("storage.index_interval_bytes must be positive, got %d", cfg.Storage.IndexIntervalBytes)
	}
	if cfg.Storage.IndexIntervalBytes > int(cfg.Storage.SegmentMaxBytes) {
		return Config{}, fmt.Errorf("storage.index_interval_bytes (%d) cannot be greater than storage.segment_max_bytes (%d)", cfg.Storage.IndexIntervalBytes, cfg.Storage.SegmentMaxBytes)
	}

	flushMode := strings.ToLower(cfg.Storage.FlushMode)
	switch flushMode {
	case "sync", "async":
		cfg.Storage.FlushMode = flushMode
	default:
		return Config{}, fmt.Errorf("invalid storage.flush_mode %q (must be sync, async)", cfg.Storage.FlushMode)
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

	// Retention validations
	maxAge, err := time.ParseDuration(cfg.Retention.DefaultMaxAgeStr)
	if err != nil {
		return Config{}, fmt.Errorf("invalid retention.max_age %q: %w", cfg.Retention.DefaultMaxAgeStr, err)
	}
	if maxAge < time.Second {
		return Config{}, fmt.Errorf("retention.max_age must be at least 1s, got %v", maxAge)
	}
	if maxAge%time.Second != 0 {
		return Config{}, fmt.Errorf("retention.max_age must be a whole number of seconds, got %v", maxAge)
	}
	cfg.Retention.DefaultMaxAge = maxAge

	checkInterval, err := time.ParseDuration(cfg.Retention.CheckIntervalStr)
	if err != nil {
		return Config{}, fmt.Errorf("invalid retention.check_interval %q: %w", cfg.Retention.CheckIntervalStr, err)
	}
	if checkInterval < time.Second {
		return Config{}, fmt.Errorf("retention.check_interval must be at least 1s, got %v", checkInterval)
	}
	cfg.Retention.CheckInterval = checkInterval

	if cfg.Retention.DefaultMaxPartitionBytes <= 0 {
		return Config{}, fmt.Errorf("retention.max_partition_bytes must be positive, got %d", cfg.Retention.DefaultMaxPartitionBytes)
	}

	return cfg, nil
}

func validateAddr(address string, name string) error {
	if address == "" {
		return fmt.Errorf("%s cannot be empty", name)
	}
	_, portStr, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("invalid %s %q: %w", name, address, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 0 || port > 65535 {
		return fmt.Errorf("invalid %s port %q: port must be between 0 and 65535", name, portStr)
	}
	return nil
}

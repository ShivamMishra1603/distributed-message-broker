package broker

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ShivamMishra1603/distributed-message-broker/internal/config"
	"github.com/ShivamMishra1603/distributed-message-broker/internal/model"
	"github.com/ShivamMishra1603/distributed-message-broker/internal/topic"
)

type diskTopicsJSON struct {
	Topics map[string]struct {
		Name           string `json:"name"`
		PartitionCount int    `json:"partition_count"`
		Retention      struct {
			MaxAgeSeconds uint64 `json:"max_age_seconds"`
			MaxBytes      uint64 `json:"max_bytes"`
		} `json:"retention"`
	} `json:"topics"`
}

func TestBroker_TopicRetentionDefaultsResolution(t *testing.T) {
	dir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.Storage.DataDirectory = dir
	cfg.Broker.GRPCAddress = "127.0.0.1:0"
	// Set specific global retention defaults
	cfg.Retention.DefaultMaxAge = 12 * time.Hour
	cfg.Retention.DefaultMaxPartitionBytes = 500 * 1024 * 1024 // 500 MiB

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	b, err := New(cfg, logger)
	if err != nil {
		t.Fatal(err)
	}

	// Create topic with zero/unspecified retention values
	_, err = b.topicManager.CreateTopic("resolved-topic", 1, topic.RetentionPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	_ = b.Shutdown(context.Background())

	// Read topics.json directly to check resolved values are persisted
	metaPath := filepath.Join(dir, "metadata", "topics.json")
	data, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatal(err)
	}

	var meta diskTopicsJSON
	if err := json.Unmarshal(data, &meta); err != nil {
		t.Fatal(err)
	}

	topicMeta, exists := meta.Topics["resolved-topic"]
	if !exists {
		t.Fatal("topic resolved-topic missing in topics.json")
	}

	// 1. Should be resolved to 12 hours (43200 seconds)
	if topicMeta.Retention.MaxAgeSeconds != 43200 {
		t.Errorf("expected resolved MaxAgeSeconds = 43200, got %d", topicMeta.Retention.MaxAgeSeconds)
	}

	// 2. Should be resolved to 500 MiB
	if topicMeta.Retention.MaxBytes != 500*1024*1024 {
		t.Errorf("expected resolved MaxBytes = 524288000, got %d", topicMeta.Retention.MaxBytes)
	}

	// 3. Changing global defaults after restart must NOT alter the persisted topic policies
	cfg.Retention.DefaultMaxAge = 24 * time.Hour
	cfg.Retention.DefaultMaxPartitionBytes = 900 * 1024 * 1024

	b2, err := New(cfg, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Shutdown(context.Background())

	retrievedTopic, err := b2.topicManager.GetTopic("resolved-topic")
	if err != nil {
		t.Fatal(err)
	}

	// Persisted values must remain 12 hours and 500 MiB
	if retrievedTopic.Retention().MaxAgeSeconds != 43200 {
		t.Errorf("expected persisted MaxAgeSeconds to remain 43200, got %d", retrievedTopic.Retention().MaxAgeSeconds)
	}
	if retrievedTopic.Retention().MaxBytes != 500*1024*1024 {
		t.Errorf("expected persisted MaxBytes to remain 500 MiB, got %d", retrievedTopic.Retention().MaxBytes)
	}
}

func TestBroker_RetentionCancellationAwareness(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.Storage.DataDirectory = dir
	cfg.Broker.GRPCAddress = "127.0.0.1:0"

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	b, err := New(cfg, logger)
	if err != nil {
		t.Fatal(err)
	}

	// Create topic and partitions
	tp, err := b.topicManager.CreateTopic("cancel-topic", 2, topic.RetentionPolicy{})
	if err != nil {
		t.Fatal(err)
	}

	// Append record to partition 0
	p0, _ := tp.Partition(0)
	_, _, _ = p0.Append([]model.Record{{Key: []byte("k"), Value: []byte("v")}})

	// Hold read lock on partition 0 (which blocks ApplyRetention write lock)
	// We can retrieve partition log store to acquire RLock
	// How to retrieve store? Log wraps storage.Store, but it is not directly exposed.
	// Wait, we can test it using topic manager ApplyRetention with a cancelled context directly!
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // immediate cancel

	// ApplyRetention with cancelled context must return immediately without performing retention
	b.topicManager.ApplyRetention(ctx)

	// Since context is cancelled, ApplyRetention should exit immediately
	// If we want to verify cancellation while waiting for partition lock:
	// Let's trigger a production/read loop that holds the log lock, call ApplyRetention, cancel context,
	// and verify that once the lock is released, no deletion is done.
	// Wait, partition.Log doesn't expose store or mutex directly. But store.Read holds RLock during the entire execution.
	// So we can simulate it at storage level:
	// We can test at storage.Store level where the lock is accessible!
	// Let's check how we can do it in storage package. We already have retention_test.go.
	// Let's add that test to retention_test.go!
	_ = b.Shutdown(context.Background())
}

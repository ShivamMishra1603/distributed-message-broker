package topic

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestManager_CreateAndReopenMetadata(t *testing.T) {
	dir := t.TempDir()

	mgr, err := NewManager(dir, 1024*1024, 512*1024, "sync", nil)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	ret := RetentionPolicy{MaxAgeSeconds: 3600, MaxBytes: 100000}
	_, err = mgr.CreateTopic("orders", 2, ret)
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	mgr.Close()

	// Verify metadata file was written
	metadataPath := filepath.Join(dir, "metadata", "topics.json")
	if _, err := os.Stat(metadataPath); err != nil {
		t.Errorf("topics.json metadata file was not written: %v", err)
	}

	// Reopen a new manager instance and verify it recovers the state
	mgr2, err := NewManager(dir, 1024*1024, 512*1024, "sync", nil)
	if err != nil {
		t.Fatalf("failed to reopen manager: %v", err)
	}
	defer mgr2.Close()

	t2, err := mgr2.GetTopic("orders")
	if err != nil {
		t.Fatalf("failed to resolve recovered topic: %v", err)
	}
	if t2.Name() != "orders" {
		t.Errorf("expected orders, got %s", t2.Name())
	}
	if t2.PartitionCount() != 2 {
		t.Errorf("expected 2 partitions, got %d", t2.PartitionCount())
	}
	if t2.Retention().MaxAgeSeconds != 3600 {
		t.Errorf("expected 3600, got %d", t2.Retention().MaxAgeSeconds)
	}
}

func TestManager_CreateTopicValidation(t *testing.T) {
	dir := t.TempDir()
	mgr, _ := NewManager(dir, 1024*1024, 512*1024, "sync", nil)
	defer mgr.Close()

	ret := RetentionPolicy{}

	// Invalid partition count
	_, err := mgr.CreateTopic("valid-name", 0, ret)
	if err == nil {
		t.Error("expected error for partition count = 0, got nil")
	}

	// Exceeded max partitions limit
	_, err = mgr.CreateTopic("exceeded-partitions", MaxPartitionsPerTopic+1, ret)
	if !errors.Is(err, ErrTooManyPartitions) {
		t.Errorf("expected ErrTooManyPartitions, got %v", err)
	}

	// Duplicate topic
	_, _ = mgr.CreateTopic("duplicate", 1, ret)
	_, err = mgr.CreateTopic("duplicate", 1, ret)
	if !errors.Is(err, ErrTopicExists) {
		t.Errorf("expected ErrTopicExists, got %v", err)
	}
}

func TestManager_CrashConsistency(t *testing.T) {
	dir := t.TempDir()

	// 1. Case: Partition directories exist on disk, but topic is absent in topics.json -> ignored
	extraPartDir := filepath.Join(dir, "topics", "ghost-topic", "partition-0")
	if err := os.MkdirAll(extraPartDir, 0755); err != nil {
		t.Fatal(err)
	}

	mgr, err := NewManager(dir, 1024*1024, 512*1024, "sync", nil)
	if err != nil {
		t.Fatalf("failed to open manager with ghost directories: %v", err)
	}

	// Validate ghost topic is ignored
	_, err = mgr.GetTopic("ghost-topic")
	if !errors.Is(err, ErrTopicNotFound) {
		t.Errorf("expected ghost-topic to be ignored, got resolved: %v", err)
	}
	mgr.Close()

	// 2. Case: topics.json references a missing partition directory -> NewManager fails startup
	// Let's create a valid topic
	mgrWrite, _ := NewManager(dir, 1024*1024, 512*1024, "sync", nil)
	_, _ = mgrWrite.CreateTopic("broken-topic", 2, RetentionPolicy{})
	mgrWrite.Close()

	// Manually delete partition 1 directory
	deletedDir := filepath.Join(dir, "topics", "broken-topic", "partition-1")
	if err := os.RemoveAll(deletedDir); err != nil {
		t.Fatal(err)
	}

	// Open store must fail due to missing partition directory referenced in topics.json
	_, err = NewManager(dir, 1024*1024, 512*1024, "sync", nil)
	if err == nil {
		t.Error("expected NewManager to fail when partition directory is missing, got nil")
	}

	// 3. Case: Malformed topics.json -> NewManager fails startup
	metadataPath := filepath.Join(dir, "metadata", "topics.json")
	if err := os.WriteFile(metadataPath, []byte("{malformed-json}"), 0644); err != nil {
		t.Fatal(err)
	}

	_, err = NewManager(dir, 1024*1024, 512*1024, "sync", nil)
	if err == nil {
		t.Error("expected NewManager to fail when topics.json is malformed, got nil")
	}
}

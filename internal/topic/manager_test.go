package topic

import (
	"errors"
	"testing"
)

func TestManager_CreateTopic(t *testing.T) {
	mgr := NewManager()

	ret := RetentionPolicy{MaxAgeSeconds: 3600, MaxBytes: 100000}
	topic, err := mgr.CreateTopic("orders-v1", 3, ret)
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	if topic.Name() != "orders-v1" {
		t.Errorf("expected topic name orders-v1, got %q", topic.Name())
	}

	if topic.PartitionCount() != 3 {
		t.Errorf("expected partition count 3, got %d", topic.PartitionCount())
	}

	if topic.Retention().MaxAgeSeconds != 3600 {
		t.Errorf("expected retention max age 3600, got %d", topic.Retention().MaxAgeSeconds)
	}

	// Try getting valid partition
	p, err := topic.Partition(0)
	if err != nil {
		t.Fatalf("failed to get partition 0: %v", err)
	}
	if p == nil {
		t.Fatal("expected partition log to be non-nil")
	}

	// Try getting invalid partition
	_, err = topic.Partition(3)
	if !errors.Is(err, ErrPartitionNotFound) {
		t.Errorf("expected ErrPartitionNotFound, got %v", err)
	}
}

func TestManager_CreateTopicValidation(t *testing.T) {
	mgr := NewManager()
	ret := RetentionPolicy{}

	// Invalid partition count
	_, err := mgr.CreateTopic("valid-name", 0, ret)
	if err == nil {
		t.Error("expected error for partition count = 0, got nil")
	}

	// Duplicate topic
	_, err = mgr.CreateTopic("duplicate", 1, ret)
	if err != nil {
		t.Fatalf("failed to create initial topic: %v", err)
	}
	_, err = mgr.CreateTopic("duplicate", 1, ret)
	if !errors.Is(err, ErrTopicExists) {
		t.Errorf("expected ErrTopicExists, got %v", err)
	}

	// Invalid names
	invalidNames := []string{
		"",               // empty
		"-starts-with",   // starts with hyphen
		".starts-with",   // starts with dot
		"invalid/char",   // invalid slash
		"invalid@char",   // invalid @
		string(make([]byte, 256)), // too long (256 chars)
	}

	for _, name := range invalidNames {
		_, err := mgr.CreateTopic(name, 1, ret)
		if !errors.Is(err, ErrInvalidTopicName) {
			t.Errorf("expected ErrInvalidTopicName for %q, got %v", name, err)
		}
	}
}

func TestManager_GetAndListTopics(t *testing.T) {
	mgr := NewManager()
	ret := RetentionPolicy{}

	// Create topics in non-alphabetical order
	_, _ = mgr.CreateTopic("charlie", 1, ret)
	_, _ = mgr.CreateTopic("alpha", 1, ret)
	_, _ = mgr.CreateTopic("bravo", 1, ret)

	// ListTopics should return them alphabetically sorted
	list := mgr.ListTopics()
	if len(list) != 3 {
		t.Fatalf("expected 3 topics, got %d", len(list))
	}
	if list[0].Name() != "alpha" || list[1].Name() != "bravo" || list[2].Name() != "charlie" {
		t.Errorf("expected sorted list (alpha, bravo, charlie), got (%s, %s, %s)",
			list[0].Name(), list[1].Name(), list[2].Name())
	}

	// Resolve topic
	top, err := mgr.GetTopic("bravo")
	if err != nil {
		t.Fatalf("failed to get topic bravo: %v", err)
	}
	if top.Name() != "bravo" {
		t.Errorf("expected bravo, got %s", top.Name())
	}

	// Missing topic
	_, err = mgr.GetTopic("missing")
	if !errors.Is(err, ErrTopicNotFound) {
		t.Errorf("expected ErrTopicNotFound, got %v", err)
	}
}

func TestManager_GetPartition(t *testing.T) {
	mgr := NewManager()
	ret := RetentionPolicy{}

	_, err := mgr.CreateTopic("events", 2, ret)
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Success case
	p, err := mgr.GetPartition("events", 1)
	if err != nil {
		t.Fatalf("failed to resolve partition: %v", err)
	}
	if p == nil {
		t.Fatal("expected resolved partition to be non-nil")
	}

	// Non-existent topic
	_, err = mgr.GetPartition("missing", 0)
	if !errors.Is(err, ErrTopicNotFound) {
		t.Errorf("expected ErrTopicNotFound, got %v", err)
	}

	// Non-existent partition
	_, err = mgr.GetPartition("events", 2)
	if !errors.Is(err, ErrPartitionNotFound) {
		t.Errorf("expected ErrPartitionNotFound, got %v", err)
	}
}

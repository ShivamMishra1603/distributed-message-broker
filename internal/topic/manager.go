package topic

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"sync"

	"github.com/ShivamMishra1603/distributed-message-broker/internal/partition"
)

var (
	ErrTopicExists       = errors.New("topic already exists")
	ErrTopicNotFound     = errors.New("topic not found")
	ErrPartitionNotFound = errors.New("partition not found")
	ErrInvalidTopicName  = errors.New("invalid topic name")
)

var topicNameRegex = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,254}$`)

// RetentionPolicy defines topic retention limits.
type RetentionPolicy struct {
	MaxAgeSeconds uint64
	MaxBytes      uint64
}

// Topic is an immutable representation of a topic's metadata and partitions.
type Topic struct {
	name       string
	partitions []*partition.Log
	retention  RetentionPolicy
}

func (t *Topic) Name() string {
	return t.name
}

func (t *Topic) PartitionCount() int {
	return len(t.partitions)
}

func (t *Topic) Retention() RetentionPolicy {
	return t.retention
}

func (t *Topic) Partition(id uint32) (*partition.Log, error) {
	if id >= uint32(len(t.partitions)) {
		return nil, ErrPartitionNotFound
	}
	return t.partitions[id], nil
}

// Manager orchestrates topic metadata and partition lifetime.
type Manager struct {
	mu     sync.RWMutex
	topics map[string]*Topic
}

// NewManager constructs a Topic Manager.
func NewManager() *Manager {
	return &Manager{
		topics: make(map[string]*Topic),
	}
}

// CreateTopic validates settings and instantiates partition logs.
func (m *Manager) CreateTopic(name string, partitionCount int, retention RetentionPolicy) (*Topic, error) {
	if !topicNameRegex.MatchString(name) {
		return nil, fmt.Errorf("%w: name %q must be 1-255 characters, alphanumeric plus dots/dashes/underscores, starting with alphanumeric", ErrInvalidTopicName, name)
	}

	if partitionCount <= 0 {
		return nil, fmt.Errorf("partition count must be greater than zero, got %d", partitionCount)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.topics[name]; exists {
		return nil, ErrTopicExists
	}

	partitions := make([]*partition.Log, partitionCount)
	for i := 0; i < partitionCount; i++ {
		partitions[i] = partition.NewLog(name, uint32(i), nil)
	}

	t := &Topic{
		name:       name,
		partitions: partitions,
		retention:  retention,
	}

	m.topics[name] = t
	return t, nil
}

// GetTopic returns the topic or ErrTopicNotFound.
func (m *Manager) GetTopic(name string) (*Topic, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	t, exists := m.topics[name]
	if !exists {
		return nil, ErrTopicNotFound
	}
	return t, nil
}

// ListTopics returns all topics sorted alphabetically by name.
func (m *Manager) ListTopics() []*Topic {
	m.mu.RLock()
	defer m.mu.RUnlock()

	list := make([]*Topic, 0, len(m.topics))
	for _, t := range m.topics {
		list = append(list, t)
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].name < list[j].name
	})

	return list
}

// GetPartition helper resolves partition Log directly.
func (m *Manager) GetPartition(topicName string, partitionID uint32) (*partition.Log, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	t, exists := m.topics[topicName]
	if !exists {
		return nil, ErrTopicNotFound
	}

	return t.Partition(partitionID)
}

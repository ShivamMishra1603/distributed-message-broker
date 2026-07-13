package topic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/ShivamMishra1603/distributed-message-broker/internal/partition"
)

const MaxPartitionsPerTopic = 1024

var (
	ErrTopicExists       = errors.New("topic already exists")
	ErrTopicNotFound     = errors.New("topic not found")
	ErrPartitionNotFound = errors.New("partition not found")
	ErrInvalidTopicName  = errors.New("invalid topic name")
	ErrTooManyPartitions = errors.New("too many partitions requested")
)

var topicNameRegex = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,254}$`)

// RetentionPolicy defines topic retention limits.
type RetentionPolicy struct {
	MaxAgeSeconds uint64 `json:"max_age_seconds"`
	MaxBytes      uint64 `json:"max_bytes"`
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

type DiskTopic struct {
	Name           string          `json:"name"`
	PartitionCount int             `json:"partition_count"`
	Retention      RetentionPolicy `json:"retention"`
}

type TopicsMetadata struct {
	Topics map[string]DiskTopic `json:"topics"`
}

// Manager orchestrates topic metadata and partition lifetime on disk.
type Manager struct {
	mu                   sync.RWMutex
	topics               map[string]*Topic
	dataDir              string
	segmentMaxBytes      int64
	maxBatchBytes        int64
	indexIntervalBytes   int
	flushMode            string
	defaultMaxAgeSeconds uint64
	defaultMaxBytes      uint64
	logger               *slog.Logger
}

// SetRetentionDefaults configures defaults for topic retention policies when omitted.
func (m *Manager) SetRetentionDefaults(maxAgeSeconds uint64, maxBytes uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.defaultMaxAgeSeconds = maxAgeSeconds
	m.defaultMaxBytes = maxBytes
}

// NewManager loads metadata, reconstructs partitions from disk, and handles directory mappings.
func NewManager(dataDir string, segmentMaxBytes int64, maxBatchBytes int64, indexIntervalBytes int, flushMode string, logger *slog.Logger) (*Manager, error) {
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(os.Stdout, nil))
	}

	mgr := &Manager{
		topics:             make(map[string]*Topic),
		dataDir:            dataDir,
		segmentMaxBytes:    segmentMaxBytes,
		maxBatchBytes:      maxBatchBytes,
		indexIntervalBytes: indexIntervalBytes,
		flushMode:          flushMode,
		logger:             logger,
	}

	// 1. Read topics.json if exists
	metadataPath := filepath.Join(dataDir, "metadata", "topics.json")
	if _, err := os.Stat(metadataPath); err == nil {
		data, err := os.ReadFile(metadataPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read metadata file: %w", err)
		}

		var meta TopicsMetadata
		if err := json.Unmarshal(data, &meta); err != nil {
			return nil, fmt.Errorf("malformed topics.json: %w", err)
		}

		// Reconstruct each topic from metadata configuration
		for name, dt := range meta.Topics {
			if name != dt.Name {
				mgr.Close()
				return nil, fmt.Errorf("metadata corruption: map key %q does not match topic name %q", name, dt.Name)
			}
			if !topicNameRegex.MatchString(dt.Name) {
				mgr.Close()
				return nil, fmt.Errorf("metadata corruption: topic name %q is invalid", dt.Name)
			}
			if dt.PartitionCount < 1 || dt.PartitionCount > MaxPartitionsPerTopic {
				mgr.Close()
				return nil, fmt.Errorf("metadata corruption: topic %q has invalid partition count %d", dt.Name, dt.PartitionCount)
			}

			partitions := make([]*partition.Log, dt.PartitionCount)
			for i := 0; i < dt.PartitionCount; i++ {
				// Assert that directory exists, otherwise fail startup as per crash-consistency requirements
				pDir := filepath.Join(dataDir, "topics", name, fmt.Sprintf("partition-%d", i))
				if _, err := os.Stat(pDir); os.IsNotExist(err) {
					// Clean up any successfully opened stores in this startup attempt
					for _, p := range partitions {
						if p != nil {
							_ = p.Close()
						}
					}
					mgr.Close()
					return nil, fmt.Errorf("metadata references missing partition directory: %s", pDir)
				}

				pLog, err := partition.NewLog(name, uint32(i), dataDir, segmentMaxBytes, maxBatchBytes, indexIntervalBytes, flushMode, nil)
				if err != nil {
					for _, p := range partitions {
						if p != nil {
							_ = p.Close()
						}
					}
					mgr.Close()
					return nil, fmt.Errorf("failed to open partition store for %s-%d: %w", name, i, err)
				}
				partitions[i] = pLog
			}

			mgr.topics[name] = &Topic{
				name:       dt.Name,
				partitions: partitions,
				retention:  dt.Retention,
			}
		}
	}

	return mgr, nil
}

// CreateTopic implements atomic directory creation and topics.json update transaction.
func (m *Manager) CreateTopic(name string, partitionCount int, retention RetentionPolicy) (*Topic, error) {
	if !topicNameRegex.MatchString(name) {
		return nil, fmt.Errorf("%w: name %q must be 1-255 characters, alphanumeric plus dots/dashes/underscores, starting with alphanumeric", ErrInvalidTopicName, name)
	}

	if partitionCount <= 0 {
		return nil, fmt.Errorf("partition count must be greater than zero, got %d", partitionCount)
	}

	if partitionCount > MaxPartitionsPerTopic {
		return nil, fmt.Errorf("%w: requested %d, maximum is %d", ErrTooManyPartitions, partitionCount, MaxPartitionsPerTopic)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.topics[name]; exists {
		return nil, ErrTopicExists
	}

	if retention.MaxAgeSeconds == 0 {
		retention.MaxAgeSeconds = m.defaultMaxAgeSeconds
	}
	if retention.MaxBytes == 0 {
		retention.MaxBytes = m.defaultMaxBytes
	}

	// 1. Create partition log folders and open stores
	partitions := make([]*partition.Log, partitionCount)
	var createdDirs []string
	var err error

	for i := 0; i < partitionCount; i++ {
		pDir := filepath.Join(m.dataDir, "topics", name, fmt.Sprintf("partition-%d", i))
		createdDirs = append(createdDirs, pDir)

		partitions[i], err = partition.NewLog(name, uint32(i), m.dataDir, m.segmentMaxBytes, m.maxBatchBytes, m.indexIntervalBytes, m.flushMode, nil)
		if err != nil {
			// Rollback partition logs
			for _, p := range partitions {
				if p != nil {
					p.Close()
				}
			}
			// Clean up created directories
			for _, d := range createdDirs {
				os.RemoveAll(d)
			}
			// Clean up main topic directory if empty
			os.Remove(filepath.Join(m.dataDir, "topics", name))
			return nil, fmt.Errorf("failed to initialize partition stores: %w", err)
		}
	}

	topic := &Topic{
		name:       name,
		partitions: partitions,
		retention:  retention,
	}

	// 2. Persist metadata update atomically
	committed, err := m.saveMetadata(name, DiskTopic{
		Name:           name,
		PartitionCount: partitionCount,
		Retention:      retention,
	})
	if err != nil {
		if !committed {
			// Rollback partition logs
			for _, p := range partitions {
				p.Close()
			}
			// Clean up folders
			for _, d := range createdDirs {
				os.RemoveAll(d)
			}
			os.Remove(filepath.Join(m.dataDir, "topics", name))
			return nil, fmt.Errorf("failed to persist topic metadata: %w", err)
		}
		// Rename succeeded, only directory sync failed. Log it and publish the topic.
		m.logger.Warn("metadata directory sync failed but topics.json write succeeded", "err", err)
	}

	// 3. Publish to in-memory registry map
	m.topics[name] = topic
	m.logger.Info("created topic", "name", name, "partitions", partitionCount)

	return topic, nil
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

// Close gracefully flushes and closes all partition logs.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var errs []error
	for _, t := range m.topics {
		for _, p := range t.partitions {
			if err := p.Close(); err != nil {
				errs = append(errs, err)
			}
		}
	}

	return errors.Join(errs...)
}

func (m *Manager) saveMetadata(newName string, newTopic DiskTopic) (committed bool, err error) {
	metadataDir := filepath.Join(m.dataDir, "metadata")
	if err := os.MkdirAll(metadataDir, 0755); err != nil {
		return false, err
	}

	// Read existing metadata list first to merge
	meta := TopicsMetadata{
		Topics: make(map[string]DiskTopic),
	}
	metadataPath := filepath.Join(metadataDir, "topics.json")
	if _, err := os.Stat(metadataPath); err == nil {
		data, err := os.ReadFile(metadataPath)
		if err != nil {
			return false, err
		}
		if err := json.Unmarshal(data, &meta); err != nil {
			return false, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}

	// Add new topic
	meta.Topics[newName] = newTopic

	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return false, err
	}

	tmpPath := metadataPath + ".tmp"
	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return false, err
	}

	n, err := tmpFile.Write(data)
	if err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return false, err
	}
	if n != len(data) {
		tmpFile.Close()
		os.Remove(tmpPath)
		return false, io.ErrShortWrite
	}

	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return false, err
	}

	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpPath)
		return false, err
	}

	if err := os.Rename(tmpPath, metadataPath); err != nil {
		os.Remove(tmpPath)
		return false, err
	}

	// Rename succeeded, rename is committed
	committed = true

	// Sync metadata directory to commit naming updates
	if err := syncDirectory(metadataDir); err != nil {
		return committed, err
	}

	return committed, nil
}

func syncDirectory(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// ApplyRetention runs the retention policies on all partitions of all managed topics.
// It checks context cancellation before and after calling partition retention to exit cleanly.
func (m *Manager) ApplyRetention(ctx context.Context) {
	m.mu.Lock()
	// Get snapshots under the lock to iterate safely without holding manager lock during disk I/O.
	type partInfo struct {
		log            *partition.Log
		maxAge         time.Duration
		partitionBytes uint64
	}
	var targets []partInfo
	for _, t := range m.topics {
		maxAge := time.Duration(t.retention.MaxAgeSeconds) * time.Second
		maxBytes := t.retention.MaxBytes
		for _, p := range t.partitions {
			targets = append(targets, partInfo{
				log:            p,
				maxAge:         maxAge,
				partitionBytes: maxBytes,
			})
		}
	}
	m.mu.Unlock()

	for _, target := range targets {
		if ctx.Err() != nil {
			return
		}
		if err := target.log.ApplyRetention(target.maxAge, target.partitionBytes); err != nil {
			m.logger.Error("failed to apply retention on partition", "err", err)
		}
		if ctx.Err() != nil {
			return
		}
	}
}

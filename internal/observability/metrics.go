package observability

import (
	"fmt"
	"time"

	"github.com/ShivamMishra1603/distributed-message-broker/internal/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

type Metrics struct {
	Registry *prometheus.Registry

	// RPC metrics
	ProduceRequests *prometheus.CounterVec
	FetchRequests   *prometheus.CounterVec
	OffsetCommits   *prometheus.CounterVec

	ProduceDuration *prometheus.HistogramVec
	FetchDuration   *prometheus.HistogramVec

	// Record and Byte counts.
	// Note: RecordsAppended and BytesWritten count data that has been successfully appended and synchronized (post-fsync).
	RecordsAppended prometheus.Counter
	RecordsFetched  prometheus.Counter
	BytesWritten    prometheus.Counter
	BytesRead       prometheus.Counter

	// Storage operation metrics
	StorageErrors        *prometheus.CounterVec
	IndexRebuilds        prometheus.Counter
	RetentionSegmentsDel prometheus.Counter

	AppendDuration      prometheus.Histogram
	StorageReadDuration prometheus.Histogram
	RecoveryDuration    prometheus.Histogram
	FsyncDuration       prometheus.Histogram

	// Gauges
	PartitionLogSize      *prometheus.GaugeVec
	PartitionSegmentCount *prometheus.GaugeVec
	OpenConnections       prometheus.Gauge
}

// NewMetrics constructs a private registry, creates all collectors, and registers them.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()

	// Register standard Go runtime and process collectors
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	m := &Metrics{
		Registry: reg,

		ProduceRequests: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "broker_produce_requests_total",
				Help: "Total number of gRPC produce requests received.",
			},
			[]string{"operation", "status"},
		),
		FetchRequests: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "broker_fetch_requests_total",
				Help: "Total number of gRPC fetch requests received.",
			},
			[]string{"operation", "status"},
		),
		OffsetCommits: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "broker_offset_commits_total",
				Help: "Total number of gRPC offset commit requests received.",
			},
			[]string{"operation", "status"},
		),

		ProduceDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "broker_produce_duration_seconds",
				Help:    "Latency of produce requests.",
				Buckets: []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
			},
			[]string{"operation", "status"},
		),
		FetchDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "broker_fetch_duration_seconds",
				Help:    "Latency of fetch requests.",
				Buckets: []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
			},
			[]string{"operation", "status"},
		),

		RecordsAppended: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "broker_records_appended_total",
				Help: "Total number of records acknowledged as successfully appended (post-fsync).",
			},
		),
		RecordsFetched: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "broker_records_fetched_total",
				Help: "Total number of records successfully fetched by clients.",
			},
		),
		BytesWritten: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "broker_bytes_written_total",
				Help: "Total number of bytes acknowledged as successfully appended (post-fsync).",
			},
		),
		BytesRead: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "broker_bytes_read_total",
				Help: "Total number of bytes read from segment log files.",
			},
		),

		StorageErrors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "broker_storage_errors_total",
				Help: "Total number of storage-related operation errors.",
			},
			[]string{"operation"},
		),
		IndexRebuilds: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "broker_index_rebuilds_total",
				Help: "Total number of sparse index rebuild operations.",
			},
		),
		RetentionSegmentsDel: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "broker_retention_segments_deleted_total",
				Help: "Total number of segments deleted by background retention cleanup.",
			},
		),

		AppendDuration: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "broker_append_duration_seconds",
				Help:    "Latency of partition log append operations.",
				Buckets: []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
			},
		),
		StorageReadDuration: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "broker_storage_read_duration_seconds",
				Help:    "Latency of storage read operations.",
				Buckets: []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
			},
		),
		RecoveryDuration: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "broker_recovery_duration_seconds",
				Help:    "Latency of partition store recovery scan on startup.",
				Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 2.5, 5, 10, 30, 60},
			},
		),
		FsyncDuration: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "broker_fsync_duration_seconds",
				Help:    "Latency of segment log fsync operations.",
				Buckets: []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
			},
		),

		PartitionLogSize: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "broker_partition_log_size_bytes",
				Help: "Current size of the partition log in bytes.",
			},
			[]string{"topic", "partition"},
		),
		PartitionSegmentCount: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "broker_partition_segment_count",
				Help: "Current segment count of the partition.",
			},
			[]string{"topic", "partition"},
		),
		OpenConnections: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "broker_open_connections",
				Help: "Current number of open gRPC client connections.",
			},
		),
	}

	// Register all collectors
	reg.MustRegister(
		m.ProduceRequests,
		m.FetchRequests,
		m.OffsetCommits,
		m.ProduceDuration,
		m.FetchDuration,
		m.RecordsAppended,
		m.RecordsFetched,
		m.BytesWritten,
		m.BytesRead,
		m.StorageErrors,
		m.IndexRebuilds,
		m.RetentionSegmentsDel,
		m.AppendDuration,
		m.StorageReadDuration,
		m.RecoveryDuration,
		m.FsyncDuration,
		m.PartitionLogSize,
		m.PartitionSegmentCount,
		m.OpenConnections,
	)

	return m
}

// PartitionObserver implements storage.Observer for a fixed partition.
type PartitionObserver struct {
	metrics   *Metrics
	topic     string
	partition string
}

var _ storage.Observer = (*PartitionObserver)(nil)

// NewPartitionObserver constructs an observer for a specific topic and partition ID.
func (m *Metrics) NewPartitionObserver(topic string, partitionID uint32) *PartitionObserver {
	return &PartitionObserver{
		metrics:   m,
		topic:     topic,
		partition: fmt.Sprintf("%d", partitionID),
	}
}

func (p *PartitionObserver) ObserveAppend(duration time.Duration, records int, bytes int, err error) {
	p.metrics.AppendDuration.Observe(duration.Seconds())
	if err != nil {
		p.metrics.StorageErrors.WithLabelValues("append").Inc()
	} else {
		p.metrics.RecordsAppended.Add(float64(records))
		p.metrics.BytesWritten.Add(float64(bytes))
	}
}

func (p *PartitionObserver) ObserveRead(duration time.Duration, records int, bytes int, err error) {
	p.metrics.StorageReadDuration.Observe(duration.Seconds())
	if err != nil {
		p.metrics.StorageErrors.WithLabelValues("read").Inc()
	} else {
		p.metrics.RecordsFetched.Add(float64(records))
		p.metrics.BytesRead.Add(float64(bytes))
	}
}

func (p *PartitionObserver) ObserveFsync(duration time.Duration, err error) {
	p.metrics.FsyncDuration.Observe(duration.Seconds())
	if err != nil {
		p.metrics.StorageErrors.WithLabelValues("fsync").Inc()
	}
}

func (p *PartitionObserver) ObserveRecovery(duration time.Duration, err error) {
	p.metrics.RecoveryDuration.Observe(duration.Seconds())
	if err != nil {
		p.metrics.StorageErrors.WithLabelValues("recovery").Inc()
	}
}

func (p *PartitionObserver) IndexRebuilt() {
	p.metrics.IndexRebuilds.Inc()
}

func (p *PartitionObserver) SegmentDeleted() {
	p.metrics.RetentionSegmentsDel.Inc()
}

func (p *PartitionObserver) SetPartitionState(logBytes int64, segmentCount int) {
	p.metrics.PartitionLogSize.WithLabelValues(p.topic, p.partition).Set(float64(logBytes))
	p.metrics.PartitionSegmentCount.WithLabelValues(p.topic, p.partition).Set(float64(segmentCount))
}

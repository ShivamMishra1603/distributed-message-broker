package bench

type SystemMetadata struct {
	RunID         string `json:"run_id"`
	RunNumber     int    `json:"run_number"`
	Warmup        bool   `json:"warmup"`
	Commit        string `json:"commit"`
	Dirty         bool   `json:"dirty"`
	BuildTime     string `json:"build_time"`
	GoVersion     string `json:"go_version"`
	OS            string `json:"os"`
	Arch          string `json:"arch"`
	BrokerVersion string `json:"broker_version"`
}

type ProducerResult struct {
	Metadata            SystemMetadata   `json:"metadata"`
	Topic               string           `json:"topic"`
	Partitions          uint32           `json:"partitions"`
	PartitionMode       string           `json:"partition_mode"` // "single", "explicit", "round-robin"
	FlushMode           string           `json:"flush_mode"`
	RequestedRecords    int64            `json:"requested_records"`
	AcknowledgedRecords int64            `json:"acknowledged_records"`
	FailedRecords       int64            `json:"failed_records"`
	RPCCount            int64            `json:"rpc_count"`
	RecordSizeBytes     int              `json:"record_size_bytes"`
	BatchSize           int              `json:"batch_size"`
	Concurrency         int              `json:"concurrency"`
	DurationSeconds     float64          `json:"duration_seconds"`
	MessagesPerSecond   float64          `json:"messages_per_second"`
	PayloadMiBPerSecond float64          `json:"payload_mib_per_second"`
	Latency             LatencyReport    `json:"latency"`
	Errors              map[string]int64 `json:"errors"`
}

type ConsumerResult struct {
	Metadata            SystemMetadata   `json:"metadata"`
	Topic               string           `json:"topic"`
	PartitionMode       string           `json:"partition_mode"` // "single", "all-partitions"
	TargetRecords       int64            `json:"target_records"`
	ObservedRecords     int64            `json:"observed_records"`
	CountedRecords      int64            `json:"counted_records"`
	OvershootRecords    int64            `json:"overshoot_records"`
	RPCCount            int64            `json:"rpc_count"`
	MaxFetchBytes       int              `json:"max_fetch_bytes"`
	Concurrency         int              `json:"concurrency"`
	DurationSeconds     float64          `json:"duration_seconds"`
	MessagesPerSecond   float64          `json:"messages_per_second"`
	PayloadMiBPerSecond float64          `json:"payload_mib_per_second"`
	Latency             LatencyReport    `json:"latency"`
	Errors              map[string]int64 `json:"errors"`
}

package grpcserver

import (
	"context"
	"errors"
	"log/slog"

	brokerpb "github.com/ShivamMishra1603/distributed-message-broker/gen/proto/broker/v1"
	"github.com/ShivamMishra1603/distributed-message-broker/internal/config"
	"github.com/ShivamMishra1603/distributed-message-broker/internal/partition"
	"github.com/ShivamMishra1603/distributed-message-broker/internal/topic"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type BrokerServer struct {
	brokerpb.UnimplementedBrokerServiceServer
	logger       *slog.Logger
	topicManager *topic.Manager
	storageCfg   config.StorageConfig
}

func NewBrokerServer(logger *slog.Logger, topicManager *topic.Manager, storageCfg config.StorageConfig) *BrokerServer {
	return &BrokerServer{
		logger:       logger,
		topicManager: topicManager,
		storageCfg:   storageCfg,
	}
}

func (b *BrokerServer) Produce(ctx context.Context, req *brokerpb.ProduceRequest) (*brokerpb.ProduceResponse, error) {
	if len(req.GetRecords()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "cannot produce empty record batch")
	}

	// 1. Resolve partition
	log, err := b.topicManager.GetPartition(req.GetTopic(), req.GetPartition())
	if err != nil {
		if errors.Is(err, topic.ErrTopicNotFound) || errors.Is(err, topic.ErrPartitionNotFound) {
			return nil, status.Errorf(codes.NotFound, "topic or partition not found: %v", err)
		}
		return nil, status.Errorf(codes.Internal, "failed to resolve partition: %v", err)
	}

	// 2. Validate batch and record payload sizes
	totalBatchPayloadSize := 0
	internalRecs := make([]partition.Record, len(req.GetRecords()))

	for i, r := range req.GetRecords() {
		recPayloadSize := len(r.GetKey()) + len(r.GetValue())
		if recPayloadSize > b.storageCfg.MaxRecordBytes {
			return nil, status.Errorf(codes.ResourceExhausted, "record %d payload size (%d) exceeds max_record_bytes (%d)", i, recPayloadSize, b.storageCfg.MaxRecordBytes)
		}
		totalBatchPayloadSize += recPayloadSize

		headers := make([]partition.Header, len(r.GetHeaders()))
		for j, h := range r.GetHeaders() {
			headers[j] = partition.Header{
				Key:   h.GetKey(),
				Value: h.GetValue(),
			}
		}

		internalRecs[i] = partition.Record{
			Key:       r.GetKey(),
			Value:     r.GetValue(),
			Headers:   headers,
			Timestamp: r.GetTimestamp(),
		}
	}

	if totalBatchPayloadSize > b.storageCfg.MaxBatchBytes {
		return nil, status.Errorf(codes.ResourceExhausted, "total batch payload size (%d) exceeds max_batch_bytes (%d)", totalBatchPayloadSize, b.storageCfg.MaxBatchBytes)
	}

	// 3. Append to log
	baseOffset, lastOffset, err := log.Append(internalRecs)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to append records to log: %v", err)
	}

	return &brokerpb.ProduceResponse{
		BaseOffset:  baseOffset,
		LastOffset:  lastOffset,
		RecordCount: uint32(len(req.GetRecords())),
	}, nil
}

func (b *BrokerServer) Fetch(ctx context.Context, req *brokerpb.FetchRequest) (*brokerpb.FetchResponse, error) {
	if req.GetMaxBytes() == 0 {
		return nil, status.Error(codes.InvalidArgument, "max_bytes must be greater than zero")
	}

	// 1. Resolve partition
	log, err := b.topicManager.GetPartition(req.GetTopic(), req.GetPartition())
	if err != nil {
		if errors.Is(err, topic.ErrTopicNotFound) || errors.Is(err, topic.ErrPartitionNotFound) {
			return nil, status.Errorf(codes.NotFound, "topic or partition not found: %v", err)
		}
		return nil, status.Errorf(codes.Internal, "failed to resolve partition: %v", err)
	}

	// 2. Read records
	stored, err := log.Read(req.GetOffset(), int(req.GetMaxBytes()))
	if err != nil {
		if errors.Is(err, partition.ErrOffsetOutOfRange) {
			return nil, status.Errorf(codes.OutOfRange, "requested offset %d is out of range for partition", req.GetOffset())
		}
		return nil, status.Errorf(codes.Internal, "failed to read records: %v", err)
	}

	protoRecs := make([]*brokerpb.StoredRecord, len(stored))
	for i, r := range stored {
		protoHeaders := make([]*brokerpb.Header, len(r.Headers))
		for j, h := range r.Headers {
			protoHeaders[j] = &brokerpb.Header{
				Key:   h.Key,
				Value: h.Value,
			}
		}
		protoRecs[i] = &brokerpb.StoredRecord{
			Offset:    r.Offset,
			Timestamp: r.Timestamp,
			Key:       r.Key,
			Value:     r.Value,
			Headers:   protoHeaders,
		}
	}

	return &brokerpb.FetchResponse{
		Records:        protoRecs,
		EarliestOffset: log.EarliestOffset(),
		LogEndOffset:   log.LogEndOffset(),
	}, nil
}

func (b *BrokerServer) CommitOffset(ctx context.Context, req *brokerpb.CommitOffsetRequest) (*brokerpb.CommitOffsetResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "CommitOffset is not implemented in Milestone 2")
}

func (b *BrokerServer) FetchCommittedOffset(ctx context.Context, req *brokerpb.FetchCommittedOffsetRequest) (*brokerpb.FetchCommittedOffsetResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "FetchCommittedOffset is not implemented in Milestone 2")
}

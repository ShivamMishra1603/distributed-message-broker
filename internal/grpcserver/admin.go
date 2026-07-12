package grpcserver

import (
	"context"
	"errors"
	"log/slog"

	brokerpb "github.com/ShivamMishra1603/distributed-message-broker/gen/proto/broker/v1"
	"github.com/ShivamMishra1603/distributed-message-broker/internal/topic"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type AdminServer struct {
	brokerpb.UnimplementedAdminServiceServer
	logger       *slog.Logger
	topicManager *topic.Manager
}

func NewAdminServer(logger *slog.Logger, topicManager *topic.Manager) *AdminServer {
	return &AdminServer{
		logger:       logger,
		topicManager: topicManager,
	}
}

func (a *AdminServer) CreateTopic(ctx context.Context, req *brokerpb.CreateTopicRequest) (*brokerpb.CreateTopicResponse, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "topic name cannot be empty")
	}
	if req.GetPartitionCount() == 0 {
		return nil, status.Error(codes.InvalidArgument, "partition count must be greater than zero")
	}

	retention := topic.RetentionPolicy{}
	if req.GetRetention() != nil {
		retention.MaxAgeSeconds = req.GetRetention().GetMaxAgeSeconds()
		retention.MaxBytes = req.GetRetention().GetMaxBytes()
	}

	t, err := a.topicManager.CreateTopic(req.GetName(), int(req.GetPartitionCount()), retention)
	if err != nil {
		if errors.Is(err, topic.ErrTopicExists) {
			return nil, status.Errorf(codes.AlreadyExists, "topic %q already exists", req.GetName())
		}
		if errors.Is(err, topic.ErrInvalidTopicName) {
			return nil, status.Errorf(codes.InvalidArgument, "invalid topic name: %v", err)
		}
		return nil, status.Errorf(codes.Internal, "failed to create topic: %v", err)
	}

	a.logger.Info("created topic", "name", req.GetName(), "partitions", req.GetPartitionCount())

	return &brokerpb.CreateTopicResponse{
		Topic: convertToProtoMetadata(t),
	}, nil
}

func (a *AdminServer) ListTopics(ctx context.Context, req *brokerpb.ListTopicsRequest) (*brokerpb.ListTopicsResponse, error) {
	topics := a.topicManager.ListTopics()
	protoTopics := make([]*brokerpb.TopicMetadata, len(topics))
	for i, t := range topics {
		protoTopics[i] = convertToProtoMetadata(t)
	}

	return &brokerpb.ListTopicsResponse{
		Topics: protoTopics,
	}, nil
}

func (a *AdminServer) DescribeTopic(ctx context.Context, req *brokerpb.DescribeTopicRequest) (*brokerpb.DescribeTopicResponse, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "topic name cannot be empty")
	}

	t, err := a.topicManager.GetTopic(req.GetName())
	if err != nil {
		if errors.Is(err, topic.ErrTopicNotFound) {
			return nil, status.Errorf(codes.NotFound, "topic %q not found", req.GetName())
		}
		return nil, status.Errorf(codes.Internal, "failed to resolve topic: %v", err)
	}

	return &brokerpb.DescribeTopicResponse{
		Topic: convertToProtoMetadata(t),
	}, nil
}

func convertToProtoMetadata(t *topic.Topic) *brokerpb.TopicMetadata {
	protoParts := make([]*brokerpb.PartitionMetadata, t.PartitionCount())
	for i := 0; i < t.PartitionCount(); i++ {
		p, _ := t.Partition(uint32(i))
		protoParts[i] = &brokerpb.PartitionMetadata{
			PartitionId:    uint32(i),
			EarliestOffset: p.EarliestOffset(),
			LogEndOffset:   p.LogEndOffset(),
		}
	}

	return &brokerpb.TopicMetadata{
		Name:           t.Name(),
		PartitionCount: uint32(t.PartitionCount()),
		Partitions:     protoParts,
		Retention: &brokerpb.RetentionPolicy{
			MaxAgeSeconds: t.Retention().MaxAgeSeconds,
			MaxBytes:      t.Retention().MaxBytes,
		},
	}
}

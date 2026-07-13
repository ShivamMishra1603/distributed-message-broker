package grpcserver

import (
	"context"
	"sync/atomic"

	"github.com/ShivamMishra1603/distributed-message-broker/internal/observability"
	"google.golang.org/grpc/stats"
)

type ConnStatsHandler struct {
	metrics   *observability.Metrics
	connCount int64
}

func NewConnStatsHandler(m *observability.Metrics) *ConnStatsHandler {
	return &ConnStatsHandler{metrics: m}
}

// Compile-time assertion
var _ stats.Handler = (*ConnStatsHandler)(nil)

func (h *ConnStatsHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	return ctx
}

func (h *ConnStatsHandler) HandleRPC(ctx context.Context, stat stats.RPCStats) {}

func (h *ConnStatsHandler) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	return ctx
}

func (h *ConnStatsHandler) HandleConn(ctx context.Context, stat stats.ConnStats) {
	if h.metrics == nil {
		return
	}
	switch stat.(type) {
	case *stats.ConnBegin:
		val := atomic.AddInt64(&h.connCount, 1)
		h.metrics.OpenConnections.Set(float64(val))
	case *stats.ConnEnd:
		val := atomic.AddInt64(&h.connCount, -1)
		if val < 0 {
			atomic.StoreInt64(&h.connCount, 0)
			val = 0
		}
		h.metrics.OpenConnections.Set(float64(val))
	}
}

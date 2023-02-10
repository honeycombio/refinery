package route

import (
	"context"
	collectormetric "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc/metadata"
)

type MetricServer struct {
	router *Router
	client collectormetric.MetricsServiceClient

	collectormetric.UnimplementedMetricsServiceServer
}

func NewMetricServer(router *Router) *MetricServer {
	return &MetricServer{
		router: router,
		client: collectormetric.NewMetricsServiceClient(router.grpcProxyClientConn),
	}
}

func (t *MetricServer) Export(ctx context.Context, req *collectormetric.ExportMetricsServiceRequest) (*collectormetric.ExportMetricsServiceResponse, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		ctx = metadata.NewOutgoingContext(ctx, md)
	}
	response, err := t.client.Export(ctx, req)
	if err != nil {
		t.router.Logger.Error().Logf("failed to proxy metrics: %s", err)
	}
	return response, nil
}

package route

import (
	"context"
	"google.golang.org/grpc/metadata"

	collectorlog "go.opentelemetry.io/proto/otlp/collector/logs/v1"
)

type LogServer struct {
	router *Router
	client collectorlog.LogsServiceClient

	collectorlog.UnimplementedLogsServiceServer
}

func NewLogServer(router *Router) *LogServer {
	return &LogServer{
		router: router,
		client: collectorlog.NewLogsServiceClient(router.grpcProxyClientConn),
	}
}

func (t *LogServer) Export(ctx context.Context, req *collectorlog.ExportLogsServiceRequest) (*collectorlog.ExportLogsServiceResponse, error) {
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

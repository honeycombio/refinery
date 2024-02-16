package route

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	huskyotlp "github.com/honeycombio/husky/otlp"
	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
)

func (r *Router) postOTLPLogs(w http.ResponseWriter, req *http.Request) {
	ri := huskyotlp.GetRequestInfoFromHttpHeaders(req.Header)

	if !r.Config.IsAPIKeyValid(ri.ApiKey) {
		err := fmt.Errorf("api key %s not found in list of authorized keys", ri.ApiKey)
		r.handlerReturnWithError(w, ErrAuthNeeded, err)
		return
	}

	if err := ri.ValidateLogsHeaders(); err != nil {
		if errors.Is(err, huskyotlp.ErrInvalidContentType) {
			r.handlerReturnWithError(w, ErrInvalidContentType, err)
		} else {
			r.handlerReturnWithError(w, ErrAuthNeeded, err)
		}
		return
	}

	result, err := huskyotlp.TranslateLogsRequestFromReader(req.Body, ri)
	if err != nil {
		r.handlerReturnWithError(w, ErrUpstreamFailed, err)
		return
	}

	if err := processOtlpRequest(req.Context(), r, result.Batches, ri.ApiKey); err != nil {
		r.handlerReturnWithError(w, ErrUpstreamFailed, err)
	}
}

type LogsServer struct {
	router *Router
	collectorlogs.UnimplementedLogsServiceServer
}

func NewLogsServer(router *Router) *LogsServer {
	logsServer := LogsServer{router: router}
	return &logsServer
}

func (t *LogsServer) Export(ctx context.Context, req *collectorlogs.ExportLogsServiceRequest) (*collectorlogs.ExportLogsServiceResponse, error) {
	ri := huskyotlp.GetRequestInfoFromGrpcMetadata(ctx)
	if err := ri.ValidateLogsHeaders(); err != nil {
		return nil, huskyotlp.AsGRPCError(err)
	}

	result, err := huskyotlp.TranslateLogsRequest(req, ri)
	if err != nil {
		return nil, huskyotlp.AsGRPCError(err)
	}

	if err := processOtlpRequest(ctx, t.router, result.Batches, ri.ApiKey); err != nil {
		return nil, huskyotlp.AsGRPCError(err)
	}

	return &collectorlogs.ExportLogsServiceResponse{}, nil
}

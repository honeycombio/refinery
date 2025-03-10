package route

import (
	"context"
	"errors"
	"net/http"

	huskyotlp "github.com/honeycombio/husky/otlp"
	"github.com/honeycombio/refinery/internal/otelutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
)

func (r *Router) postOTLPLogs(w http.ResponseWriter, req *http.Request) {
	ctx, span := otelutil.StartSpan(req.Context(), r.Tracer, "postOTLPLogs")
	defer span.End()

	ri := huskyotlp.GetRequestInfoFromHttpHeaders(req.Header)

	if err := ri.ValidateLogsHeaders(); err != nil {
		if errors.Is(err, huskyotlp.ErrInvalidContentType) {
			r.handlerReturnWithError(w, ErrInvalidContentType, err)
		} else {
			r.handleOTLPFailureResponse(w, req, huskyotlp.OTLPError{Message: err.Error(), HTTPStatusCode: http.StatusUnauthorized})
		}
		return
	}

	apicfg := r.Config.GetAccessKeyConfig()
	keyToUse, err := apicfg.GetReplaceKey(ri.ApiKey)
	if err != nil {
		r.handleOTLPFailureResponse(w, req, huskyotlp.OTLPError{Message: err.Error(), HTTPStatusCode: http.StatusUnauthorized})
		return
	}

	result, err := huskyotlp.TranslateLogsRequestFromReader(ctx, req.Body, ri)
	if err != nil {
		r.handleOTLPFailureResponse(w, req, huskyotlp.OTLPError{Message: err.Error(), HTTPStatusCode: http.StatusInternalServerError})
		return
	}

	if err := r.processOTLPRequest(ctx, result.Batches, keyToUse, ri.UserAgent); err != nil {
		r.handleOTLPFailureResponse(w, req, huskyotlp.OTLPError{Message: err.Error(), HTTPStatusCode: http.StatusInternalServerError})
		return
	}

	_ = huskyotlp.WriteOtlpHttpTraceSuccessResponse(w, req)
}

type LogsServer struct {
	router *Router
	collectorlogs.UnimplementedLogsServiceServer
}

func NewLogsServer(router *Router) *LogsServer {
	logsServer := LogsServer{router: router}
	return &logsServer
}

func (l *LogsServer) Export(ctx context.Context, req *collectorlogs.ExportLogsServiceRequest) (*collectorlogs.ExportLogsServiceResponse, error) {
	ctx, span := otelutil.StartSpan(ctx, l.router.Tracer, "ExportOTLPLogs")
	defer span.End()

	ri := huskyotlp.GetRequestInfoFromGrpcMetadata(ctx)
	if err := ri.ValidateLogsHeaders(); err != nil {
		return nil, huskyotlp.AsGRPCError(err)
	}

	apicfg := l.router.Config.GetAccessKeyConfig()
	if err := apicfg.IsAccepted(ri.ApiKey); err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	keyToUse, err := apicfg.GetReplaceKey(ri.ApiKey)

	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}

	result, err := huskyotlp.TranslateLogsRequest(ctx, req, ri)
	if err != nil {
		return nil, huskyotlp.AsGRPCError(err)
	}

	if err := l.router.processOTLPRequest(ctx, result.Batches, keyToUse, ri.UserAgent); err != nil {
		return nil, huskyotlp.AsGRPCError(err)
	}

	return &collectorlogs.ExportLogsServiceResponse{}, nil
}

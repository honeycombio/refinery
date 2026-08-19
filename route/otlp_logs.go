package route

import (
	"context"
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
	apicfg := r.Config.GetAccessKeyConfig()
	keyID := ""
	if apicfg.HasKeyIDs() {
		keyID = r.getKeyID(ri.ApiKey)
	}
	if err := apicfg.IsAccepted(ri.ApiKey, keyID); err != nil {
		r.handleOTLPFailureResponse(w, req, huskyotlp.OTLPError{Message: err.Error(), HTTPStatusCode: http.StatusUnauthorized})
		return
	}
	keyToUse, _ := apicfg.GetReplaceKey(ri.ApiKey, keyID)

	// Look up the environment with the key that will actually be sent, so a
	// replaced key resolves its own environment. A failed lookup leaves the
	// name blank, which validates as an unmigrated Classic environment.
	envName, envErr := r.getEnvironmentName(keyToUse)
	if envErr != nil {
		r.Logger.Error().WithField("error", envErr).Logf("failed to look up environment name for OTLP logs request")
	}
	ri.EnvironmentName = envName

	if err := ri.ValidateHeaders(); err != nil {
		switch err {
		case huskyotlp.ErrInvalidContentType:
			r.handleOTLPFailureResponse(w, req, huskyotlp.ErrInvalidContentType)
			return
		case huskyotlp.ErrMissingAPIKeyHeader, huskyotlp.ErrMissingDatasetHeader:
			if len(keyToUse) == 0 {
				r.handleOTLPFailureResponse(w, req, huskyotlp.OTLPError{Message: err.Error(), HTTPStatusCode: http.StatusUnauthorized})
				return
			}
		default:
			r.handleOTLPFailureResponse(w, req, huskyotlp.OTLPError{Message: err.Error(), HTTPStatusCode: http.StatusUnauthorized})
			return
		}
	}

	switch ri.ContentType {
	case "application/json":
		r.Metrics.Increment(r.metricsNames.routerOtlpLogHttpJson)
	case "application/x-protobuf", "application/protobuf":
		r.Metrics.Increment(r.metricsNames.routerOtlpLogHttpProto)
	}

	ri.ApiKey = keyToUse
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

	l.router.Metrics.Increment(l.router.metricsNames.routerOtlpLogGrpc)
	ri := huskyotlp.GetRequestInfoFromGrpcMetadata(ctx)
	apicfg := l.router.Config.GetAccessKeyConfig()
	keyID := ""
	if apicfg.HasKeyIDs() {
		keyID = l.router.getKeyID(ri.ApiKey)
	}
	if err := apicfg.IsAccepted(ri.ApiKey, keyID); err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	keyToUse, _ := apicfg.GetReplaceKey(ri.ApiKey, keyID)

	// Look up the environment with the key that will actually be sent, so a
	// replaced key resolves its own environment. A failed lookup leaves the
	// name blank, which validates as an unmigrated Classic environment.
	envName, envErr := l.router.getEnvironmentName(keyToUse)
	if envErr != nil {
		l.router.Logger.Error().WithField("error", envErr).Logf("failed to look up environment name for OTLP logs request")
	}
	ri.EnvironmentName = envName

	if err := ri.ValidateHeaders(); err != nil && err != huskyotlp.ErrMissingAPIKeyHeader {
		return nil, huskyotlp.AsGRPCError(err)
	}

	ri.ApiKey = keyToUse

	result, err := huskyotlp.TranslateLogsRequest(ctx, req, ri)
	if err != nil {
		return nil, huskyotlp.AsGRPCError(err)
	}

	if err := l.router.processOTLPRequest(ctx, result.Batches, keyToUse, ri.UserAgent); err != nil {
		return nil, huskyotlp.AsGRPCError(err)
	}

	return &collectorlogs.ExportLogsServiceResponse{}, nil
}

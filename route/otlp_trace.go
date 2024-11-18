package route

import (
	"context"
	"errors"
	"net/http"

	huskyotlp "github.com/honeycombio/husky/otlp"
	"github.com/honeycombio/refinery/internal/otelutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

func (r *Router) postOTLPTrace(w http.ResponseWriter, req *http.Request) {
	ctx, span := otelutil.StartSpan(req.Context(), r.Tracer, "postOTLPTrace")
	defer span.End()

	ri := huskyotlp.GetRequestInfoFromHttpHeaders(req.Header)
	if err := ri.ValidateTracesHeaders(); err != nil {
		if errors.Is(err, huskyotlp.ErrInvalidContentType) {
			r.handleOTLPFailureResponse(w, req, huskyotlp.ErrInvalidContentType)
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

	result, err := huskyotlp.TranslateTraceRequestFromReader(ctx, req.Body, ri)
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

type TraceServer struct {
	router *Router
	collectortrace.UnimplementedTraceServiceServer
}

func NewTraceServer(router *Router) *TraceServer {
	traceServer := TraceServer{router: router}
	return &traceServer
}

func (t *TraceServer) Export(ctx context.Context, req *collectortrace.ExportTraceServiceRequest) (*collectortrace.ExportTraceServiceResponse, error) {
	ctx, span := otelutil.StartSpan(ctx, t.router.Tracer, "ExportOTLPTrace")
	defer span.End()

	ri := huskyotlp.GetRequestInfoFromGrpcMetadata(ctx)
	if err := ri.ValidateTracesHeaders(); err != nil {
		return nil, huskyotlp.AsGRPCError(err)
	}

	apicfg := t.router.Config.GetAccessKeyConfig()
	if err := apicfg.IsAccepted(ri.ApiKey); err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}

	keyToUse, err := apicfg.GetReplaceKey(ri.ApiKey)

	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}

	result, err := huskyotlp.TranslateTraceRequest(ctx, req, ri)
	if err != nil {
		return nil, huskyotlp.AsGRPCError(err)
	}

	if err := t.router.processOTLPRequest(ctx, result.Batches, keyToUse, ri.UserAgent); err != nil {
		return nil, huskyotlp.AsGRPCError(err)
	}

	return &collectortrace.ExportTraceServiceResponse{}, nil
}

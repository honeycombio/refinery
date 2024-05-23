package route

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	huskyotlp "github.com/honeycombio/husky/otlp"

	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

func (r *Router) postOTLPTrace(w http.ResponseWriter, req *http.Request) {
	ri := huskyotlp.GetRequestInfoFromHttpHeaders(req.Header)

	if err := ri.ValidateTracesHeaders(); err != nil {
		if errors.Is(err, huskyotlp.ErrInvalidContentType) {
			r.handleOTLPFailureResponse(w, req, huskyotlp.ErrInvalidContentType)
		} else {
			r.handleOTLPFailureResponse(w, req, huskyotlp.OTLPError{Message: err.Error(), HTTPStatusCode: http.StatusUnauthorized})
		}
		return
	}

	if !r.Config.IsAPIKeyValid(ri.ApiKey) {
		r.handleOTLPFailureResponse(w, req, huskyotlp.OTLPError{Message: fmt.Sprintf("api key %s not found in list of authorized keys", ri.ApiKey), HTTPStatusCode: http.StatusUnauthorized})
		return
	}

	result, err := huskyotlp.TranslateTraceRequestFromReader(req.Context(), req.Body, ri)
	if err != nil {
		r.handleOTLPFailureResponse(w, req, huskyotlp.OTLPError{Message: err.Error(), HTTPStatusCode: http.StatusInternalServerError})
		return
	}

	if err := r.processOTLPRequest(req.Context(), result.Batches, ri.ApiKey); err != nil {
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
	ri := huskyotlp.GetRequestInfoFromGrpcMetadata(ctx)
	if err := ri.ValidateTracesHeaders(); err != nil {
		return nil, huskyotlp.AsGRPCError(err)
	}

	result, err := huskyotlp.TranslateTraceRequest(ctx, req, ri)
	if err != nil {
		return nil, huskyotlp.AsGRPCError(err)
	}

	if err := t.router.processOTLPRequest(ctx, result.Batches, ri.ApiKey); err != nil {
		return nil, huskyotlp.AsGRPCError(err)
	}

	return &collectortrace.ExportTraceServiceResponse{}, nil
}

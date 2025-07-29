package route

import (
	"context"
	"net/http"

	huskyotlp "github.com/honeycombio/husky/otlp"
	"github.com/honeycombio/refinery/internal/otelutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

// copied from husky
var defaultMaxRequestBodySize = 20 * 1024 * 1024 // 20MiB

func (r *Router) postOTLPTrace(w http.ResponseWriter, req *http.Request) {
	ctx, span := otelutil.StartSpan(req.Context(), r.Tracer, "postOTLPTrace")
	defer span.End()

	ri := huskyotlp.GetRequestInfoFromHttpHeaders(req.Header)
	apicfg := r.Config.GetAccessKeyConfig()
	keyToUse, _ := apicfg.GetReplaceKey(ri.ApiKey)

	if err := ri.ValidateTracesHeaders(); err != nil {
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

	ri.ApiKey = keyToUse

	switch ri.ContentType {
	case "application/json":
		r.Metrics.Increment(r.metricsNames.routerOtlpHttpJson)
		r.processOTLPRequestWithMsgp(ctx, w, req, ri, keyToUse)
	case "application/x-protobuf", "application/protobuf":
		r.Metrics.Increment(r.metricsNames.routerOtlpHttpProto)
		r.processOTLPRequestWithMsgp(ctx, w, req, ri, keyToUse)
	default:
		result, err := huskyotlp.TranslateTraceRequestFromReader(ctx, req.Body, ri)
		if err != nil {
			r.handleOTLPFailureResponse(w, req, huskyotlp.OTLPError{Message: err.Error(), HTTPStatusCode: http.StatusInternalServerError})
			return
		}

		if err := r.processOTLPRequest(ctx, result.Batches, keyToUse, ri.UserAgent); err != nil {
			r.handleOTLPFailureResponse(w, req, huskyotlp.OTLPError{Message: err.Error(), HTTPStatusCode: http.StatusInternalServerError})
			return
		}
	}

	_ = huskyotlp.WriteOtlpHttpTraceSuccessResponse(w, req)
}

func (r *Router) processOTLPRequestWithMsgp(ctx context.Context, w http.ResponseWriter, req *http.Request, ri huskyotlp.RequestInfo, keyToUse string) {
	result, err := huskyotlp.TranslateTraceRequestFromReaderSizedWithMsgp(ctx, req.Body, ri, int64(defaultMaxRequestBodySize))
	if err != nil {
		r.handleOTLPFailureResponse(w, req, huskyotlp.OTLPError{Message: err.Error(), HTTPStatusCode: http.StatusInternalServerError})
		return
	}

	if err := r.processOTLPRequestBatchMsgp(ctx, result.Batches, keyToUse, ri.UserAgent); err != nil {
		r.handleOTLPFailureResponse(w, req, huskyotlp.OTLPError{Message: err.Error(), HTTPStatusCode: http.StatusInternalServerError})
		return
	}
}

// CustomTraceServiceServer defines our custom interface for the trace service.
// It processes pre-translated trace data using msgpack optimization rather than
// accepting raw OTLP protobuf requests.
type CustomTraceServiceServer interface {
	ExportTraceData(context.Context, *huskyotlp.TranslateOTLPRequestResultMsgp, huskyotlp.RequestInfo) (*collectortrace.ExportTraceServiceResponse, error)
}

type TraceServer struct {
	router *Router
}

func NewTraceServer(router *Router) *TraceServer {
	traceServer := TraceServer{router: router}
	return &traceServer
}

// ExportTraceData processes translated trace data using msgpack optimization
func (t *TraceServer) ExportTraceData(ctx context.Context, result *huskyotlp.TranslateOTLPRequestResultMsgp, ri huskyotlp.RequestInfo) (*collectortrace.ExportTraceServiceResponse, error) {
	ctx, span := otelutil.StartSpan(ctx, t.router.Tracer, "ExportOTLPTrace")
	defer span.End()

	// Perform final authentication check (key processing already done in handler)
	apicfg := t.router.Config.GetAccessKeyConfig()
	if err := apicfg.IsAccepted(ri.ApiKey); err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}

	// Use optimized msgpack processing path with already translated data
	if err := t.router.processOTLPRequestBatchMsgp(ctx, result.Batches, ri.ApiKey, ri.UserAgent); err != nil {
		return nil, huskyotlp.AsGRPCError(err)
	}

	return &collectortrace.ExportTraceServiceResponse{}, nil
}

// translatedTraceServiceRequest is a wrapper that implements proto.Message but
// actually produces a translated TranslateOTLPRequestResultMsgp instead of a
// real unserialized protobuf struct.
type translatedTraceServiceRequest struct {
	requestInfo huskyotlp.RequestInfo
	result      *huskyotlp.TranslateOTLPRequestResultMsgp
	ctx         context.Context
}

// Implement proto.Message interface.
func (r *translatedTraceServiceRequest) Reset() {
	r.result = nil
}

func (r *translatedTraceServiceRequest) String() string {
	return "translatedTraceServiceRequest"
}

func (r *translatedTraceServiceRequest) ProtoMessage() {}

// Unmarshal implements a custom unmarshaling method that performs translation
// during the unmarshaling step using husky's msgpack optimization.
func (r *translatedTraceServiceRequest) Unmarshal(data []byte) error {
	result, err := huskyotlp.UnmarshalTraceRequestDirectMsgp(r.ctx, data, r.requestInfo)
	if err != nil {
		return err
	}
	r.result = result
	return nil
}

// customTraceExportHandler is a custom GRPC handler that uses husky's optimized
// translation codepath.
func customTraceExportHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	traceServer := srv.(*TraceServer)

	// Get request info from GRPC metadata and prepare our custom wrapper.
	ri := huskyotlp.GetRequestInfoFromGrpcMetadata(ctx)

	// Handle SendKeyMode logic before validation, similar to HTTP handler
	apicfg := traceServer.router.Config.GetAccessKeyConfig()
	keyToUse, err := apicfg.GetReplaceKey(ri.ApiKey)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}

	// Update the RequestInfo with the processed key for the unmarshal step
	ri.ApiKey = keyToUse

	in := &translatedTraceServiceRequest{
		requestInfo: ri,
		ctx:         ctx,
	}

	if err := dec(in); err != nil {
		return nil, err
	}

	// Translation already happened during unmarshaling - just get the result
	result := in.result
	if result == nil {
		return nil, status.Error(codes.Internal, "no processed result available")
	}

	if interceptor == nil {
		return traceServer.ExportTraceData(ctx, result, ri)
	}

	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/opentelemetry.proto.collector.trace.v1.TraceService/Export",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return traceServer.ExportTraceData(ctx, result, ri)
	}
	return interceptor(ctx, in, info, handler)
}

package route

import (
	"context"
	"errors"
	"net/http"

	huskyotlp "github.com/honeycombio/husky/otlp"
	"github.com/honeycombio/refinery/internal/otelutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

var (
	// copied from husky
	defaultMaxRequestBodySize = 20 * 1024 * 1024 // 20MiB
	ErrTranslateTraceRequest  = errors.New("failed to translate trace request")
)

func (r *Router) postOTLPTrace(w http.ResponseWriter, req *http.Request) {
	ctx, span := otelutil.StartSpan(req.Context(), r.Tracer, "postOTLPTrace")
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

	var err error
	switch ri.ContentType {
	case "application/json":
		r.Metrics.Increment(r.metricsNames.routerOtlpTraceHttpJson)
		err = r.processOTLPRequestWithMsgp(ctx, w, req, ri, keyToUse)
	case "application/x-protobuf", "application/protobuf":
		r.Metrics.Increment(r.metricsNames.routerOtlpTraceHttpProto)
		err = r.processOTLPRequestWithMsgp(ctx, w, req, ri, keyToUse)
	default:
		err = errors.New("unsupported content type")
	}

	if err != nil {
		switch {
		case errors.Is(err, huskyotlp.ErrFailedParseBody):
			r.handleOTLPFailureResponse(w, req, huskyotlp.ErrFailedParseBody)
		case errors.Is(err, ErrTranslateTraceRequest):
			r.handleOTLPFailureResponse(w, req, huskyotlp.OTLPError{Message: err.Error(), HTTPStatusCode: http.StatusBadRequest})
		case errors.Is(err, huskyotlp.ErrInvalidContentType):
			r.handleOTLPFailureResponse(w, req, huskyotlp.ErrInvalidContentType)
		case errors.Is(err, huskyotlp.ErrMissingAPIKeyHeader), errors.Is(err, huskyotlp.ErrMissingDatasetHeader):
			r.handleOTLPFailureResponse(w, req, huskyotlp.OTLPError{Message: err.Error(), HTTPStatusCode: http.StatusUnauthorized})
		default:
			r.handleOTLPFailureResponse(w, req, huskyotlp.OTLPError{Message: err.Error(), HTTPStatusCode: http.StatusInternalServerError})
		}
		return
	}
	err = huskyotlp.WriteOtlpHttpTraceSuccessResponse(w, req)
	if err != nil {
		r.Logger.Error().WithField("error", err).Logf("failed to write OTLP HTTP trace success response")
	}
	return
}

func (r *Router) processOTLPRequestWithMsgp(ctx context.Context, w http.ResponseWriter, req *http.Request, ri huskyotlp.RequestInfo, keyToUse string) error {
	result, err := huskyotlp.TranslateTraceRequestFromReaderSizedWithMsgp(ctx, req.Body, ri, int64(defaultMaxRequestBodySize))
	if err != nil {
		// Check for specific error types from husky to provide better error handling to caller
		switch {
		case errors.Is(err, huskyotlp.ErrInvalidContentType):
			return err
		case errors.Is(err, huskyotlp.ErrFailedParseBody):
			return err
		case errors.Is(err, huskyotlp.ErrMissingAPIKeyHeader), errors.Is(err, huskyotlp.ErrMissingDatasetHeader):
			return err
		default:
			// For any other translate errors (parsing, unmarshaling, etc.), wrap with specific error
			return errors.Join(ErrTranslateTraceRequest, err)
		}
	}

	if err := r.processOTLPRequestBatchMsgp(ctx, result.Batches, keyToUse, ri.UserAgent); err != nil {
		return err
	}

	return nil
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
func (t *TraceServer) ExportTraceData(
	ctx context.Context,
	result *huskyotlp.TranslateOTLPRequestResultMsgp,
	ri huskyotlp.RequestInfo,
) (*collectortrace.ExportTraceServiceResponse, error) {
	ctx, span := otelutil.StartSpan(ctx, t.router.Tracer, "ExportOTLPTrace")
	defer span.End()

	t.router.Metrics.Increment(t.router.metricsNames.routerOtlpTraceGrpc)

	// Perform final authentication check (key processing already done in handler)
	apicfg := t.router.Config.GetAccessKeyConfig()
	keyID := ""
	if apicfg.HasKeyIDs() {
		keyID = t.router.getKeyID(ri.ApiKey)
	}
	if err := apicfg.IsAccepted(ri.ApiKey, keyID); err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}

	// Use optimized msgpack processing path with already translated data
	if err := t.router.processOTLPRequestBatchMsgp(ctx, result.Batches, ri.ApiKey, ri.UserAgent); err != nil {
		return nil, huskyotlp.AsGRPCError(err)
	}

	return &collectortrace.ExportTraceServiceResponse{}, nil
}

// translatedTraceServiceRequest is a wrapper that implements protoiface.MessageV1
// but actually produces a translated TranslateOTLPRequestResultMsgp instead of a
// real unserialized protobuf struct. We're using the V1 API because the V2 API
// is tightly bound to proto's modern-but-slow way of doing everything.
type translatedTraceServiceRequest struct {
	requestInfo huskyotlp.RequestInfo
	result      *huskyotlp.TranslateOTLPRequestResultMsgp
	ctx         context.Context
}

// Implement the protoiface.MessageV1 interface. This is required by proto.Unmarshal
// at runtime, but we don't expect these methods to actually be called.
func (r *translatedTraceServiceRequest) Reset() {
	r.result = nil
}

func (r *translatedTraceServiceRequest) String() string {
	return "translatedTraceServiceRequest"
}

func (r *translatedTraceServiceRequest) ProtoMessage() {}

// Unmarshal performs translation during the unmarshaling step using husky's
// msgpack optimization. We assume the supplied data field is a re-used buffer
// and we're not allowed to keep any references to it after this call returns,
// although that doesn't seem to be explicitly documented anywhere. Note this
// is an optional method defined in protoiface.Methods. See:
// https://pkg.go.dev/google.golang.org/protobuf/runtime/protoiface#Methods
func (r *translatedTraceServiceRequest) Unmarshal(data []byte) error {
	result, err := huskyotlp.UnmarshalTraceRequestDirectMsgp(r.ctx, data, r.requestInfo)
	if err != nil {
		return err
	}
	r.result = result
	return nil
}

// customTraceExportHandler is a custom GRPC handler that uses husky's optimized
// translation codepath. The dec() callback will call proto.Unmarshal on the supplied
// translatedTraceServiceRequest, (which will in turn call our Unmarshal method).
// That is the ONLY place we'll get access to the input body. I believe that GRPC
// obfuscates the body buffer like this because it's re-using buffers between requests
// and doesn't want people keeping references to it.
func customTraceExportHandler(
	srv any,
	ctx context.Context,
	dec func(any) error,
	interceptor grpc.UnaryServerInterceptor,
) (any, error) {
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

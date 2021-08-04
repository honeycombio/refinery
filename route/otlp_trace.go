package route

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/honeycombio/refinery/types"
	"github.com/klauspost/compress/zstd"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	collectortrace "github.com/honeycombio/refinery/internal/opentelemetry-proto-gen/collector/trace/v1"
	common "github.com/honeycombio/refinery/internal/opentelemetry-proto-gen/common/v1"
	trace "github.com/honeycombio/refinery/internal/opentelemetry-proto-gen/trace/v1"
)

func (router *Router) postOTLP(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	contentType := req.Header.Get("content-type")
	if contentType != "application/protobuf" && contentType != "application/x-protobuf" {
		router.handlerReturnWithError(w, ErrInvalidContentType, errors.New("invalid content-type"))
		return
	}

	apiKey, datasetName, err := getAPIKeyDatasetAndTokenFromHttpHeaders(req)
	if err != nil {
		router.handlerReturnWithError(w, ErrAuthNeeded, err)
		return
	}

	request, cleanup, err := parseOTLPBody(req, router.zstdDecoders)
	defer cleanup()
	if err != nil {
		router.handlerReturnWithError(w, ErrPostBody, err)
	}

	if err := processTraceRequest(req.Context(), router, request, apiKey, datasetName); err != nil {
		router.handlerReturnWithError(w, ErrUpstreamFailed, err)
	}
}

func (router *Router) Export(ctx context.Context, req *collectortrace.ExportTraceServiceRequest) (*collectortrace.ExportTraceServiceResponse, error) {
	apiKey, datasetName, err := getAPIKeyDatasetAndTokenFromMetadata(ctx)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}

	if err := processTraceRequest(ctx, router, req, apiKey, datasetName); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &collectortrace.ExportTraceServiceResponse{}, nil
}

func processTraceRequest(
	ctx context.Context,
	router *Router,
	request *collectortrace.ExportTraceServiceRequest,
	apiKey string,
	datasetName string) error {

	var requestID types.RequestIDContextKey
	debugLog := router.iopLogger.Debug().WithField("request_id", requestID)

	apiHost, err := router.Config.GetHoneycombAPI()
	if err != nil {
		router.Logger.Error().Logf("Unable to retrieve APIHost from config while processing OTLP batch")
		return err
	}

	for _, resourceSpan := range request.ResourceSpans {
		resourceAttrs := make(map[string]interface{})

		if resourceSpan.Resource != nil {
			addAttributesToMap(resourceAttrs, resourceSpan.Resource.Attributes)
		}

		for _, librarySpan := range resourceSpan.InstrumentationLibrarySpans {
			library := librarySpan.InstrumentationLibrary
			if library != nil {
				if len(library.Name) > 0 {
					resourceAttrs["library.name"] = library.Name
				}
				if len(library.Version) > 0 {
					resourceAttrs["library.version"] = library.Version
				}
			}

			for _, span := range librarySpan.GetSpans() {
				traceID := bytesToTraceID(span.TraceId)
				spanID := hex.EncodeToString(span.SpanId)
				timestamp := time.Unix(0, int64(span.StartTimeUnixNano)).UTC()

				spanKind := getSpanKind(span.Kind)
				eventAttrs := map[string]interface{}{
					"trace.trace_id": traceID,
					"trace.span_id":  spanID,
					"type":           spanKind,
					"span.kind":      spanKind,
					"name":           span.Name,
					"duration_ms":    float64(span.EndTimeUnixNano-span.StartTimeUnixNano) / float64(time.Millisecond),
					"status_code":    int32(getSpanStatusCode(span.Status)),
				}
				if span.ParentSpanId != nil {
					eventAttrs["trace.parent_id"] = hex.EncodeToString(span.ParentSpanId)
				}
				if getSpanStatusCode(span.Status) == trace.Status_STATUS_CODE_ERROR {
					eventAttrs["error"] = true
				}
				if span.Status != nil && len(span.Status.Message) > 0 {
					eventAttrs["status_message"] = span.Status.Message
				}
				if span.Attributes != nil {
					addAttributesToMap(eventAttrs, span.Attributes)
				}

				sampleRate, err := getSampleRateFromAttributes(eventAttrs)
				if err != nil {
					debugLog.WithField("error", err.Error()).WithField("sampleRate", eventAttrs["sampleRate"]).Logf("error parsing sampleRate")
				}

				// copy resource attributes to event attributes
				for k, v := range resourceAttrs {
					eventAttrs[k] = v
				}

				events := make([]*types.Event, 0, 1+len(span.Events)+len(span.Links))
				events = append(events, &types.Event{
					Context:    ctx,
					APIHost:    apiHost,
					APIKey:     apiKey,
					Dataset:    datasetName,
					SampleRate: uint(sampleRate),
					Timestamp:  timestamp,
					Data:       eventAttrs,
				})

				for _, sevent := range span.Events {
					timestamp := time.Unix(0, int64(sevent.TimeUnixNano)).UTC()
					attrs := map[string]interface{}{
						"trace.trace_id":       traceID,
						"trace.parent_id":      spanID,
						"name":                 sevent.Name,
						"parent_name":          span.Name,
						"meta.annotation_type": "span_event",
					}

					if sevent.Attributes != nil {
						addAttributesToMap(attrs, sevent.Attributes)
					}
					for k, v := range resourceAttrs {
						attrs[k] = v
					}
					sampleRate, err := getSampleRateFromAttributes(attrs)
					if err != nil {
						debugLog.
							WithField("error", err.Error()).
							WithField("sampleRate", attrs["sampleRate"]).
							Logf("error parsing sampleRate")
					}
					events = append(events, &types.Event{
						Context:    ctx,
						APIHost:    apiHost,
						APIKey:     apiKey,
						Dataset:    datasetName,
						SampleRate: uint(sampleRate),
						Timestamp:  timestamp,
						Data:       attrs,
					})
				}

				for _, slink := range span.Links {
					attrs := map[string]interface{}{
						"trace.trace_id":       traceID,
						"trace.parent_id":      spanID,
						"trace.link.trace_id":  bytesToTraceID(slink.TraceId),
						"trace.link.span_id":   hex.EncodeToString(slink.SpanId),
						"parent_name":          span.Name,
						"meta.annotation_type": "link",
					}

					if slink.Attributes != nil {
						addAttributesToMap(attrs, slink.Attributes)
					}
					for k, v := range resourceAttrs {
						attrs[k] = v
					}
					sampleRate, err := getSampleRateFromAttributes(attrs)
					if err != nil {
						debugLog.
							WithField("error", err.Error()).
							WithField("sampleRate", attrs["sampleRate"]).
							Logf("error parsing sampleRate")
					}
					events = append(events, &types.Event{
						Context:    ctx,
						APIHost:    apiHost,
						APIKey:     apiKey,
						Dataset:    datasetName,
						SampleRate: uint(sampleRate),
						Timestamp:  time.Time{}, //links don't have timestamps, so use empty time
						Data:       attrs,
					})
				}

				for _, evt := range events {
					err = router.processEvent(evt, requestID)
					if err != nil {
						router.Logger.Error().Logf("Error processing event: " + err.Error())
					}
				}

			}
		}
	}

	return nil
}

func addAttributesToMap(attrs map[string]interface{}, attributes []*common.KeyValue) {
	for _, attr := range attributes {
		if attr.Key == "" {
			continue
		}
		switch attr.Value.Value.(type) {
		case *common.AnyValue_StringValue:
			attrs[attr.Key] = attr.Value.GetStringValue()
		case *common.AnyValue_BoolValue:
			attrs[attr.Key] = attr.Value.GetBoolValue()
		case *common.AnyValue_DoubleValue:
			attrs[attr.Key] = attr.Value.GetDoubleValue()
		case *common.AnyValue_IntValue:
			attrs[attr.Key] = attr.Value.GetIntValue()
		}
	}
}

func getSpanKind(kind trace.Span_SpanKind) string {
	switch kind {
	case trace.Span_SPAN_KIND_CLIENT:
		return "client"
	case trace.Span_SPAN_KIND_SERVER:
		return "server"
	case trace.Span_SPAN_KIND_PRODUCER:
		return "producer"
	case trace.Span_SPAN_KIND_CONSUMER:
		return "consumer"
	case trace.Span_SPAN_KIND_INTERNAL:
		return "internal"
	case trace.Span_SPAN_KIND_UNSPECIFIED:
		fallthrough
	default:
		return "unspecified"
	}
}

// bytesToTraceID returns an ID suitable for use for spans and traces. Before
// encoding the bytes as a hex string, we want to handle cases where we are
// given 128-bit IDs with zero padding, e.g. 0000000000000000f798a1e7f33c8af6.
// To do this, we borrow a strategy from Jaeger [1] wherein we split the byte
// sequence into two parts. The leftmost part could contain all zeros. We use
// that to determine whether to return a 64-bit hex encoded string or a 128-bit
// one.
//
// [1]: https://github.com/jaegertracing/jaeger/blob/cd19b64413eca0f06b61d92fe29bebce1321d0b0/model/ids.go#L81
func bytesToTraceID(traceID []byte) string {
	// binary.BigEndian.Uint64() does a bounds check on traceID which will
	// cause a panic if traceID is fewer than 8 bytes. In this case, we don't
	// need to check for zero padding on the high part anyway, so just return a
	// hex string.
	if len(traceID) < traceIDShortLength {
		return fmt.Sprintf("%x", traceID)
	}
	var low uint64
	if len(traceID) == traceIDLongLength {
		low = binary.BigEndian.Uint64(traceID[traceIDShortLength:])
		if high := binary.BigEndian.Uint64(traceID[:traceIDShortLength]); high != 0 {
			return fmt.Sprintf("%016x%016x", high, low)
		}
	} else {
		low = binary.BigEndian.Uint64(traceID)
	}

	return fmt.Sprintf("%016x", low)
}

// getSpanStatusCode checks the value of both the deprecated code and code fields
// on the span status and using the rules specified in the backward compatibility
// notes in the protobuf definitions. See:
//
// https://github.com/open-telemetry/opentelemetry-proto/blob/59c488bfb8fb6d0458ad6425758b70259ff4a2bd/opentelemetry/proto/trace/v1/trace.proto#L230
func getSpanStatusCode(status *trace.Status) trace.Status_StatusCode {
	if status == nil {
		return trace.Status_STATUS_CODE_UNSET
	}
	if status.Code == trace.Status_STATUS_CODE_UNSET {
		if status.DeprecatedCode == trace.Status_DEPRECATED_STATUS_CODE_OK {
			return trace.Status_STATUS_CODE_UNSET
		}
		return trace.Status_STATUS_CODE_ERROR
	}
	return status.Code
}

func getAPIKeyDatasetAndTokenFromMetadata(ctx context.Context) (
	apiKey string,
	datasetName string,
	err error) {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		apiKey = getValueFromMetadata(md, "x-honeycomb-team")
		datasetName = getValueFromMetadata(md, "x-honeycomb-dataset")
	}

	if err := validateHeaders(apiKey, datasetName); err != nil {
		return "", "", err
	}
	return apiKey, datasetName, nil
}

func getValueFromMetadata(md metadata.MD, key string) string {
	if vals := md.Get(key); len(vals) > 0 {
		return vals[0]
	}
	return ""
}

func getAPIKeyDatasetAndTokenFromHttpHeaders(r *http.Request) (
	apiKey string,
	datasetName string,
	err error) {
	apiKey = r.Header.Get("x-honeycomb-team")
	datasetName = r.Header.Get("x-honeycomb-dataset")

	if err := validateHeaders(apiKey, datasetName); err != nil {
		return "", "", err
	}
	return apiKey, datasetName, nil
}

func validateHeaders(apiKey string, datasetName string) error {
	if apiKey == "" {
		return errors.New("missing x-honeycomb-team header")
	}
	if datasetName == "" {
		return errors.New("missing x-honeycomb-team header")
	}
	return nil
}

func parseOTLPBody(r *http.Request, zstdDecoders chan *zstd.Decoder) (request *collectortrace.ExportTraceServiceRequest, cleanup func(), err error) {
	cleanup = func() { /* empty cleanup */ }
	bodyBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, cleanup, err
	}
	bodyReader := bytes.NewReader(bodyBytes)

	var reader io.Reader
	switch r.Header.Get("Content-Encoding") {
	case "gzip":
		var err error
		reader, err = gzip.NewReader(bodyReader)
		if err != nil {
			return nil, cleanup, err
		}
	case "zstd":
		zReader := <-zstdDecoders
		cleanup = func() {
			zReader.Reset(nil)
			zstdDecoders <- zReader
		}

		err = zReader.Reset(bodyReader)
		if err != nil {
			return nil, cleanup, err
		}

		reader = zReader
	default:
		reader = bodyReader
	}

	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, cleanup, err
	}

	otlpRequet := &collectortrace.ExportTraceServiceRequest{}
	err = proto.Unmarshal(bytes, otlpRequet)
	if err != nil {
		return nil, cleanup, err
	}

	return otlpRequet, cleanup, nil
}

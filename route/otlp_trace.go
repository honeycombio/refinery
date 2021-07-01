package route

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/honeycombio/refinery/types"
	"google.golang.org/grpc/metadata"

	collectortrace "github.com/honeycombio/refinery/internal/opentelemetry-proto-gen/collector/trace/v1"
	common "github.com/honeycombio/refinery/internal/opentelemetry-proto-gen/common/v1"
	trace "github.com/honeycombio/refinery/internal/opentelemetry-proto-gen/trace/v1"
)

func (r *Router) Export(ctx context.Context, req *collectortrace.ExportTraceServiceRequest) (*collectortrace.ExportTraceServiceResponse, error) {

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		r.Logger.Error().Logf("Unable to retreive metadata from OTLP request.")
		return &collectortrace.ExportTraceServiceResponse{}, nil
	}

	// requestID is used to track a requst as it moves between refinery nodes (peers)
	// the OTLP handler only receives incoming (not peer) requests for now so will be empty here
	var requestID types.RequestIDContextKey
	debugLog := r.iopLogger.Debug().WithField("request_id", requestID)

	apiKey, dataset := getAPIKeyAndDatasetFromMetadata(md)
	if apiKey == "" {
		r.Logger.Error().Logf("Received OTLP request without Honeycomb APIKey header")
		return &collectortrace.ExportTraceServiceResponse{}, nil
	}
	if dataset == "" {
		r.Logger.Error().Logf("Received OTLP request without Honeycomb dataset header")
		return &collectortrace.ExportTraceServiceResponse{}, nil
	}

	apiHost, err := r.Config.GetHoneycombAPI()
	if err != nil {
		r.Logger.Error().Logf("Unable to retrieve APIHost from config while processing OTLP batch")
		return &collectortrace.ExportTraceServiceResponse{}, nil
	}

	var grpcRequestEncoding string
	if val := md.Get("grpc-accept-encoding"); val != nil {
		grpcRequestEncoding = strings.Join(val, ",")
	}

	for _, resourceSpan := range req.ResourceSpans {
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

				eventAttrs := map[string]interface{}{
					"trace.trace_id": traceID,
					"trace.span_id":  spanID,
					"type":           getSpanKind(span.Kind),
					"name":           span.Name,
					"duration_ms":    float64(span.EndTimeUnixNano-span.StartTimeUnixNano) / float64(time.Millisecond),
					"status_code":    int32(r.getSpanStatusCode(span.Status)),
				}
				if span.ParentSpanId != nil {
					eventAttrs["trace.parent_id"] = hex.EncodeToString(span.ParentSpanId)
				}
				if r.getSpanStatusCode(span.Status) == trace.Status_STATUS_CODE_ERROR {
					eventAttrs["error"] = true
				}
				if span.Status != nil && len(span.Status.Message) > 0 {
					eventAttrs["status_message"] = span.Status.Message
				}
				if span.Attributes != nil {
					addAttributesToMap(eventAttrs, span.Attributes)
				}
				if grpcRequestEncoding != "" {
					eventAttrs["grpc_request_encoding"] = grpcRequestEncoding
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
					Dataset:    dataset,
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
						Dataset:    dataset,
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
						Dataset:    dataset,
						SampleRate: uint(sampleRate),
						Timestamp:  time.Time{}, //links don't have timestamps, so use empty time
						Data:       attrs,
					})
				}

				for _, evt := range events {
					err = r.processEvent(evt, requestID)
					if err != nil {
						r.Logger.Error().Logf("Error processing event: " + err.Error())
					}
				}

			}
		}
	}

	return &collectortrace.ExportTraceServiceResponse{}, nil
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
func (r *Router) getSpanStatusCode(status *trace.Status) trace.Status_StatusCode {
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

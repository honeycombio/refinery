package route

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	huskyotlp "github.com/honeycombio/husky/otlp"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/transmit"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	resource "go.opentelemetry.io/proto/otlp/resource/v1"
	trace "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc/metadata"
)

const legacyAPIKey = "c9945edf5d245834089a1bd6cc9ad01e"

func TestOTLPHandler(t *testing.T) {
	md := metadata.New(map[string]string{"x-honeycomb-team": legacyAPIKey, "x-honeycomb-dataset": "ds"})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	mockMetrics := metrics.MockMetrics{}
	mockMetrics.Start()
	mockTransmission := &transmit.MockTransmission{}
	mockTransmission.Start()
	decoders, err := makeDecoders(1)
	if err != nil {
		t.Error(err)
	}
	router := &Router{
		Config:               &config.MockConfig{},
		Metrics:              &mockMetrics,
		UpstreamTransmission: mockTransmission,
		iopLogger: iopLogger{
			Logger:         &logger.MockLogger{},
			incomingOrPeer: "incoming",
		},
		Logger:           &logger.MockLogger{},
		zstdDecoders:     decoders,
		environmentCache: newEnvironmentCache(time.Second, nil),
	}

	conf := &config.MockConfig{
		GetSendDelayVal:    0,
		GetTraceTimeoutVal: 60 * time.Second,
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		SendTickerVal:      2 * time.Millisecond,
		GetInMemoryCollectorCacheCapacityVal: config.InMemoryCollectorCacheCapacity{
			CacheCapacity: 100,
			MaxAlloc:      100,
		},
	}

	t.Run("span with status", func(t *testing.T) {
		req := &collectortrace.ExportTraceServiceRequest{
			ResourceSpans: []*trace.ResourceSpans{{
				InstrumentationLibrarySpans: []*trace.InstrumentationLibrarySpans{{
					Spans: helperOTLPRequestSpansWithStatus(),
				}},
			}},
		}
		_, err := router.Export(ctx, req)
		if err != nil {
			t.Errorf(`Unexpected error: %s`, err)
		}
		assert.Equal(t, 2, len(mockTransmission.Events))
		mockTransmission.Flush()
	})

	t.Run("span without status", func(t *testing.T) {
		req := &collectortrace.ExportTraceServiceRequest{
			ResourceSpans: []*trace.ResourceSpans{{
				InstrumentationLibrarySpans: []*trace.InstrumentationLibrarySpans{{
					Spans: helperOTLPRequestSpansWithoutStatus(),
				}},
			}},
		}
		_, err := router.Export(ctx, req)
		if err != nil {
			t.Errorf(`Unexpected error: %s`, err)
		}
		assert.Equal(t, 2, len(mockTransmission.Events))
		mockTransmission.Flush()
	})

	// TODO: (MG) figuure out how we can test JSON created from OTLP requests
	// Below is example, but requires significant usage of collector, sampler, conf, etc
	t.Run("creates events for span events", func(t *testing.T) {
		t.Skip("need additional work to support inspecting outbound JSON")

		traceID := []byte{0, 0, 0, 0, 1}
		spanID := []byte{1, 0, 0, 0, 0}
		req := &collectortrace.ExportTraceServiceRequest{
			ResourceSpans: []*trace.ResourceSpans{{
				InstrumentationLibrarySpans: []*trace.InstrumentationLibrarySpans{{
					Spans: []*trace.Span{{
						TraceId: traceID,
						SpanId:  spanID,
						Name:    "span_with_event",
						Events: []*trace.Span_Event{{
							TimeUnixNano: 12345,
							Name:         "span_link",
							Attributes: []*common.KeyValue{{
								Key: "event_attr_key", Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "event_attr_val"}},
							}},
						}},
					}},
				}},
			}},
		}
		_, err := router.Export(ctx, req)
		if err != nil {
			t.Errorf(`Unexpected error: %s`, err)
		}

		time.Sleep(conf.SendTickerVal * 2)

		mockTransmission.Mux.Lock()
		assert.Equal(t, 2, len(mockTransmission.Events))

		spanEvent := mockTransmission.Events[0]
		// assert.Equal(t, time.Unix(0, int64(12345)).UTC(), spanEvent.Timestamp)
		assert.Equal(t, huskyotlp.BytesToTraceID(traceID), spanEvent.Data["trace.trace_id"])
		assert.Equal(t, hex.EncodeToString(spanID), spanEvent.Data["trace.span_id"])
		assert.Equal(t, "span_link", spanEvent.Data["span.name"])
		assert.Equal(t, "span_with_event", spanEvent.Data["parent.name"])
		assert.Equal(t, "span_event", spanEvent.Data["meta.annotation_type"])
		assert.Equal(t, "event_attr_key", spanEvent.Data["event_attr_val"])
		mockTransmission.Mux.Unlock()
		mockTransmission.Flush()
	})

	t.Run("creates events for span links", func(t *testing.T) {
		t.Skip("need additional work to support inspecting outbound JSON")

		traceID := []byte{0, 0, 0, 0, 1}
		spanID := []byte{1, 0, 0, 0, 0}
		linkTraceID := []byte{0, 0, 0, 0, 2}
		linkSpanID := []byte{2, 0, 0, 0, 0}

		req := &collectortrace.ExportTraceServiceRequest{
			ResourceSpans: []*trace.ResourceSpans{{
				InstrumentationLibrarySpans: []*trace.InstrumentationLibrarySpans{{
					Spans: []*trace.Span{{
						Name:    "span_with_link",
						TraceId: traceID,
						SpanId:  spanID,
						Links: []*trace.Span_Link{{
							TraceId:    traceID,
							SpanId:     spanID,
							TraceState: "link_trace_state",
							Attributes: []*common.KeyValue{{
								Key: "link_attr_key", Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "link_attr_val"}},
							}},
						}},
					}},
				}},
			}},
		}
		_, err := router.Export(ctx, req)
		if err != nil {
			t.Errorf(`Unexpected error: %s`, err)
		}

		time.Sleep(conf.SendTickerVal * 2)
		assert.Equal(t, 2, len(mockTransmission.Events))

		spanLink := mockTransmission.Events[1]
		assert.Equal(t, huskyotlp.BytesToTraceID(traceID), spanLink.Data["trace.trace_id"])
		assert.Equal(t, hex.EncodeToString(spanID), spanLink.Data["trace.span_id"])
		assert.Equal(t, huskyotlp.BytesToTraceID(linkTraceID), spanLink.Data["trace.link.trace_id"])
		assert.Equal(t, hex.EncodeToString(linkSpanID), spanLink.Data["trace.link.span_id"])
		assert.Equal(t, "link", spanLink.Data["meta.annotation_type"])
		assert.Equal(t, "link_attr_val", spanLink.Data["link_attr_key"])
		mockTransmission.Flush()
	})

	t.Run("can receive OTLP over HTTP/protobuf", func(t *testing.T) {
		req := &collectortrace.ExportTraceServiceRequest{
			ResourceSpans: []*trace.ResourceSpans{{
				InstrumentationLibrarySpans: []*trace.InstrumentationLibrarySpans{{
					Spans: helperOTLPRequestSpansWithStatus(),
				}},
			}},
		}
		body, err := proto.Marshal(req)
		if err != nil {
			t.Error(err)
		}

		request, _ := http.NewRequest("POST", "/v1/traces", strings.NewReader(string(body)))
		request.Header = http.Header{}
		request.Header.Set("content-type", "application/protobuf")
		request.Header.Set("x-honeycomb-team", legacyAPIKey)
		request.Header.Set("x-honeycomb-dataset", "dataset")

		w := httptest.NewRecorder()
		router.postOTLP(w, request)
		assert.Equal(t, w.Code, http.StatusOK)

		assert.Equal(t, 2, len(mockTransmission.Events))
		mockTransmission.Flush()
	})

	t.Run("can receive OTLP over HTTP/protobuf with gzip encoding", func(t *testing.T) {
		req := &collectortrace.ExportTraceServiceRequest{
			ResourceSpans: []*trace.ResourceSpans{{
				InstrumentationLibrarySpans: []*trace.InstrumentationLibrarySpans{{
					Spans: helperOTLPRequestSpansWithStatus(),
				}},
			}},
		}
		body, err := proto.Marshal(req)
		if err != nil {
			t.Error(err)
		}

		buf := new(bytes.Buffer)
		writer := gzip.NewWriter(buf)
		writer.Write(body)
		writer.Close()
		if err != nil {
			t.Error(err)
		}

		request, _ := http.NewRequest("POST", "/v1/traces", strings.NewReader(buf.String()))
		request.Header = http.Header{}
		request.Header.Set("content-type", "application/protobuf")
		request.Header.Set("content-encoding", "gzip")
		request.Header.Set("x-honeycomb-team", legacyAPIKey)
		request.Header.Set("x-honeycomb-dataset", "dataset")

		w := httptest.NewRecorder()
		router.postOTLP(w, request)
		assert.Equal(t, w.Code, http.StatusOK)

		assert.Equal(t, 2, len(mockTransmission.Events))
		mockTransmission.Flush()
	})

	t.Run("can receive OTLP over HTTP/protobuf with zstd encoding", func(t *testing.T) {
		req := &collectortrace.ExportTraceServiceRequest{
			ResourceSpans: []*trace.ResourceSpans{{
				InstrumentationLibrarySpans: []*trace.InstrumentationLibrarySpans{{
					Spans: helperOTLPRequestSpansWithStatus(),
				}},
			}},
		}
		body, err := proto.Marshal(req)
		if err != nil {
			t.Error(err)
		}

		buf := new(bytes.Buffer)
		writer, err := zstd.NewWriter(buf)
		if err != nil {
			t.Error(err)
		}
		writer.Write(body)
		writer.Close()
		if err != nil {
			t.Error(err)
		}

		request, _ := http.NewRequest("POST", "/v1/traces", strings.NewReader(buf.String()))
		request.Header = http.Header{}
		request.Header.Set("content-type", "application/protobuf")
		request.Header.Set("content-encoding", "zstd")
		request.Header.Set("x-honeycomb-team", legacyAPIKey)
		request.Header.Set("x-honeycomb-dataset", "dataset")

		w := httptest.NewRecorder()
		router.postOTLP(w, request)
		assert.Equal(t, w.Code, http.StatusOK)

		assert.Equal(t, 2, len(mockTransmission.Events))
		mockTransmission.Flush()
	})

	t.Run("rejects OTLP over HTTP/JSON ", func(t *testing.T) {
		request, _ := http.NewRequest("POST", "/v1/traces", strings.NewReader("{}"))
		request.Header = http.Header{}
		request.Header.Set("content-type", "application/json")
		request.Header.Set("x-honeycomb-team", legacyAPIKey)
		request.Header.Set("x-honeycomb-dataset", "dataset")

		w := httptest.NewRecorder()
		router.postOTLP(w, request)
		assert.Equal(t, w.Code, http.StatusNotImplemented)
		assert.Equal(t, `{"source":"refinery","error":"invalid content-type - only 'application/protobuf' is supported"}`, string(w.Body.String()))

		assert.Equal(t, 0, len(mockTransmission.Events))
		mockTransmission.Flush()
	})

	t.Run("events created with legacy keys use dataset header", func(t *testing.T) {
		md := metadata.New(map[string]string{"x-honeycomb-team": legacyAPIKey, "x-honeycomb-dataset": "my-dataset"})
		ctx := metadata.NewIncomingContext(context.Background(), md)

		req := &collectortrace.ExportTraceServiceRequest{
			ResourceSpans: []*trace.ResourceSpans{{
				Resource: &resource.Resource{
					Attributes: []*common.KeyValue{
						{Key: "service.name", Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "my-service"}}},
					},
				},
				InstrumentationLibrarySpans: []*trace.InstrumentationLibrarySpans{{
					Spans: []*trace.Span{{
						Name: "my-span",
					}},
				}},
			}},
		}
		_, err := router.Export(ctx, req)
		if err != nil {
			t.Errorf(`Unexpected error: %s`, err)
		}
		assert.Equal(t, 1, len(mockTransmission.Events))
		event := mockTransmission.Events[0]
		assert.Equal(t, "my-dataset", event.Dataset)
		assert.Equal(t, "", event.Environment)
		mockTransmission.Flush()
	})

	t.Run("events created with non-legacy keys lookup and use envionment name", func(t *testing.T) {
		apiKey := "my-api-key"
		md := metadata.New(map[string]string{"x-honeycomb-team": apiKey})
		ctx := metadata.NewIncomingContext(context.Background(), md)

		// add cached environment lookup
		router.environmentCache.addItem(apiKey, "local", time.Minute)

		req := &collectortrace.ExportTraceServiceRequest{
			ResourceSpans: []*trace.ResourceSpans{{
				Resource: &resource.Resource{
					Attributes: []*common.KeyValue{
						{Key: "service.name", Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "my-service"}}},
					},
				},
				InstrumentationLibrarySpans: []*trace.InstrumentationLibrarySpans{{
					Spans: []*trace.Span{{
						Name: "my-span",
					}},
				}},
			}},
		}
		_, err := router.Export(ctx, req)
		if err != nil {
			t.Errorf(`Unexpected error: %s`, err)
		}
		assert.Equal(t, 1, len(mockTransmission.Events))
		event := mockTransmission.Events[0]
		assert.Equal(t, "my-service", event.Dataset)
		assert.Equal(t, "local", event.Environment)
		mockTransmission.Flush()
	})
}

func helperOTLPRequestSpansWithoutStatus() []*trace.Span {
	now := time.Now()
	return []*trace.Span{
		{
			StartTimeUnixNano: uint64(now.UnixNano()),
			Events: []*trace.Span_Event{
				{
					TimeUnixNano: uint64(now.UnixNano()),
					Attributes: []*common.KeyValue{
						{
							Key: "attribute_key",
							Value: &common.AnyValue{
								Value: &common.AnyValue_StringValue{StringValue: "attribute_value"},
							},
						},
					},
				},
			},
		},
	}
}

func helperOTLPRequestSpansWithStatus() []*trace.Span {
	now := time.Now()
	return []*trace.Span{
		{
			StartTimeUnixNano: uint64(now.UnixNano()),
			Events: []*trace.Span_Event{
				{
					TimeUnixNano: uint64(now.UnixNano()),
					Attributes: []*common.KeyValue{
						{
							Key: "attribute_key",
							Value: &common.AnyValue{
								Value: &common.AnyValue_StringValue{StringValue: "attribute_value"},
							},
						},
					},
				},
			},
			Status: &trace.Status{Code: trace.Status_STATUS_CODE_OK},
		},
	}
}

package route

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	huskyotlp "github.com/honeycombio/husky/otlp"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/transmit"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	resource "go.opentelemetry.io/proto/otlp/resource/v1"
	trace "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
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

	conf := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:   config.Duration(2 * time.Millisecond),
			SendDelay:    config.Duration(1 * time.Millisecond),
			TraceTimeout: config.Duration(60 * time.Second),
			MaxBatchSize: 500,
		},
		GetSamplerTypeVal: &config.DeterministicSamplerConfig{SampleRate: 1},
		GetCollectionConfigVal: config.CollectionConfig{
			CacheCapacity: 100,
			MaxAlloc:      100,
		},
	}

	router := &Router{
		Config:               conf,
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

	t.Run("span with status", func(t *testing.T) {
		req := &collectortrace.ExportTraceServiceRequest{
			ResourceSpans: []*trace.ResourceSpans{{
				ScopeSpans: []*trace.ScopeSpans{{
					Spans: helperOTLPRequestSpansWithStatus(),
				}},
			}},
		}
		traceServer := NewTraceServer(router)
		_, err := traceServer.Export(ctx, req)
		if err != nil {
			t.Errorf(`Unexpected error: %s`, err)
		}
		assert.Equal(t, 2, len(mockTransmission.Events))
		mockTransmission.Flush()
	})

	t.Run("span without status", func(t *testing.T) {
		req := &collectortrace.ExportTraceServiceRequest{
			ResourceSpans: []*trace.ResourceSpans{{
				ScopeSpans: []*trace.ScopeSpans{{
					Spans: helperOTLPRequestSpansWithoutStatus(),
				}},
			}},
		}
		traceServer := NewTraceServer(router)
		_, err := traceServer.Export(ctx, req)
		if err != nil {
			t.Errorf(`Unexpected error: %s`, err)
		}
		assert.Equal(t, 2, len(mockTransmission.Events))
		mockTransmission.Flush()
	})

	// TODO: (MG) figure out how we can test JSON created from OTLP requests
	// Below is example, but requires significant usage of collector, sampler, conf, etc
	t.Run("creates events for span events", func(t *testing.T) {
		t.Skip("need additional work to support inspecting outbound JSON")

		traceID := []byte{0, 0, 0, 0, 1}
		spanID := []byte{1, 0, 0, 0, 0}
		req := &collectortrace.ExportTraceServiceRequest{
			ResourceSpans: []*trace.ResourceSpans{{
				ScopeSpans: []*trace.ScopeSpans{{
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
		traceServer := NewTraceServer(router)
		_, err := traceServer.Export(ctx, req)
		if err != nil {
			t.Errorf(`Unexpected error: %s`, err)
		}

		time.Sleep(conf.GetTracesConfigVal.GetSendTickerValue() * 2)

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
				ScopeSpans: []*trace.ScopeSpans{{
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
		traceServer := NewTraceServer(router)
		_, err := traceServer.Export(ctx, req)
		if err != nil {
			t.Errorf(`Unexpected error: %s`, err)
		}

		time.Sleep(conf.GetTracesConfigVal.GetSendTickerValue() * 2)
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

	t.Run("invalid headers", func(t *testing.T) {
		req := &collectortrace.ExportTraceServiceRequest{}
		body, err := proto.Marshal(req)
		assert.NoError(t, err)
		anEmptyRequestBody := bytes.NewReader(body) // Empty because we're testing headers, not the body.

		testCases := []struct {
			name                        string
			requestContentType          string
			expectedResponseStatus      int
			expectedResponseContentType string
			expectedResponseBody        string
		}{
			{
				name:                        "no key/bad content-type",
				requestContentType:          "application/nope",
				expectedResponseStatus:      http.StatusUnsupportedMediaType, // Prioritize erroring on bad content type over other header issues.
				expectedResponseContentType: "text/plain",
				expectedResponseBody:        huskyotlp.ErrInvalidContentType.Message,
			},
			{
				name:                        "no key/json",
				requestContentType:          "application/json",
				expectedResponseStatus:      http.StatusUnauthorized,
				expectedResponseContentType: "application/json",
				expectedResponseBody:        fmt.Sprintf("{\"message\":\"%s\"}", huskyotlp.ErrMissingAPIKeyHeader.Message),
			},
			{
				name:                        "no key/protobuf",
				requestContentType:          "application/protobuf",
				expectedResponseStatus:      http.StatusUnauthorized,
				expectedResponseContentType: "application/protobuf",
				expectedResponseBody:        fmt.Sprintf("\x12!%s", huskyotlp.ErrMissingAPIKeyHeader.Message),
			},
		}

		for _, tC := range testCases {
			t.Run(tC.name, func(t *testing.T) {
				request, err := http.NewRequest("POST", "/v1/traces", anEmptyRequestBody)
				require.NoError(t, err)
				request.Header = http.Header{}
				request.Header.Set("content-type", tC.requestContentType)
				response := httptest.NewRecorder()
				router.postOTLPTrace(response, request)

				assert.Equal(t, tC.expectedResponseStatus, response.Code)
				assert.Equal(t, tC.expectedResponseContentType, response.Header().Get("content-type"))
				assert.Equal(t, tC.expectedResponseBody, response.Body.String())
			})
		}
	})

	t.Run("can receive OTLP over HTTP/protobuf", func(t *testing.T) {
		req := &collectortrace.ExportTraceServiceRequest{
			ResourceSpans: []*trace.ResourceSpans{{
				ScopeSpans: []*trace.ScopeSpans{{
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
		router.postOTLPTrace(w, request)
		assert.Equal(t, w.Code, http.StatusOK)

		assert.Equal(t, 2, len(mockTransmission.Events))
		mockTransmission.Flush()
	})

	t.Run("can receive OTLP over HTTP/protobuf with gzip encoding", func(t *testing.T) {
		req := &collectortrace.ExportTraceServiceRequest{
			ResourceSpans: []*trace.ResourceSpans{{
				ScopeSpans: []*trace.ScopeSpans{{
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
		router.postOTLPTrace(w, request)
		assert.Equal(t, w.Code, http.StatusOK)

		assert.Equal(t, 2, len(mockTransmission.Events))
		mockTransmission.Flush()
	})

	t.Run("can receive OTLP over HTTP/protobuf with zstd encoding", func(t *testing.T) {
		req := &collectortrace.ExportTraceServiceRequest{
			ResourceSpans: []*trace.ResourceSpans{{
				ScopeSpans: []*trace.ScopeSpans{{
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
		router.postOTLPTrace(w, request)
		assert.Equal(t, w.Code, http.StatusOK)

		assert.Equal(t, 2, len(mockTransmission.Events))
		mockTransmission.Flush()
	})

	t.Run("accepts OTLP over HTTP/JSON ", func(t *testing.T) {
		req := &collectortrace.ExportTraceServiceRequest{
			ResourceSpans: []*trace.ResourceSpans{{
				ScopeSpans: []*trace.ScopeSpans{{
					Spans: helperOTLPRequestSpansWithStatus(),
				}},
			}},
		}
		body, err := protojson.Marshal(req)
		if err != nil {
			t.Error(err)
		}

		request, _ := http.NewRequest("POST", "/v1/traces", bytes.NewReader(body))
		request.Header = http.Header{}
		request.Header.Set("content-type", "application/json")
		request.Header.Set("x-honeycomb-team", legacyAPIKey)
		request.Header.Set("x-honeycomb-dataset", "dataset")

		w := httptest.NewRecorder()
		router.postOTLPTrace(w, request)
		assert.Equal(t, w.Code, http.StatusOK)
		assert.Equal(t, "{}", w.Body.String())

		assert.Equal(t, 2, len(mockTransmission.Events))
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
				ScopeSpans: []*trace.ScopeSpans{{
					Spans: []*trace.Span{{
						Name: "my-span",
					}},
				}},
			}},
		}
		traceServer := NewTraceServer(router)
		_, err := traceServer.Export(ctx, req)
		if err != nil {
			t.Errorf(`Unexpected error: %s`, err)
		}
		assert.Equal(t, 1, len(mockTransmission.Events))
		event := mockTransmission.Events[0]
		assert.Equal(t, "my-dataset", event.Dataset)
		assert.Equal(t, "", event.Environment)
		mockTransmission.Flush()
	})

	t.Run("events created with non-legacy keys lookup and use environment name", func(t *testing.T) {
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
				ScopeSpans: []*trace.ScopeSpans{{
					Spans: []*trace.Span{{
						Name: "my-span",
					}},
				}},
			}},
		}
		traceServer := NewTraceServer(router)
		_, err := traceServer.Export(ctx, req)
		if err != nil {
			t.Errorf(`Unexpected error: %s`, err)
		}
		assert.Equal(t, 1, len(mockTransmission.Events))
		event := mockTransmission.Events[0]
		assert.Equal(t, "my-service", event.Dataset)
		assert.Equal(t, "local", event.Environment)
		mockTransmission.Flush()
	})

	t.Run("rejects bad API keys - HTTP", func(t *testing.T) {
		router.Config.(*config.MockConfig).GetAccessKeyConfigVal = config.AccessKeyConfig{
			ReceiveKeys:          []string{},
			AcceptOnlyListedKeys: true,
		}
		req := &collectortrace.ExportTraceServiceRequest{
			ResourceSpans: []*trace.ResourceSpans{{
				ScopeSpans: []*trace.ScopeSpans{{
					Spans: helperOTLPRequestSpansWithStatus(),
				}},
			}},
		}
		body, err := protojson.Marshal(req)
		if err != nil {
			t.Error(err)
		}

		request, _ := http.NewRequest("POST", "/v1/traces", bytes.NewReader(body))
		request.Header = http.Header{}
		request.Header.Set("content-type", "application/json")
		request.Header.Set("x-honeycomb-team", legacyAPIKey)
		request.Header.Set("x-honeycomb-dataset", "dataset")

		w := httptest.NewRecorder()
		router.postOTLPTrace(w, request)
		assert.Equal(t, http.StatusUnauthorized, w.Code)
		assert.Contains(t, w.Body.String(), "not found in list of authorized keys")

		assert.Equal(t, 0, len(mockTransmission.Events))
		mockTransmission.Flush()
	})

	t.Run("rejects bad API keys - gRPC", func(t *testing.T) {
		router.Config.(*config.MockConfig).GetAccessKeyConfigVal = config.AccessKeyConfig{
			ReceiveKeys:          []string{},
			AcceptOnlyListedKeys: true,
		}
		req := &collectortrace.ExportTraceServiceRequest{
			ResourceSpans: []*trace.ResourceSpans{{
				ScopeSpans: []*trace.ScopeSpans{{
					Spans: helperOTLPRequestSpansWithStatus(),
				}},
			}},
		}
		traceServer := NewTraceServer(router)
		_, err := traceServer.Export(ctx, req)
		assert.Equal(t, codes.Unauthenticated, status.Code(err))
		assert.Contains(t, err.Error(), "not found in list of authorized keys")
		assert.Equal(t, 0, len(mockTransmission.Events))
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

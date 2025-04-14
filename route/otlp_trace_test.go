package route

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	huskyotlp "github.com/honeycombio/husky/otlp"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/transmit"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
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
	defer mockTransmission.Stop()
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
		Tracer:           noop.Tracer{},
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

		events := mockTransmission.GetBlock(2)
		assert.Equal(t, 2, len(events))
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
		events := mockTransmission.GetBlock(2)
		assert.Equal(t, 2, len(events))
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

		events := mockTransmission.GetBlock(2)
		assert.Equal(t, 2, len(events))

		spanEvent := events[0]
		// assert.Equal(t, time.Unix(0, int64(12345)).UTC(), spanEvent.Timestamp)
		assert.Equal(t, huskyotlp.BytesToTraceID(traceID), spanEvent.Data["trace.trace_id"])
		assert.Equal(t, hex.EncodeToString(spanID), spanEvent.Data["trace.span_id"])
		assert.Equal(t, "span_link", spanEvent.Data["span.name"])
		assert.Equal(t, "span_with_event", spanEvent.Data["parent.name"])
		assert.Equal(t, "span_event", spanEvent.Data["meta.annotation_type"])
		assert.Equal(t, "event_attr_key", spanEvent.Data["event_attr_val"])
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

		events := mockTransmission.GetBlock(2)
		assert.Equal(t, 2, len(events))

		spanLink := events[1]
		assert.Equal(t, huskyotlp.BytesToTraceID(traceID), spanLink.Data["trace.trace_id"])
		assert.Equal(t, hex.EncodeToString(spanID), spanLink.Data["trace.span_id"])
		assert.Equal(t, huskyotlp.BytesToTraceID(linkTraceID), spanLink.Data["trace.link.trace_id"])
		assert.Equal(t, hex.EncodeToString(linkSpanID), spanLink.Data["trace.link.span_id"])
		assert.Equal(t, "link", spanLink.Data["meta.annotation_type"])
		assert.Equal(t, "link_attr_val", spanLink.Data["link_attr_key"])
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
				muxxer := mux.NewRouter()
				muxxer.Use(router.apiKeyProcessor)
				router.AddOTLPMuxxer(muxxer)
				server := httptest.NewServer(muxxer)
				defer server.Close()

				request, err := http.NewRequest("POST", server.URL+"/v1/traces", anEmptyRequestBody)
				require.NoError(t, err)
				request.Header = http.Header{}
				request.Header.Set("content-type", tC.requestContentType)

				resp, err := http.DefaultClient.Do(request)
				require.NoError(t, err)

				respBody, err := io.ReadAll(resp.Body)
				require.NoError(t, err)

				assert.Equal(t, tC.expectedResponseStatus, resp.StatusCode)
				assert.Equal(t, tC.expectedResponseContentType, resp.Header.Get("content-type"))
				assert.Equal(t, tC.expectedResponseBody, string(respBody))
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

		events := mockTransmission.GetBlock(2)
		assert.Equal(t, 2, len(events))
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

		events := mockTransmission.GetBlock(2)
		assert.Equal(t, 2, len(events))
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

		events := mockTransmission.GetBlock(2)
		assert.Equal(t, 2, len(events))
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

		events := mockTransmission.GetBlock(2)
		assert.Equal(t, 2, len(events))
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

		events := mockTransmission.GetBlock(1)
		assert.Equal(t, 1, len(events))

		event := events[0]
		assert.Equal(t, "my-dataset", event.Dataset)
		assert.Equal(t, "", event.Environment)
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

		events := mockTransmission.GetBlock(1)
		assert.Equal(t, 1, len(events))

		event := events[0]
		assert.Equal(t, "my-service", event.Dataset)
		assert.Equal(t, "local", event.Environment)
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

		muxxer := mux.NewRouter()
		muxxer.Use(router.apiKeyProcessor)
		router.AddOTLPMuxxer(muxxer)
		server := httptest.NewServer(muxxer)
		defer server.Close()

		request, _ := http.NewRequest("POST", server.URL+"/v1/traces", bytes.NewReader(body))
		request.Header = http.Header{}
		request.Header.Set("content-type", "application/json")
		request.Header.Set("x-honeycomb-team", legacyAPIKey)
		request.Header.Set("x-honeycomb-dataset", "dataset")

		resp, err := http.DefaultClient.Do(request)
		require.NoError(t, err)

		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
		respBody, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Contains(t, string(respBody), "not found in list of authorized keys")

		events := mockTransmission.GetBlock(0)
		assert.Equal(t, 0, len(events))
	})

	t.Run("rejects bad API keys - gRPC", func(t *testing.T) {
		router.Config.(*config.MockConfig).GetAccessKeyConfigVal = config.AccessKeyConfig{
			ReceiveKeys:          []string{},
			AcceptOnlyListedKeys: true,
		}
		defer func() {
			router.Config.(*config.MockConfig).GetAccessKeyConfigVal = config.AccessKeyConfig{
				ReceiveKeys:          []string{legacyAPIKey},
				AcceptOnlyListedKeys: false,
			}
		}()

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

		events := mockTransmission.GetBlock(0)
		assert.Equal(t, 0, len(events))
	})

	t.Run("spans record incoming user agent - gRPC", func(t *testing.T) {
		md := metadata.New(map[string]string{"x-honeycomb-team": legacyAPIKey, "x-honeycomb-dataset": "ds", "user-agent": "my-user-agent"})
		ctx := metadata.NewIncomingContext(context.Background(), md)

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

		events := mockTransmission.GetBlock(2)
		assert.Equal(t, 2, len(events))

		event := events[0]
		assert.Equal(t, "my-user-agent", event.Data["meta.refinery.incoming_user_agent"])
	})

	t.Run("spans record incoming user agent - HTTP", func(t *testing.T) {
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
		request.Header.Set("user-agent", "my-user-agent")

		w := httptest.NewRecorder()
		router.postOTLPTrace(w, request)

		events := mockTransmission.GetBlock(2)
		assert.Equal(t, 2, len(events))

		event := events[0]
		assert.Equal(t, "my-user-agent", event.Data["meta.refinery.incoming_user_agent"])
		mockTransmission.Flush()
	})

	t.Run("use SendKeyMode override - HTTP", func(t *testing.T) {
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

		for _, tt := range []struct {
			apiKey         string
			sendKey        string
			receiverKeys   []string
			mode           string
			wantStatus     int
			wantBody       string
			wantEventCount int
		}{
			{
				sendKey:    "my-send-key",
				mode:       "none",
				wantStatus: http.StatusUnauthorized,
				wantBody:   `{"message":"missing 'x-honeycomb-team' header"}`,
			},
			{
				mode:       "none",
				wantStatus: http.StatusUnauthorized,
				wantBody:   `{"message":"missing 'x-honeycomb-team' header"}`,
			},
			{
				sendKey:        "my-send-key",
				mode:           "all",
				wantStatus:     http.StatusOK,
				wantBody:       "{}",
				wantEventCount: 2,
			},
			{
				mode:       "all",
				wantStatus: http.StatusUnauthorized,
				wantBody:   `{"message":"missing 'x-honeycomb-team' header"}`,
			},
			{
				sendKey:    "my-send-key",
				mode:       "nonblank",
				wantStatus: http.StatusUnauthorized,
				wantBody:   `{"message":"missing 'x-honeycomb-team' header"}`,
			},
			{
				mode:       "nonblank",
				wantStatus: http.StatusUnauthorized,
				wantBody:   `{"message":"missing 'x-honeycomb-team' header"}`,
			},
			{
				apiKey:         "my-api-key",
				sendKey:        "my-send-key",
				mode:           "nonblank",
				wantStatus:     http.StatusOK,
				wantBody:       "{}",
				wantEventCount: 2,
			},
			{
				apiKey:         "my-api-key",
				mode:           "nonblank",
				wantStatus:     http.StatusOK,
				wantBody:       "{}",
				wantEventCount: 2,
			},
			{
				sendKey:    "my-send-key",
				mode:       "invalid-mode",
				wantStatus: http.StatusUnauthorized,
				wantBody:   `{"message":"missing 'x-honeycomb-team' header"}`,
			},
			{
				mode:       "invalid-mode",
				wantStatus: http.StatusUnauthorized,
				wantBody:   `{"message":"missing 'x-honeycomb-team' header"}`,
			},
			{
				apiKey:         "my-api-key",
				sendKey:        "my-send-key",
				receiverKeys:   []string{"my-api-key"},
				mode:           "listedonly",
				wantStatus:     http.StatusOK,
				wantBody:       "{}",
				wantEventCount: 2,
			},
			{
				sendKey:    "my-send-key",
				mode:       "listedonly",
				wantStatus: http.StatusUnauthorized,
				wantBody:   `{"message":"missing 'x-honeycomb-team' header"}`,
			},
			{
				mode:       "listedonly",
				wantStatus: http.StatusUnauthorized,
				wantBody:   `{"message":"missing 'x-honeycomb-team' header"}`,
			},
			{
				sendKey:        "my-send-key",
				mode:           "missingonly",
				wantStatus:     http.StatusOK,
				wantBody:       "{}",
				wantEventCount: 2,
			},
			{
				mode:       "missingonly",
				wantStatus: http.StatusUnauthorized,
				wantBody:   `{"message":"missing 'x-honeycomb-team' header"}`,
			},
			{
				apiKey:         legacyAPIKey,
				sendKey:        "my-send-key",
				receiverKeys:   []string{},
				mode:           "unlisted",
				wantStatus:     http.StatusOK,
				wantBody:       "{}",
				wantEventCount: 2,
			},
			{
				sendKey:    "my-send-key",
				mode:       "unlisted",
				wantStatus: http.StatusUnauthorized,
				wantBody:   `{"message":"missing 'x-honeycomb-team' header"}`,
			},
			{
				mode:       "unlisted",
				wantStatus: http.StatusUnauthorized,
				wantBody:   `{"message":"missing 'x-honeycomb-team' header"}`,
			},
		} {
			t.Run(fmt.Sprintf("ApiKey %s SendKeyMode %s SendKey %s", tt.apiKey, tt.mode, tt.sendKey), func(t *testing.T) {
				router.environmentCache.addItem(tt.apiKey, "local", time.Minute)
				router.environmentCache.addItem(tt.sendKey, "local", time.Minute)

				// HTTP
				request, _ := http.NewRequest("POST", "/v1/traces", bytes.NewReader(body))
				request.Header = http.Header{}
				if len(tt.apiKey) > 0 {
					request.Header.Set("x-honeycomb-team", tt.apiKey)
				}
				request.Header.Set("content-type", "application/json")
				w := httptest.NewRecorder()
				router.Config.(*config.MockConfig).GetAccessKeyConfigVal = config.AccessKeyConfig{
					SendKey:     tt.sendKey,
					SendKeyMode: tt.mode,
					ReceiveKeys: tt.receiverKeys,
				}
				router.postOTLPTrace(w, request)
				require.Equal(t, tt.wantStatus, w.Code)
				require.Equal(t, tt.wantBody, w.Body.String())

				if tt.wantEventCount > 0 {
					events := mockTransmission.GetBlock(tt.wantEventCount)
					assert.Equal(t, tt.wantEventCount, len(events))
				}

				// gRPC
				opts := map[string]string{}
				if len(tt.apiKey) > 0 {
					opts["x-honeycomb-team"] = tt.apiKey
				}
				md := metadata.New(opts)
				ctx := metadata.NewIncomingContext(context.Background(), md)
				traceServer := NewTraceServer(router)
				_, err := traceServer.Export(ctx, req)
				if tt.wantStatus == http.StatusOK {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
				}

				if tt.wantEventCount > 0 {
					events := mockTransmission.GetBlock(tt.wantEventCount)
					assert.Equal(t, tt.wantEventCount, len(events))
				}
			})
		}
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

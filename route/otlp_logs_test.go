package route

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	huskyotlp "github.com/honeycombio/husky/otlp"
	"github.com/honeycombio/refinery/collect"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/sharder"
	"github.com/honeycombio/refinery/transmit"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	logs "go.opentelemetry.io/proto/otlp/logs/v1"
	resource "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func TestLogsOTLPHandler(t *testing.T) {
	md := metadata.New(map[string]string{"x-honeycomb-team": legacyAPIKey, "x-honeycomb-dataset": "ds"})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	mockMetrics := metrics.MockMetrics{}
	mockMetrics.Start()
	mockTransmission := &transmit.MockTransmission{}
	mockTransmission.Start()
	defer mockTransmission.Stop()
	mockCollector := collect.NewMockCollector()
	decoders, err := makeDecoders(1)
	if err != nil {
		t.Error(err)
	}
	logger := &logger.MockLogger{}
	router := &Router{
		Config: &config.MockConfig{
			TraceIdFieldNames: []string{"trace.trace_id"},
		},
		Metrics:              &mockMetrics,
		UpstreamTransmission: mockTransmission,
		iopLogger: iopLogger{
			Logger:         logger,
			incomingOrPeer: "incoming",
		},
		Logger:           logger,
		zstdDecoders:     decoders,
		environmentCache: newEnvironmentCache(time.Second, nil),
		Sharder: &sharder.SingleServerSharder{
			Logger: logger,
		},
		Collector:      mockCollector,
		incomingOrPeer: "incoming",
		Tracer:         noop.Tracer{},
	}
	logsServer := NewLogsServer(router)

	t.Run("can receive OTLP over gRPC", func(t *testing.T) {
		req := &collectorlogs.ExportLogsServiceRequest{
			ResourceLogs: []*logs.ResourceLogs{{
				ScopeLogs: []*logs.ScopeLogs{{
					LogRecords: createLogsRecords(),
				}},
			}},
		}
		_, err := logsServer.Export(ctx, req)
		if err != nil {
			t.Errorf(`Unexpected error: %s`, err)
		}
		events := mockTransmission.GetBlock(1)
		assert.Equal(t, 1, len(events))
	})

	t.Run("invalid headers", func(t *testing.T) {
		req := &collectorlogs.ExportLogsServiceRequest{}
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
				assert.Equal(t, tC.expectedResponseStatus, resp.StatusCode)
				assert.Equal(t, tC.expectedResponseContentType, resp.Header.Get("content-type"))
				body, err := io.ReadAll(resp.Body)
				require.NoError(t, err)

				assert.Equal(t, tC.expectedResponseBody, string(body))
			})
		}
	})

	t.Run("can receive OTLP over HTTP/protobuf", func(t *testing.T) {
		req := &collectorlogs.ExportLogsServiceRequest{
			ResourceLogs: []*logs.ResourceLogs{{
				ScopeLogs: []*logs.ScopeLogs{{
					LogRecords: createLogsRecords(),
				}},
			}},
		}
		body, err := proto.Marshal(req)
		if err != nil {
			t.Error(err)
		}

		muxxer := mux.NewRouter()
		muxxer.Use(router.apiKeyProcessor)
		router.AddOTLPMuxxer(muxxer)
		server := httptest.NewServer(muxxer)
		defer server.Close()

		request, _ := http.NewRequest("POST", server.URL+"/v1/logs", strings.NewReader(string(body)))
		request.Header = http.Header{}
		request.Header.Set("content-type", "application/protobuf")
		request.Header.Set("x-honeycomb-team", legacyAPIKey)
		request.Header.Set("x-honeycomb-dataset", "dataset")

		resp, err := http.DefaultClient.Do(request)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		events := mockTransmission.GetBlock(1)
		assert.Equal(t, 1, len(events))
	})

	t.Run("can receive OTLP over HTTP/protobuf with gzip encoding", func(t *testing.T) {
		req := &collectorlogs.ExportLogsServiceRequest{
			ResourceLogs: []*logs.ResourceLogs{{
				ScopeLogs: []*logs.ScopeLogs{{
					LogRecords: createLogsRecords(),
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

		request, _ := http.NewRequest("POST", "/v1/logs", strings.NewReader(buf.String()))
		request.Header = http.Header{}
		request.Header.Set("content-type", "application/protobuf")
		request.Header.Set("content-encoding", "gzip")
		request.Header.Set("x-honeycomb-team", legacyAPIKey)
		request.Header.Set("x-honeycomb-dataset", "dataset")

		w := httptest.NewRecorder()
		router.postOTLPLogs(w, request)
		assert.Equal(t, w.Code, http.StatusOK)

		events := mockTransmission.GetBlock(1)
		assert.Equal(t, 1, len(events))
	})

	t.Run("can receive OTLP over HTTP/protobuf with zstd encoding", func(t *testing.T) {
		req := &collectorlogs.ExportLogsServiceRequest{
			ResourceLogs: []*logs.ResourceLogs{{
				ScopeLogs: []*logs.ScopeLogs{{
					LogRecords: createLogsRecords(),
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

		request, _ := http.NewRequest("POST", "/v1/logs", strings.NewReader(buf.String()))
		request.Header = http.Header{}
		request.Header.Set("content-type", "application/protobuf")
		request.Header.Set("content-encoding", "zstd")
		request.Header.Set("x-honeycomb-team", legacyAPIKey)
		request.Header.Set("x-honeycomb-dataset", "dataset")

		w := httptest.NewRecorder()
		router.postOTLPLogs(w, request)
		assert.Equal(t, w.Code, http.StatusOK)

		events := mockTransmission.GetBlock(1)
		assert.Equal(t, 1, len(events))
	})

	t.Run("accepts OTLP over HTTP/JSON ", func(t *testing.T) {
		req := &collectorlogs.ExportLogsServiceRequest{
			ResourceLogs: []*logs.ResourceLogs{{
				ScopeLogs: []*logs.ScopeLogs{{
					LogRecords: createLogsRecords(),
				}},
			}},
		}
		body, err := protojson.Marshal(req)
		if err != nil {
			t.Error(err)
		}

		request, _ := http.NewRequest("POST", "/v1/logs", bytes.NewReader(body))
		request.Header = http.Header{}
		request.Header.Set("content-type", "application/json")
		request.Header.Set("x-honeycomb-team", legacyAPIKey)
		request.Header.Set("x-honeycomb-dataset", "dataset")

		w := httptest.NewRecorder()
		router.postOTLPLogs(w, request)
		assert.Equal(t, w.Code, http.StatusOK)
		assert.Equal(t, "{}", w.Body.String())

		events := mockTransmission.GetBlock(1)
		assert.Equal(t, 1, len(events))
	})

	t.Run("rejects bad API keys - HTTP", func(t *testing.T) {
		router.Config.(*config.MockConfig).GetAccessKeyConfigVal = config.AccessKeyConfig{
			ReceiveKeys:          []string{},
			AcceptOnlyListedKeys: true,
		}
		defer func() {
			router.Config.(*config.MockConfig).GetAccessKeyConfigVal = config.AccessKeyConfig{
				ReceiveKeys:          []string{},
				AcceptOnlyListedKeys: false,
			}
		}()
		req := &collectorlogs.ExportLogsServiceRequest{
			ResourceLogs: []*logs.ResourceLogs{{
				ScopeLogs: []*logs.ScopeLogs{{
					LogRecords: createLogsRecords(),
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

		request, _ := http.NewRequest("POST", server.URL+"/v1/logs", bytes.NewReader(body))
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
				ReceiveKeys:          []string{},
				AcceptOnlyListedKeys: false,
			}
		}()
		req := &collectorlogs.ExportLogsServiceRequest{
			ResourceLogs: []*logs.ResourceLogs{{
				ScopeLogs: []*logs.ScopeLogs{{
					LogRecords: createLogsRecords(),
				}},
			}},
		}
		_, err := logsServer.Export(ctx, req)
		assert.Equal(t, codes.Unauthenticated, status.Code(err))
		assert.Contains(t, err.Error(), "not found in list of authorized keys")
		events := mockTransmission.GetBlock(0)
		assert.Equal(t, 0, len(events))
	})

	t.Run("logs with trace ID are added to collector", func(t *testing.T) {
		req := &collectorlogs.ExportLogsServiceRequest{
			ResourceLogs: []*logs.ResourceLogs{{
				Resource: createResource(),
				ScopeLogs: []*logs.ScopeLogs{{
					LogRecords: []*logs.LogRecord{{
						TimeUnixNano: uint64(time.Now().UnixNano()),
						Attributes: []*common.KeyValue{{
							Key: "trace.trace_id",
							Value: &common.AnyValue{
								Value: &common.AnyValue_StringValue{StringValue: "1234567890abcdef"},
							},
						}},
					}},
				}},
			}},
		}
		_, err := logsServer.Export(ctx, req)
		if err != nil {
			t.Errorf(`Unexpected error: %s`, err)
		}
		events := mockTransmission.GetBlock(0)
		assert.Equal(t, 0, len(events))

		assert.Equal(t, 1, len(router.Collector.(*collect.MockCollector).Spans))
		mockCollector.Flush()
	})

	t.Run("logs without trace ID are added to transmission", func(t *testing.T) {
		req := &collectorlogs.ExportLogsServiceRequest{
			ResourceLogs: []*logs.ResourceLogs{{
				Resource: createResource(),
				ScopeLogs: []*logs.ScopeLogs{{
					LogRecords: []*logs.LogRecord{{
						TimeUnixNano: uint64(time.Now().UnixNano()),
					}},
				}},
			}},
		}
		_, err := logsServer.Export(ctx, req)
		if err != nil {
			t.Errorf(`Unexpected error: %s`, err)
		}

		events := mockTransmission.GetBlock(1)
		assert.Equal(t, 1, len(events))

		assert.Equal(t, 0, len(router.Collector.(*collect.MockCollector).Spans))
		mockCollector.Flush()
	})

	t.Run("logs record incoming user agent - gRPC", func(t *testing.T) {
		md := metadata.New(map[string]string{"x-honeycomb-team": legacyAPIKey, "x-honeycomb-dataset": "ds", "user-agent": "my-user-agent"})
		ctx := metadata.NewIncomingContext(context.Background(), md)

		req := &collectorlogs.ExportLogsServiceRequest{
			ResourceLogs: []*logs.ResourceLogs{{
				ScopeLogs: []*logs.ScopeLogs{{
					LogRecords: createLogsRecords(),
				}},
			}},
		}
		_, err := logsServer.Export(ctx, req)
		if err != nil {
			t.Errorf(`Unexpected error: %s`, err)
		}

		events := mockTransmission.GetBlock(1)
		assert.Equal(t, 1, len(events))

		event := events[0]
		assert.Equal(t, "my-user-agent", event.Data["meta.refinery.incoming_user_agent"])

		assert.Equal(t, 0, len(router.Collector.(*collect.MockCollector).Spans))
		mockCollector.Flush()
	})

	t.Run("logs record incoming user agent - HTTP", func(t *testing.T) {
		req := &collectorlogs.ExportLogsServiceRequest{
			ResourceLogs: []*logs.ResourceLogs{{
				ScopeLogs: []*logs.ScopeLogs{{
					LogRecords: createLogsRecords(),
				}},
			}},
		}
		body, err := protojson.Marshal(req)
		if err != nil {
			t.Error(err)
		}

		request, _ := http.NewRequest("POST", "/v1/logs", bytes.NewReader(body))
		request.Header = http.Header{}
		request.Header.Set("content-type", "application/json")
		request.Header.Set("x-honeycomb-team", legacyAPIKey)
		request.Header.Set("x-honeycomb-dataset", "dataset")
		request.Header.Set("user-agent", "my-user-agent")

		w := httptest.NewRecorder()
		router.postOTLPLogs(w, request)
		assert.Equal(t, w.Code, http.StatusOK)

		events := mockTransmission.GetBlock(1)
		assert.Equal(t, 1, len(events))
		event := events[0]
		assert.Equal(t, "my-user-agent", event.Data["meta.refinery.incoming_user_agent"])

		assert.Equal(t, 0, len(router.Collector.(*collect.MockCollector).Spans))
		mockCollector.Flush()
	})

	t.Run("use SendKeyMode override", func(t *testing.T) {
		req := &collectorlogs.ExportLogsServiceRequest{
			ResourceLogs: []*logs.ResourceLogs{{
				ScopeLogs: []*logs.ScopeLogs{{
					LogRecords: createLogsRecords(),
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
				wantEventCount: 1,
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
				wantEventCount: 1,
			},
			{
				apiKey:         "my-api-key",
				mode:           "nonblank",
				wantStatus:     http.StatusOK,
				wantBody:       "{}",
				wantEventCount: 1,
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
				wantEventCount: 1,
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
				wantEventCount: 1,
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
				wantEventCount: 1,
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
				request, _ := http.NewRequest("POST", "/v1/logs", bytes.NewReader(body))
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
				router.postOTLPLogs(w, request)
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
				_, err := logsServer.Export(ctx, req)
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

func createLogsRecords() []*logs.LogRecord {
	now := time.Now()
	return []*logs.LogRecord{
		{
			TimeUnixNano: uint64(now.UnixNano()),
			Body: &common.AnyValue{
				Value: &common.AnyValue_StringValue{StringValue: "log message"},
			},
			Attributes: []*common.KeyValue{
				{
					Key: "attribute_key",
					Value: &common.AnyValue{
						Value: &common.AnyValue_StringValue{StringValue: "attribute_value"},
					},
				},
			},
			SeverityText: "INFO",
		},
	}
}

func createResource() *resource.Resource {
	return &resource.Resource{
		Attributes: []*common.KeyValue{
			{Key: "service.name", Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "my-service"}}},
		},
	}
}

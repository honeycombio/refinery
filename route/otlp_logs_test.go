package route

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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
		assert.Equal(t, 1, len(mockTransmission.Events))
		mockTransmission.Flush()
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

		request, _ := http.NewRequest("POST", "/v1/logs", strings.NewReader(string(body)))
		request.Header = http.Header{}
		request.Header.Set("content-type", "application/protobuf")
		request.Header.Set("x-honeycomb-team", legacyAPIKey)
		request.Header.Set("x-honeycomb-dataset", "dataset")

		w := httptest.NewRecorder()
		router.postOTLPLogs(w, request)
		assert.Equal(t, w.Code, http.StatusOK)

		assert.Equal(t, 1, len(mockTransmission.Events))
		mockTransmission.Flush()
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

		assert.Equal(t, 1, len(mockTransmission.Events))
		mockTransmission.Flush()
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

		assert.Equal(t, 1, len(mockTransmission.Events))
		mockTransmission.Flush()
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

		assert.Equal(t, 1, len(mockTransmission.Events))
		mockTransmission.Flush()
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

		request, _ := http.NewRequest("POST", "/v1/logs", bytes.NewReader(body))
		request.Header = http.Header{}
		request.Header.Set("content-type", "application/json")
		request.Header.Set("x-honeycomb-team", legacyAPIKey)
		request.Header.Set("x-honeycomb-dataset", "dataset")

		w := httptest.NewRecorder()
		router.postOTLPLogs(w, request)
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
		assert.Equal(t, 0, len(mockTransmission.Events))
		mockTransmission.Flush()
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
		assert.Equal(t, 0, len(mockTransmission.Events))
		mockTransmission.Flush()
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
		assert.Equal(t, 1, len(mockTransmission.Events))
		mockTransmission.Flush()
		assert.Equal(t, 0, len(router.Collector.(*collect.MockCollector).Spans))
		mockCollector.Flush()
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

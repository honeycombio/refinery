package route

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/honeycombio/refinery/collect"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/sharder"
	"github.com/honeycombio/refinery/transmit"
	"github.com/honeycombio/refinery/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resource "go.opentelemetry.io/proto/otlp/resource/v1"
	trace "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
)

// This file is the BDD acceptance suite for OTEL-383's `General.EnableMigratedClassicAsEnvironment`
// config option. Each subtest is numbered to match the row in the plan's 17-row
// behavior table. Twelve rows describe behavior that does not change and pass
// today. Five rows (9, 11, 12 go green at Cycle 5; 8, 13 go green at Cycle 6)
// describe the ticket's target behavior and are expected to fail until those
// cycles land. Do not skip or delete a failing row - it is the signal that the
// ticket is not finished.

const (
	unmigratedClassicAPIKey = legacyAPIKey
	migratedClassicAPIKey   = "aaaabbbbccccddddeeeeffff00001111"
	esAPIKey                = "abc123DEF456ghi789jklm"
	migratedEnvName         = "migrated-env"
	esEnvName               = "es-env"
	acceptanceServiceName   = "my-service"
	acceptanceDatasetHeader = "my-dataset"
)

func newEnableMigratedClassicAsEnvironmentTestRouter(t *testing.T) (*Router, *transmit.MockTransmission, *collect.MockCollector) {
	mockMetrics := &metrics.MockMetrics{}
	mockMetrics.Start()
	mockTransmission := &transmit.MockTransmission{}
	mockTransmission.Start()
	t.Cleanup(func() { _ = mockTransmission.Stop() })
	mockCollector := collect.NewMockCollector()
	zstdDecoder, err := makeDecoders(1)
	require.NoError(t, err)

	router := &Router{
		Config: &config.MockConfig{
			TraceIdFieldNames: []string{"trace.trace_id"},
		},
		Metrics:              mockMetrics,
		UpstreamTransmission: mockTransmission,
		iopLogger: iopLogger{
			Logger:         &logger.MockLogger{},
			incomingOrPeer: "incoming",
		},
		Logger:           &logger.MockLogger{},
		zstdDecoder:      zstdDecoder,
		environmentCache: newEnvironmentCache(time.Minute, nil),
		Sharder:          &sharder.SingleServerSharder{Logger: &logger.MockLogger{}},
		Collector:        mockCollector,
		routerType:       types.RouterTypeIncoming,
		Tracer:           noop.Tracer{},
	}
	router.registerMetricNames()

	// Simulate /1/auth results that would already be cached for a migrated
	// Classic environment and for an Environments & Services environment.
	router.environmentCache.addItem(migratedClassicAPIKey, authData{environment: migratedEnvName}, time.Hour)
	router.environmentCache.addItem(esAPIKey, authData{environment: esEnvName}, time.Hour)

	return router, mockTransmission, mockCollector
}

func setEnableMigratedClassicAsEnvironment(router *Router, on bool) {
	router.Config.(*config.MockConfig).EnableMigratedClassicAsEnvironment = on
}

func acceptanceResource(serviceName string) *resource.Resource {
	return &resource.Resource{
		Attributes: []*common.KeyValue{
			{Key: "service.name", Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: serviceName}}},
		},
	}
}

// acceptanceSpan builds a span with no `trace.trace_id` attribute, so with
// TraceIdFieldNames configured, it has a blank MetaTraceID and goes straight
// to UpstreamTransmission rather than the Collector.
func acceptanceSpanRequest(serviceName string) *collectortrace.ExportTraceServiceRequest {
	return &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*trace.ResourceSpans{{
			Resource: acceptanceResource(serviceName),
			ScopeSpans: []*trace.ScopeSpans{{
				Spans: []*trace.Span{{Name: "my-span"}},
			}},
		}},
	}
}

// acceptanceSpanWithTraceID builds a span carrying an explicit `trace.trace_id`
// attribute, so it is routed to the Collector instead of UpstreamTransmission.
func acceptanceSpanWithTraceIDRequest(serviceName, traceID string) *collectortrace.ExportTraceServiceRequest {
	return &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*trace.ResourceSpans{{
			Resource: acceptanceResource(serviceName),
			ScopeSpans: []*trace.ScopeSpans{{
				Spans: []*trace.Span{{
					Name: "my-span",
					Attributes: []*common.KeyValue{
						{Key: "trace.trace_id", Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: traceID}}},
					},
				}},
			}},
		}},
	}
}

func acceptanceLogRequest(serviceName string) *collectorlogs.ExportLogsServiceRequest {
	return &collectorlogs.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{{
			Resource: acceptanceResource(serviceName),
			ScopeLogs: []*logspb.ScopeLogs{{
				LogRecords: []*logspb.LogRecord{{TimeUnixNano: uint64(time.Now().UnixNano())}},
			}},
		}},
	}
}

func acceptanceLogWithTraceIDRequest(serviceName, traceID string) *collectorlogs.ExportLogsServiceRequest {
	return &collectorlogs.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{{
			Resource: acceptanceResource(serviceName),
			ScopeLogs: []*logspb.ScopeLogs{{
				LogRecords: []*logspb.LogRecord{{
					TimeUnixNano: uint64(time.Now().UnixNano()),
					Attributes: []*common.KeyValue{
						{Key: "trace.trace_id", Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: traceID}}},
					},
				}},
			}},
		}},
	}
}

// sendAcceptanceTrace POSTs the given trace request through postOTLPTrace and
// returns the recorded HTTP response.
func sendAcceptanceTrace(t *testing.T, router *Router, apiKey string, withHeader bool, req *collectortrace.ExportTraceServiceRequest) *httptest.ResponseRecorder {
	body, err := protojson.Marshal(req)
	require.NoError(t, err)

	request, _ := http.NewRequest("POST", "/v1/traces", bytes.NewReader(body))
	request.Header = http.Header{}
	request.Header.Set("content-type", "application/json")
	request.Header.Set("x-honeycomb-team", apiKey)
	if withHeader {
		request.Header.Set("x-honeycomb-dataset", acceptanceDatasetHeader)
	}

	w := httptest.NewRecorder()
	router.postOTLPTrace(w, request)
	return w
}

// sendAcceptanceLogs sends the given logs request through a LogsServer's
// Export (gRPC) and returns the error, if any.
func sendAcceptanceLogs(logsServer *LogsServer, apiKey string, withHeader bool, req *collectorlogs.ExportLogsServiceRequest) error {
	headers := map[string]string{"x-honeycomb-team": apiKey}
	if withHeader {
		headers["x-honeycomb-dataset"] = acceptanceDatasetHeader
	}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.New(headers))
	_, err := logsServer.Export(ctx, req)
	return err
}

func TestEnableMigratedClassicAsEnvironmentAcceptance(t *testing.T) {
	router, mockTransmission, mockCollector := newEnableMigratedClassicAsEnvironmentTestRouter(t)
	logsServer := NewLogsServer(router)

	t.Run("row 1: classic unmigrated, traces, header present, option false -> accept, dataset from header", func(t *testing.T) {
		setEnableMigratedClassicAsEnvironment(router, false)
		w := sendAcceptanceTrace(t, router, unmigratedClassicAPIKey, true, acceptanceSpanRequest(acceptanceServiceName))
		require.Equal(t, http.StatusOK, w.Code, "a classic key with the dataset header present is always accepted")
		events := mockTransmission.GetBlock(1)
		require.Equal(t, 1, len(events), "exactly one accepted event should reach transmission")
		assert.Equal(t, acceptanceDatasetHeader, events[0].Dataset, "dataset should come from the x-honeycomb-dataset header for a classic key")
		assert.Equal(t, "", events[0].Environment, "a classic key gets no Environment when the option is off")
	})

	t.Run("row 2: classic unmigrated, traces, header absent, option false -> reject", func(t *testing.T) {
		setEnableMigratedClassicAsEnvironment(router, false)
		w := sendAcceptanceTrace(t, router, unmigratedClassicAPIKey, false, acceptanceSpanRequest(acceptanceServiceName))
		assert.Equal(t, http.StatusUnauthorized, w.Code, "a classic key with no dataset header is rejected when the option is off")
		events := mockTransmission.GetBlock(0)
		assert.Equal(t, 0, len(events), "a rejected request must not produce a transmitted event")
	})

	t.Run("row 3: classic unmigrated, traces, header present, option true -> accept, dataset from header (no change)", func(t *testing.T) {
		setEnableMigratedClassicAsEnvironment(router, true)
		w := sendAcceptanceTrace(t, router, unmigratedClassicAPIKey, true, acceptanceSpanRequest(acceptanceServiceName))
		require.Equal(t, http.StatusOK, w.Code, "the option does not apply to an unmigrated Classic environment")
		events := mockTransmission.GetBlock(1)
		require.Equal(t, 1, len(events), "exactly one accepted event should reach transmission")
		assert.Equal(t, acceptanceDatasetHeader, events[0].Dataset, "an unmigrated classic key still uses the dataset header, option on or off")
		assert.Equal(t, "", events[0].Environment, "an unmigrated classic key never gets an Environment, option on or off")
	})

	t.Run("row 4: classic unmigrated, traces, header absent, option true -> reject (no change)", func(t *testing.T) {
		setEnableMigratedClassicAsEnvironment(router, true)
		w := sendAcceptanceTrace(t, router, unmigratedClassicAPIKey, false, acceptanceSpanRequest(acceptanceServiceName))
		assert.Equal(t, http.StatusUnauthorized, w.Code, "the option does not rescue a classic key in an unmigrated environment from a missing dataset header")
		events := mockTransmission.GetBlock(0)
		assert.Equal(t, 0, len(events), "a rejected request must not produce a transmitted event")
	})

	t.Run("row 5: classic unmigrated, logs no trace_id, option true -> accept, dataset from service.name, EnvironmentName stays blank", func(t *testing.T) {
		setEnableMigratedClassicAsEnvironment(router, true)
		err := sendAcceptanceLogs(logsServer, unmigratedClassicAPIKey, false, acceptanceLogRequest(acceptanceServiceName))
		require.NoError(t, err, "a standalone log is never rejected for a missing dataset header")
		events := mockTransmission.GetBlock(1)
		require.Equal(t, 1, len(events), "exactly one accepted event should reach transmission")
		assert.Equal(t, acceptanceServiceName, events[0].Dataset, "a log's dataset always comes from service.name, never the header")
		assert.Equal(t, "", events[0].Environment, "an unmigrated classic key's Environment stays blank even with the option on")
	})

	t.Run("row 6: migrated, traces, header present, option false -> accept, dataset from header", func(t *testing.T) {
		setEnableMigratedClassicAsEnvironment(router, false)
		w := sendAcceptanceTrace(t, router, migratedClassicAPIKey, true, acceptanceSpanRequest(acceptanceServiceName))
		require.Equal(t, http.StatusOK, w.Code, "a migrated classic key with the dataset header present is accepted when the option is off")
		events := mockTransmission.GetBlock(1)
		require.Equal(t, 1, len(events), "exactly one accepted event should reach transmission")
		assert.Equal(t, acceptanceDatasetHeader, events[0].Dataset, "with the option off, the dataset still comes from the header even for a migrated environment")
		assert.Equal(t, "", events[0].Environment, "with the option off, Refinery never looks up the environment for a classic key")
	})

	t.Run("row 7: migrated, traces, header absent, option false -> reject", func(t *testing.T) {
		setEnableMigratedClassicAsEnvironment(router, false)
		w := sendAcceptanceTrace(t, router, migratedClassicAPIKey, false, acceptanceSpanRequest(acceptanceServiceName))
		assert.Equal(t, http.StatusUnauthorized, w.Code, "with the option off, a migrated classic key without the dataset header is rejected the same as an unmigrated one")
		events := mockTransmission.GetBlock(0)
		assert.Equal(t, 0, len(events), "a rejected request must not produce a transmitted event")
	})

	t.Run("row 8: migrated, traces, header present, option true -> accept, dataset from service.name, header ignored (Cycle 6)", func(t *testing.T) {
		setEnableMigratedClassicAsEnvironment(router, true)
		w := sendAcceptanceTrace(t, router, migratedClassicAPIKey, true, acceptanceSpanRequest(acceptanceServiceName))
		require.Equal(t, http.StatusOK, w.Code, "a migrated environment with the header present is always accepted")
		events := mockTransmission.GetBlock(1)
		require.Equal(t, 1, len(events), "exactly one accepted event should reach transmission")
		assert.Equal(t, acceptanceServiceName, events[0].Dataset, "Cycle 6: with the option on, the header should be ignored and the dataset should come from service.name")
		assert.Equal(t, migratedEnvName, events[0].Environment, "Cycle 5: with the option on, a migrated classic key's Environment should be looked up")
	})

	t.Run("row 9: migrated, traces, header absent, option true -> accept, dataset from service.name (Cycle 5)", func(t *testing.T) {
		setEnableMigratedClassicAsEnvironment(router, true)
		w := sendAcceptanceTrace(t, router, migratedClassicAPIKey, false, acceptanceSpanRequest(acceptanceServiceName))
		require.Equal(t, http.StatusOK, w.Code, "Cycle 5: with the option on, a migrated classic key should be accepted even without the dataset header")
		events := mockTransmission.GetBlock(1)
		require.Equal(t, 1, len(events), "exactly one accepted event should reach transmission")
		assert.Equal(t, acceptanceServiceName, events[0].Dataset, "with the header absent, the dataset should already come from service.name")
		assert.Equal(t, migratedEnvName, events[0].Environment, "Cycle 5: with the option on, a migrated classic key's Environment should be looked up")
	})

	t.Run("row 10: migrated, logs no trace_id, option false -> accept, dataset from service.name, no lookup performed", func(t *testing.T) {
		setEnableMigratedClassicAsEnvironment(router, false)
		err := sendAcceptanceLogs(logsServer, migratedClassicAPIKey, false, acceptanceLogRequest(acceptanceServiceName))
		require.NoError(t, err, "a standalone log is never rejected for a missing dataset header")
		events := mockTransmission.GetBlock(1)
		require.Equal(t, 1, len(events), "exactly one accepted event should reach transmission")
		assert.Equal(t, acceptanceServiceName, events[0].Dataset, "a log's dataset always comes from service.name")
		assert.Equal(t, "", events[0].Environment, "with the option off, Refinery never looks up the environment for a classic key")
	})

	t.Run("row 11: migrated, logs no trace_id, option true -> accept, dataset from service.name, EnvironmentName set to migrated name (Cycle 5)", func(t *testing.T) {
		setEnableMigratedClassicAsEnvironment(router, true)
		err := sendAcceptanceLogs(logsServer, migratedClassicAPIKey, false, acceptanceLogRequest(acceptanceServiceName))
		require.NoError(t, err, "a standalone log is never rejected for a missing dataset header")
		events := mockTransmission.GetBlock(1)
		require.Equal(t, 1, len(events), "exactly one accepted event should reach transmission")
		assert.Equal(t, acceptanceServiceName, events[0].Dataset, "a log's dataset always comes from service.name, option on or off")
		assert.Equal(t, migratedEnvName, events[0].Environment, "Cycle 5: with the option on, a migrated classic key's Environment should be looked up even for a standalone log")
	})

	t.Run("row 12: migrated, logs with trace_id, header absent, option true -> accepted log's trace uses environment-keyed sampler selection (Cycle 5)", func(t *testing.T) {
		setEnableMigratedClassicAsEnvironment(router, true)
		err := sendAcceptanceLogs(logsServer, migratedClassicAPIKey, false, acceptanceLogWithTraceIDRequest(acceptanceServiceName, "row12trace"))
		require.NoError(t, err, "a log with a trace_id is still accepted")

		require.Equal(t, 1, len(mockCollector.Spans), "a log with a trace_id should be routed to the Collector, not transmission")
		span := <-mockCollector.Spans
		assert.Equal(t, acceptanceServiceName, span.Event.Dataset, "a log's dataset always comes from service.name")
		assert.Equal(t, migratedEnvName, span.Event.Environment, "Cycle 5: the log's span should carry the migrated environment name into the shared trace pipeline")
		mockCollector.Flush()
	})

	t.Run("row 13: migrated, logs with trace_id, header present on sibling span, option true -> same, header ignored on the span too (Cycle 6)", func(t *testing.T) {
		setEnableMigratedClassicAsEnvironment(router, true)
		traceID := "row13trace"

		// The sibling span carries the dataset header.
		w := sendAcceptanceTrace(t, router, migratedClassicAPIKey, true, acceptanceSpanWithTraceIDRequest(acceptanceServiceName, traceID))
		require.Equal(t, http.StatusOK, w.Code, "the sibling span with the header present is still accepted")

		// The log, in the same trace, carries no header.
		err := sendAcceptanceLogs(logsServer, migratedClassicAPIKey, false, acceptanceLogWithTraceIDRequest(acceptanceServiceName, traceID))
		require.NoError(t, err, "the log sharing the trace is still accepted")

		require.Equal(t, 2, len(mockCollector.Spans), "both the sibling span and the log should be routed to the Collector, sharing one trace")
		first := <-mockCollector.Spans
		second := <-mockCollector.Spans
		for _, span := range []*types.Span{first, second} {
			assert.Equal(t, acceptanceServiceName, span.Event.Dataset, "Cycle 6: the header on the sibling span should not stop this event's dataset from coming from service.name")
			assert.Equal(t, migratedEnvName, span.Event.Environment, "Cycle 5: every event in the trace should carry the migrated environment name, regardless of which one carried the header")
		}
		mockCollector.Flush()
	})

	t.Run("row 14: E&S, traces, header absent, option false -> accept, dataset from service.name, sampler by env (no change)", func(t *testing.T) {
		setEnableMigratedClassicAsEnvironment(router, false)
		w := sendAcceptanceTrace(t, router, esAPIKey, false, acceptanceSpanRequest(acceptanceServiceName))
		require.Equal(t, http.StatusOK, w.Code, "an E&S key is never subject to the dataset header requirement")
		events := mockTransmission.GetBlock(1)
		require.Equal(t, 1, len(events), "exactly one accepted event should reach transmission")
		assert.Equal(t, acceptanceServiceName, events[0].Dataset, "an E&S key's dataset always comes from service.name")
		assert.Equal(t, esEnvName, events[0].Environment, "an E&S key's Environment always comes from the existing lookup, option on or off")
	})

	t.Run("row 15: E&S, traces, header absent, option true -> accept, dataset from service.name, sampler by env (no change)", func(t *testing.T) {
		setEnableMigratedClassicAsEnvironment(router, true)
		w := sendAcceptanceTrace(t, router, esAPIKey, false, acceptanceSpanRequest(acceptanceServiceName))
		require.Equal(t, http.StatusOK, w.Code, "the option must not change acceptance for an E&S key")
		events := mockTransmission.GetBlock(1)
		require.Equal(t, 1, len(events), "exactly one accepted event should reach transmission")
		assert.Equal(t, acceptanceServiceName, events[0].Dataset, "the option must not change an E&S key's dataset from service.name")
		assert.Equal(t, esEnvName, events[0].Environment, "the option must not change an E&S key's Environment lookup")
	})

	t.Run("row 16: E&S, logs no trace_id, option false -> accept, dataset from service.name, EnvironmentName from existing lookup (no change)", func(t *testing.T) {
		setEnableMigratedClassicAsEnvironment(router, false)
		err := sendAcceptanceLogs(logsServer, esAPIKey, false, acceptanceLogRequest(acceptanceServiceName))
		require.NoError(t, err, "an E&S log is never rejected for a missing dataset header")
		events := mockTransmission.GetBlock(1)
		require.Equal(t, 1, len(events), "exactly one accepted event should reach transmission")
		assert.Equal(t, acceptanceServiceName, events[0].Dataset, "a log's dataset always comes from service.name")
		assert.Equal(t, esEnvName, events[0].Environment, "an E&S key's Environment always comes from the existing lookup")
	})

	t.Run("row 17: E&S, logs no trace_id, option true -> same as row 16", func(t *testing.T) {
		setEnableMigratedClassicAsEnvironment(router, true)
		err := sendAcceptanceLogs(logsServer, esAPIKey, false, acceptanceLogRequest(acceptanceServiceName))
		require.NoError(t, err, "an E&S log is never rejected for a missing dataset header")
		events := mockTransmission.GetBlock(1)
		require.Equal(t, 1, len(events), "exactly one accepted event should reach transmission")
		assert.Equal(t, acceptanceServiceName, events[0].Dataset, "the option must not change an E&S log's dataset")
		assert.Equal(t, esEnvName, events[0].Environment, "the option must not change an E&S log's Environment lookup")
	})
}

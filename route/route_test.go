package route

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/facebookgo/inject"
	"github.com/honeycombio/refinery/collect"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/health"
	"github.com/honeycombio/refinery/internal/peer"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/pubsub"
	"github.com/honeycombio/refinery/sharder"
	"github.com/honeycombio/refinery/transmit"
	"github.com/honeycombio/refinery/types"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	resource "go.opentelemetry.io/proto/otlp/resource/v1"
	trace "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/gorilla/mux"
	"github.com/klauspost/compress/zstd"
	"github.com/vmihailenco/msgpack/v5"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestDecompression(t *testing.T) {
	payload := "payload"
	pReader := strings.NewReader(payload)

	zstdDecoder, err := makeDecoders(1)
	if err != nil {
		t.Errorf("unexpected err: %s", err.Error())
	}

	router := &Router{zstdDecoder: zstdDecoder}
	req := &http.Request{
		Body:   io.NopCloser(pReader),
		Header: http.Header{},
	}
	reader, err := router.readAndCloseMaybeCompressedBody(req)
	if err != nil {
		t.Errorf("unexpected err: %s", err.Error())
	}

	b, err := io.ReadAll(reader)
	if err != nil {
		t.Errorf("unexpected err: %s", err.Error())
	}
	if string(b) != payload {
		t.Errorf("%s != %s", string(b), payload)
	}

	buf := &bytes.Buffer{}
	w := gzip.NewWriter(buf)
	_, err = w.Write([]byte(payload))
	if err != nil {
		t.Errorf("unexpected err: %s", err.Error())
	}
	w.Close()

	req.Body = io.NopCloser(buf)
	req.Header.Set("Content-Encoding", "gzip")
	reader, err = router.readAndCloseMaybeCompressedBody(req)
	if err != nil {
		t.Errorf("unexpected err: %s", err.Error())
	}

	b, err = io.ReadAll(reader)
	if err != nil {
		t.Errorf("unexpected err: %s", err.Error())
	}
	if string(b) != payload {
		t.Errorf("%s != %s", string(b), payload)
	}

	buf = &bytes.Buffer{}
	zstdW, err := zstd.NewWriter(buf)
	if err != nil {
		t.Errorf("unexpected err: %s", err.Error())
	}
	_, err = zstdW.Write([]byte(payload))
	if err != nil {
		t.Errorf("unexpected err: %s", err.Error())
	}
	zstdW.Close()

	req.Body = io.NopCloser(buf)
	req.Header.Set("Content-Encoding", "zstd")
	reader, err = router.readAndCloseMaybeCompressedBody(req)
	if err != nil {
		t.Errorf("unexpected err: %s", err.Error())
	}

	b, err = io.ReadAll(reader)
	if err != nil {
		t.Errorf("unexpected err: %s", err.Error())
	}
	if string(b) != payload {
		t.Errorf("%s != %s", string(b), payload)
	}
}

func TestUnmarshal(t *testing.T) {
	now := time.Now().UTC()
	mockCfg := &config.MockConfig{
		TraceIdFieldNames:  []string{"trace.trace_id"},
		ParentIdFieldNames: []string{"trace.span_id"},
		GetSamplerTypeVal: &config.DynamicSamplerConfig{
			FieldList: []string{"service.name", "status_code"},
		},
	}

	// Common test data - using only floats to avoid JSON type conversion issues
	testData := map[string]interface{}{
		"trace.trace_id": "test-trace-id",
		"trace.span_id":  "test-span-id",
		"service.name":   "test-service",
		"operation.name": "test-operation",
		"duration_ms":    150.5,
		"status_code":    200.0,
		"user_id":        12345.0,
		"is_error":       false,
	}

	t.Run("invalid content type defaults to JSON", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/test", bytes.NewBufferString("{}"))
		req.Header.Set("Content-Type", "nope")

		var data map[string]interface{}
		err := unmarshal(req, readAll(t, req.Body), &data)
		// Should succeed because invalid content type defaults to JSON
		assert.NoError(t, err)
	})

	// Test map[string]interface{} unmarshaling (used in requestToEvent)
	t.Run("map[string]interface{}", func(t *testing.T) {
		t.Run("json", func(t *testing.T) {
			jsonData, err := json.Marshal(testData)
			require.NoError(t, err)

			for _, contentType := range []string{"application/json", "application/json; charset=utf-8"} {
				t.Run(contentType, func(t *testing.T) {
					req := httptest.NewRequest("POST", "/test", bytes.NewReader(jsonData))
					req.Header.Set("Content-Type", contentType)

					var result map[string]interface{}
					err = unmarshal(req, readAll(t, req.Body), &result)
					require.NoError(t, err)

					// Compare directly to test data
					assert.Equal(t, testData, result)
				})
			}
		})

		t.Run("msgpack", func(t *testing.T) {
			for _, contentType := range []string{"application/msgpack", "application/x-msgpack"} {
				t.Run(contentType, func(t *testing.T) {
					buf := &bytes.Buffer{}
					encoder := msgpack.NewEncoder(buf)
					err := encoder.Encode(testData)
					require.NoError(t, err)

					req := httptest.NewRequest("POST", "/test", buf)
					req.Header.Set("Content-Type", contentType)

					var result map[string]interface{}
					err = unmarshal(req, readAll(t, req.Body), &result)
					require.NoError(t, err)

					// Compare directly to test data
					assert.Equal(t, testData, result)
				})
			}
		})
	})

	// Test batchedEvents unmarshaling (used in batch)
	t.Run("batchedEvents", func(t *testing.T) {
		t.Run("json", func(t *testing.T) {
			batch := newBatchedEvents(types.CoreFieldsUnmarshalerOptions{
				Config:  mockCfg,
				APIKey:  "api-key",
				Env:     "",
				Dataset: "",
			})
			batch.events = []batchedEvent{
				{
					Timestamp:  now.Format(time.RFC3339Nano),
					SampleRate: 2,
					Data:       types.NewPayload(mockCfg, testData),
					cfg:        mockCfg,
				},
				{
					Timestamp:  now.Add(time.Second).Format(time.RFC3339Nano),
					SampleRate: 4,
					Data:       types.NewPayload(mockCfg, testData),
					cfg:        mockCfg,
				},
			}
			jsonData, err := json.Marshal(batch)
			require.NoError(t, err)

			for _, contentType := range []string{"application/json", "application/json; charset=utf-8"} {
				t.Run(contentType, func(t *testing.T) {
					req := httptest.NewRequest("POST", "/test", bytes.NewReader(jsonData))
					req.Header.Set("Content-Type", contentType)

					result := newBatchedEvents(types.CoreFieldsUnmarshalerOptions{
						Config:  mockCfg,
						APIKey:  "api-key",
						Env:     "env",
						Dataset: "dataset",
					})
					err = unmarshal(req, readAll(t, req.Body), result)
					require.NoError(t, err)
					require.Len(t, result.events, 2)

					assert.Equal(t, now.UTC(), result.events[0].getEventTime())
					assert.Equal(t, uint(2), result.events[0].getSampleRate())
					assert.Equal(t, testData, maps.Collect(result.events[0].Data.All()))

					assert.Equal(t, now.Add(time.Second).UTC(), result.events[1].getEventTime())
					assert.Equal(t, uint(4), result.events[1].getSampleRate())
					assert.Equal(t, testData, maps.Collect(result.events[1].Data.All()))
				})
			}
		})

		t.Run("msgpack", func(t *testing.T) {
			// Create test data as a simple struct that can be marshaled/unmarshaled
			type testBatchEvent struct {
				MsgPackTimestamp *time.Time             `msgpack:"time,omitempty"`
				SampleRate       int64                  `msgpack:"samplerate"`
				Data             map[string]interface{} `msgpack:"data"`
			}

			later := now.Add(time.Second)
			testEvents := []testBatchEvent{
				{
					MsgPackTimestamp: &now,
					SampleRate:       3,
					Data:             testData,
				},
				{
					MsgPackTimestamp: &later,
					SampleRate:       6,
					Data:             testData,
				},
			}

			for _, contentType := range []string{"application/msgpack", "application/x-msgpack"} {
				t.Run(contentType, func(t *testing.T) {
					buf := &bytes.Buffer{}
					encoder := msgpack.NewEncoder(buf)
					err := encoder.Encode(testEvents)
					require.NoError(t, err)

					req := httptest.NewRequest("POST", "/test", buf)
					req.Header.Set("Content-Type", contentType)

					result := newBatchedEvents(types.CoreFieldsUnmarshalerOptions{
						Config:  mockCfg,
						APIKey:  "api-key",
						Env:     "env",
						Dataset: "dataset",
					})
					err = unmarshal(req, readAll(t, req.Body), result)
					require.NoError(t, err)
					require.Len(t, result.events, 2)

					assert.Equal(t, now.UTC(), result.events[0].getEventTime())
					assert.Equal(t, uint(3), result.events[0].getSampleRate())
					assert.Equal(t, testData, maps.Collect(result.events[0].Data.All()))

					assert.Equal(t, now.Add(time.Second).UTC(), result.events[1].getEventTime())
					assert.Equal(t, uint(6), result.events[1].getSampleRate())
					assert.Equal(t, testData, maps.Collect(result.events[1].Data.All()))

				})
			}
		})
	})
}

func TestGetAPIKeyAndDatasetFromMetadataCaseInsensitive(t *testing.T) {
	const (
		apiKeyValue  = "test-apikey"
		datasetValue = "test-dataset"
	)

	tests := []struct {
		name          string
		apikeyHeader  string
		datasetHeader string
	}{
		{
			name:          "lowercase",
			apikeyHeader:  "x-honeycomb-team",
			datasetHeader: "x-honeycomb-dataset",
		},
		{
			name:          "uppercase",
			apikeyHeader:  "X-HONEYCOMB-TEAM",
			datasetHeader: "X-HONEYCOMB-DATASET",
		},
		{
			name:          "mixed-case",
			apikeyHeader:  "x-HoNeYcOmB-tEaM",
			datasetHeader: "X-hOnEyCoMb-DaTaSeT",
		},
		{
			name:          "lowercase-short",
			apikeyHeader:  "x-hny-team",
			datasetHeader: "x-honeycomb-dataset",
		},
		{
			name:          "uppercase-short",
			apikeyHeader:  "X-HNY-TEAM",
			datasetHeader: "X-HONEYCOMB-DATASET",
		},
		{
			name:          "mixed-case-short",
			apikeyHeader:  "X-hNy-TeAm",
			datasetHeader: "X-hOnEyCoMb-DaTaSeT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			md := metadata.MD{}
			md.Set(tt.apikeyHeader, apiKeyValue)
			md.Set(tt.datasetHeader, datasetValue)

			apikey, dataset := getAPIKeyAndDatasetFromMetadata(md)
			if apikey != apiKeyValue {
				t.Errorf("got: %s\n\twant: %v", apikey, apiKeyValue)
			}
			if dataset != datasetValue {
				t.Errorf("got: %s\n\twant: %v", dataset, datasetValue)
			}
		})
	}
}

func TestDebugTrace(t *testing.T) {
	req, _ := http.NewRequest("GET", "/debug/trace/123abcdef", nil)
	req = mux.SetURLVars(req, map[string]string{"traceID": "123abcdef"})

	rr := httptest.NewRecorder()
	router := &Router{
		Sharder: &sharder.MockSharder{
			Self:  &sharder.TestShard{Addr: "http://localhost:12345"},
			Other: &sharder.TestShard{Addr: "http://localhost:12345", TraceIDs: []string{"123abcdef"}},
		},
	}

	router.debugTrace(rr, req)
	if body := rr.Body.String(); body != `{"traceID":"123abcdef","node":"http://localhost:12345"}` {
		t.Error(body)
	}
}

func TestOTLPRequest(t *testing.T) {
	mockMetrics := metrics.MockMetrics{}
	mockMetrics.Start()
	mockTransmission := &transmit.MockTransmission{}
	mockTransmission.Start()
	router := &Router{
		Config: &config.MockConfig{
			TraceIdFieldNames:  []string{"trace.trace_id"},
			ParentIdFieldNames: []string{"trace.parent_id"},
		},
		Metrics:              &mockMetrics,
		UpstreamTransmission: mockTransmission,
		routerType:           types.RouterTypeIncoming,
		iopLogger: iopLogger{
			Logger:         &logger.MockLogger{},
			incomingOrPeer: "incoming",
		},
		Logger:           &logger.MockLogger{},
		environmentCache: newEnvironmentCache(time.Second, nil),
		Tracer:           noop.Tracer{},
		Collector:        collect.NewMockCollector(),
		Sharder: &sharder.MockSharder{
			Self: &sharder.TestShard{Addr: "http://test"},
		},
	}

	router.registerMetricNames()

	muxxer := mux.NewRouter()
	muxxer.Use(router.apiKeyProcessor)
	router.AddOTLPMuxxer(muxxer)
	server := httptest.NewServer(muxxer)
	defer server.Close()

	request := &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*trace.ResourceSpans{{
			ScopeSpans: []*trace.ScopeSpans{{
				Spans: helperOTLPRequestSpansWithStatus(),
			}},
		}},
	}
	body, err := protojson.Marshal(request)
	if err != nil {
		t.Error(err)
	}

	for i, tracePath := range []string{"/v1/traces", "/v1/traces/"} {
		t.Run(tracePath, func(t *testing.T) {
			req, _ := http.NewRequest("POST", server.URL+tracePath, bytes.NewReader(body))
			req.Header = http.Header{}
			req.Header.Set("content-type", "application/json")
			req.Header.Set("x-honeycomb-team", legacyAPIKey)
			req.Header.Set("x-honeycomb-dataset", "dataset")

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, resp.StatusCode)

			v, ok := mockMetrics.Get("incoming_router_span")
			require.True(t, ok)
			require.Equal(t, float64(i+1)*2, v, "each request should contain 2 spans")
		})
	}
}

func TestDebugAllRules(t *testing.T) {
	tests := []struct {
		format string
		expect string
	}{
		{
			format: "json",
			expect: `{"rulesversion":0,"samplers":{"dataset1":{"deterministicsampler":{"samplerate":0},"rulesbasedsampler":null,"dynamicsampler":null,"emadynamicsampler":null,"emathroughputsampler":null,"windowedthroughputsampler":null,"totalthroughputsampler":null}}}`,
		},
		{
			format: "toml",
			expect: "RulesVersion = 0\n\n[Samplers]\n[Samplers.dataset1]\n[Samplers.dataset1.DeterministicSampler]\nSampleRate = 0\n",
		},
		{
			format: "yaml",
			expect: "RulesVersion: 0\nSamplers:\n    dataset1:\n        DeterministicSampler: {}\n",
		},
		{
			format: "bogus",
			expect: "invalid format 'bogus' when marshaling\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.format, func(t *testing.T) {

			req, _ := http.NewRequest("GET", "/debug/allrules/"+tt.format, nil)
			req = mux.SetURLVars(req, map[string]string{"format": tt.format})

			rr := httptest.NewRecorder()
			router := &Router{
				Config: &config.MockConfig{
					GetSamplerTypeVal: &config.DeterministicSamplerConfig{},
				},
			}

			router.getAllSamplerRules(rr, req)
			assert.Equal(t, tt.expect, rr.Body.String())
		})
	}
}

func TestDebugRules(t *testing.T) {
	tests := []struct {
		format  string
		dataset string
		expect  string
	}{
		{
			format:  "json",
			dataset: "dataset1",
			expect:  `{"FakeSamplerName":"FakeSamplerType"}`,
		},
		{
			format:  "toml",
			dataset: "dataset1",
			expect:  "FakeSamplerName = 'FakeSamplerType'\n",
		},
		{
			format:  "yaml",
			dataset: "dataset1",
			expect:  "FakeSamplerName: FakeSamplerType\n",
		},
		{
			format:  "bogus",
			dataset: "dataset1",
			expect:  "invalid format 'bogus' when marshaling\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.format, func(t *testing.T) {

			req, _ := http.NewRequest("GET", "/debug/rules/"+tt.format+"/"+tt.format, nil)
			req = mux.SetURLVars(req, map[string]string{
				"format":  tt.format,
				"dataset": tt.dataset,
			})

			rr := httptest.NewRecorder()
			router := &Router{
				Config: &config.MockConfig{
					GetSamplerTypeVal:  "FakeSamplerType",
					GetSamplerTypeName: "FakeSamplerName",
				},
			}

			router.getSamplerRules(rr, req)
			assert.Equal(t, tt.expect, rr.Body.String())
		})
	}
}

func TestDependencyInjection(t *testing.T) {
	var g inject.Graph
	err := g.Provide(
		&inject.Object{Value: &Router{}},

		&inject.Object{Value: &config.MockConfig{}},
		&inject.Object{Value: &logger.NullLogger{}},
		&inject.Object{Value: noop.NewTracerProvider().Tracer("test"), Name: "tracer"},
		&inject.Object{Value: http.DefaultTransport, Name: "upstreamTransport"},
		&inject.Object{Value: &transmit.MockTransmission{}, Name: "upstreamTransmission"},
		&inject.Object{Value: &transmit.MockTransmission{}, Name: "peerTransmission"},
		&inject.Object{Value: &pubsub.LocalPubSub{}},
		&inject.Object{Value: &sharder.MockSharder{}},
		&inject.Object{Value: &collect.InMemCollector{}},
		&inject.Object{Value: &metrics.NullMetrics{}, Name: "metrics"},
		&inject.Object{Value: &collect.MockStressReliever{}, Name: "stressRelief"},
		&inject.Object{Value: &peer.MockPeers{}},
		&inject.Object{Value: &health.Health{}},
		&inject.Object{Value: clockwork.NewFakeClock()},
	)
	if err != nil {
		t.Error(err)
	}
	if err := g.Populate(); err != nil {
		t.Error(err)
	}
}

func TestEnvironmentCache(t *testing.T) {
	t.Run("calls getFn on cache miss", func(t *testing.T) {
		cache := newEnvironmentCache(time.Second, func(key string) (string, error) {
			if key != "key" {
				t.Errorf("expected %s - got %s", "key", key)
			}
			return "test", nil
		})

		val, err := cache.get("key")
		if err != nil {
			t.Errorf("got error calling getOrSet - %e", err)
		}
		if val != "test" {
			t.Errorf("expected %s - got %s", "test", val)
		}
	})

	t.Run("does not call getFn on cache hit", func(t *testing.T) {
		cache := newEnvironmentCache(time.Second, func(key string) (string, error) {
			t.Errorf("should not have called getFn")
			return "", nil
		})
		cache.addItem("key", "value", time.Second)

		val, err := cache.get("key")
		if err != nil {
			t.Errorf("got error calling getOrSet - %e", err)
		}
		if val != "value" {
			t.Errorf("expected %s - got %s", "value", val)
		}
	})

	t.Run("ignores expired items", func(t *testing.T) {
		called := false
		cache := newEnvironmentCache(time.Millisecond, func(key string) (string, error) {
			called = true
			return "value", nil
		})
		cache.addItem("key", "value", time.Millisecond)
		time.Sleep(time.Millisecond * 5)

		val, err := cache.get("key")
		if err != nil {
			t.Errorf("got error calling getOrSet - %e", err)
		}
		if val != "value" {
			t.Errorf("expected %s - got %s", "value", val)
		}
		if !called {
			t.Errorf("expected to call getFn")
		}
	})

	t.Run("errors returned from getFn are propagated", func(t *testing.T) {
		expectedErr := errors.New("error")
		cache := newEnvironmentCache(time.Second, func(key string) (string, error) {
			return "", expectedErr
		})

		_, err := cache.get("key")
		if err != expectedErr {
			t.Errorf("expected %e - got %e", expectedErr, err)
		}
	})
}

func TestGRPCHealthProbeCheck(t *testing.T) {
	router := &Router{
		Config: &config.MockConfig{},
		iopLogger: iopLogger{
			Logger:         &logger.MockLogger{},
			incomingOrPeer: "incoming",
		},
	}

	req := &grpc_health_v1.HealthCheckRequest{}
	resp, err := router.Check(context.Background(), req)
	if err != nil {
		t.Errorf(`Unexpected error: %s`, err)
	}
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.Status)
}

func TestGRPCHealthProbeWatch(t *testing.T) {
	router := &Router{
		Config: &config.MockConfig{},
		iopLogger: iopLogger{
			Logger:         &logger.MockLogger{},
			incomingOrPeer: "incoming",
		},
	}

	mockServer := &MockGRPCHealthWatchServer{}
	err := router.Watch(&grpc_health_v1.HealthCheckRequest{}, mockServer)
	if err != nil {
		t.Errorf(`Unexpected error: %s`, err)
	}
	assert.Equal(t, 1, len(mockServer.GetSentMessages()))

	sentMessage := mockServer.GetSentMessages()[0]
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, sentMessage.Status)
}

func TestGetDatasetFromRequest(t *testing.T) {
	testCases := []struct {
		name                string
		datasetName         string
		expectedDatasetName string
		expectedError       error
	}{
		{
			name:          "empty dataset name",
			datasetName:   "",
			expectedError: fmt.Errorf("missing dataset name"),
		},
		{
			name:          "dataset name with invalid URL encoding",
			datasetName:   "foo%2",
			expectedError: url.EscapeError("%2"),
		},
		{
			name:                "normal dataset name",
			datasetName:         "foo",
			expectedDatasetName: "foo",
		},
		{
			name:                "dataset name with numbers",
			datasetName:         "foo123",
			expectedDatasetName: "foo123",
		},
		{
			name:                "dataset name with hyphen",
			datasetName:         "foo-bar",
			expectedDatasetName: "foo-bar",
		},
		{
			name:                "dataset name with underscore",
			datasetName:         "foo_bar",
			expectedDatasetName: "foo_bar",
		},
		{
			name:                "dataset name with tilde",
			datasetName:         "foo~bar",
			expectedDatasetName: "foo~bar",
		},
		{
			name:                "dataset name with period",
			datasetName:         "foo.bar",
			expectedDatasetName: "foo.bar",
		},
		{
			name:                "dataset name with URL encoded hyphen",
			datasetName:         "foo%2Dbar",
			expectedDatasetName: "foo-bar",
		},
		{
			name:                "dataset name with URL encoded underscore",
			datasetName:         "foo%5Fbar",
			expectedDatasetName: "foo_bar",
		},
		{
			name:                "dataset name with URL encoded tilde",
			datasetName:         "foo%7Ebar",
			expectedDatasetName: "foo~bar",
		},
		{
			name:                "dataset name with URL encoded period",
			datasetName:         "foo%2Ebar",
			expectedDatasetName: "foo.bar",
		},
		{
			name:                "dataset name with URL encoded forward slash",
			datasetName:         "foo%2Fbar",
			expectedDatasetName: "foo/bar",
		},
		{
			name:                "dataset name with URL encoded colon",
			datasetName:         "foo%3Abar",
			expectedDatasetName: "foo:bar",
		},
		{
			name:                "dataset name with URL encoded square brackets",
			datasetName:         "foo%5Bbar%5D",
			expectedDatasetName: "foo[bar]",
		},
		{
			name:                "dataset name with URL encoded parentheses",
			datasetName:         "foo%28bar%29",
			expectedDatasetName: "foo(bar)",
		},
		{
			name:                "dataset name with URL encoded curly braces",
			datasetName:         "foo%7Bbar%7D",
			expectedDatasetName: "foo{bar}",
		},
		{
			name:                "dataset name with URL encoded percent",
			datasetName:         "foo%25bar",
			expectedDatasetName: "foo%bar",
		},
		{
			name:                "dataset name with URL encoded ampersand",
			datasetName:         "foo%26bar",
			expectedDatasetName: "foo&bar",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := http.NewRequest("GET", "/1/events/dataset", nil)
			req = mux.SetURLVars(req, map[string]string{"datasetName": tc.datasetName})

			dataset, err := getDatasetFromRequest(req)
			assert.Equal(t, tc.expectedError, err)
			assert.Equal(t, tc.expectedDatasetName, dataset)
		})
	}
}
func TestExtractMetadataTraceID(t *testing.T) {
	mockCfg := &config.MockConfig{
		TraceIdFieldNames: []string{"trace.trace_id", "traceId"},
	}
	testCases := []struct {
		name     string
		event    types.Event
		expected string
	}{
		{
			name: "trace id from meta.trace_id",
			event: types.Event{
				Data: types.NewPayload(mockCfg, map[string]interface{}{
					"meta.trace_id": "trace123",
				}),
			},
			expected: "trace123",
		},
		{
			name: "trace id from trace.trace_id field",
			event: types.Event{
				Data: types.NewPayload(mockCfg, map[string]interface{}{
					"trace.trace_id": "trace456",
				}),
			},
			expected: "trace456",
		},
		{
			name: "trace id from traceId field",
			event: types.Event{
				Data: types.NewPayload(mockCfg, map[string]interface{}{
					"traceId": "trace789",
				}),
			},
			expected: "trace789",
		},
		{
			name: "no trace id",
			event: types.Event{
				Data: types.NewPayload(mockCfg, nil),
			},
			expected: "",
		},
		{
			name: "prefer meta.trace_id over other fields",
			event: types.Event{
				Data: types.NewPayload(mockCfg, map[string]interface{}{
					"meta.trace_id":  "meta-trace",
					"trace.trace_id": "field-trace",
				}),
			},
			expected: "meta-trace",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.event.Data.ExtractMetadata()
			assert.Equal(t, tc.expected, tc.event.Data.MetaTraceID)
		})
	}
}

func TestAddIncomingUserAgent(t *testing.T) {
	t.Run("no incoming user agent", func(t *testing.T) {
		payload := types.NewPayload(&config.MockConfig{}, nil)
		event := &types.Event{
			Data: payload,
		}

		addIncomingUserAgent(event, "test-agent")
		require.Equal(t, "test-agent", event.Data.MetaRefineryIncomingUserAgent)
	})

	t.Run("existing incoming user agent", func(t *testing.T) {
		payload := types.NewPayload(&config.MockConfig{}, map[string]interface{}{
			"meta.refinery.incoming_user_agent": "test-agent",
		})
		payload.ExtractMetadata()
		event := &types.Event{
			Data: payload,
		}

		addIncomingUserAgent(event, "another-test-agent")
		require.Equal(t, "test-agent", event.Data.MetaRefineryIncomingUserAgent)
	})
}

func TestProcessEventMetrics(t *testing.T) {
	tests := []struct {
		name          string
		routerType    types.RouterType
		opampEnabled  bool
		recordUsage   config.DefaultTrue
		signalType    string
		expectedCount int64
		metricName    string
	}{
		{
			name:          "log event with opamp enabled and record usage",
			routerType:    types.RouterTypeIncoming,
			opampEnabled:  true,
			recordUsage:   config.DefaultTrue(true),
			signalType:    "log",
			expectedCount: 91,
			metricName:    "bytes_received_logs",
		},
		{
			name:          "trace event with opamp enabled and record usage",
			routerType:    types.RouterTypeIncoming,
			opampEnabled:  true,
			recordUsage:   config.DefaultTrue(true),
			signalType:    "trace",
			expectedCount: 93,
			metricName:    "bytes_received_traces",
		},
		{
			name:          "log event with opamp disabled",
			routerType:    types.RouterTypeIncoming,
			opampEnabled:  false,
			recordUsage:   config.DefaultTrue(true),
			signalType:    "log",
			expectedCount: 0,
		},
		{
			name:          "log event with record usage disabled",
			routerType:    types.RouterTypeIncoming,
			opampEnabled:  true,
			recordUsage:   config.DefaultTrue(false),
			signalType:    "log",
			expectedCount: 0,
		},
		{
			name:          "log event from peer",
			routerType:    types.RouterTypePeer,
			opampEnabled:  true,
			recordUsage:   config.DefaultTrue(true),
			signalType:    "log",
			expectedCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name+"/"+tt.signalType, func(t *testing.T) {
			mockMetrics := &metrics.MockMetrics{}
			mockMetrics.Start()

			mockConfig := &config.MockConfig{
				GetOpAmpConfigVal: config.OpAMPConfig{
					Enabled:     tt.opampEnabled,
					RecordUsage: &tt.recordUsage,
				},
				TraceIdFieldNames: []string{"trace.trace_id"},
			}

			// Setup mock transmissions
			mockUpstream := &transmit.MockTransmission{}
			mockUpstream.Start()
			mockPeer := &transmit.MockTransmission{}
			mockPeer.Start()

			mockSharder := &sharder.MockSharder{
				Self: &sharder.TestShard{
					Addr: "http://localhost:12345",
				},
			}

			router := &Router{
				Config:               mockConfig,
				Logger:               &logger.NullLogger{},
				Metrics:              mockMetrics,
				UpstreamTransmission: mockUpstream,
				PeerTransmission:     mockPeer,
				Collector:            collect.NewMockCollector(),
				Sharder:              mockSharder,
				routerType:           tt.routerType,
				iopLogger:            iopLogger{Logger: &logger.NullLogger{}, incomingOrPeer: tt.routerType.String()},
			}
			router.registerMetricNames()

			// Create test event with traceID and signal type
			event := &types.Event{
				Context:   context.Background(),
				APIHost:   "test.honeycomb.io",
				Dataset:   "test-dataset",
				Timestamp: time.Now(),
				Data: types.NewPayload(mockConfig, map[string]interface{}{
					"trace.trace_id":    "trace-123",
					"meta.signal_type":  tt.signalType,
					"test_attribute":    "test_value",
					"another_attribute": 123,
				}),
			}
			event.Data.ExtractMetadata()
			span := &types.Span{
				Event:   *event,
				TraceID: "trace-123",
				IsRoot:  true,
			}
			size := span.GetDataSize()
			if tt.expectedCount > 0 {
				assert.Equal(t, tt.expectedCount, int64(size))
			}

			// Call processEvent
			err := router.processEvent(event, "request-123")
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedCount, mockMetrics.CounterIncrements[tt.metricName])
		})
	}
}

func newBatchRouter(t testing.TB) *Router {
	// Set up mock collector and transmission
	mockCollector := collect.NewMockCollector()
	mockTransmission := &transmit.MockTransmission{}
	mockTransmission.Start()

	mockMetrics := metrics.MockMetrics{}
	mockMetrics.Start()

	t.Cleanup(func() {
		err := mockTransmission.Stop()
		assert.NoError(t, err)

		mockMetrics.Stop()
	})

	// Set up config with required field names
	mockConfig := &config.MockConfig{
		TraceIdFieldNames:  []string{"trace.trace_id"},
		ParentIdFieldNames: []string{"trace.parent_id"},
	}

	// Set up a mock sharder
	mockSharder := &sharder.MockSharder{
		Self: &sharder.TestShard{Addr: "http://localhost:8080"},
	}

	r := &Router{
		Config:               mockConfig,
		Metrics:              &mockMetrics,
		UpstreamTransmission: mockTransmission,
		Collector:            mockCollector,
		Sharder:              mockSharder,
		routerType:           types.RouterTypeIncoming,
		iopLogger:            iopLogger{Logger: &logger.NullLogger{}, incomingOrPeer: types.RouterTypeIncoming.String()},
		environmentCache:     newEnvironmentCache(time.Second, func(key string) (string, error) { return "test", nil }),
		Tracer:               noop.Tracer{},
	}
	var err error
	r.zstdDecoder, err = makeDecoders(1)
	if err != nil {
		t.Fatal(err)
	}
	r.registerMetricNames()
	return r
}

func createBatchEvents(mockCfg config.Config) *batchedEvents {
	now := time.Now().UTC()
	batch := newBatchedEvents(types.CoreFieldsUnmarshalerOptions{
		Config:  mockCfg,
		APIKey:  "api-key",
		Env:     "env",
		Dataset: "dataset",
	})
	batch.events = []batchedEvent{
		{
			Timestamp:  now.Format(time.RFC3339Nano),
			SampleRate: 2,
			Data: types.NewPayload(mockCfg, map[string]interface{}{
				"trace.trace_id":  "trace-1",
				"trace.span_id":   "span-1",
				"trace.parent_id": "",
				"service.name":    "test-service",
				"operation.name":  "test-operation-1",
				"duration_ms":     100.0,
				"int.field":       int64(1),
				"bool.field":      true,
			}),
		},
		{
			Timestamp:  now.Format(time.RFC3339Nano),
			SampleRate: 2,
			Data: types.NewPayload(mockCfg, map[string]interface{}{
				"trace.trace_id":  "trace-1",
				"trace.span_id":   "span-2",
				"trace.parent_id": "span-1",
				"service.name":    "test-service",
				"operation.name":  "test-operation-2",
				"duration_ms":     50.0,
				"int.field":       int64(2),
				"bool.field":      false,
			}),
		},
		{
			Timestamp:  now.Format(time.RFC3339Nano),
			SampleRate: 4,
			Data: types.NewPayload(mockCfg, map[string]interface{}{
				"trace.trace_id":  "trace-2",
				"trace.span_id":   "span-3",
				"trace.parent_id": "",
				"service.name":    "another-service",
				"operation.name":  "another-operation",
				"duration_ms":     200.0,
				"int.field":       int64(3),
			}),
		},
	}
	return batch
}

func TestRouterBatch(t *testing.T) {
	t.Parallel()

	router := newBatchRouter(t)
	batch := createBatchEvents(router.Config)
	batchMsgpack, err := msgpack.Marshal(batch.events)
	require.NoError(t, err)

	// Create HTTP request directly without server
	req, err := http.NewRequest("POST", "/1/batch/test-dataset", bytes.NewReader(batchMsgpack))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-msgpack")
	req.Header.Set("X-Honeycomb-Team", "test-api-key")

	// Set up mux variables for dataset extraction
	req = mux.SetURLVars(req, map[string]string{"datasetName": "test-dataset"})

	// Call router.batch directly
	w := httptest.NewRecorder()
	router.batch(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	var responses []*BatchResponse
	err = json.Unmarshal(w.Body.Bytes(), &responses)
	require.NoError(t, err)
	assert.Len(t, responses, len(batch.events))

	// Verify all responses are successful
	for i, resp := range responses {
		assert.Equal(t, http.StatusAccepted, resp.Status, "Response %d should be accepted", i)
		assert.Empty(t, resp.Error, "Response %d should have no error", i)
	}

	mockMetrics := router.Metrics.(*metrics.MockMetrics)
	assert.Equal(t, int64(1), mockMetrics.CounterIncrements["incoming_router_batch"])
	assert.Equal(t, int64(3), mockMetrics.CounterIncrements["incoming_router_batch_events"])
	assert.Equal(t, int64(3), mockMetrics.CounterIncrements["incoming_router_span"])

	var spans []*types.Span
	for len(spans) < len(batch.events) {
		select {
		case span := <-router.Collector.(*collect.MockCollector).Spans:
			spans = append(spans, span)
		default:
			// All the spans should be in the channel before batch() returns.
			t.Fatal("didn't get enough spans")
		}
	}

	assert.Len(t, spans, len(batch.events))
	for i, span := range spans {
		assert.Equal(t, batch.events[i].Data.Get("trace.trace_id"), span.TraceID)
		assert.Equal(t, uint(batch.events[i].SampleRate), span.SampleRate)
		// Compare data values
		for k, v := range batch.events[i].Data.All() {
			assert.Equal(t, v, span.Data.Get(k), "Data field %s should match", k)
		}
	}
}

// discardResponseWriter implements http.ResponseWriter that discards all writes
type discardResponseWriter struct {
	header http.Header
}

func (d *discardResponseWriter) Header() http.Header {
	if d.header == nil {
		d.header = make(http.Header)
	}
	return d.header
}

func (d *discardResponseWriter) Write(p []byte) (int, error) {
	return len(p), nil // Discard all writes
}

func (d *discardResponseWriter) WriteHeader(statusCode int) {
	// Discard status code
}

func createBatchEventsWithLargeAttributes(numEvents int, cfg config.Config) *batchedEvents {
	now := time.Now().UTC()

	// Large text values for testing
	largeText := strings.Repeat("x", 1000)
	mediumText := strings.Repeat("y", 500)
	smallText := strings.Repeat("z", 100)

	batchEvents := newBatchedEvents(types.CoreFieldsUnmarshalerOptions{
		Config:  cfg,
		APIKey:  "api-key",
		Env:     "env",
		Dataset: "dataset",
	})
	batchEvents.events = make([]batchedEvent, numEvents)

	for i := 0; i < numEvents; i++ {
		data := make(map[string]interface{})

		// Core tracing fields (always present)
		traceID := fmt.Sprintf("trace-%040d", i)
		spanID := fmt.Sprintf("span-%018d", i)
		parentID := ""
		if i%3 != 0 { // 2/3 of spans have parents
			parentID = fmt.Sprintf("span-%018d", i-1)
		}

		data["trace.trace_id"] = traceID
		data["trace.span_id"] = spanID
		data["trace.parent_id"] = parentID
		data["service.name"] = fmt.Sprintf("service-%d", i%3)
		data["duration_ms"] = 100.0

		data["meta.signal_type"] = "trace"
		data["meta.annotation_type"] = "span"
		data["meta.refinery.incoming_user_agent"] = "refinery/v2.4.1"

		// Large text fields (3 large fields)
		data["large_field_1"] = largeText
		data["large_field_2"] = mediumText
		data["large_field_3"] = smallText

		// many string, float, int, and bool fields
		for j := 0; j < 25; j++ {
			data[fmt.Sprintf("string.field_%d", j)] = fmt.Sprintf("string_value_%d", j)
			data[fmt.Sprintf("int.field_%d", j)] = int64(j)
			data[fmt.Sprintf("float.field_%d", j)] = float64(j)
			data[fmt.Sprintf("bool.field_%d", j)] = (i+j)%2 == 0
		}

		// Array field
		data["tags"] = []string{
			fmt.Sprintf("tag_%d", i),
			fmt.Sprintf("tag_%d", i+1),
			fmt.Sprintf("tag_%d", i+2),
		}

		// Nested map field
		data["custom.metadata"] = map[string]interface{}{
			"nested_field_1": map[string]interface{}{
				"key1": fmt.Sprintf("value_%d", i),
				"key2": i * 10,
				"key3": i%2 == 0,
			},
			"nested_field_2": map[string]interface{}{
				"key1": true,
				"key2": false,
				"key3": i + 100,
			},
		}

		ts := now.Add(time.Duration(i*100) * time.Millisecond)
		batchEvents.events[i] = batchedEvent{
			MsgPackTimestamp: &ts,
			SampleRate:       2,
			Data:             types.NewPayload(cfg, data),
			cfg:              cfg,
		}
	}

	return batchEvents
}

// convertBatchedEventsToOTLP converts batchedEvents to OTLP ExportTraceServiceRequest
func convertBatchedEventsToOTLP(events *batchedEvents) (*collectortrace.ExportTraceServiceRequest, error) {
	spans := make([]*trace.Span, len(events.events))

	for i, event := range events.events {
		// Convert the event data to OTLP span
		span := &trace.Span{
			Name:              "benchmark_span",
			StartTimeUnixNano: uint64(event.getEventTime().UnixNano()),
			EndTimeUnixNano:   uint64(event.getEventTime().Add(100 * time.Millisecond).UnixNano()),
			Kind:              trace.Span_SPAN_KIND_INTERNAL,
		}

		// Extract trace and span IDs from the payload
		if traceIDStr := event.Data.Get("trace.trace_id"); traceIDStr != nil {
			if traceID, ok := traceIDStr.(string); ok && traceID != "" {
				// Convert trace ID string to bytes (simplified for benchmark)
				span.TraceId = []byte(fmt.Sprintf("%-16s", traceID)[:16])
			}
		}

		if spanIDStr := event.Data.Get("trace.span_id"); spanIDStr != nil {
			if spanID, ok := spanIDStr.(string); ok && spanID != "" {
				// Convert span ID string to bytes (simplified for benchmark)
				span.SpanId = []byte(fmt.Sprintf("%-8s", spanID)[:8])
			}
		}

		if parentIDStr := event.Data.Get("trace.parent_id"); parentIDStr != nil {
			if parentID, ok := parentIDStr.(string); ok && parentID != "" {
				// Convert parent ID string to bytes (simplified for benchmark)
				span.ParentSpanId = []byte(fmt.Sprintf("%-8s", parentID)[:8])
			}
		}

		// Convert all data fields to OTLP attributes
		var attributes []*common.KeyValue
		for key, value := range event.Data.All() {
			// Skip trace fields as they're handled separately
			if strings.HasPrefix(key, "trace.") {
				continue
			}

			attr := &common.KeyValue{Key: key}

			// Convert value to appropriate OTLP type
			switch v := value.(type) {
			case string:
				attr.Value = &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: v}}
			case int64:
				attr.Value = &common.AnyValue{Value: &common.AnyValue_IntValue{IntValue: v}}
			case float64:
				attr.Value = &common.AnyValue{Value: &common.AnyValue_DoubleValue{DoubleValue: v}}
			case bool:
				attr.Value = &common.AnyValue{Value: &common.AnyValue_BoolValue{BoolValue: v}}
			case []string:
				arrayValues := make([]*common.AnyValue, len(v))
				for j, str := range v {
					arrayValues[j] = &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: str}}
				}
				attr.Value = &common.AnyValue{
					Value: &common.AnyValue_ArrayValue{
						ArrayValue: &common.ArrayValue{Values: arrayValues},
					},
				}
			default:
				// Convert anything else to string
				attr.Value = &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: fmt.Sprintf("%v", v)}}
			}

			attributes = append(attributes, attr)
		}

		span.Attributes = attributes
		spans[i] = span
	}

	// Create the OTLP request structure
	req := &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*trace.ResourceSpans{{
			Resource: &resource.Resource{
				Attributes: []*common.KeyValue{
					{
						Key:   "service.name",
						Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "benchmark-service"}},
					},
				},
			},
			ScopeSpans: []*trace.ScopeSpans{{
				Spans: spans,
			}},
		}},
	}

	return req, nil
}

func BenchmarkRouterBatch(b *testing.B) {
	router := newBatchRouter(b)
	batchEvents := createBatchEventsWithLargeAttributes(3, router.Config)
	mockCollector := router.Collector.(*collect.MockCollector)

	b.Run("batch_msgpack", func(b *testing.B) {
		batchMsgpack, err := msgpack.Marshal(batchEvents.events)
		require.NoError(b, err)

		// Create HTTP request directly without server
		req, err := http.NewRequest("POST", "/1/batch/test-dataset", nil)
		if err != nil {
			b.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/x-msgpack")
		req.Header.Set("X-Honeycomb-Team", "test-api-key")

		// Set up mux variables for dataset extraction
		req = mux.SetURLVars(req, map[string]string{"datasetName": "test-dataset"})

		// Create reusable discard response writer
		w := &discardResponseWriter{}

		b.ResetTimer()
		for b.Loop() {
			req.Body = io.NopCloser(bytes.NewReader(batchMsgpack))

			router.batch(w, req)

			// Drain spans to prevent channel from filling up
			for len(mockCollector.Spans) > 0 {
				<-mockCollector.Spans
			}
		}
	})

	b.Run("batch_json", func(b *testing.B) {
		batchMsgpack, err := json.Marshal(batchEvents.events)
		require.NoError(b, err)

		// Create HTTP request directly without server
		req, err := http.NewRequest("POST", "/1/batch/test-dataset", nil)
		if err != nil {
			b.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Honeycomb-Team", "test-api-key")

		// Set up mux variables for dataset extraction
		req = mux.SetURLVars(req, map[string]string{"datasetName": "test-dataset"})

		// Create reusable discard response writer
		w := &discardResponseWriter{}

		b.ResetTimer()
		for b.Loop() {
			req.Body = io.NopCloser(bytes.NewReader(batchMsgpack))

			router.batch(w, req)

			// Drain spans to prevent channel from filling up
			for len(mockCollector.Spans) > 0 {
				<-mockCollector.Spans
			}
		}
	})

	b.Run("otlp/proto", func(b *testing.B) {
		// Convert batchedEvents to OTLP format
		otlpReq, err := convertBatchedEventsToOTLP(batchEvents)
		require.NoError(b, err)

		// Serialize to protobuf
		otlpData, err := proto.Marshal(otlpReq)
		require.NoError(b, err)

		// Create HTTP request for OTLP endpoint
		req, err := http.NewRequest("POST", "/v1/traces", nil)
		if err != nil {
			b.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/x-protobuf")
		req.Header.Set("X-Honeycomb-Team", "test-api-key")
		req.Header.Set("X-Honeycomb-Dataset", "test-dataset")

		// Create reusable discard response writer
		w := &discardResponseWriter{}

		b.ResetTimer()
		for b.Loop() {
			req.Body = io.NopCloser(bytes.NewReader(otlpData))

			router.postOTLPTrace(w, req)

			// Drain spans to prevent channel from filling up
			for len(mockCollector.Spans) > 0 {
				<-mockCollector.Spans
			}
		}
	})

	b.Run("otlp_json", func(b *testing.B) {
		// Convert batchedEvents to OTLP format
		otlpReq, err := convertBatchedEventsToOTLP(batchEvents)
		require.NoError(b, err)

		// Serialize to JSON using protojson
		otlpData, err := protojson.Marshal(otlpReq)
		require.NoError(b, err)

		// Create HTTP request for OTLP endpoint
		req, err := http.NewRequest("POST", "/v1/traces", nil)
		if err != nil {
			b.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Honeycomb-Team", "test-api-key")
		req.Header.Set("X-Honeycomb-Dataset", "test-dataset")

		// Create reusable discard response writer
		w := &discardResponseWriter{}

		b.ResetTimer()
		for b.Loop() {
			req.Body = io.NopCloser(bytes.NewReader(otlpData))

			router.postOTLPTrace(w, req)

			// Drain spans to prevent channel from filling up
			for len(mockCollector.Spans) > 0 {
				<-mockCollector.Spans
			}
		}
	})
}

func readAll(t testing.TB, r io.Reader) []byte {
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	return got
}

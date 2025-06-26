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
	trace "go.opentelemetry.io/proto/otlp/trace/v1"

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

	decoders, err := makeDecoders(numZstdDecoders)
	if err != nil {
		t.Errorf("unexpected err: %s", err.Error())
	}

	router := &Router{zstdDecoders: decoders}
	req := &http.Request{
		Body:   io.NopCloser(pReader),
		Header: http.Header{},
	}
	reader, err := router.getMaybeCompressedBody(req)
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
	reader, err = router.getMaybeCompressedBody(req)
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
	reader, err = router.getMaybeCompressedBody(req)
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
			batchEvents := batchedEvents{
				{
					Timestamp:  now.Format(time.RFC3339Nano),
					SampleRate: 2,
					Data:       types.NewPayload(testData),
				},
				{
					Timestamp:  now.Add(time.Second).Format(time.RFC3339Nano),
					SampleRate: 4,
					Data:       types.NewPayload(testData),
				},
			}
			jsonData, err := json.Marshal(batchEvents)
			require.NoError(t, err)

			for _, contentType := range []string{"application/json", "application/json; charset=utf-8"} {
				t.Run(contentType, func(t *testing.T) {
					req := httptest.NewRequest("POST", "/test", bytes.NewReader(jsonData))
					req.Header.Set("Content-Type", contentType)

					var result batchedEvents
					err = unmarshal(req, readAll(t, req.Body), &result)
					require.NoError(t, err)
					require.Len(t, result, 2)

					assert.Equal(t, now.UTC(), result[0].getEventTime())
					assert.Equal(t, uint(2), result[0].getSampleRate())
					assert.Equal(t, testData, maps.Collect(result[0].Data.All()))

					assert.Equal(t, now.Add(time.Second).UTC(), result[1].getEventTime())
					assert.Equal(t, uint(4), result[1].getSampleRate())
					assert.Equal(t, testData, maps.Collect(result[1].Data.All()))
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

					var result batchedEvents
					err = unmarshal(req, readAll(t, req.Body), &result)
					require.NoError(t, err)
					require.Len(t, result, 2)

					assert.Equal(t, now.UTC(), result[0].getEventTime())
					assert.Equal(t, uint(3), result[0].getSampleRate())
					assert.Equal(t, testData, maps.Collect(result[0].Data.All()))

					assert.Equal(t, now.Add(time.Second).UTC(), result[1].getEventTime())
					assert.Equal(t, uint(6), result[1].getSampleRate())
					assert.Equal(t, testData, maps.Collect(result[1].Data.All()))

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
		Config:               &config.MockConfig{},
		Metrics:              &mockMetrics,
		UpstreamTransmission: mockTransmission,
		iopLogger: iopLogger{
			Logger:         &logger.MockLogger{},
			incomingOrPeer: "incoming",
		},
		Logger:           &logger.MockLogger{},
		environmentCache: newEnvironmentCache(time.Second, nil),
		Tracer:           noop.Tracer{},
	}

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

	for _, tracePath := range []string{"/v1/traces", "/v1/traces/"} {
		req, _ := http.NewRequest("POST", server.URL+tracePath, bytes.NewReader(body))
		req.Header = http.Header{}
		req.Header.Set("content-type", "application/json")
		req.Header.Set("x-honeycomb-team", legacyAPIKey)
		req.Header.Set("x-honeycomb-dataset", "dataset")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
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
		&inject.Object{Value: &metrics.NullMetrics{}, Name: "genericMetrics"},
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
func TestIsRootSpan(t *testing.T) {
	tesCases := []struct {
		name     string
		event    types.Event
		expected bool
	}{
		{
			name: "root span - no parent id",
			event: types.Event{
				Data: types.NewPayload(map[string]interface{}{}),
			},
			expected: true,
		},
		{
			name: "root span - empty parent id",
			event: types.Event{
				Data: types.NewPayload(map[string]interface{}{
					"trace.parent_id": "",
				}),
			},
			expected: true,
		},
		{
			name: "non-root span - parent id",
			event: types.Event{
				Data: types.NewPayload(map[string]interface{}{
					"trace.parent_id": "some-id",
				}),
			},
			expected: false,
		},
		{
			name: "non-root span - no parent id but has signal_type of log",
			event: types.Event{
				Data: types.NewPayload(map[string]interface{}{
					"meta.signal_type": "log",
				}),
			},
			expected: false,
		},
	}

	cfg := &config.MockConfig{
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
	}

	for _, tc := range tesCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, isRootSpan(&tc.event, cfg))
		})
	}
}

func TestAddIncomingUserAgent(t *testing.T) {
	t.Run("no incoming user agent", func(t *testing.T) {
		event := &types.Event{
			Data: types.NewPayload(map[string]interface{}{}),
		}

		addIncomingUserAgent(event, "test-agent")
		require.Equal(t, "test-agent", event.Data.Get("meta.refinery.incoming_user_agent"))
	})

	t.Run("existing incoming user agent", func(t *testing.T) {
		event := &types.Event{
			Data: types.NewPayload(map[string]interface{}{
				"meta.refinery.incoming_user_agent": "test-agent",
			}),
		}

		addIncomingUserAgent(event, "another-test-agent")
		require.Equal(t, "test-agent", event.Data.Get("meta.refinery.incoming_user_agent"))
	})
}

func TestProcessEventMetrics(t *testing.T) {

	tests := []struct {
		name           string
		incomingOrPeer string
		opampEnabled   bool
		recordUsage    config.DefaultTrue
		signalType     string
		expectedCount  int64
		metricName     string
	}{
		{
			name:           "log event with opamp enabled and record usage",
			incomingOrPeer: "incoming",
			opampEnabled:   true,
			recordUsage:    config.DefaultTrue(true),
			signalType:     "log",
			expectedCount:  91,
			metricName:     "bytes_received_logs",
		},
		{
			name:           "trace event with opamp enabled and record usage",
			incomingOrPeer: "incoming",
			opampEnabled:   true,
			recordUsage:    config.DefaultTrue(true),
			signalType:     "trace",
			expectedCount:  93,
			metricName:     "bytes_received_traces",
		},
		{
			name:           "log event with opamp disabled",
			incomingOrPeer: "incoming",
			opampEnabled:   false,
			recordUsage:    config.DefaultTrue(true),
			signalType:     "log",
			expectedCount:  0,
		},
		{
			name:           "log event with record usage disabled",
			incomingOrPeer: "incoming",
			opampEnabled:   true,
			recordUsage:    config.DefaultTrue(false),
			signalType:     "log",
			expectedCount:  0,
		},
		{
			name:           "log event from peer",
			incomingOrPeer: "peer",
			opampEnabled:   true,
			recordUsage:    config.DefaultTrue(true),
			signalType:     "log",
			expectedCount:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
				incomingOrPeer:       tt.incomingOrPeer,
				iopLogger:            iopLogger{Logger: &logger.NullLogger{}, incomingOrPeer: tt.incomingOrPeer},
			}

			// Create test event with traceID and signal type
			event := &types.Event{
				Context:   context.Background(),
				APIHost:   "test.honeycomb.io",
				Dataset:   "test-dataset",
				Timestamp: time.Now(),
				Data: types.NewPayload(map[string]interface{}{
					"trace.trace_id":    "trace-123",
					"meta.signal_type":  tt.signalType,
					"test_attribute":    "test_value",
					"another_attribute": 123,
				}),
			}
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

	return &Router{
		Config:               mockConfig,
		Metrics:              &mockMetrics,
		UpstreamTransmission: mockTransmission,
		Collector:            mockCollector,
		Sharder:              mockSharder,
		incomingOrPeer:       "incoming",
		iopLogger:            iopLogger{Logger: &logger.NullLogger{}, incomingOrPeer: "incoming"},
		environmentCache:     newEnvironmentCache(time.Second, func(key string) (string, error) { return "test", nil }),
	}
}

func createBatchEvents() batchedEvents {
	now := time.Now().UTC()
	batchEvents := batchedEvents{
		{
			Timestamp:  now.Format(time.RFC3339Nano),
			SampleRate: 2,
			Data: types.NewPayload(map[string]interface{}{
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
			Data: types.NewPayload(map[string]interface{}{
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
			Data: types.NewPayload(map[string]interface{}{
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
	return batchEvents
}

func TestRouterBatch(t *testing.T) {
	t.Parallel()

	router := newBatchRouter(t)
	batchEvents := createBatchEvents()
	batchMsgpack, err := msgpack.Marshal(batchEvents)
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
	assert.Len(t, responses, len(batchEvents))

	// Verify all responses are successful
	for i, resp := range responses {
		assert.Equal(t, http.StatusAccepted, resp.Status, "Response %d should be accepted", i)
		assert.Empty(t, resp.Error, "Response %d should have no error", i)
	}

	mockMetrics := router.Metrics.(*metrics.MockMetrics)
	assert.Equal(t, int64(1), mockMetrics.CounterIncrements["incoming_router_batch"])
	assert.Equal(t, int64(3), mockMetrics.CounterIncrements["incoming_router_batch_events"])

	var spans []*types.Span
	for len(spans) < len(batchEvents) {
		select {
		case span := <-router.Collector.(*collect.MockCollector).Spans:
			spans = append(spans, span)
		default:
			// All the spans should be in the channel before batch() returns.
			t.Fatal("didn't get enough spans")
		}
	}

	assert.Len(t, spans, len(batchEvents))
	for i, span := range spans {
		assert.Equal(t, batchEvents[i].Data.Get("trace.trace_id"), span.TraceID)
		assert.Equal(t, uint(batchEvents[i].SampleRate), span.SampleRate)
		// Compare data values
		for k, v := range batchEvents[i].Data.All() {
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

func createBatchEventsWithLargeAttributes() batchedEvents {
	now := time.Now().UTC()

	// Large text values for testing
	largeDescription := strings.Repeat("This is a very detailed operation description that contains a lot of information about what the service is doing. ", 10)
	largeErrorMessage := strings.Repeat("Error occurred during processing: connection timeout, retry attempts failed, network unreachable, service unavailable. ", 8)
	largeUserAgent := "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Custom-Agent/1.2.3"

	// Large structured data
	largeRequestBody := `{"user":{"id":12345,"email":"user@test.com","profile":{"name":"John Doe","age":30,"preferences":{"theme":"dark","language":"en","notifications":{"email":true,"push":false,"sms":true}}}},"request":{"method":"POST","url":"https://api.example.com/v2/users/12345/profile","headers":{"authorization":"Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...","content-type":"application/json","user-agent":"` + largeUserAgent + `"},"body":{"operation":"update_profile","data":{"preferences":{"notifications":{"email":true,"push":false}}}}}`

	batchEvents := batchedEvents{
		{
			Timestamp:  now.Format(time.RFC3339Nano),
			SampleRate: 2,
			Data: types.NewPayload(map[string]interface{}{
				// Metadata fields from metadataFields list
				"meta.signal_type":                  "trace",
				"meta.annotation_type":              "span",
				"meta.refinery.incoming_user_agent": "refinery/v2.4.1",

				// Core tracing fields
				"trace.trace_id":      "trace-abcdef123456789012345678901234567890",
				"trace.span_id":       "span-1234567890123456",
				"trace.parent_id":     "",
				"service.name":        "user-authentication-service",
				"service.version":     "v2.4.1",
				"service.environment": "production",
				"operation.name":      "authenticate_user_with_multi_factor",
				"span.kind":           "server",

				// Timing and performance
				"duration_ms":       1250.75,
				"duration_ns":       1250750000,
				"start_time":        now.UnixNano(),
				"end_time":          now.Add(1250 * time.Millisecond).UnixNano(),
				"cpu_time_ms":       890.25,
				"memory_used_bytes": int64(52428800), // 50MB
				"gc_pause_ms":       12.5,

				// HTTP details
				"http.method":          "POST",
				"http.url":             "https://api.example.com/v2/auth/login",
				"http.status_code":     int64(200),
				"http.request_size":    int64(2048),
				"http.response_size":   int64(1024),
				"http.user_agent":      largeUserAgent,
				"http.remote_addr":     "192.168.1.100:45678",
				"http.x_forwarded_for": "203.0.113.45, 192.168.1.1",

				// Database operations
				"db.system":        "postgresql",
				"db.name":          "user_accounts",
				"db.operation":     "SELECT",
				"db.table":         "users, user_sessions, user_preferences",
				"db.rows_affected": int64(3),
				"db.duration_ms":   145.8,
				"db.query":         "SELECT u.id, u.email, us.session_token, up.preferences FROM users u JOIN user_sessions us ON u.id = us.user_id JOIN user_preferences up ON u.id = up.user_id WHERE u.email = $1 AND us.expires_at > NOW()",

				// User and business context
				"user.id":           int64(1234567890),
				"user.email":        "john.doe@enterprise-corp.com",
				"user.role":         "admin",
				"user.organization": "Enterprise Corp - Technology Division",
				"session.id":        "sess_abcdef1234567890abcdef1234567890",
				"request.id":        "req_9876543210abcdef9876543210abcdef",
				"correlation.id":    "corr_fedcba0987654321fedcba0987654321",

				// Custom business metrics
				"business.feature":     "multi_factor_authentication",
				"business.tier":        "enterprise",
				"business.cost_center": "engineering-auth-team",
				"auth.method":          "password_and_totp",
				"auth.attempts":        int64(1),
				"auth.success":         true,
				"mfa.provider":         "google_authenticator",
				"mfa.verified":         true,

				// Large text fields
				"operation.description": largeDescription,
				"request.body":          largeRequestBody,
				"debug.stack_trace":     "at UserService.authenticate (user-service.js:145:12)\n  at AuthController.login (auth-controller.js:67:23)\n  at Router.handle (express-router.js:234:15)\n  at next (express-router.js:208:7)\n  at AuthMiddleware.verify (auth-middleware.js:89:5)",

				// Arrays and complex data
				"tags": []string{"authentication", "security", "user-management", "enterprise", "production"},
				"custom.metadata": map[string]interface{}{
					"client_info": map[string]interface{}{
						"platform": "web",
						"version":  "3.2.1",
						"build":    "20240115-1234",
					},
					"feature_flags": map[string]interface{}{
						"enhanced_security": true,
						"rate_limiting":     true,
						"audit_logging":     true,
					},
				},

				// Numeric fields with various types
				"int.field":          int64(1),
				"float.field":        3.14159265359,
				"bool.field":         true,
				"counter.requests":   int64(15678),
				"gauge.active_users": int64(8924),
				"histogram.latency":  []float64{12.5, 45.8, 89.2, 156.7, 234.1},
			}),
		},
		{
			Timestamp:  now.Format(time.RFC3339Nano),
			SampleRate: 2,
			Data: types.NewPayload(map[string]interface{}{
				// Metadata fields from metadataFields list
				"meta.signal_type":                  "trace",
				"meta.trace_id":                     "trace-abcdef123456789012345678901234567890",
				"meta.annotation_type":              "span",
				"meta.refinery.incoming_user_agent": "refinery/v2.4.1",

				// Core tracing fields
				"trace.trace_id":      "trace-abcdef123456789012345678901234567890",
				"trace.span_id":       "span-2345678901234567",
				"trace.parent_id":     "span-1234567890123456",
				"service.name":        "user-profile-service",
				"service.version":     "v1.8.3",
				"service.environment": "production",
				"operation.name":      "fetch_user_profile_with_preferences",
				"span.kind":           "internal",

				// Timing and performance
				"duration_ms":       345.25,
				"duration_ns":       345250000,
				"start_time":        now.Add(50 * time.Millisecond).UnixNano(),
				"end_time":          now.Add(395 * time.Millisecond).UnixNano(),
				"cpu_time_ms":       287.5,
				"memory_used_bytes": int64(31457280), // 30MB
				"gc_pause_ms":       8.2,

				// Cache operations
				"cache.system":      "redis",
				"cache.operation":   "GET",
				"cache.key":         "user_profile:1234567890:v2",
				"cache.hit":         false,
				"cache.ttl_seconds": int64(3600),
				"cache.size_bytes":  int64(4096),

				// Database operations
				"db.system":        "postgresql",
				"db.name":          "user_profiles",
				"db.operation":     "SELECT",
				"db.table":         "user_profiles, user_settings",
				"db.rows_affected": int64(1),
				"db.duration_ms":   89.5,
				"db.query":         "SELECT up.*, us.theme, us.language, us.timezone FROM user_profiles up LEFT JOIN user_settings us ON up.user_id = us.user_id WHERE up.user_id = $1",

				// Error information
				"error.type":      "CacheConnectionError",
				"error.message":   largeErrorMessage,
				"error.code":      "CACHE_TIMEOUT_001",
				"error.stack":     "CacheConnectionError: Connection timeout after 5000ms\n  at RedisClient.connect (redis-client.js:234:15)\n  at CacheService.get (cache-service.js:89:12)\n  at ProfileService.getUserProfile (profile-service.js:156:8)",
				"error.recovered": true,
				"error.fallback":  "database_direct_query",

				// User context
				"user.id":        int64(1234567890),
				"user.segment":   "enterprise_premium",
				"request.id":     "req_9876543210abcdef9876543210abcdef",
				"correlation.id": "corr_fedcba0987654321fedcba0987654321",

				// Large text fields
				"operation.description": largeDescription,
				"response.body":         `{"user_id":1234567890,"profile":{"display_name":"John Doe","avatar_url":"https://cdn.example.com/avatars/large/1234567890.jpg","bio":"` + strings.Repeat("Software engineering manager with 15+ years of experience. ", 20) + `","location":"San Francisco, CA","website":"https://johndoe.tech"}}`,

				// Arrays and metadata
				"tags": []string{"profile", "user-data", "cache-miss", "database", "performance"},
				"metrics.custom": map[string]interface{}{
					"profile_completeness": 0.85,
					"last_updated_days":    int64(7),
					"view_count":           int64(1247),
				},

				// Numeric fields
				"int.field":            int64(2),
				"float.field":          2.71828182846,
				"bool.field":           false,
				"counter.cache_misses": int64(23),
				"gauge.db_connections": int64(12),
				"timer.response_time":  345.25,
			}),
		},
		{
			Timestamp:  now.Format(time.RFC3339Nano),
			SampleRate: 4,
			Data: types.NewPayload(map[string]interface{}{
				// Metadata fields from metadataFields list
				"meta.signal_type":                  "trace",
				"meta.trace_id":                     "trace-fedcba098765432109876543210987654321",
				"meta.annotation_type":              "span",
				"meta.refinery.incoming_user_agent": "refinery/v2.4.1",
				"meta.refinery.send_by":             now.Add(45 * time.Second).Unix(),

				// Core tracing fields
				"trace.trace_id":      "trace-fedcba098765432109876543210987654321",
				"trace.span_id":       "span-3456789012345678",
				"trace.parent_id":     "",
				"service.name":        "payment-processing-service",
				"service.version":     "v3.1.2",
				"service.environment": "production",
				"operation.name":      "process_high_value_payment_with_fraud_detection",
				"span.kind":           "server",

				// Timing and performance
				"duration_ms":       2847.89,
				"duration_ns":       2847890000,
				"start_time":        now.Add(100 * time.Millisecond).UnixNano(),
				"end_time":          now.Add(2947 * time.Millisecond).UnixNano(),
				"cpu_time_ms":       1956.7,
				"memory_used_bytes": int64(104857600), // 100MB
				"gc_pause_ms":       45.8,

				// Payment specific
				"payment.id":         "pay_abcdef1234567890abcdef1234567890",
				"payment.amount":     float64(15999.99),
				"payment.currency":   "USD",
				"payment.method":     "credit_card",
				"payment.provider":   "stripe",
				"payment.merchant":   "enterprise-corp-payments",
				"payment.country":    "US",
				"payment.risk_score": float64(0.23),

				// Fraud detection
				"fraud.score":           float64(0.15),
				"fraud.rules_triggered": []string{"velocity_check", "geo_location", "device_fingerprint"},
				"fraud.decision":        "approved",
				"fraud.model_version":   "v2.4.1",
				"fraud.features": map[string]interface{}{
					"transaction_frequency": int64(3),
					"geo_distance_km":       float64(12.7),
					"device_trusted":        true,
					"merchant_history":      "good",
				},

				// External service calls
				"external.stripe.duration_ms":       456.7,
				"external.fraud_api.duration_ms":    789.2,
				"external.bank_api.duration_ms":     1234.5,
				"external.notification.duration_ms": 123.4,

				// Business context
				"merchant.id":          int64(9876543210),
				"merchant.tier":        "enterprise",
				"customer.id":          int64(5678901234),
				"customer.segment":     "high_value",
				"transaction.type":     "purchase",
				"transaction.category": "electronics",

				// Large structured data
				"request.body":   `{"payment":{"amount":15999.99,"currency":"USD","payment_method":"card_1234567890abcdef","description":"` + strings.Repeat("High-value electronics purchase from premium merchant. ", 15) + `"},"customer":{"id":"cust_5678901234","email":"customer@enterprise.com","billing_address":{"line1":"123 Enterprise Way","city":"San Francisco","state":"CA","postal_code":"94105","country":"US"}},"metadata":{"order_id":"order_abcdef1234567890","source":"web_checkout","campaign":"summer_sale_2024"}}`,
				"fraud.analysis": largeDescription + " Additional fraud analysis details: " + strings.Repeat("Machine learning model analyzed 247 features including transaction history, device fingerprinting, geolocation data, and behavioral patterns. ", 5),

				// Arrays and complex data
				"tags": []string{"payment", "high-value", "fraud-detection", "stripe", "enterprise", "electronics"},
				"custom.business_metrics": map[string]interface{}{
					"profit_margin":   0.23,
					"processing_fee":  47.99,
					"conversion_rate": 0.0847,
					"customer_ltv":    float64(45890.50),
				},

				// Numeric fields
				"int.field":              int64(3),
				"float.field":            1.41421356237,
				"bool.field":             true,
				"counter.transactions":   int64(8924),
				"gauge.fraud_queue":      int64(156),
				"histogram.amounts":      []float64{99.99, 299.99, 599.99, 1299.99, 15999.99},
				"timer.total_processing": 2847.89,
			}),
		},
	}
	return batchEvents
}

func BenchmarkRouterBatch(b *testing.B) {
	router := newBatchRouter(b)
	batchEvents := createBatchEventsWithLargeAttributes()
	batchMsgpack, err := msgpack.Marshal(batchEvents)
	require.NoError(b, err)

	mockCollector := router.Collector.(*collect.MockCollector)

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
}

func readAll(t testing.TB, r io.Reader) []byte {
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	return got
}

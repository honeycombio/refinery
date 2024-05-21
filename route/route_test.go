package route

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/facebookgo/inject"
	"github.com/honeycombio/refinery/centralstore"
	"github.com/honeycombio/refinery/collect"
	"github.com/honeycombio/refinery/collect/cache"
	"github.com/honeycombio/refinery/collect/stressRelief"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/gossip"
	"github.com/honeycombio/refinery/internal/health"
	"github.com/honeycombio/refinery/internal/peer"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/redis"
	"github.com/honeycombio/refinery/sample"
	"github.com/honeycombio/refinery/transmit"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/gorilla/mux"
	"github.com/honeycombio/refinery/sharder"
	"github.com/klauspost/compress/zstd"
	"github.com/vmihailenco/msgpack/v5"
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

func unmarshalRequest(w *httptest.ResponseRecorder, content string, body io.Reader) {
	http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data map[string]interface{}
		err := unmarshal(r, r.Body, &data)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(err.Error()))
			return
		}

		var traceID string
		if trID, ok := data["trace.trace_id"]; ok {
			traceID = trID.(string)
		} else if trID, ok := data["traceId"]; ok {
			traceID = trID.(string)
		}

		w.Write([]byte(traceID))
	}).ServeHTTP(w, &http.Request{
		Body: io.NopCloser(body),
		Header: http.Header{
			"Content-Type": []string{content},
		},
	})
}

func unmarshalBatchRequest(w *httptest.ResponseRecorder, content string, body io.Reader) {
	http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var e batchedEvent
		err := unmarshal(r, r.Body, &e)

		if err != nil {
			w.Write([]byte(err.Error()))
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		w.Write([]byte(e.getEventTime().Format(time.RFC3339Nano)))
	}).ServeHTTP(w, &http.Request{
		Body: io.NopCloser(body),
		Header: http.Header{
			"Content-Type": []string{content},
		},
	})
}

func TestUnmarshal(t *testing.T) {
	var w *httptest.ResponseRecorder
	var body io.Reader
	now := time.Now().UTC()

	w = httptest.NewRecorder()
	body = bytes.NewBufferString("")
	unmarshalRequest(w, "nope", body)

	if w.Code != http.StatusBadRequest {
		t.Error("Expecting", http.StatusBadRequest, "Received", w.Code)
	}

	w = httptest.NewRecorder()
	body = bytes.NewBufferString(`{"trace.trace_id": "test"}`)
	unmarshalRequest(w, "application/json", body)

	if b := w.Body.String(); b != "test" {
		t.Error("Expecting test")
	}

	w = httptest.NewRecorder()
	body = bytes.NewBufferString(`{"traceId": "test"}`)
	unmarshalRequest(w, "application/json; charset=utf-8", body)

	if b := w.Body.String(); b != "test" {
		t.Error("Expecting test")
	}

	w = httptest.NewRecorder()
	body = bytes.NewBufferString(fmt.Sprintf(`{"time": "%s"}`, now.Format(time.RFC3339Nano)))
	unmarshalBatchRequest(w, "application/json", body)

	if b := w.Body.String(); b != now.Format(time.RFC3339Nano) {
		t.Error("Expecting", now, "Received", b)
	}

	var buf *bytes.Buffer
	var e *msgpack.Encoder
	var in map[string]interface{}
	var err error

	w = httptest.NewRecorder()
	buf = &bytes.Buffer{}
	e = msgpack.NewEncoder(buf)
	in = map[string]interface{}{"trace.trace_id": "test"}
	err = e.Encode(in)

	if err != nil {
		t.Error(err)
	}

	body = buf
	unmarshalRequest(w, "application/msgpack", body)

	if b := w.Body.String(); b != "test" {
		t.Error("Expecting test")
	}

	w = httptest.NewRecorder()
	buf = &bytes.Buffer{}
	e = msgpack.NewEncoder(buf)
	in = map[string]interface{}{"traceId": "test"}
	err = e.Encode(in)

	if err != nil {
		t.Error(err)
	}

	body = buf
	unmarshalRequest(w, "application/msgpack", body)

	if b := w.Body.String(); b != "test" {
		t.Error("Expecting test")
	}

	w = httptest.NewRecorder()
	buf = &bytes.Buffer{}
	e = msgpack.NewEncoder(buf)
	in = map[string]interface{}{"time": now}
	err = e.Encode(in)

	if err != nil {
		t.Error(err)
	}

	body = buf
	unmarshalBatchRequest(w, "application/msgpack", body)

	if b := w.Body.String(); b != now.Format(time.RFC3339Nano) {
		t.Error("Expecting", now, "Received", b)
	}
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
	router := &Router{}

	router.debugTrace(rr, req)
	if body := rr.Body.String(); body != `{"traceID":"123abcdef"}` {
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
	}

	muxxer := mux.NewRouter()
	router.AddOTLPMuxxer(muxxer)
	server := httptest.NewServer(muxxer)
	defer server.Close()

	request := &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*tracev1.ResourceSpans{{
			ScopeSpans: []*tracev1.ScopeSpans{{
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
	basicStore := &centralstore.RedisBasicStore{}
	sw := &centralstore.SmartWrapper{}
	spanCache := &cache.SpanCache_basic{}
	redis := &redis.TestService{}
	samplerFactory := &sample.SamplerFactory{
		Config: &config.MockConfig{},
		Logger: &logger.NullLogger{},
	}
	err := g.Provide(
		&inject.Object{Value: &Router{}},

		&inject.Object{Value: &config.MockConfig{}},
		&inject.Object{Value: &logger.NullLogger{}},
		&inject.Object{Value: http.DefaultTransport, Name: "upstreamTransport"},
		&inject.Object{Value: &transmit.MockTransmission{}, Name: "upstreamTransmission"},
		&inject.Object{Value: &TestSharder{}},
		&inject.Object{Value: trace.Tracer(noop.Tracer{}), Name: "tracer"},
		&inject.Object{Value: clockwork.NewRealClock()},
		&inject.Object{Value: basicStore},
		&inject.Object{Value: sw},
		&inject.Object{Value: spanCache},
		&inject.Object{Value: redis, Name: "redis"},
		&inject.Object{Value: samplerFactory},
		&inject.Object{Value: &collect.CentralCollector{}, Name: "collector"},
		&inject.Object{Value: &metrics.NullMetrics{}, Name: "metrics"},
		&inject.Object{Value: &metrics.NullMetrics{}, Name: "genericMetrics"},
		&inject.Object{Value: &stressRelief.MockStressReliever{}, Name: "stressRelief"},
		&inject.Object{Value: &peer.MockPeers{}},
		&inject.Object{Value: &cache.CuckooSentCache{}},
		&inject.Object{Value: &health.Health{}},
		&inject.Object{Value: &gossip.InMemoryGossip{}, Name: "gossip"},
	)
	if err != nil {
		t.Error(err)
	}
	if err := g.Populate(); err != nil {
		t.Error(err)
	}
}

type TestSharder struct{}

func (s *TestSharder) MyShard() sharder.Shard { return nil }

func (s *TestSharder) WhichShard(string) sharder.Shard {
	return &TestShard{
		addr: "http://localhost:12345",
	}
}

type TestShard struct {
	addr string
}

func (s *TestShard) Equals(other sharder.Shard) bool { return true }
func (s *TestShard) GetAddress() string              { return s.addr }

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

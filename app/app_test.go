package app

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/facebookgo/inject"
	"github.com/facebookgo/startstop"
	"github.com/google/uuid"
	"github.com/jonboulle/clockwork"
	"github.com/klauspost/compress/zstd"
	"github.com/sourcegraph/conc/pool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/honeycombio/refinery/collect"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/health"
	"github.com/honeycombio/refinery/internal/peer"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/pubsub"
	"github.com/honeycombio/refinery/sample"
	"github.com/honeycombio/refinery/sharder"
	"github.com/honeycombio/refinery/transmit"
	"github.com/honeycombio/refinery/types"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	trace "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const legacyAPIKey = "c9945edf5d245834089a1bd6cc9ad01e"
const nonLegacyAPIKey = "d245834089a1bd6cc9ad01e"

var now = time.Now().UTC()

type testAPIServer struct {
	server *httptest.Server
	events []*types.Event
	mutex  sync.RWMutex
	cfg    *config.MockConfig
}

func newTestAPIServer(t testing.TB) *testAPIServer {
	server := &testAPIServer{
		cfg: &config.MockConfig{
			TraceIdFieldNames:  []string{"trace.trace_id"},
			ParentIdFieldNames: []string{"trace.parent_id"},
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/1/batch/", server.handleBatch)

	server.server = httptest.NewServer(mux)
	t.Cleanup(server.server.Close)
	return server
}

func (t *testAPIServer) handleBatch(w http.ResponseWriter, r *http.Request) {
	var reader io.Reader = r.Body

	// Handle compression
	if encoding := r.Header.Get("Content-Encoding"); encoding != "" {
		switch encoding {
		case "gzip":
			gr, err := gzip.NewReader(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			defer gr.Close()
			reader = gr
		case "zstd":
			zr, err := zstd.NewReader(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			defer zr.Close()
			reader = zr
		}
	}

	var events []map[string]interface{}
	contentType := r.Header.Get("Content-Type")
	if contentType == "application/x-msgpack" || contentType == "application/msgpack" {
		decoder := msgpack.NewDecoder(reader)
		decoder.UseLooseInterfaceDecoding(true)
		if err := decoder.Decode(&events); err != nil {
			http.Error(w, fmt.Sprintf("Msgpack decode error: %v", err), http.StatusBadRequest)
			return
		}
	} else {
		if err := json.NewDecoder(reader).Decode(&events); err != nil {
			http.Error(w, fmt.Sprintf("JSON decode error: %v", err), http.StatusBadRequest)
			return
		}
	}

	dataset := strings.TrimPrefix(r.URL.Path, "/1/batch/")
	apiKey := r.Header.Get("X-Honeycomb-Team")

	t.mutex.Lock()
	defer t.mutex.Unlock()

	responses := make([]map[string]interface{}, 0, len(events))
	for _, eventData := range events {
		sampleRate := 1
		if sr, ok := eventData["samplerate"]; ok {
			if srf, ok := sr.(float64); ok {
				sampleRate = int(srf)
			}
		}

		timestamp := time.Now()
		if ts, ok := eventData["time"]; ok {
			if tss, ok := ts.(string); ok {
				if parsed, err := time.Parse(time.RFC3339Nano, tss); err == nil {
					timestamp = parsed
				}
			}
		}

		event := &types.Event{
			APIKey:     apiKey,
			Dataset:    dataset,
			SampleRate: uint(sampleRate),
			APIHost:    t.server.URL,
			Timestamp:  timestamp,
		}
		if dataMap, ok := eventData["data"].(map[string]interface{}); ok {
			event.Data = types.NewPayload(t.cfg, dataMap)
			event.Data.ExtractMetadata()
		}

		t.events = append(t.events, event)
		responses = append(responses, map[string]interface{}{"status": 202})
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	responseBody, _ := json.Marshal(responses)
	w.Write(responseBody)
}

func (t *testAPIServer) getEvents() []*types.Event {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	events := make([]*types.Event, len(t.events))
	copy(events, t.events)
	return events
}

// Drops everything sent into it.
type countingTransmission struct {
	count atomic.Int64
}

func (d *countingTransmission) EnqueueEvent(ev *types.Event) {
	d.count.Add(1)
}

func (d *countingTransmission) EnqueueSpan(ev *types.Span) {
	d.count.Add(1)
}

func (d *countingTransmission) RegisterMetrics() {
}

func (w *countingTransmission) resetCount() {
	w.count.Store(0)
}

func (w *countingTransmission) waitForCount(t testing.TB, n int) {
	require.Eventually(t, func() bool {
		return w.count.Load() >= int64(n)
	}, 5*time.Second, time.Millisecond)
}

// defaultConfig returns a config with the given basePort and redisDB.
// tests in this file are running in parallel, so we need to ensure that
// each test gets a unique port and redisDB.
//
// by default, every Redis instance supports 16 databases, we use redisDB as a way to separate test data
func defaultConfig(basePort int, redisDB int, apiURL string) *config.MockConfig {
	return defaultConfigWithGRPC(basePort, redisDB, apiURL, false)
}

func defaultConfigWithGRPC(basePort int, redisDB int, apiURL string, enableGRPC bool) *config.MockConfig {
	if redisDB >= 16 {
		panic("redisDB must be less than 16")
	}
	if apiURL == "" {
		apiURL = "http://api.honeycomb.io"
	}

	cfg := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:   config.Duration(2 * time.Millisecond),
			SendDelay:    config.Duration(1 * time.Millisecond),
			TraceTimeout: config.Duration(10 * time.Millisecond),
			MaxBatchSize: 500,
		},
		GetSamplerTypeVal:    &config.DeterministicSamplerConfig{SampleRate: 1},
		AddRuleReasonToTrace: true,
		PeerManagementType:   "redis",
		GetRedisPeerManagementVal: config.RedisPeerManagementConfig{
			Prefix:   "refinery-app-test",
			Timeout:  config.Duration(1 * time.Second),
			Database: redisDB,
		},
		GetListenAddrVal:     "127.0.0.1:" + strconv.Itoa(basePort),
		GetPeerListenAddrVal: "127.0.0.1:" + strconv.Itoa(basePort+1),
		GetHoneycombAPIVal:   apiURL,
		GetCollectionConfigVal: config.CollectionConfig{
			ShutdownDelay:      config.Duration(1 * time.Second),
			HealthCheckTimeout: config.Duration(3 * time.Second),
			IncomingQueueSize:  30000,
			PeerQueueSize:      30000,
		},
		TraceIdFieldNames:  []string{"trace.trace_id"},
		ParentIdFieldNames: []string{"trace.parent_id"},
		SampleCache:        config.SampleCacheConfig{KeptSize: 10000, DroppedSize: 100000, SizeCheckInterval: config.Duration(10 * time.Second)},
		GetAccessKeyConfigVal: config.AccessKeyConfig{
			ReceiveKeys:          []string{legacyAPIKey, nonLegacyAPIKey},
			AcceptOnlyListedKeys: true,
		},
	}

	if enableGRPC {
		cfg.GetGRPCEnabledVal = true
		cfg.GetGRPCListenAddrVal = "127.0.0.1:" + strconv.Itoa(basePort+2)
		cfg.GetGRPCServerParameters = config.GRPCServerParameters{
			Enabled:               func() *config.DefaultTrue { dt := config.DefaultTrue(true); return &dt }(),
			ListenAddr:            "127.0.0.1:" + strconv.Itoa(basePort+2),
			MaxConnectionIdle:     config.Duration(1 * time.Minute),
			MaxConnectionAge:      config.Duration(3 * time.Minute),
			MaxConnectionAgeGrace: config.Duration(1 * time.Minute),
			KeepAlive:             config.Duration(1 * time.Minute),
			KeepAliveTimeout:      config.Duration(20 * time.Second),
			MaxSendMsgSize:        config.MemorySize(15 << 20), // 15MB
			MaxRecvMsgSize:        config.MemorySize(15 << 20), // 15MB
		}
	}

	return cfg
}

func newStartedApp(
	t testing.TB,
	mockTransmission transmit.Transmission,
	peers peer.Peers,
	cfg *config.MockConfig,
) (*App, inject.Graph) {
	c := cfg
	if peers == nil {
		peers = &peer.FilePeers{Cfg: c, Metrics: &metrics.NullMetrics{}}
	}

	a := App{}

	lgr := &logger.StdoutLogger{
		Config: c,
	}
	lgr.SetLevel("error")
	lgr.Start()

	// TODO use real metrics
	metricsr := &metrics.MockMetrics{}
	metricsr.Start()

	collector := &collect.InMemCollector{
		BlockOnAddSpan: true,
	}

	peerList, err := peers.GetPeers()
	assert.NoError(t, err)

	var shrdr sharder.Sharder
	if len(peerList) > 1 {
		shrdr = &sharder.DeterministicSharder{}
	} else {
		shrdr = &sharder.SingleServerSharder{}
	}

	samplerFactory := &sample.SamplerFactory{}

	// Create upstream transmission - use mock if provided, otherwise create real client
	var upstreamTransmission transmit.Transmission
	if mockTransmission != nil {
		upstreamTransmission = mockTransmission
	} else {
		upstreamTransmission = transmit.NewDirectTransmission(types.TransmitTypeUpstream, http.DefaultTransport.(*http.Transport), 500, 100*time.Millisecond, 100*time.Millisecond, true)
	}

	// Always create real peer transmission using DirectTransmission
	peerTransport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		Dial: (&net.Dialer{
			Timeout: 3 * time.Second,
		}).Dial,
	}
	peerTransmissionWrapper := transmit.NewDirectTransmission(types.TransmitTypePeer, peerTransport, int(cfg.GetTracesConfigVal.MaxBatchSize), 100*time.Millisecond, 100*time.Millisecond, false)

	var g inject.Graph
	err = g.Provide(
		&inject.Object{Value: c},
		&inject.Object{Value: peers},
		&inject.Object{Value: lgr},
		&inject.Object{Value: http.DefaultTransport, Name: "upstreamTransport"},
		&inject.Object{Value: upstreamTransmission, Name: "upstreamTransmission"},
		&inject.Object{Value: peerTransmissionWrapper, Name: "peerTransmission"},
		&inject.Object{Value: shrdr},
		&inject.Object{Value: noop.NewTracerProvider().Tracer("test"), Name: "tracer"},
		&inject.Object{Value: collector},
		&inject.Object{Value: &pubsub.GoRedisPubSub{}},
		&inject.Object{Value: metricsr, Name: "metrics"},
		&inject.Object{Value: "test", Name: "version"},
		&inject.Object{Value: samplerFactory},
		&inject.Object{Value: &health.Health{}},
		&inject.Object{Value: clockwork.NewRealClock()},
		&inject.Object{Value: &collect.MockStressReliever{}, Name: "stressRelief"},
		&inject.Object{Value: &a},
	)
	assert.NoError(t, err)

	err = g.Populate()
	assert.NoError(t, err)

	err = startstop.Start(g.Objects(), nil)
	assert.NoError(t, err)

	// Racy: wait just a moment for ListenAndServe to start up.
	time.Sleep(15 * time.Millisecond)
	return &a, g
}

func post(t testing.TB, req *http.Request) {
	req.Header.Set("User-Agent", "Test-Client")
	resp, err := httpClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}

// getTestSpanJSON returns JSON for a test span (for non-benchmark tests)
func getTestSpanJSON() string {
	data, _ := json.Marshal(batchedEvent{
		Timestamp:  now.Format(time.RFC3339Nano),
		SampleRate: 2,
		Data: map[string]interface{}{
			"trace.trace_id":  "2",
			"trace.span_id":   "10",
			"trace.parent_id": "0000000000",
			"key":             "value",
			"field0":          0,
			"field1":          1,
			"field2":          2,
			"field3":          3,
			"field4":          4,
			"field5":          5,
			"field6":          6,
			"field7":          7,
			"field8":          8,
			"field9":          9,
			"field10":         10,
			"long":            "this is a test of the emergency broadcast system",
			"foo":             "bar",
		},
	})
	return string(data)
}

func TestAppIntegration(t *testing.T) {
	t.Parallel()
	port := 10500
	redisDB := 2

	testServer := newTestAPIServer(t)
	cfg := defaultConfig(port, redisDB, testServer.server.URL)
	app, graph := newStartedApp(t, nil, nil, cfg)

	// Send a root span, it should be sent in short order.
	req := httptest.NewRequest(
		"POST",
		fmt.Sprintf("http://localhost:%d/1/batch/dataset", port),
		strings.NewReader(`[{"data":{"trace.trace_id":"1","foo":"bar"}}]`),
	)
	req.Header.Set("X-Honeycomb-Team", legacyAPIKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultTransport.RoundTrip(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	time.Sleep(5 * app.Config.GetTracesConfig().GetSendTickerValue())

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		events := testServer.getEvents()
		require.Len(collect, events, 1)
		assert.Equal(collect, "dataset", events[0].Dataset)
		assert.Equal(collect, "bar", events[0].Data.Get("foo"))
		assert.Equal(collect, "1", events[0].Data.Get("trace.trace_id"))
		assert.Equal(collect, int64(1), events[0].Data.Get(types.MetaRefineryOriginalSampleRate))
	}, 2*time.Second, 10*time.Millisecond)

	err = startstop.Stop(graph.Objects(), nil)
	assert.NoError(t, err)
}

func TestAppIntegrationSendKey(t *testing.T) {
	t.Parallel()
	port := 10550
	redisDB := 1

	testServer := newTestAPIServer(t)
	cfg := defaultConfig(port, redisDB, testServer.server.URL)
	cfg.GetAccessKeyConfigVal = config.AccessKeyConfig{
		SendKey:     nonLegacyAPIKey,
		SendKeyMode: "all",
	}
	cfg.Samplers = map[string]*config.V2SamplerChoice{
		"test": {
			RulesBasedSampler: &config.RulesBasedSamplerConfig{
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "test",
						SampleRate: 1,
					},
				},
			},
		},
		"__default__": {
			RulesBasedSampler: &config.RulesBasedSamplerConfig{
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "default",
						SampleRate: 1,
					},
				},
			},
		},
	}
	app, graph := newStartedApp(t, nil, nil, cfg)
	app.IncomingRouter.SetEnvironmentCache(time.Second, func(s string) (string, error) { return "test", nil })
	app.PeerRouter.SetEnvironmentCache(time.Second, func(s string) (string, error) { return "test", nil })

	// Send a root span, it should be sent in short order.
	req := httptest.NewRequest(
		"POST",
		fmt.Sprintf("http://localhost:%d/1/batch/dataset", port),
		strings.NewReader(`[{"data":{"trace.trace_id":"1","foo":"bar"}}]`),
	)
	req.Header.Set("X-Honeycomb-Team", "bogus-key")
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultTransport.RoundTrip(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	time.Sleep(5 * app.Config.GetTracesConfig().GetSendTickerValue())

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		events := testServer.getEvents()
		require.Len(collect, events, 1)
		assert.Equal(collect, "dataset", events[0].Dataset)
		assert.Equal(collect, "bar", events[0].Data.Get("foo"))
		assert.Equal(collect, "1", events[0].Data.Get("trace.trace_id"))
		assert.Equal(collect, int64(1), events[0].Data.Get(types.MetaRefineryOriginalSampleRate))
		assert.Equal(collect, "rules/trace/test", events[0].Data.Get(types.MetaRefineryReason), "reason should match with the rule defined for test environment")
	}, 2*time.Second, 10*time.Millisecond)

	err = startstop.Stop(graph.Objects(), nil)
	assert.NoError(t, err)
}
func TestAppIntegrationWithNonLegacyKey(t *testing.T) {
	// Parallel integration tests need different ports!
	t.Parallel()
	port := 10600
	redisDB := 3

	testServer := newTestAPIServer(t)
	cfg := defaultConfig(port, redisDB, testServer.server.URL)
	a, graph := newStartedApp(t, nil, nil, cfg)
	a.IncomingRouter.SetEnvironmentCache(time.Second, func(s string) (string, error) { return "test", nil })
	a.PeerRouter.SetEnvironmentCache(time.Second, func(s string) (string, error) { return "test", nil })

	// Send a root span, it should be sent in short order.
	req := httptest.NewRequest(
		"POST",
		fmt.Sprintf("http://localhost:%d/1/batch/dataset", port),
		strings.NewReader(`[{"data":{"trace.trace_id":"1","foo":"bar"}}]`),
	)
	req.Header.Set("X-Honeycomb-Team", nonLegacyAPIKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultTransport.RoundTrip(req)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	// Wait for span to be sent.
	var events []*types.Event
	require.Eventually(t, func() bool {
		events = testServer.getEvents()
		return len(events) == 1
	}, 2*time.Second, 2*time.Millisecond)

	assert.Equal(t, "dataset", events[0].Dataset)
	assert.Equal(t, "bar", events[0].Data.Get("foo"))
	assert.Equal(t, "1", events[0].Data.Get("trace.trace_id"))
	assert.Equal(t, int64(1), events[0].Data.Get(types.MetaRefineryOriginalSampleRate))

	err = startstop.Stop(graph.Objects(), nil)
	assert.NoError(t, err)
}

func TestAppIntegrationWithUnauthorizedKey(t *testing.T) {
	// Parallel integration tests need different ports!
	t.Parallel()
	port := 10700
	redisDB := 4

	testServer := newTestAPIServer(t)
	cfg := defaultConfig(port, redisDB, testServer.server.URL)
	a, graph := newStartedApp(t, nil, nil, cfg)
	a.IncomingRouter.SetEnvironmentCache(time.Second, func(s string) (string, error) { return "test", nil })
	a.PeerRouter.SetEnvironmentCache(time.Second, func(s string) (string, error) { return "test", nil })

	// Send a root span, it should be sent in short order.
	req := httptest.NewRequest(
		"POST",
		fmt.Sprintf("http://localhost:%d/v1/traces", port),
		strings.NewReader(`[{"data":{"trace.trace_id":"1","foo":"bar"}}]`),
	)
	req.Header.Set("X-Honeycomb-Team", "badkey")
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultTransport.RoundTrip(req)
	assert.NoError(t, err)
	assert.Equal(t, 401, resp.StatusCode)
	data, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.NoError(t, err)
	assert.Contains(t, string(data), "not found in list of authorized keys")

	err = startstop.Stop(graph.Objects(), nil)
	assert.NoError(t, err)
}

func TestAppIntegrationEmptyEvent(t *testing.T) {
	t.Parallel()
	port := 19010
	redisDB := 8

	cfg := defaultConfig(port, redisDB, "")
	_, graph := newStartedApp(t, nil, nil, cfg)

	tt := []struct {
		name                       string
		data                       string
		expectedResponseStatusCode int
		expectedResponse           string
	}{
		{
			name:                       "batch",
			data:                       `[{"data":{}}]`, // Empty event data
			expectedResponseStatusCode: http.StatusOK,
			expectedResponse:           "status\":400,\"error\":\"empty event data\"",
		},
		{
			name:                       "events",
			data:                       `{}`,
			expectedResponseStatusCode: http.StatusBadRequest,
			expectedResponse:           "{\"source\":\"refinery\",\"error\":\"failed to parse event\"",
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			// Send an empty event, it should return a 400 error but the batch request is processed with 200.
			req := httptest.NewRequest(
				"POST",
				fmt.Sprintf("http://localhost:%d/1/%s/dataset", port, tc.name),
				strings.NewReader(tc.data),
			)
			req.Header.Set("X-Honeycomb-Team", legacyAPIKey)
			req.Header.Set("Content-Type", "application/json")

			resp, err := http.DefaultTransport.RoundTrip(req)
			assert.NoError(t, err)

			assert.Equal(t, tc.expectedResponseStatusCode, resp.StatusCode)
			data, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			assert.NoError(t, err)
			assert.Contains(t, string(data), tc.expectedResponse)
		})
	}

	err := startstop.Stop(graph.Objects(), nil)
	assert.NoError(t, err)
}

func TestPeerRouting(t *testing.T) {
	// Parallel integration tests need different ports!
	t.Parallel()

	peerList := []string{"http://localhost:11001", "http://localhost:11003"}

	var apps [2]*App
	var senders [2]*transmit.MockTransmission
	for i := range apps {
		var graph inject.Graph
		basePort := 11000 + (i * 2)
		senders[i] = &transmit.MockTransmission{}
		peers := &peer.MockPeers{
			Peers: peerList,
			ID:    peerList[i],
		}
		redisDB := 5 + i
		cfg := defaultConfig(basePort, redisDB, "")

		apps[i], graph = newStartedApp(t, senders[i], peers, cfg)
		defer startstop.Stop(graph.Objects(), nil)
	}

	// Deliver to host 1, it should be passed to host 0 and emitted there.
	req, err := http.NewRequest(
		"POST",
		"http://localhost:11002/1/batch/dataset",
		nil,
	)
	assert.NoError(t, err)
	req.Header.Set("X-Honeycomb-Team", legacyAPIKey)
	req.Header.Set("Content-Type", "application/json")

	// this span data was chosen because it hashes to the appropriate shard for this
	// test. You can't change it and expect the test to pass.
	blob := `[` + getTestSpanJSON() + `]`
	req.Body = io.NopCloser(strings.NewReader(blob))
	post(t, req)
	assert.Eventually(t, func() bool {
		return len(senders[0].Events) >= 1
	}, 5*time.Second, 2*time.Millisecond)

	events := senders[0].GetBlock(1)
	// Compare individual fields since types.Event doesn't have Metadata field
	require.Len(t, events, 1)
	event := events[0]
	assert.Equal(t, legacyAPIKey, event.APIKey)
	assert.Equal(t, "dataset", event.Dataset)
	assert.Equal(t, uint(2), event.SampleRate)
	assert.Equal(t, "http://api.honeycomb.io", event.APIHost)
	assert.Equal(t, now, event.Timestamp)

	// Check specific data fields
	assert.Equal(t, "2", event.Data.Get("trace.trace_id"))
	assert.Equal(t, "10", event.Data.Get("trace.span_id"))
	assert.Equal(t, "0000000000", event.Data.Get("trace.parent_id"))
	assert.Equal(t, "value", event.Data.Get("key"))
	assert.Equal(t, "bar", event.Data.Get("foo"))
	assert.Equal(t, int64(2), event.Data.Get(types.MetaRefineryOriginalSampleRate))
	assert.Equal(t, "Test-Client", event.Data.MetaRefineryIncomingUserAgent)
	// Repeat, but deliver to host 1 on the peer channel, it should be
	// passed to host 0 since that's who the trace belongs to.
	req, err = http.NewRequest(
		"POST",
		"http://localhost:11003/1/batch/dataset",
		nil,
	)
	assert.NoError(t, err)
	req.Header.Set("X-Honeycomb-Team", legacyAPIKey)
	req.Header.Set("Content-Type", "application/json")

	req.Body = io.NopCloser(strings.NewReader(blob))
	post(t, req)
	require.Eventually(t, func() bool {
		return len(senders[0].Events) >= 1
	}, 5*time.Second, 2*time.Millisecond)

	events = senders[0].GetBlock(1)
	require.Len(t, events, 1)
	event = events[0]
	assert.Equal(t, legacyAPIKey, event.APIKey)
	assert.Equal(t, "dataset", event.Dataset)
	assert.Equal(t, uint(2), event.SampleRate)
	assert.Equal(t, "2", event.Data.Get("trace.trace_id"))
}

func TestHostMetadataSpanAdditions(t *testing.T) {
	t.Parallel()
	port := 14000
	redisDB := 7

	testServer := newTestAPIServer(t)
	cfg := defaultConfig(port, redisDB, testServer.server.URL)
	cfg.AddHostMetadataToTrace = true
	app, graph := newStartedApp(t, nil, nil, cfg)

	// Send a root span, it should be sent in short order.
	req := httptest.NewRequest(
		"POST",
		fmt.Sprintf("http://localhost:%d/1/batch/dataset", port),
		strings.NewReader(`[{"data":{"foo":"bar","trace.trace_id":"1"}}]`),
	)
	req.Header.Set("X-Honeycomb-Team", legacyAPIKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultTransport.RoundTrip(req)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	time.Sleep(5 * app.Config.GetTracesConfig().GetSendTickerValue())

	var events []*types.Event
	require.Eventually(t, func() bool {
		events = testServer.getEvents()
		return len(events) == 1
	}, 2*time.Second, 10*time.Millisecond)

	assert.Equal(t, "dataset", events[0].Dataset)
	assert.Equal(t, "bar", events[0].Data.Get("foo"))
	assert.Equal(t, "1", events[0].Data.Get("trace.trace_id"))
	assert.Equal(t, int64(1), events[0].Data.Get(types.MetaRefineryOriginalSampleRate))
	hostname, _ := os.Hostname()
	assert.Equal(t, hostname, events[0].Data.Get(types.MetaRefineryLocalHostname))

	err = startstop.Stop(graph.Objects(), nil)
	assert.NoError(t, err)
}

func TestEventsEndpoint(t *testing.T) {
	t.Parallel()

	peerList := []string{
		"http://localhost:13001",
		"http://localhost:13003",
	}

	var apps [2]*App
	var senders [2]*transmit.MockTransmission
	for i := range apps {
		var graph inject.Graph
		basePort := 13000 + (i * 2)
		senders[i] = &transmit.MockTransmission{}
		peers := &peer.MockPeers{
			Peers: peerList,
			ID:    peerList[i],
		}
		redisDB := 8 + i

		cfg := defaultConfig(basePort, redisDB, "")
		apps[i], graph = newStartedApp(t, senders[i], peers, cfg)
		defer startstop.Stop(graph.Objects(), nil)
	}

	// Deliver to host 1, it should be passed to host 0
	zEnc, _ := zstd.NewWriter(nil)
	blob := zEnc.EncodeAll([]byte(`{"foo":"bar","trace.trace_id":"1"}`), nil)
	req, err := http.NewRequest(
		"POST",
		"http://localhost:13002/1/events/dataset",
		bytes.NewReader(blob),
	)
	assert.NoError(t, err)
	req.Header.Set("X-Honeycomb-Team", legacyAPIKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "zstd")
	req.Header.Set("X-Honeycomb-Event-Time", now.Format(time.RFC3339Nano))
	req.Header.Set("X-Honeycomb-Samplerate", "10")

	post(t, req)
	assert.Eventually(t, func() bool {
		return len(senders[0].Events) >= 1
	}, 2*time.Second, 2*time.Millisecond)

	events := senders[0].GetBlock(1)
	require.Len(t, events, 1)
	event := events[0]
	assert.Equal(t, legacyAPIKey, event.APIKey)
	assert.Equal(t, "dataset", event.Dataset)
	assert.Equal(t, uint(10), event.SampleRate)
	assert.Equal(t, "http://api.honeycomb.io", event.APIHost)
	assert.Equal(t, now, event.Timestamp)
	assert.Equal(t, "1", event.Data.Get("trace.trace_id"))
	assert.Equal(t, "bar", event.Data.Get("foo"))
	assert.Equal(t, int64(10), event.Data.Get(types.MetaRefineryOriginalSampleRate))
	assert.Equal(t, "Test-Client", event.Data.MetaRefineryIncomingUserAgent)
	// Repeat, but deliver to host 1 on the peer channel, it should be
	// passed to host 0 since that's the host this trace belongs to.

	blob = blob[:0]
	buf := bytes.NewBuffer(blob)
	gz := gzip.NewWriter(buf)
	gz.Write([]byte(`{"foo":"bar","trace.trace_id":"1"}`))
	gz.Close()

	req, err = http.NewRequest(
		"POST",
		"http://localhost:13003/1/events/dataset",
		buf,
	)
	assert.NoError(t, err)
	req.Header.Set("X-Honeycomb-Team", legacyAPIKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set("X-Honeycomb-Event-Time", now.Format(time.RFC3339Nano))
	req.Header.Set("X-Honeycomb-Samplerate", "10")

	post(t, req)
	assert.Eventually(t, func() bool {
		return len(senders[0].Events) >= 1
	}, 2*time.Second, 2*time.Millisecond)

	events = senders[0].GetBlock(1)
	require.Len(t, events, 1)
	event = events[0]
	assert.Equal(t, legacyAPIKey, event.APIKey)
	assert.Equal(t, "dataset", event.Dataset)
	assert.Equal(t, uint(10), event.SampleRate)
	assert.Equal(t, "http://api.honeycomb.io", event.APIHost)
	assert.Equal(t, now, event.Timestamp)
	assert.Equal(t, "1", event.Data.Get("trace.trace_id"))
	assert.Equal(t, "bar", event.Data.Get("foo"))
	assert.Equal(t, int64(10), event.Data.Get(types.MetaRefineryOriginalSampleRate))
	assert.Equal(t, "Test-Client", event.Data.MetaRefineryIncomingUserAgent)
}

func TestEventsEndpointWithNonLegacyKey(t *testing.T) {
	t.Parallel()

	peerList := []string{
		"http://localhost:15001",
		"http://localhost:15003",
	}
	// this traceID was chosen because it hashes to the appropriate shard for this
	// test. You can't change it or the number of peers and still expect the test to pass.
	traceID := "4"

	var apps [2]*App
	var senders [2]*transmit.MockTransmission
	for i := range apps {
		basePort := 15000 + (i * 2)
		senders[i] = &transmit.MockTransmission{}
		peers := &peer.MockPeers{
			Peers: peerList,
			ID:    peerList[i],
		}

		redisDB := 10 + i
		cfg := defaultConfig(basePort, redisDB, "")

		app, graph := newStartedApp(t, senders[i], peers, cfg)
		app.IncomingRouter.SetEnvironmentCache(time.Second, func(s string) (string, error) { return "test", nil })
		app.PeerRouter.SetEnvironmentCache(time.Second, func(s string) (string, error) { return "test", nil })
		apps[i] = app
		defer startstop.Stop(graph.Objects(), nil)
	}

	traceData := []byte(fmt.Sprintf(`{"foo":"bar","trace.trace_id":"%s"}`, traceID))
	// Deliver to host 1, it should be passed to host 0 and emitted there.
	zEnc, _ := zstd.NewWriter(nil)
	blob := zEnc.EncodeAll(traceData, nil)
	req, err := http.NewRequest(
		"POST",
		"http://localhost:15002/1/events/dataset",
		bytes.NewReader(blob),
	)
	assert.NoError(t, err)
	req.Header.Set("X-Honeycomb-Team", nonLegacyAPIKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "zstd")
	req.Header.Set("X-Honeycomb-Event-Time", now.Format(time.RFC3339Nano))
	req.Header.Set("X-Honeycomb-Samplerate", "10")

	post(t, req)
	assert.Eventually(t, func() bool {
		return len(senders[0].Events) >= 1
	}, 2*time.Second, 2*time.Millisecond)

	events := senders[0].GetBlock(1)
	require.Len(t, events, 1)
	event := events[0]
	assert.Equal(t, nonLegacyAPIKey, event.APIKey)
	assert.Equal(t, "dataset", event.Dataset)
	assert.Equal(t, uint(10), event.SampleRate)
	assert.Equal(t, "http://api.honeycomb.io", event.APIHost)
	assert.Equal(t, now, event.Timestamp)
	assert.Equal(t, traceID, event.Data.Get("trace.trace_id"))
	assert.Equal(t, "bar", event.Data.Get("foo"))
	assert.Equal(t, int64(10), event.Data.Get(types.MetaRefineryOriginalSampleRate))
	assert.Equal(t, "Test-Client", event.Data.MetaRefineryIncomingUserAgent)
	// Repeat, but deliver to host 1 on the peer channel, it should be
	// passed to host 0.

	blob = blob[:0]
	buf := bytes.NewBuffer(blob)
	gz := gzip.NewWriter(buf)
	gz.Write(traceData)
	gz.Close()

	req, err = http.NewRequest(
		"POST",
		"http://localhost:15003/1/events/dataset",
		buf,
	)
	assert.NoError(t, err)
	req.Header.Set("X-Honeycomb-Team", nonLegacyAPIKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set("X-Honeycomb-Event-Time", now.Format(time.RFC3339Nano))
	req.Header.Set("X-Honeycomb-Samplerate", "10")

	post(t, req)
	assert.Eventually(t, func() bool {
		return len(senders[0].Events) >= 1
	}, 2*time.Second, 2*time.Millisecond)

	events = senders[0].GetBlock(1)
	require.Len(t, events, 1)
	event = events[0]
	assert.Equal(t, nonLegacyAPIKey, event.APIKey)
	assert.Equal(t, "dataset", event.Dataset)
	assert.Equal(t, uint(10), event.SampleRate)
	assert.Equal(t, "http://api.honeycomb.io", event.APIHost)
	assert.Equal(t, now, event.Timestamp)
	assert.Equal(t, traceID, event.Data.Get("trace.trace_id"))
	assert.Equal(t, "bar", event.Data.Get("foo"))
	assert.Equal(t, int64(10), event.Data.Get(types.MetaRefineryOriginalSampleRate))
	assert.Equal(t, "Test-Client", event.Data.MetaRefineryIncomingUserAgent)
}

func TestPeerRouting_TraceLocalityDisabled(t *testing.T) {
	// Parallel integration tests need different ports!
	t.Parallel()

	peerList := []string{"http://localhost:17001", "http://localhost:17003"}

	var apps [2]*App
	var senders [2]*transmit.MockTransmission
	for i := range apps {
		var graph inject.Graph
		basePort := 17000 + (i * 2)
		senders[i] = &transmit.MockTransmission{}
		peers := &peer.MockPeers{
			Peers: peerList,
			ID:    peerList[i],
		}
		redisDB := 12 + i
		cfg := defaultConfig(basePort, redisDB, "")
		collectionCfg := cfg.GetCollectionConfig()
		collectionCfg.TraceLocalityMode = "distributed"
		cfg.GetCollectionConfigVal = collectionCfg

		apps[i], graph = newStartedApp(t, senders[i], peers, cfg)
		defer startstop.Stop(graph.Objects(), nil)
	}

	// Deliver to host 1, it should be passed to host 0 and emitted there.
	req, err := http.NewRequest(
		"POST",
		"http://localhost:17002/1/batch/dataset",
		nil,
	)
	assert.NoError(t, err)
	req.Header.Set("X-Honeycomb-Team", legacyAPIKey)
	req.Header.Set("Content-Type", "application/json")

	// this span data was chosen because it hashes to the appropriate shard for this
	// test. You can't change it and expect the test to pass.
	blob := `[` + getTestSpanJSON() + `]`
	req.Body = io.NopCloser(strings.NewReader(blob))
	post(t, req)
	assert.Eventually(t, func() bool {
		return len(senders[1].Events) >= 1
	}, 5*time.Second, 2*time.Millisecond)

	events := senders[1].GetBlock(1)
	require.Len(t, events, 1)
	event := events[0]
	assert.Equal(t, legacyAPIKey, event.APIKey)
	assert.Equal(t, "dataset", event.Dataset)
	assert.Equal(t, uint(2), event.SampleRate)
	assert.Equal(t, "http://api.honeycomb.io", event.APIHost)
	assert.Equal(t, now, event.Timestamp)
	assert.Equal(t, "2", event.Data.Get("trace.trace_id"))
	assert.Equal(t, "10", event.Data.Get("trace.span_id"))
	assert.Equal(t, "0000000000", event.Data.Get("trace.parent_id"))
	assert.Equal(t, "value", event.Data.Get("key"))
	assert.Equal(t, "bar", event.Data.Get("foo"))
	assert.Equal(t, int64(2), event.Data.Get(types.MetaRefineryOriginalSampleRate))
	assert.Equal(t, "Test-Client", event.Data.MetaRefineryIncomingUserAgent)
	// Repeat, but deliver to host 1 on the peer channel, it should be
	// passed to host 0 since that's who the trace belongs to.
	req, err = http.NewRequest(
		"POST",
		"http://localhost:17003/1/batch/dataset",
		nil,
	)
	assert.NoError(t, err)
	req.Header.Set("X-Honeycomb-Team", legacyAPIKey)
	req.Header.Set("Content-Type", "application/json")

	req.Body = io.NopCloser(strings.NewReader(blob))
	post(t, req)

	// Wait for the second event to arrive, but don't consume any events yet
	require.Eventually(t, func() bool {
		// Check the channel length without consuming
		return len(senders[1].Events) >= 1
	}, 5*time.Second, 2*time.Millisecond)

	// Get the second event
	events = senders[1].GetBlock(1)
	require.Len(t, events, 1)
	event = events[0] // Second event
	assert.Equal(t, legacyAPIKey, event.APIKey)
	assert.Equal(t, "dataset", event.Dataset)
	assert.Equal(t, uint(2), event.SampleRate)
	assert.Equal(t, "2", event.Data.Get("trace.trace_id"))
}

func TestOTLPProtobufIntegration(t *testing.T) {
	t.Parallel()
	port := 16000
	redisDB := 14

	testServer := newTestAPIServer(t)
	cfg := defaultConfigWithGRPC(port, redisDB, testServer.server.URL, true)
	app, graph := newStartedApp(t, nil, nil, cfg)

	// Create OTLP protobuf request
	req := &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*trace.ResourceSpans{{
			ScopeSpans: []*trace.ScopeSpans{{
				Spans: []*trace.Span{{
					TraceId:           []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
					SpanId:            []byte{0, 0, 0, 0, 0, 0, 0, 1},
					Name:              "test-span",
					StartTimeUnixNano: uint64(time.Now().UnixNano()),
					EndTimeUnixNano:   uint64(time.Now().UnixNano()),
					Attributes: []*common.KeyValue{{
						Key:   "test.key",
						Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "test-value"}},
					}},
				}},
			}},
		}},
	}

	t.Run("HTTP", func(t *testing.T) {
		// Marshal to protobuf
		body, err := proto.Marshal(req)
		require.NoError(t, err)

		// Send OTLP protobuf request via HTTP
		httpReq := httptest.NewRequest(
			"POST",
			fmt.Sprintf("http://localhost:%d/v1/traces", port),
			bytes.NewReader(body),
		)
		httpReq.Header.Set("X-Honeycomb-Team", legacyAPIKey)
		httpReq.Header.Set("X-Honeycomb-Dataset", "test-dataset")
		httpReq.Header.Set("Content-Type", "application/protobuf")

		resp, err := http.DefaultTransport.RoundTrip(httpReq)
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		resp.Body.Close()

		time.Sleep(5 * app.Config.GetTracesConfig().GetSendTickerValue())

		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			events := testServer.getEvents()
			require.Len(collect, events, 1)
			assert.Equal(collect, "test-dataset", events[0].Dataset)
			assert.Equal(collect, "test-value", events[0].Data.Get("test.key"))
			assert.Equal(collect, "test-span", events[0].Data.Get("name"))
		}, 2*time.Second, 10*time.Millisecond)
	})

	t.Run("gRPC", func(t *testing.T) {
		// Connect to gRPC server
		grpcAddr := fmt.Sprintf("localhost:%d", port+2)
		conn, err := grpc.Dial(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		defer conn.Close()

		// Create gRPC client
		client := collectortrace.NewTraceServiceClient(conn)

		// Create gRPC context with metadata
		ctx := metadata.AppendToOutgoingContext(context.Background(),
			"x-honeycomb-team", legacyAPIKey,
			"x-honeycomb-dataset", "test-dataset-grpc",
		)

		// Send OTLP protobuf request via gRPC
		_, err = client.Export(ctx, req)
		require.NoError(t, err)

		time.Sleep(5 * app.Config.GetTracesConfig().GetSendTickerValue())

		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			events := testServer.getEvents()
			require.GreaterOrEqual(collect, len(events), 1)
			// Find the gRPC event (it may not be the first one if HTTP test ran first)
			var grpcEvent *types.Event
			for _, event := range events {
				if event.Dataset == "test-dataset-grpc" {
					grpcEvent = event
					break
				}
			}
			require.NotNil(collect, grpcEvent)
			assert.Equal(collect, "test-dataset-grpc", grpcEvent.Dataset)
			assert.Equal(collect, "test-value", grpcEvent.Data.Get("test.key"))
			assert.Equal(collect, "test-span", grpcEvent.Data.Get("name"))
		}, 2*time.Second, 10*time.Millisecond)
	})

	err := startstop.Stop(graph.Objects(), nil)
	assert.NoError(t, err)
}

func TestOTLPGRPCConcurrency(t *testing.T) {
	t.Parallel()
	port := 19000
	redisDB := 15

	testServer := newTestAPIServer(t)
	cfg := defaultConfigWithGRPC(port, redisDB, testServer.server.URL, true)
	_, graph := newStartedApp(t, nil, nil, cfg)

	// Connect to gRPC server
	grpcAddr := fmt.Sprintf("localhost:%d", port+2)
	conn, err := grpc.Dial(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	client := collectortrace.NewTraceServiceClient(conn)

	// Create multiple different OTLP requests to ensure race conditions are triggered
	numRequests := 10
	numConcurrent := 5

	// Launch concurrent goroutines sending different requests
	p := pool.New().WithErrors().WithMaxGoroutines(numConcurrent)
	for i := range numConcurrent {
		routineID := i
		p.Go(func() error {
			for j := 0; j < numRequests; j++ {
				// Create unique trace data for each request to detect corruption
				traceID := make([]byte, 16)
				spanID := make([]byte, 8)
				// Use routine ID and request number to create unique IDs
				binary.BigEndian.PutUint64(traceID[8:], uint64(routineID*1000+j))
				binary.BigEndian.PutUint64(spanID, uint64(routineID*1000+j))

				req := &collectortrace.ExportTraceServiceRequest{
					ResourceSpans: []*trace.ResourceSpans{{
						ScopeSpans: []*trace.ScopeSpans{{
							Spans: []*trace.Span{{
								TraceId:           traceID,
								SpanId:            spanID,
								Name:              fmt.Sprintf("concurrent-span-r%d-j%d", routineID, j),
								StartTimeUnixNano: uint64(time.Now().UnixNano()),
								EndTimeUnixNano:   uint64(time.Now().UnixNano()),
								Attributes: []*common.KeyValue{{
									Key:   fmt.Sprintf("routine.id.%d.request.%d", routineID, j),
									Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: fmt.Sprintf("routine-%d-request-%d", routineID, j)}},
								}},
							}},
						}},
					}},
				}

				// Create gRPC context with metadata
				ctx := metadata.AppendToOutgoingContext(context.Background(),
					"x-honeycomb-team", legacyAPIKey,
					"x-honeycomb-dataset", fmt.Sprintf("test-dataset-r%d-j%d", routineID, j),
				)

				// Send OTLP protobuf request via gRPC
				_, err := client.Export(ctx, req)
				if err != nil {
					return fmt.Errorf("routine %d request %d failed: %w", routineID, j, err)
				}
			}
			return nil
		})
	}

	// Wait for all requests to complete and check for errors
	err = p.Wait()
	require.NoError(t, err)

	// Verify that events were processed
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		events := testServer.getEvents()
		// We expect exactly numRequests * numConcurrent events
		assert.GreaterOrEqual(collect, len(events), numRequests*numConcurrent)
	}, 5*time.Second, 10*time.Millisecond)

	err = startstop.Stop(graph.Objects(), nil)
	assert.NoError(t, err)
}

var (
	benchmarkMutex  sync.Mutex
	benchmarkEvents []batchedEvent
	httpClient      = &http.Client{Transport: &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   1 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          100,
		MaxConnsPerHost:       100,
		IdleConnTimeout:       90 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}}
)

// Pre-build benchmark events
func createBenchmarkEvents(n int) {
	benchmarkMutex.Lock()
	defer benchmarkMutex.Unlock()

	if len(benchmarkEvents) < n {
		benchmarkEvents = make([]batchedEvent, n)
		for i := range benchmarkEvents {
			benchmarkEvents[i] = batchedEvent{
				Timestamp:        now.Format(time.RFC3339Nano),
				MsgPackTimestamp: &now,
				SampleRate:       2,
				Data: map[string]interface{}{
					"trace.trace_id":  uuid.NewString(),
					"trace.span_id":   uuid.NewString(),
					"trace.parent_id": "0000000000",
					"key":             "value",
					"field0":          0,
					"field1":          1,
					"field2":          2,
					"field3":          3,
					"field4":          4,
					"field5":          5,
					"field6":          6,
					"field7":          7,
					"field8":          8,
					"field9":          9,
					"field10":         10,
					"long":            "this is a test of the emergency broadcast system",
					"foo":             "bar",
				},
			}
		}
	}
}

// encoding configurations
var encodings = []struct {
	name        string
	contentType string
	encoding    string
}{
	{"json-gzip", "application/json", "gzip"},
	{"msgpack-zstd", "application/x-msgpack", "zstd"},
}

// batchedEvent represents the structure expected by the batch endpoint
type batchedEvent struct {
	Timestamp        string                 `json:"time"`
	MsgPackTimestamp *time.Time             `msgpack:"time,omitempty"`
	SampleRate       int64                  `json:"samplerate" msgpack:"samplerate"`
	Data             map[string]interface{} `json:"data" msgpack:"data"`
}

var zstdEncoder *zstd.Encoder

func init() {
	zstdEncoder, _ = zstd.NewWriter(nil)
}

func encodeAndCompress(events []batchedEvent, contentType, compression string) ([]byte, error) {
	var data []byte
	var err error
	if contentType == "application/x-msgpack" {
		data, err = msgpack.Marshal(events)
	} else {
		data, err = json.Marshal(events)
	}
	if err != nil {
		return nil, err
	}

	var out []byte
	switch compression {
	case "gzip":
		var buf bytes.Buffer
		w := gzip.NewWriter(&buf)
		w.Write(data)
		w.Close()
		out = buf.Bytes()
	case "zstd":
		out = zstdEncoder.EncodeAll(data, nil)
	default:
		out = data
	}
	return out, nil
}

// Pre-build OTLP benchmark requests
var (
	benchmarkOTLPMutex    sync.Mutex
	benchmarkOTLPRequests []*collectortrace.ExportTraceServiceRequest
)

func createBenchmarkOTLPRequest() *collectortrace.ExportTraceServiceRequest {
	benchmarkOTLPMutex.Lock()
	defer benchmarkOTLPMutex.Unlock()

	if len(benchmarkOTLPRequests) == 0 {
		// Create a single request with 50 spans
		spans := make([]*trace.Span, 50)
		for i := 0; i < 50; i++ {
			traceID := make([]byte, 16)
			spanID := make([]byte, 8)
			binary.BigEndian.PutUint64(traceID[8:], uint64(i))
			binary.BigEndian.PutUint64(spanID, uint64(i))

			spans[i] = &trace.Span{
				TraceId:           traceID,
				SpanId:            spanID,
				Name:              "benchmark-span",
				StartTimeUnixNano: uint64(now.UnixNano()),
				EndTimeUnixNano:   uint64(now.UnixNano()),
				Attributes: []*common.KeyValue{
					{Key: "key", Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "value"}}},
					{Key: "field0", Value: &common.AnyValue{Value: &common.AnyValue_IntValue{IntValue: int64(i % 10)}}},
					{Key: "field1", Value: &common.AnyValue{Value: &common.AnyValue_IntValue{IntValue: int64((i + 1) % 10)}}},
					{Key: "field2", Value: &common.AnyValue{Value: &common.AnyValue_IntValue{IntValue: int64((i + 2) % 10)}}},
					{Key: "field3", Value: &common.AnyValue{Value: &common.AnyValue_IntValue{IntValue: int64((i + 3) % 10)}}},
					{Key: "field4", Value: &common.AnyValue{Value: &common.AnyValue_IntValue{IntValue: int64((i + 4) % 10)}}},
					{Key: "field5", Value: &common.AnyValue{Value: &common.AnyValue_IntValue{IntValue: int64((i + 5) % 10)}}},
					{Key: "field6", Value: &common.AnyValue{Value: &common.AnyValue_IntValue{IntValue: int64((i + 6) % 10)}}},
					{Key: "field7", Value: &common.AnyValue{Value: &common.AnyValue_IntValue{IntValue: int64((i + 7) % 10)}}},
					{Key: "field8", Value: &common.AnyValue{Value: &common.AnyValue_IntValue{IntValue: int64((i + 8) % 10)}}},
					{Key: "field9", Value: &common.AnyValue{Value: &common.AnyValue_IntValue{IntValue: int64((i + 9) % 10)}}},
					{Key: "field10", Value: &common.AnyValue{Value: &common.AnyValue_IntValue{IntValue: int64((i + 10) % 10)}}},
					{Key: "long", Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "this is a test of the emergency broadcast system"}}},
					{Key: "foo", Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "bar"}}},
				},
			}
		}

		benchmarkOTLPRequests = []*collectortrace.ExportTraceServiceRequest{{
			ResourceSpans: []*trace.ResourceSpans{{
				ScopeSpans: []*trace.ScopeSpans{{
					Spans: spans,
				}},
			}},
		}}
	}
	return benchmarkOTLPRequests[0]
}

func BenchmarkTracesOTLP(b *testing.B) {
	sender := &countingTransmission{}
	redisDB := 15
	cfg := defaultConfigWithGRPC(18000, redisDB, "", true)
	_, graph := newStartedApp(b, sender, nil, cfg)
	defer func() {
		err := startstop.Stop(graph.Objects(), nil)
		assert.NoError(b, err)
	}()

	b.Run("http", func(b *testing.B) {
		// Pre-compute single protobuf blob with 50 spans
		otlpReq := createBenchmarkOTLPRequest()
		blob, err := proto.Marshal(otlpReq)
		assert.NoError(b, err)

		req, err := http.NewRequest(
			"POST",
			"http://localhost:18000/v1/traces",
			nil,
		)
		assert.NoError(b, err)
		req.Header.Set("X-Honeycomb-Team", legacyAPIKey)
		req.Header.Set("X-Honeycomb-Dataset", "benchmark")
		req.Header.Set("Content-Type", "application/protobuf")

		sender.resetCount()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			req.Body = io.NopCloser(bytes.NewReader(blob))
			post(b, req)
		}
		sender.waitForCount(b, b.N*50) // 50 spans per request
	})

	b.Run("http-zstd", func(b *testing.B) {
		// Pre-compute single protobuf blob with 50 spans
		otlpReq := createBenchmarkOTLPRequest()
		blob, err := proto.Marshal(otlpReq)
		assert.NoError(b, err)

		blob = zstdEncoder.EncodeAll(blob, nil)

		req, err := http.NewRequest(
			"POST",
			"http://localhost:18000/v1/traces",
			nil,
		)
		assert.NoError(b, err)
		req.Header.Set("X-Honeycomb-Team", legacyAPIKey)
		req.Header.Set("X-Honeycomb-Dataset", "benchmark")
		req.Header.Set("Content-Type", "application/protobuf")
		req.Header.Set("Content-Encoding", "zstd")

		sender.resetCount()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			req.Body = io.NopCloser(bytes.NewReader(blob))
			post(b, req)
		}
		sender.waitForCount(b, b.N*50) // 50 spans per request
	})

	b.Run("http-json", func(b *testing.B) {
		// Pre-compute single JSON blob with 50 spans
		otlpReq := createBenchmarkOTLPRequest()
		blob, err := protojson.Marshal(otlpReq)
		assert.NoError(b, err)

		req, err := http.NewRequest(
			"POST",
			"http://localhost:18000/v1/traces",
			nil,
		)
		assert.NoError(b, err)
		req.Header.Set("X-Honeycomb-Team", legacyAPIKey)
		req.Header.Set("X-Honeycomb-Dataset", "benchmark")
		req.Header.Set("Content-Type", "application/json")

		sender.resetCount()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			req.Body = io.NopCloser(bytes.NewReader(blob))
			post(b, req)
		}
		sender.waitForCount(b, b.N*50) // 50 spans per request
	})

	b.Run("grpc", func(b *testing.B) {
		// Connect to gRPC server
		conn, err := grpc.Dial("localhost:18002", grpc.WithTransportCredentials(insecure.NewCredentials()))
		assert.NoError(b, err)
		defer conn.Close()

		client := collectortrace.NewTraceServiceClient(conn)
		ctx := metadata.AppendToOutgoingContext(context.Background(),
			"x-honeycomb-team", legacyAPIKey,
			"x-honeycomb-dataset", "benchmark",
		)

		// Pre-compute single request with 50 spans
		otlpReq := createBenchmarkOTLPRequest()

		sender.resetCount()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			_, err := client.Export(ctx, otlpReq)
			assert.NoError(b, err)
		}
		sender.waitForCount(b, b.N*50) // 50 spans per request
	})
}

func BenchmarkTraces(b *testing.B) {
	sender := &countingTransmission{}
	redisDB := 1
	cfg := defaultConfig(11000, redisDB, "")
	_, graph := newStartedApp(b, sender, nil, cfg)
	defer func() {
		err := startstop.Stop(graph.Objects(), nil)
		assert.NoError(b, err)
	}()

	for _, encoding := range encodings {
		b.Run(encoding.name, func(b *testing.B) {
			// Pre-compute single batch with 50 events
			createBenchmarkEvents(50)
			events := make([]batchedEvent, 50)
			for i := 0; i < 50; i++ {
				events[i] = benchmarkEvents[i]
			}
			blob, err := encodeAndCompress(events, encoding.contentType, encoding.encoding)
			assert.NoError(b, err)

			req, err := http.NewRequest(
				"POST",
				"http://localhost:11000/1/batch/dataset",
				nil,
			)
			assert.NoError(b, err)
			req.Header.Set("X-Honeycomb-Team", legacyAPIKey)
			req.Header.Set("Content-Type", encoding.contentType)
			req.Header.Set("Content-Encoding", encoding.encoding)

			sender.resetCount()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				req.Body = io.NopCloser(bytes.NewReader(blob))
				post(b, req)
			}
			sender.waitForCount(b, b.N*50) // 50 events per request
		})
	}
}

func BenchmarkDistributedTraces(b *testing.B) {
	sender := &countingTransmission{}

	peerList := []string{
		"http://localhost:12001",
		"http://localhost:12003",
		"http://localhost:12005",
		"http://localhost:12007",
		"http://localhost:12009",
	}

	var apps [5]*App
	var addrs [5]string
	for i := range apps {
		var graph inject.Graph
		basePort := 12000 + (i * 2)
		peers := &peer.MockPeers{
			Peers: peerList,
			ID:    peerList[i],
		}

		redisDB := 2 + i
		cfg := defaultConfig(basePort, redisDB, "")
		apps[i], graph = newStartedApp(b, sender, peers, cfg)
		defer startstop.Stop(graph.Objects(), nil)

		addrs[i] = "localhost:" + strconv.Itoa(basePort)
	}

	for _, encoding := range encodings {
		b.Run(encoding.name, func(b *testing.B) {
			req, err := http.NewRequest(
				"POST",
				"http://localhost:12000/1/batch/dataset",
				nil,
			)
			assert.NoError(b, err)
			req.Header.Set("X-Honeycomb-Team", legacyAPIKey)
			req.Header.Set("Content-Type", encoding.contentType)
			req.Header.Set("Content-Encoding", encoding.encoding)

			b.Run("single", func(b *testing.B) {
				// Pre-compute blobs
				createBenchmarkEvents(b.N)
				blobs := make([][]byte, b.N)
				for n := 0; n < b.N; n++ {
					blobs[n], err = encodeAndCompress([]batchedEvent{benchmarkEvents[n%len(benchmarkEvents)]}, encoding.contentType, encoding.encoding)
					assert.NoError(b, err)
				}

				sender.resetCount()
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					req.Body = io.NopCloser(bytes.NewReader(blobs[n]))
					req.URL.Host = addrs[n%len(addrs)]
					post(b, req)
				}
				sender.waitForCount(b, b.N)
			})

			b.Run("batch", func(b *testing.B) {
				// Pre-compute blobs
				createBenchmarkEvents(b.N)
				numBatches := (b.N / 50) + 1
				blobs := make([][]byte, numBatches)
				for n := 0; n < numBatches; n++ {
					events := make([]batchedEvent, 50)
					for i := 0; i < 50; i++ {
						events[i] = benchmarkEvents[((n*50)+i)%len(benchmarkEvents)]
					}
					blobs[n], err = encodeAndCompress(events, encoding.contentType, encoding.encoding)
					assert.NoError(b, err)
				}

				sender.resetCount()
				b.ResetTimer()
				for n := 0; n < numBatches; n++ {
					req.Body = io.NopCloser(bytes.NewReader(blobs[n]))
					req.URL.Host = addrs[n%len(addrs)]
					post(b, req)
				}
				sender.waitForCount(b, b.N)
			})
		})
	}
}

package app

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/facebookgo/inject"
	"github.com/facebookgo/startstop"
	"github.com/jonboulle/clockwork"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/honeycombio/libhoney-go"
	"github.com/honeycombio/libhoney-go/transmission"
	"github.com/honeycombio/refinery/centralstore"
	"github.com/honeycombio/refinery/collect"
	"github.com/honeycombio/refinery/collect/cache"
	"github.com/honeycombio/refinery/collect/stressRelief"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/gossip"
	"github.com/honeycombio/refinery/internal/health"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/redis"
	"github.com/honeycombio/refinery/sample"
	"github.com/honeycombio/refinery/transmit"
)

const legacyAPIKey = "c9945edf5d245834089a1bd6cc9ad01e"
const nonLegacyAPIKey = "d245834089a1bd6cc9ad01e"

type countingWriterSender struct {
	transmission.WriterSender

	count  int
	target int
	ch     chan struct{}
	mutex  sync.Mutex
}

func (w *countingWriterSender) Add(ev *transmission.Event) {
	w.WriterSender.Add(ev)

	w.mutex.Lock()
	defer w.mutex.Unlock()

	w.count++
	if w.ch != nil && w.count >= w.target {
		close(w.ch)
		w.ch = nil
	}
}

func (w *countingWriterSender) resetCount() {
	w.mutex.Lock()
	w.count = 0
	w.mutex.Unlock()
}

func (w *countingWriterSender) waitForCount(t testing.TB, target int) {
	w.mutex.Lock()
	if w.count >= target {
		w.mutex.Unlock()
		return
	}

	ch := make(chan struct{})
	w.ch = ch
	w.target = target
	w.mutex.Unlock()

	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Errorf("timed out waiting for %d events", target)
	}
}

var configCallback = func(c *config.MockConfig) {}

func newStartedApp(
	t testing.TB,
	libhoneyT transmission.Sender,
	basePort int,
	redisDB int,
	enableHostMetadata bool,
) (*App, inject.Graph, func()) {
	c := &config.MockConfig{
		GetTraceTimeoutVal:       10 * time.Millisecond,
		GetMaxBatchSizeVal:       500,
		GetSamplerTypeVal:        &config.DeterministicSamplerConfig{SampleRate: 1},
		SendTickerVal:            20 * time.Millisecond,
		PeerManagementType:       "file",
		GetUpstreamBufferSizeVal: 10000,
		GetRedisDatabaseVal:      redisDB,
		AddRuleReasonToTrace:     true,
		GetListenAddrVal:         "127.0.0.1:" + strconv.Itoa(basePort),
		IsAPIKeyValidFunc:        func(k string) bool { return k == legacyAPIKey || k == nonLegacyAPIKey },
		GetHoneycombAPIVal:       "http://api.honeycomb.io",
		GetCollectionConfigVal: config.CollectionConfig{
			CacheCapacity:        10000,
			SenderCycleDuration:  config.Duration(100 * time.Millisecond),
			DeciderCycleDuration: config.Duration(10 * time.Millisecond),
			ShutdownDelay:        config.Duration(1 * time.Millisecond),
		},
		GetParallelismVal:      10,
		AddHostMetadataToTrace: enableHostMetadata,
		TraceIdFieldNames:      []string{"trace.trace_id"},
		ParentIdFieldNames:     []string{"trace.parent_id"},
		SampleCache:            config.SampleCacheConfig{KeptSize: 10000, DroppedSize: 100000, SizeCheckInterval: config.Duration(10 * time.Second)},
		StoreOptions: config.SmartWrapperOptions{
			StateTicker:     config.Duration(50 * time.Millisecond),
			BasicStoreType:  "redis",
			SpanChannelSize: 10000,
			SendDelay:       config.Duration(2 * time.Millisecond),
			DecisionTimeout: config.Duration(100 * time.Millisecond),
		},
	}

	// give the test a chance to override parts of the config
	configCallback(c)

	fmt.Println("Using Redis database", c.GetRedisDatabaseVal)

	var err error
	a := App{}

	lgr := &logger.StdoutLogger{
		Config: c,
	}
	lgr.SetLevel("error")
	lgr.Start()

	// TODO use real metrics
	metricsr := &metrics.MockMetrics{}
	metricsr.Start()

	collector := collect.GetCollectorImplementation(nil)

	samplerFactory := &sample.SamplerFactory{}

	upstreamClient, err := libhoney.NewClient(libhoney.ClientConfig{
		Transmission: libhoneyT,
	})
	assert.NoError(t, err)

	var store centralstore.BasicStorer
	if c.StoreOptions.BasicStoreType == "local" {
		store = &centralstore.LocalStore{}
	} else {
		store = &centralstore.RedisBasicStore{}
	}
	sw := &centralstore.SmartWrapper{}
	redis := &redis.DefaultClient{}
	var g inject.Graph
	err = g.Provide(
		&inject.Object{Value: c},
		&inject.Object{Value: lgr},
		&inject.Object{Value: http.DefaultTransport, Name: "upstreamTransport"},
		&inject.Object{Value: transmit.NewDefaultTransmission(upstreamClient, metricsr, "upstream"), Name: "upstreamTransmission"},
		&inject.Object{Value: clockwork.NewRealClock()},
		&inject.Object{Value: trace.Tracer(noop.Tracer{}), Name: "tracer"},
		&inject.Object{Value: &cache.SpanCache_basic{}},
		&inject.Object{Value: redis, Name: "redis"},
		&inject.Object{Value: store},
		&inject.Object{Value: sw},
		&inject.Object{Value: collector, Name: "collector"},
		&inject.Object{Value: &cache.CuckooSentCache{}},
		&inject.Object{Value: metricsr, Name: "metrics"},
		&inject.Object{Value: metricsr, Name: "genericMetrics"},
		&inject.Object{Value: metricsr, Name: "upstreamMetrics"},
		&inject.Object{Value: "test", Name: "version"},
		&inject.Object{Value: samplerFactory},
		&inject.Object{Value: &health.Health{}},
		&inject.Object{Value: &gossip.GossipRedis{}, Name: "gossip"},
		&inject.Object{Value: &stressRelief.StressRelief{}, Name: "stressRelief"},
		&inject.Object{Value: &a},
	)
	require.NoError(t, err)

	err = g.Populate()
	require.NoError(t, err)

	err = startstop.Start(g.Objects(), nil)
	require.NoError(t, err)

	// Racy: wait just a moment for ListenAndServe to start up.
	time.Sleep(10 * time.Millisecond)
	return &a, g, func() {
		conn := redis.Get()
		_, err := conn.Do("FLUSHDB")
		assert.NoError(t, err)
		conn.Close()
		err = startstop.Stop(g.Objects(), nil)
		assert.NoError(t, err)
	}
}

func post(t testing.TB, req *http.Request) {
	resp, err := httpClient.Do(req)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}

func TestAppIntegration(t *testing.T) {
	port := 10500
	redisDB := 2

	sender := &transmission.MockSender{}
	_, _, stop := newStartedApp(t, sender, port, redisDB, false)
	defer stop()

	// Send a root span, it should be sent in short order.
	req := httptest.NewRequest(
		"POST",
		fmt.Sprintf("http://localhost:%d/1/batch/dataset", port),
		strings.NewReader(`[{"data":{"trace.trace_id":"1","foo":"bar"}}]`),
	)
	req.Header.Set("X-Honeycomb-Team", legacyAPIKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultTransport.RoundTrip(req)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	require.Eventually(t, func() bool {
		events := sender.Events()
		return len(events) == 1
	}, 5*time.Second, 100*time.Millisecond)

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		events := sender.Events()

		assert.Equal(collect, "dataset", events[0].Dataset)
		assert.Equal(collect, "bar", events[0].Data["foo"])
		assert.Equal(collect, "1", events[0].Data["trace.trace_id"])
		assert.Equal(collect, uint(1), events[0].Data["meta.refinery.original_sample_rate"])
	}, 5*time.Second, 100*time.Millisecond)
}

func TestAppIntegrationWithNonLegacyKey(t *testing.T) {
	port := 10600
	redisDB := 3

	sender := &transmission.MockSender{}
	a, _, stop := newStartedApp(t, sender, port, redisDB, false)
	defer stop()
	a.IncomingRouter.SetEnvironmentCache(time.Second, func(s string) (string, error) { return "test", nil })

	// Send a root span, it should be sent in short order.
	traceID := strconv.Itoa(rand.Intn(1000))
	data := `[{"data":{"trace.trace_id":"` + traceID + `","foo":"bar"}}]`
	req := httptest.NewRequest(
		"POST",
		fmt.Sprintf("http://localhost:%d/1/batch/dataset", port),
		strings.NewReader(data),
	)
	req.Header.Set("X-Honeycomb-Team", nonLegacyAPIKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultTransport.RoundTrip(req)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	require.Eventually(t, func() bool {
		events := sender.Events()
		return len(events) == 1
	}, 5*time.Second, 100*time.Millisecond)

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		events := sender.Events()
		assert.Equal(t, "dataset", events[0].Dataset)
		assert.Equal(t, "bar", events[0].Data["foo"])
		assert.Equal(t, traceID, events[0].Data["trace.trace_id"])
		assert.Equal(t, uint(1), events[0].Data["meta.refinery.original_sample_rate"])
	}, 5*time.Second, 100*time.Millisecond)
}

func TestAppIntegrationWithUnauthorizedKey(t *testing.T) {
	port := 10700
	redisDB := 4

	sender := &transmission.MockSender{}
	a, _, stop := newStartedApp(t, sender, port, redisDB, false)
	defer stop()
	a.IncomingRouter.SetEnvironmentCache(time.Second, func(s string) (string, error) { return "test", nil })

	// Send a root span, it should be sent in short order.
	traceID := strconv.Itoa(rand.Intn(1000))
	input := fmt.Sprintf(`[{"data":{"trace.trace_id":"%s","foo":"bar"}}]`, traceID)
	req := httptest.NewRequest(
		"POST",
		fmt.Sprintf("http://localhost:%d/v1/traces", port),
		strings.NewReader(input),
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
}

func TestHostMetadataSpanAdditions(t *testing.T) {
	port := 14000
	redisDB := 6

	sender := &transmission.MockSender{}
	_, _, stop := newStartedApp(t, sender, port, redisDB, true)
	defer stop()

	// Send a root span, it should be sent in short order.
	req := httptest.NewRequest(
		"POST",
		fmt.Sprintf("http://localhost:%d/1/batch/dataset", port),
		strings.NewReader(`[{"data":{"foo":"bar","trace.trace_id":"2"}}]`),
	)
	req.Header.Set("X-Honeycomb-Team", legacyAPIKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultTransport.RoundTrip(req)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	require.Eventually(t, func() bool {
		events := sender.Events()
		return len(events) == 1
	}, 5*time.Second, 100*time.Millisecond)

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		events := sender.Events()

		assert.Equal(t, "dataset", events[0].Dataset)
		assert.Equal(t, "bar", events[0].Data["foo"])
		assert.Equal(t, "2", events[0].Data["trace.trace_id"])
		assert.Equal(t, uint(1), events[0].Data["meta.refinery.original_sample_rate"])
		hostname, _ := os.Hostname()
		assert.Equal(t, hostname, events[0].Data["meta.refinery.decider.host.name"])
	}, 5*time.Second, 100*time.Millisecond)
}

func TestSamplerKeys(t *testing.T) {
	port := 14000
	redisDB := 11

	sender := &transmission.MockSender{}
	sampler := &config.MockSamplerConfig{
		SampleRate: 2,
		FieldList:  []string{"path", "status"},
	}
	configCallback = func(c *config.MockConfig) {
		c.GetSamplerTypeVal = sampler
		c.GetSamplerTypeName = "mock"
	}

	_, _, stop := newStartedApp(t, sender, port, redisDB, true)
	defer stop()

	spandata := `[
		{"data":{"trace.trace_id":"123","trace.span_id":"2","path":"/bar","status":"200","trace.parent_id":"1"}},
		{"data":{"trace.trace_id":"123","trace.span_id":"3","path":"/bar","status":"404","trace.parent_id":"2"}},
		{"data":{"trace.trace_id":"123","trace.span_id":"4","path":"/bazz","status":"200","trace.parent_id":"3"}},
		{"data":{"trace.trace_id":"123","trace.span_id":"5","path":"/buzz","status":"503","trace.parent_id":"4"}},
		{"data":{"trace.trace_id":"123","trace.span_id":"1","path":"/foo","status":"200"}}
		]`

	// send some spans
	req := httptest.NewRequest(
		"POST",
		fmt.Sprintf("http://localhost:%d/1/batch/dataset", port),
		strings.NewReader(spandata),
	)
	req.Header.Set("X-Honeycomb-Team", legacyAPIKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultTransport.RoundTrip(req)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	require.Eventually(t, func() bool {
		events := sender.Events()
		return len(events) == 5
	}, 5*time.Second, 100*time.Millisecond)

	for _, event := range sender.Events() {
		fmt.Printf("event %s, key: %v\n", event.Data["trace.span_id"], event.Data["meta.refinery.sample_key"])
	}
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		events := sender.Events()

		// these are the wrong asserts for this test, but I don't care as long as they fail; will fix
		assert.Equal(t, "dataset", events[0].Dataset)
		assert.Equal(t, "bar", events[0].Data["foo"])
		assert.Equal(t, "2", events[0].Data["trace.trace_id"])
		assert.Equal(t, uint(1), events[0].Data["meta.refinery.original_sample_rate"])
		hostname, _ := os.Hostname()
		assert.Equal(t, hostname, events[0].Data["meta.refinery.decider.host.name"])
	}, 5*time.Second, 100*time.Millisecond)
}

func TestEventsEndpoint(t *testing.T) {
	t.Skip("This is testing deterministic trace sharding, which isn't relevant for Refinery 3.")

	var apps [2]*App
	var addrs [2]string
	var senders [2]*transmission.MockSender
	redisDB := 7
	for i := range apps {
		var stop func()
		basePort := 13000 + (i * 2)
		senders[i] = &transmission.MockSender{}
		apps[i], _, stop = newStartedApp(t, senders[i], basePort, redisDB, false)
		defer stop()

		addrs[i] = "localhost:" + strconv.Itoa(basePort)
	}

	// Deliver to host 1, it should be passed to host 0 and emitted there.
	zEnc, _ := zstd.NewWriter(nil)
	blob := zEnc.EncodeAll([]byte(`{"foo":"bar","trace.trace_id":"1"}`), nil)
	req, err := http.NewRequest(
		"POST",
		"http://localhost:13000/1/events/dataset",
		bytes.NewReader(blob),
	)
	assert.NoError(t, err)
	req.Header.Set("X-Honeycomb-Team", legacyAPIKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "zstd")
	req.Header.Set("X-Honeycomb-Event-Time", now.Format(time.RFC3339Nano))
	req.Header.Set("X-Honeycomb-Samplerate", "10")

	post(t, req)
	require.Eventually(t, func() bool {
		events := senders[0].Events()
		return len(events) == 1
	}, 5*time.Second, 100*time.Millisecond)

	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		events := senders[0].Events()
		event := events[0]
		assert.Equal(
			t,
			&transmission.Event{
				APIKey:     legacyAPIKey,
				Dataset:    "dataset",
				SampleRate: 10,
				APIHost:    "http://api.honeycomb.io",
				Timestamp:  now,
				Data: map[string]interface{}{
					"trace.trace_id":                     "1",
					"foo":                                "bar",
					"meta.refinery.original_sample_rate": uint(10),
					"meta.refinery.reason":               "deterministic/always",
					"meta.event_count":                   1,
					"meta.span_count":                    1,
					"meta.span_event_count":              0,
					"meta.span_link_count":               0,
				},
				Metadata: map[string]any{
					"api_host":    "http://api.honeycomb.io",
					"dataset":     "dataset",
					"environment": "",
					"enqueued_at": event.Metadata.(map[string]any)["enqueued_at"],
				},
			},
			event,
		)
	}, 5*time.Second, 200*time.Microsecond)

	// Repeat, but deliver to host 1, it should not be
	// passed to host 0.

	blob = blob[:0]
	buf := bytes.NewBuffer(blob)
	gz := gzip.NewWriter(buf)
	gz.Write([]byte(`{"foo":"bar","trace.trace_id":"1"}`))
	gz.Close()

	req, err = http.NewRequest(
		"POST",
		"http://localhost:13002/1/events/dataset",
		buf,
	)
	assert.NoError(t, err)
	req.Header.Set("X-Honeycomb-Team", legacyAPIKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set("X-Honeycomb-Event-Time", now.Format(time.RFC3339Nano))
	req.Header.Set("X-Honeycomb-Samplerate", "10")

	post(t, req)
	require.Eventually(t, func() bool {
		events := senders[1].Events()
		return len(events) == 1
	}, 5*time.Second, 100*time.Millisecond)

	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		events := senders[1].Events()
		event := events[0]
		assert.Equal(
			t,
			&transmission.Event{
				APIKey:     legacyAPIKey,
				Dataset:    "dataset",
				SampleRate: 10,
				APIHost:    "http://api.honeycomb.io",
				Timestamp:  now,
				Data: map[string]interface{}{
					"trace.trace_id":                     "1",
					"foo":                                "bar",
					"meta.refinery.original_sample_rate": uint(10),
					"meta.refinery.reason":               "deterministic/always - late arriving span",
					"meta.refinery.send_reason":          "trace_send_late_span",
					"meta.event_count":                   2,
					"meta.span_count":                    2,
					"meta.span_event_count":              0,
					"meta.span_link_count":               0,
				},
				Metadata: map[string]any{
					"api_host":    "http://api.honeycomb.io",
					"dataset":     "dataset",
					"environment": "",
					"enqueued_at": event.Metadata.(map[string]any)["enqueued_at"],
				},
			},
			event,
		)
	}, 3*time.Second, 2*time.Millisecond)

}

func TestEventsEndpointWithNonLegacyKey(t *testing.T) {
	t.Skip("This is testing deterministic trace sharding, which isn't relevant for Refinery 3.")

	var apps [2]*App
	var addrs [2]string
	var senders [2]*transmission.MockSender
	redisDB := 9
	for i := range apps {
		basePort := 15000 + (i * 2)
		senders[i] = &transmission.MockSender{}
		app, _, stop := newStartedApp(t, senders[i], basePort, redisDB, false)
		app.IncomingRouter.SetEnvironmentCache(time.Second, func(s string) (string, error) { return "test", nil })
		apps[i] = app
		defer stop()

		addrs[i] = "localhost:" + strconv.Itoa(basePort)
	}

	traceID := "4"
	traceData := []byte(fmt.Sprintf(`{"foo":"bar","trace.trace_id":"%s"}`, traceID))
	// Deliver to host 1, it should be passed to host 0 and emitted there.
	zEnc, _ := zstd.NewWriter(nil)
	blob := zEnc.EncodeAll(traceData, nil)
	req, err := http.NewRequest(
		"POST",
		"http://localhost:15002/1/events/dataset",
		bytes.NewReader(blob),
	)
	require.NoError(t, err)
	req.Header.Set("X-Honeycomb-Team", nonLegacyAPIKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "zstd")
	req.Header.Set("X-Honeycomb-Event-Time", now.Format(time.RFC3339Nano))
	req.Header.Set("X-Honeycomb-Samplerate", "10")

	post(t, req)

	require.Eventually(t, func() bool {
		events := senders[1].Events()
		return len(events) == 1
	}, 5*time.Second, 100*time.Millisecond)

	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		events := senders[1].Events()
		require.Equal(collect, 1, len(events))
		event := events[0]
		assert.Equal(
			collect,
			&transmission.Event{
				APIKey:     nonLegacyAPIKey,
				Dataset:    "dataset",
				SampleRate: 10,
				APIHost:    "http://api.honeycomb.io",
				Timestamp:  now,
				Data: map[string]interface{}{
					"trace.trace_id":                     traceID,
					"foo":                                "bar",
					"meta.refinery.original_sample_rate": uint(10),
					"meta.event_count":                   1,
					"meta.span_count":                    1,
					"meta.span_event_count":              0,
					"meta.span_link_count":               0,
					"meta.refinery.reason":               "deterministic/always",
				},
				Metadata: map[string]any{
					"api_host":    "http://api.honeycomb.io",
					"dataset":     "dataset",
					"environment": "test",
					"enqueued_at": event.Metadata.(map[string]any)["enqueued_at"],
				},
			},
			event,
		)
	}, 5*time.Second, 200*time.Microsecond)

	// Repeat, but deliver to host 1, it should not be
	// passed to host 0.

	blob = blob[:0]
	buf := bytes.NewBuffer(blob)
	gz := gzip.NewWriter(buf)
	gz.Write(traceData)
	gz.Close()

	req, err = http.NewRequest(
		"POST",
		"http://localhost:15000/1/events/dataset",
		buf,
	)
	assert.NoError(t, err)
	req.Header.Set("X-Honeycomb-Team", nonLegacyAPIKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set("X-Honeycomb-Event-Time", now.Format(time.RFC3339Nano))
	req.Header.Set("X-Honeycomb-Samplerate", "10")

	post(t, req)
	require.Eventually(t, func() bool {
		events := senders[0].Events()
		return len(events) == 1
	}, 5*time.Second, 100*time.Millisecond)

	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		events := senders[0].Events()
		require.Equal(collect, 1, len(events))
		event := events[0]
		assert.Equal(
			collect,
			&transmission.Event{
				APIKey:     nonLegacyAPIKey,
				Dataset:    "dataset",
				SampleRate: 10,
				APIHost:    "http://api.honeycomb.io",
				Timestamp:  now,
				Data: map[string]interface{}{
					"trace.trace_id":                     traceID,
					"foo":                                "bar",
					"meta.refinery.original_sample_rate": uint(10),
					"meta.event_count":                   2,
					"meta.span_count":                    2,
					"meta.span_event_count":              0,
					"meta.span_link_count":               0,
					"meta.refinery.reason":               "deterministic/always - late arriving span",
					"meta.refinery.send_reason":          "trace_send_late_span",
				},
				Metadata: map[string]any{
					"api_host":    "http://api.honeycomb.io",
					"dataset":     "dataset",
					"environment": "test",
					"enqueued_at": event.Metadata.(map[string]any)["enqueued_at"],
				},
			},
			event,
		)
	}, 5*time.Second, 200*time.Microsecond)

}

var (
	now        = time.Now().UTC()
	nowString  = now.Format(time.RFC3339Nano)
	spanFormat = `{"data":{` +
		`"trace.trace_id":"%d",` +
		`"trace.span_id":"%d",` +
		`"trace.parent_id":"0000000000",` +
		`"key":"value",` +
		`"field0":0,` +
		`"field1":1,` +
		`"field2":2,` +
		`"field3":3,` +
		`"field4":4,` +
		`"field5":5,` +
		`"field6":6,` +
		`"field7":7,` +
		`"field8":8,` +
		`"field9":9,` +
		`"field10":10,` +
		`"long":"this is a test of the emergency broadcast system",` +
		`"foo":"bar"` +
		`},"dataset":"dataset",` +
		`"time":"` + nowString + `",` +
		`"samplerate":2` +
		`}`
	spans [][]byte

	httpClient = &http.Client{Transport: &http.Transport{
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

// Pre-build spans to send, none are root spans
func init() {
	var tid int
	spans = make([][]byte, 100000)
	for i := range spans {
		if i%10 == 0 {
			tid++
		}
		spans[i] = []byte(fmt.Sprintf(spanFormat, tid, i))
	}
}

func BenchmarkTraces(b *testing.B) {
	ctx := context.Background()

	sender := &countingWriterSender{
		WriterSender: transmission.WriterSender{
			W: io.Discard,
		},
	}
	_, _, stop := newStartedApp(b, sender, 11000, 11, false)
	defer stop()

	req, err := http.NewRequest(
		"POST",
		"http://localhost:11000/1/batch/dataset",
		nil,
	)
	assert.NoError(b, err)
	req.Header.Set("X-Honeycomb-Team", legacyAPIKey)
	req.Header.Set("Content-Type", "application/json")

	b.Run("single", func(b *testing.B) {
		sender.resetCount()
		for n := 0; n < b.N; n++ {
			blob := `[` + string(spans[n%len(spans)]) + `]`
			req.Body = io.NopCloser(strings.NewReader(blob))
			post(b, req)
		}
		sender.waitForCount(b, b.N)
	})

	b.Run("batch", func(b *testing.B) {
		sender.resetCount()

		// over-allocate blob for 50 spans
		blob := make([]byte, 0, len(spanFormat)*100)
		for n := 0; n < (b.N/50)+1; n++ {
			blob = append(blob[:0], '[')
			for i := 0; i < 50; i++ {
				blob = append(blob, spans[((n*50)+i)%len(spans)]...)
				blob = append(blob, ',')
			}
			blob[len(blob)-1] = ']'
			req.Body = io.NopCloser(bytes.NewReader(blob))

			post(b, req)
		}
		sender.waitForCount(b, b.N)
	})

	b.Run("multi", func(b *testing.B) {
		sender.resetCount()
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				req := req.Clone(ctx)
				blob := make([]byte, 0, len(spanFormat)*100)
				for n := 0; n < (b.N/500)+1; n++ {
					blob = append(blob[:0], '[')
					for i := 0; i < 50; i++ {
						blob = append(blob, spans[((n*50)+i)%len(spans)]...)
						blob = append(blob, ',')
					}
					blob[len(blob)-1] = ']'
					req.Body = io.NopCloser(bytes.NewReader(blob))

					resp, err := httpClient.Do(req)
					assert.NoError(b, err)
					if resp != nil {
						assert.Equal(b, http.StatusOK, resp.StatusCode)
						io.Copy(io.Discard, resp.Body)
						resp.Body.Close()
					}
				}
			}()
		}
		wg.Wait()
		sender.waitForCount(b, b.N)
	})
}

func BenchmarkDistributedTraces(b *testing.B) {
	sender := &countingWriterSender{
		WriterSender: transmission.WriterSender{
			W: io.Discard,
		},
	}

	var apps [5]*App
	var addrs [5]string
	for i := range apps {
		var stop func()
		basePort := 12000 + (i * 2)
		apps[i], _, stop = newStartedApp(b, sender, basePort, 11, false)
		defer stop()

		addrs[i] = "localhost:" + strconv.Itoa(basePort)
	}

	req, err := http.NewRequest(
		"POST",
		"http://localhost:12000/1/batch/dataset",
		nil,
	)
	assert.NoError(b, err)
	req.Header.Set("X-Honeycomb-Team", legacyAPIKey)
	req.Header.Set("Content-Type", "application/json")

	b.Run("single", func(b *testing.B) {
		sender.resetCount()
		for n := 0; n < b.N; n++ {
			blob := `[` + string(spans[n%len(spans)]) + `]`
			req.Body = io.NopCloser(strings.NewReader(blob))
			req.URL.Host = addrs[n%len(addrs)]
			post(b, req)
		}
		sender.waitForCount(b, b.N)
	})

	b.Run("batch", func(b *testing.B) {
		sender.resetCount()

		// over-allocate blob for 50 spans
		blob := make([]byte, 0, len(spanFormat)*100)
		for n := 0; n < (b.N/50)+1; n++ {
			blob = append(blob[:0], '[')
			for i := 0; i < 50; i++ {
				blob = append(blob, spans[((n*50)+i)%len(spans)]...)
				blob = append(blob, ',')
			}
			blob[len(blob)-1] = ']'
			req.Body = io.NopCloser(bytes.NewReader(blob))
			req.URL.Host = addrs[n%len(addrs)]

			post(b, req)
		}
		sender.waitForCount(b, b.N)
	})
}

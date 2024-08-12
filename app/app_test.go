package app

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
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
	"go.opentelemetry.io/otel/trace/noop"
	"gopkg.in/alexcesaro/statsd.v2"

	"github.com/honeycombio/libhoney-go"
	"github.com/honeycombio/libhoney-go/transmission"
	"github.com/honeycombio/refinery/collect"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/health"
	"github.com/honeycombio/refinery/internal/peer"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/sample"
	"github.com/honeycombio/refinery/sharder"
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

func newStartedApp(
	t testing.TB,
	libhoneyT transmission.Sender,
	basePort int,
	peers peer.Peers,
	enableHostMetadata bool,
) (*App, inject.Graph) {
	c := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:   config.Duration(2 * time.Millisecond),
			SendDelay:    config.Duration(1 * time.Millisecond),
			TraceTimeout: config.Duration(10 * time.Millisecond),
			MaxBatchSize: 500,
		},
		GetSamplerTypeVal:        &config.DeterministicSamplerConfig{SampleRate: 1},
		PeerManagementType:       "file",
		GetUpstreamBufferSizeVal: 10000,
		GetPeerBufferSizeVal:     10000,
		GetListenAddrVal:         "127.0.0.1:" + strconv.Itoa(basePort),
		GetPeerListenAddrVal:     "127.0.0.1:" + strconv.Itoa(basePort+1),
		GetHoneycombAPIVal:       "http://api.honeycomb.io",
		GetCollectionConfigVal:   config.CollectionConfig{CacheCapacity: 10000, ShutdownDelay: config.Duration(1 * time.Second)},
		AddHostMetadataToTrace:   enableHostMetadata,
		TraceIdFieldNames:        []string{"trace.trace_id"},
		ParentIdFieldNames:       []string{"trace.parent_id"},
		SampleCache:              config.SampleCacheConfig{KeptSize: 10000, DroppedSize: 100000, SizeCheckInterval: config.Duration(10 * time.Second)},
		GetAccessKeyConfigVal: config.AccessKeyConfig{
			ReceiveKeys:          []string{legacyAPIKey, nonLegacyAPIKey},
			AcceptOnlyListedKeys: true,
		},
	}

	var err error
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

	upstreamClient, err := libhoney.NewClient(libhoney.ClientConfig{
		Transmission: libhoneyT,
	})
	assert.NoError(t, err)

	sdPeer, _ := statsd.New(statsd.Prefix("refinery.peer"))
	peerClient, err := libhoney.NewClient(libhoney.ClientConfig{
		Transmission: &transmission.Honeycomb{
			MaxBatchSize:         c.GetTracesConfigVal.MaxBatchSize,
			BatchTimeout:         libhoney.DefaultBatchTimeout,
			MaxConcurrentBatches: libhoney.DefaultMaxConcurrentBatches,
			PendingWorkCapacity:  uint(c.GetPeerBufferSize()),
			Transport: &http.Transport{
				Proxy: http.ProxyFromEnvironment,
				Dial: (&net.Dialer{
					Timeout: 3 * time.Second,
				}).Dial,
			},
			BlockOnSend:            true,
			DisableGzipCompression: true,
			EnableMsgpackEncoding:  true,
			Metrics:                sdPeer,
		},
	})
	assert.NoError(t, err)

	var g inject.Graph
	err = g.Provide(
		&inject.Object{Value: c},
		&inject.Object{Value: peers},
		&inject.Object{Value: lgr},
		&inject.Object{Value: http.DefaultTransport, Name: "upstreamTransport"},
		&inject.Object{Value: transmit.NewDefaultTransmission(upstreamClient, metricsr, "upstream"), Name: "upstreamTransmission"},
		&inject.Object{Value: transmit.NewDefaultTransmission(peerClient, metricsr, "peer"), Name: "peerTransmission"},
		&inject.Object{Value: shrdr},
		&inject.Object{Value: noop.NewTracerProvider().Tracer("test"), Name: "tracer"},
		&inject.Object{Value: collector},
		&inject.Object{Value: metricsr, Name: "metrics"},
		&inject.Object{Value: metricsr, Name: "genericMetrics"},
		&inject.Object{Value: metricsr, Name: "upstreamMetrics"},
		&inject.Object{Value: metricsr, Name: "peerMetrics"},
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
	time.Sleep(10 * time.Millisecond)
	return &a, g
}

func post(t testing.TB, req *http.Request) {
	resp, err := httpClient.Do(req)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}

func TestAppIntegration(t *testing.T) {
	t.Parallel()
	port := 10500

	sender := &transmission.MockSender{}
	app, graph := newStartedApp(t, sender, port, nil, false)

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

	time.Sleep(5 * app.Config.GetTracesConfig().GetSendTickerValue())

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		events := sender.Events()
		require.Len(collect, events, 1)
		assert.Equal(collect, "dataset", events[0].Dataset)
		assert.Equal(collect, "bar", events[0].Data["foo"])
		assert.Equal(collect, "1", events[0].Data["trace.trace_id"])
		assert.Equal(collect, uint(1), events[0].Data["meta.refinery.original_sample_rate"])
	}, 2*time.Second, 10*time.Millisecond)

	err = startstop.Stop(graph.Objects(), nil)
	assert.NoError(t, err)
}

func TestAppIntegrationWithNonLegacyKey(t *testing.T) {
	// Parallel integration tests need different ports!
	t.Parallel()
	port := 10600

	sender := &transmission.MockSender{}
	a, graph := newStartedApp(t, sender, port, nil, false)
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
	var events []*transmission.Event
	require.Eventually(t, func() bool {
		events = sender.Events()
		return len(events) == 1
	}, 2*time.Second, 2*time.Millisecond)

	assert.Equal(t, "dataset", events[0].Dataset)
	assert.Equal(t, "bar", events[0].Data["foo"])
	assert.Equal(t, "1", events[0].Data["trace.trace_id"])
	assert.Equal(t, uint(1), events[0].Data["meta.refinery.original_sample_rate"])

	err = startstop.Stop(graph.Objects(), nil)
	assert.NoError(t, err)
}

func TestAppIntegrationWithUnauthorizedKey(t *testing.T) {
	// Parallel integration tests need different ports!
	t.Parallel()
	port := 10700

	sender := &transmission.MockSender{}
	a, graph := newStartedApp(t, sender, port, nil, false)
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

func TestPeerRouting(t *testing.T) {
	// Parallel integration tests need different ports!
	t.Parallel()

	peerList := []string{"http://localhost:11001", "http://localhost:11003"}

	var apps [2]*App
	var senders [2]*transmission.MockSender
	for i := range apps {
		var graph inject.Graph
		basePort := 11000 + (i * 2)
		senders[i] = &transmission.MockSender{}
		peers := &peer.MockPeers{
			Peers: peerList,
			ID:    peerList[i],
		}
		apps[i], graph = newStartedApp(t, senders[i], basePort, peers, false)
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

	// this span index was chosen because it hashes to the appropriate shard for this
	// test. You can't change it and expect the test to pass.
	blob := `[` + string(spans[10]) + `]`
	req.Body = io.NopCloser(strings.NewReader(blob))
	post(t, req)
	assert.Eventually(t, func() bool {
		return len(senders[0].Events()) == 1
	}, 2*time.Second, 2*time.Millisecond)

	expectedEvent := &transmission.Event{
		APIKey:     legacyAPIKey,
		Dataset:    "dataset",
		SampleRate: 2,
		APIHost:    "http://api.honeycomb.io",
		Timestamp:  now,
		Data: map[string]interface{}{
			"trace.trace_id":                     "2",
			"trace.span_id":                      "10",
			"trace.parent_id":                    "0000000000",
			"key":                                "value",
			"field0":                             float64(0),
			"field1":                             float64(1),
			"field2":                             float64(2),
			"field3":                             float64(3),
			"field4":                             float64(4),
			"field5":                             float64(5),
			"field6":                             float64(6),
			"field7":                             float64(7),
			"field8":                             float64(8),
			"field9":                             float64(9),
			"field10":                            float64(10),
			"long":                               "this is a test of the emergency broadcast system",
			"meta.refinery.original_sample_rate": uint(2),
			"foo":                                "bar",
		},
		Metadata: map[string]any{
			"api_host":    "http://api.honeycomb.io",
			"dataset":     "dataset",
			"environment": "",
			"enqueued_at": senders[0].Events()[0].Metadata.(map[string]any)["enqueued_at"],
		},
	}
	assert.Equal(t, expectedEvent, senders[0].Events()[0])

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
	assert.Eventually(t, func() bool {
		return len(senders[0].Events()) == 1
	}, 2*time.Second, 2*time.Millisecond)
	assert.Equal(t, expectedEvent, senders[0].Events()[0])
}

func TestHostMetadataSpanAdditions(t *testing.T) {
	t.Parallel()
	port := 14000

	sender := &transmission.MockSender{}
	app, graph := newStartedApp(t, sender, port, nil, true)

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

	var events []*transmission.Event
	require.Eventually(t, func() bool {
		events = sender.Events()
		return len(events) == 1
	}, 2*time.Second, 10*time.Millisecond)

	assert.Equal(t, "dataset", events[0].Dataset)
	assert.Equal(t, "bar", events[0].Data["foo"])
	assert.Equal(t, "1", events[0].Data["trace.trace_id"])
	assert.Equal(t, uint(1), events[0].Data["meta.refinery.original_sample_rate"])
	hostname, _ := os.Hostname()
	assert.Equal(t, hostname, events[0].Data["meta.refinery.local_hostname"])

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
	var senders [2]*transmission.MockSender
	for i := range apps {
		var graph inject.Graph
		basePort := 13000 + (i * 2)
		senders[i] = &transmission.MockSender{}
		peers := &peer.MockPeers{
			Peers: peerList,
			ID:    peerList[i],
		}

		apps[i], graph = newStartedApp(t, senders[i], basePort, peers, false)
		defer startstop.Stop(graph.Objects(), nil)
	}

	// Deliver to host 1, it should be passed to host 0 and emitted there.
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
		return len(senders[0].Events()) == 1
	}, 2*time.Second, 2*time.Millisecond)

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
			},
			Metadata: map[string]any{
				"api_host":    "http://api.honeycomb.io",
				"dataset":     "dataset",
				"environment": "",
				"enqueued_at": senders[0].Events()[0].Metadata.(map[string]any)["enqueued_at"],
			},
		},
		senders[0].Events()[0],
	)

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
		return len(senders[0].Events()) == 1
	}, 2*time.Second, 2*time.Millisecond)

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
			},
			Metadata: map[string]any{
				"api_host":    "http://api.honeycomb.io",
				"dataset":     "dataset",
				"environment": "",
				"enqueued_at": senders[0].Events()[0].Metadata.(map[string]any)["enqueued_at"],
			},
		},
		senders[0].Events()[0],
	)
}

func TestEventsEndpointWithNonLegacyKey(t *testing.T) {
	t.Parallel()

	peerList := []string{
		"http://localhost:15001",
		"http://localhost:15003",
	}

	var apps [2]*App
	var senders [2]*transmission.MockSender
	for i := range apps {
		basePort := 15000 + (i * 2)
		senders[i] = &transmission.MockSender{}
		peers := &peer.MockPeers{
			Peers: peerList,
			ID:    peerList[i],
		}

		app, graph := newStartedApp(t, senders[i], basePort, peers, false)
		app.IncomingRouter.SetEnvironmentCache(time.Second, func(s string) (string, error) { return "test", nil })
		app.PeerRouter.SetEnvironmentCache(time.Second, func(s string) (string, error) { return "test", nil })
		apps[i] = app
		defer startstop.Stop(graph.Objects(), nil)
	}

	// this traceID was chosen because it hashes to the appropriate shard for this
	// test. You can't change it or the number of peers and still expect the test to pass.
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
	assert.NoError(t, err)
	req.Header.Set("X-Honeycomb-Team", nonLegacyAPIKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "zstd")
	req.Header.Set("X-Honeycomb-Event-Time", now.Format(time.RFC3339Nano))
	req.Header.Set("X-Honeycomb-Samplerate", "10")

	post(t, req)
	assert.Eventually(t, func() bool {
		return len(senders[0].Events()) == 1
	}, 2*time.Second, 2*time.Millisecond)

	assert.Equal(
		t,
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
			},
			Metadata: map[string]any{
				"api_host":    "http://api.honeycomb.io",
				"dataset":     "dataset",
				"environment": "test",
				"enqueued_at": senders[0].Events()[0].Metadata.(map[string]any)["enqueued_at"],
			},
		},
		senders[0].Events()[0],
	)

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
		return len(senders[0].Events()) == 1
	}, 2*time.Second, 2*time.Millisecond)

	assert.Equal(
		t,
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
			},
			Metadata: map[string]any{
				"api_host":    "http://api.honeycomb.io",
				"dataset":     "dataset",
				"environment": "test",
				"enqueued_at": senders[0].Events()[0].Metadata.(map[string]any)["enqueued_at"],
			},
		},
		senders[0].Events()[0],
	)
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
	_, graph := newStartedApp(b, sender, 11000, nil, false)

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

	err = startstop.Stop(graph.Objects(), nil)
	assert.NoError(b, err)
}

func BenchmarkDistributedTraces(b *testing.B) {
	sender := &countingWriterSender{
		WriterSender: transmission.WriterSender{
			W: io.Discard,
		},
	}

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

		apps[i], graph = newStartedApp(b, sender, basePort, peers, false)
		defer startstop.Stop(graph.Objects(), nil)

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

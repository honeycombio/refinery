package app

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/facebookgo/inject"
	"github.com/facebookgo/startstop"
	"github.com/honeycombio/libhoney-go"
	"github.com/honeycombio/libhoney-go/transmission"
	"github.com/stretchr/testify/assert"

	"github.com/honeycombio/samproxy/collect"
	"github.com/honeycombio/samproxy/config"
	"github.com/honeycombio/samproxy/internal/peer"
	"github.com/honeycombio/samproxy/logger"
	"github.com/honeycombio/samproxy/metrics"
	"github.com/honeycombio/samproxy/sample"
	"github.com/honeycombio/samproxy/sharder"
	"github.com/honeycombio/samproxy/transmit"
)

func newStartedApp(t *testing.T, libhoneyOut io.Writer) (*App, inject.Graph) {
	c := &config.MockConfig{
		GetSendDelayVal:          0,
		GetTraceTimeoutVal:       60 * time.Second,
		GetDefaultSamplerTypeVal: "DeterministicSampler",
		SendTickerVal:            2 * time.Millisecond,
		PeerManagementType:       "file",
		GetOtherConfigVal:        `{"CacheCapacity":10000}`,
		GetUpstreamBufferSizeVal: 10000,
		GetPeerBufferSizeVal:     10000,
		GetListenAddrVal:         "127.0.0.1:11000",
		GetPeerListenAddrVal:     "127.0.0.1:11001",
		GetAPIKeysVal:            []string{"KEY"},
	}

	peers, err := peer.NewPeers(c)
	assert.NoError(t, err)

	a := App{}

	lgr := &logger.LogrusLogger{
		Config: c,
	}
	lgr.SetLevel("info")
	lgr.Start()

	// TODO use real metrics
	metricsr := &metrics.MockMetrics{}
	metricsr.Start()

	collector := &collect.InMemCollector{}
	shrdr := &sharder.SingleServerSharder{}
	samplerFactory := &sample.SamplerFactory{}

	upstreamClient, err := libhoney.NewClient(libhoney.ClientConfig{
		Transmission: &transmission.WriterSender{
			W: libhoneyOut,
		},
	})
	assert.NoError(t, err)

	peerClient, err := libhoney.NewClient(libhoney.ClientConfig{
		Transmission: &transmission.DiscardSender{},
	})
	assert.NoError(t, err)

	var g inject.Graph
	err = g.Provide(
		&inject.Object{Value: c},
		&inject.Object{Value: peers},
		&inject.Object{Value: lgr},
		&inject.Object{Value: http.DefaultTransport, Name: "upstreamTransport"},
		&inject.Object{Value: &transmit.DefaultTransmission{LibhClient: upstreamClient, Name: "upstream_"}, Name: "upstreamTransmission"},
		&inject.Object{Value: &transmit.DefaultTransmission{LibhClient: peerClient, Name: "peer_"}, Name: "peerTransmission"},
		&inject.Object{Value: shrdr},
		&inject.Object{Value: collector},
		&inject.Object{Value: metricsr},
		&inject.Object{Value: "test", Name: "version"},
		&inject.Object{Value: samplerFactory},
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

func TestAppIntegration(t *testing.T) {
	var out bytes.Buffer
	_, graph := newStartedApp(t, &out)

	// Send a root span, it should be sent in short order.
	req := httptest.NewRequest(
		"POST",
		"http://localhost:11000/1/batch/dataset",
		strings.NewReader(`[{"data":{"trace.trace_id":"1","foo":"bar"}}]`),
	)
	req.Header.Set("X-Honeycomb-Team", "KEY")
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultTransport.RoundTrip(req)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	// Wait for span to be sent.
	deadline := time.After(time.Second)
	for {
		if out.Len() > 62 {
			break
		}
		select {
		case <-deadline:
			t.Error("timed out waiting for output")
			return
		case <-time.After(time.Millisecond):
		}
	}
	assert.Equal(t, `{"data":{"foo":"bar","trace.trace_id":"1"},"dataset":"dataset"}`+"\n", out.String())

	err = startstop.Stop(graph.Objects(), nil)
	assert.NoError(t, err)
}

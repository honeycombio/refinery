package app

import (
	"io/ioutil"
	"net/http"
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

func newStartedApp(t *testing.T) (*App, inject.Graph) {
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
	}

	peers, err := peer.NewPeers(c)
	assert.Equal(t, nil, err)

	a := App{}

	lgr := &logger.LogrusLogger{
		Config: c,
	}
	lgr.SetLevel("debug")
	lgr.Start()

	// TODO use real metrics
	metricsr := &metrics.MockMetrics{}
	metricsr.Start()

	collector := &collect.InMemCollector{}
	shrdr := &sharder.SingleServerSharder{}
	samplerFactory := &sample.SamplerFactory{}

	upstreamClient, err := libhoney.NewClient(libhoney.ClientConfig{
		Transmission: &transmission.WriterSender{
			W: ioutil.Discard,
		},
	})
	assert.Equal(t, nil, err)

	peerClient, err := libhoney.NewClient(libhoney.ClientConfig{
		Transmission: &transmission.DiscardSender{},
	})
	assert.Equal(t, nil, err)

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
	assert.Equal(t, nil, err)

	err = g.Populate()
	assert.Equal(t, nil, err)

	err = startstop.Start(g.Objects(), nil)
	assert.Equal(t, nil, err)

	// Racy: wait just a moment for ListenAndServe to start up.
	time.Sleep(10 * time.Millisecond)
	return &a, g
}

func TestAppIntegration(t *testing.T) {
	_, graph := newStartedApp(t)

	err := startstop.Stop(graph.Objects(), nil)
	assert.Equal(t, nil, err)
}

package transmit

import (
	"net/http"

	libhoney "github.com/honeycombio/libhoney-go"

	"github.com/honeycombio/samproxy/config"
	"github.com/honeycombio/samproxy/logger"
	"github.com/honeycombio/samproxy/types"
)

type Transmission interface {
	// Enqueue accepts a single event and schedules it for transmission to Honeycomb
	EnqueueEvent(ev *types.Event)
	EnqueueSpan(ev *types.Span)
	// Flush flushes the in-flight queue of all events and spans
	Flush()
}

type DefaultTransmission struct {
	Config     config.Config `inject:""`
	Logger     logger.Logger `inject:""`
	HTTPClient *http.Client  `inject:"upstreamClient"`
	Version    string        `inject:"version"`

	builder *libhoney.Builder
}

func (d *DefaultTransmission) Start() error {
	upstreamAPI, err := d.Config.GetHoneycombAPI()
	if err != nil {
		return err
	}
	libhConfig := libhoney.Config{
		APIHost:   upstreamAPI,
		Transport: d.HTTPClient.Transport,
		// Logger:    &libhoney.DefaultLogger{},
	}
	libhoney.Init(libhConfig)
	libhoney.UserAgentAddition = "samproxy/" + d.Version
	d.builder = libhoney.NewBuilder()

	// listen for config reloads
	d.Config.RegisterReloadCallback(d.reloadTransmissionBuilder)
	return nil
}

func (d *DefaultTransmission) reloadTransmissionBuilder() {
	upstreamAPI, err := d.Config.GetHoneycombAPI()
	if err != nil {
		// log and skip reload
		d.Logger.Errorf("Failed to reload Honeycomb API when reloading configs:", err)
	}
	builder := libhoney.NewBuilder()
	builder.APIHost = upstreamAPI
}

func (d *DefaultTransmission) EnqueueEvent(ev *types.Event) {
	d.Logger.Debugf("sending event to %s: %+v", ev.APIHost, ev.Data)
	libhEv := d.builder.NewEvent()
	libhEv.APIHost = ev.APIHost
	libhEv.WriteKey = ev.APIKey
	libhEv.Dataset = ev.Dataset
	libhEv.SampleRate = ev.SampleRate
	libhEv.Timestamp = ev.Timestamp

	for k, v := range ev.Data {
		libhEv.AddField(k, v)
	}

	libhEv.SendPresampled()
}

func (d *DefaultTransmission) EnqueueSpan(sp *types.Span) {
	// we don't need the trace ID anymore, but it's convenient to accept spans.
	d.EnqueueEvent(&sp.Event)
}

func (d *DefaultTransmission) Flush() {
	libhoney.Flush()
}

func (d *DefaultTransmission) Stop() error {
	// purge the queue of any in-flight events
	libhoney.Flush()
	return nil
}

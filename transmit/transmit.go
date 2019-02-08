package transmit

import (
	"context"
	"net/http"

	libhoney "github.com/honeycombio/libhoney-go"

	"github.com/honeycombio/samproxy/config"
	"github.com/honeycombio/samproxy/logger"
	"github.com/honeycombio/samproxy/metrics"
	"github.com/honeycombio/samproxy/types"
)

type Transmission interface {
	// Enqueue accepts a single event and schedules it for transmission to Honeycomb
	EnqueueEvent(ev *types.Event)
	EnqueueSpan(ev *types.Span)
	// Flush flushes the in-flight queue of all events and spans
	Flush()
}

const (
	counterEnqueueErrors      = "enqueue_errors"
	counterResponse20x        = "response_20x"
	counterResponseErrorsAPI  = "response_errors_api"
	counterResponseErrorsPeer = "response_errors_peer"
)

type DefaultTransmission struct {
	Config     config.Config   `inject:""`
	Logger     logger.Logger   `inject:""`
	HTTPClient *http.Client    `inject:"upstreamClient"`
	Metrics    metrics.Metrics `inject:""`
	Version    string          `inject:"version"`

	builder          *libhoney.Builder
	responseCanceler context.CancelFunc
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
		BlockOnResponse: true,
		BlockOnSend:     true,
	}
	libhoney.Init(libhConfig)
	libhoney.UserAgentAddition = "samproxy/" + d.Version
	d.builder = libhoney.NewBuilder()

	d.Metrics.Register(counterEnqueueErrors, "counter")
	d.Metrics.Register(counterResponse20x, "counter")
	d.Metrics.Register(counterResponseErrorsAPI, "counter")
	d.Metrics.Register(counterResponseErrorsPeer, "counter")

	processCtx, canceler := context.WithCancel(context.Background())
	d.responseCanceler = canceler
	go d.processResponses(processCtx)

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
	d.Logger.WithFields(map[string]interface{}{
		"request_id": ev.Context.Value(types.RequestIDContextKey{}),
		"api_host":   ev.APIHost,
		"dataset":    ev.Dataset,
	}).Debugf("transmit sending event")
	libhEv := d.builder.NewEvent()
	libhEv.APIHost = ev.APIHost
	libhEv.WriteKey = ev.APIKey
	libhEv.Dataset = ev.Dataset
	libhEv.SampleRate = ev.SampleRate
	libhEv.Timestamp = ev.Timestamp
	libhEv.Metadata = map[string]string{
		"api_host": ev.APIHost,
		"dataset":  ev.Dataset,
	}

	for k, v := range ev.Data {
		libhEv.AddField(k, v)
	}

	err := libhEv.SendPresampled()
	if err != nil {
		d.Metrics.IncrementCounter(counterEnqueueErrors)
		d.Logger.WithFields(map[string]interface{}{
			"error":      err.Error(),
			"request_id": ev.Context.Value(types.RequestIDContextKey{}),
			"dataset":    ev.Dataset,
			"api_host":   ev.APIHost,
		}).Errorf("failed to enqueue event")
	}
}

func (d *DefaultTransmission) EnqueueSpan(sp *types.Span) {
	// we don't need the trace ID anymore, but it's convenient to accept spans.
	d.EnqueueEvent(&sp.Event)
}

func (d *DefaultTransmission) Flush() {
	libhoney.Flush()
}

func (d *DefaultTransmission) Stop() error {
	// signal processResponses to stop
	d.responseCanceler()
	// purge the queue of any in-flight events
	libhoney.Flush()
	return nil
}

func (d *DefaultTransmission) processResponses(ctx context.Context) {
	honeycombAPI, _ := d.Config.GetHoneycombAPI()
	responses := libhoney.Responses()
	for {
		select {
		case r := <-responses:
			if r.StatusCode != 200 && r.StatusCode != 202 {
				var apiHost, dataset string
				if metadata, ok := r.Metadata.(map[string]string); ok {
					apiHost = metadata["api_host"]
					dataset = metadata["dataset"]
				}
				d.Logger.WithFields(map[string]interface{}{
					"status_code": r.StatusCode,
					"error":       r.Err.Error(),
					"api_host":    apiHost,
					"dataset":     dataset,
				}).Errorf("non-20x response when sending event")
				if honeycombAPI == apiHost {
					// if the API host matches the configured honeycomb API,
					// count it as an API error
					d.Metrics.IncrementCounter(counterResponseErrorsAPI)
				} else {
					// otherwise, it's probably a peer error
					d.Metrics.IncrementCounter(counterResponseErrorsPeer)
				}
			} else {
				d.Metrics.IncrementCounter(counterResponse20x)
			}
		case <-ctx.Done():
			return
		}
	}
}

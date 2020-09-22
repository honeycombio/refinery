package transmit

import (
	"context"

	libhoney "github.com/honeycombio/libhoney-go"
	"github.com/honeycombio/libhoney-go/transmission"

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
	Metrics    metrics.Metrics `inject:""`
	Version    string          `inject:"version"`
	LibhClient *libhoney.Client

	// Type is peer or upstream, and used only for naming metrics
	Name string

	builder          *libhoney.Builder
	responseCanceler context.CancelFunc
}

func (d *DefaultTransmission) Start() error {
	d.Logger.Debug().Logf("Starting DefaultTransmission: %s type", d.Name)
	defer func() { d.Logger.Debug().Logf("Finished starting DefaultTransmission: %s type", d.Name) }()

	// upstreamAPI doesn't get set when the client is initialized, because
	// it can be reloaded from the config file while live
	upstreamAPI, err := d.Config.GetHoneycombAPI()
	if err != nil {
		return err
	}
	libhoney.UserAgentAddition = "samproxy/" + d.Version
	d.builder = d.LibhClient.NewBuilder()
	d.builder.APIHost = upstreamAPI

	d.Metrics.Register(d.Name+counterEnqueueErrors, "counter")
	d.Metrics.Register(d.Name+counterResponse20x, "counter")
	d.Metrics.Register(d.Name+counterResponseErrorsAPI, "counter")
	d.Metrics.Register(d.Name+counterResponseErrorsPeer, "counter")

	processCtx, canceler := context.WithCancel(context.Background())
	d.responseCanceler = canceler
	go d.processResponses(processCtx, d.LibhClient.TxResponses())

	// listen for config reloads
	d.Config.RegisterReloadCallback(d.reloadTransmissionBuilder)
	return nil
}

func (d *DefaultTransmission) reloadTransmissionBuilder() {
	d.Logger.Debug().Logf("reloading transmission config")
	upstreamAPI, err := d.Config.GetHoneycombAPI()
	if err != nil {
		// log and skip reload
		d.Logger.Error().Logf("Failed to reload Honeycomb API when reloading configs:", err)
	}
	builder := d.LibhClient.NewBuilder()
	builder.APIHost = upstreamAPI
}

func (d *DefaultTransmission) EnqueueEvent(ev *types.Event) {
	d.Logger.Debug().
		WithField("request_id", ev.Context.Value(types.RequestIDContextKey{})).
		WithString("api_host", ev.APIHost).
		WithString("dataset", ev.Dataset).
		WithString("type", ev.Type.String()).
		Logf("transmit sending event")
	libhEv := d.builder.NewEvent()
	libhEv.APIHost = ev.APIHost
	libhEv.WriteKey = ev.APIKey
	libhEv.Dataset = ev.Dataset
	libhEv.SampleRate = ev.SampleRate
	libhEv.Timestamp = ev.Timestamp
	libhEv.Metadata = map[string]string{
		"type":     ev.Type.String(),
		"api_host": ev.APIHost,
		"dataset":  ev.Dataset,
	}

	for k, v := range ev.Data {
		libhEv.AddField(k, v)
	}

	err := libhEv.SendPresampled()
	if err != nil {
		d.Metrics.IncrementCounter(d.Name + counterEnqueueErrors)
		d.Logger.Error().
			WithString("error", err.Error()).
			WithField("request_id", ev.Context.Value(types.RequestIDContextKey{})).
			WithString("dataset", ev.Dataset).
			WithString("api_host", ev.APIHost).
			WithString("type", ev.Type.String()).
			Logf("failed to enqueue event")
	}
}

func (d *DefaultTransmission) EnqueueSpan(sp *types.Span) {
	// we don't need the trace ID anymore, but it's convenient to accept spans.
	d.EnqueueEvent(&sp.Event)
}

func (d *DefaultTransmission) Flush() {
	d.LibhClient.Flush()
}

func (d *DefaultTransmission) Stop() error {
	// signal processResponses to stop
	if d.responseCanceler != nil {
		d.responseCanceler()
	}
	// purge the queue of any in-flight events
	d.LibhClient.Flush()
	return nil
}

func (d *DefaultTransmission) processResponses(
	ctx context.Context,
	responses chan transmission.Response,
) {
	honeycombAPI, _ := d.Config.GetHoneycombAPI()
	for {
		select {
		case r := <-responses:
			if r.Err != nil || r.StatusCode > 202 {
				var apiHost, dataset, evType, target string
				if metadata, ok := r.Metadata.(map[string]string); ok {
					apiHost = metadata["api_host"]
					dataset = metadata["dataset"]
					evType = metadata["type"]
					target = metadata["target"]
				}
				log := d.Logger.Error().WithFields(map[string]interface{}{
					"status_code": r.StatusCode,
					"api_host":    apiHost,
					"dataset":     dataset,
					"event_type":  evType,
					"target":      target,
				})
				if r.Err != nil {
					log = log.WithField("error", r.Err.Error())
				}
				log.Logf("non-20x response when sending event")
				if honeycombAPI == apiHost {
					// if the API host matches the configured honeycomb API,
					// count it as an API error
					d.Metrics.IncrementCounter(d.Name + counterResponseErrorsAPI)
				} else {
					// otherwise, it's probably a peer error
					d.Metrics.IncrementCounter(d.Name + counterResponseErrorsPeer)
				}
			} else {
				d.Metrics.IncrementCounter(d.Name + counterResponse20x)
			}
		case <-ctx.Done():
			return
		}
	}
}

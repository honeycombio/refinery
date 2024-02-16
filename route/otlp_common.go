package route

import (
	"context"
	huskyotlp "github.com/honeycombio/husky/otlp"
	"github.com/honeycombio/refinery/types"
)

func (r *Router) processOTLPRequest(
	ctx context.Context,
	batches []huskyotlp.Batch,
	apiKey string) error {

	var requestID types.RequestIDContextKey
	apiHost, err := r.Config.GetHoneycombAPI()
	if err != nil {
		r.Logger.Error().Logf("Unable to retrieve APIHost from config while processing OTLP batch")
		return err
	}

	// get environment name - will be empty for legacy keys
	environment, err := r.getEnvironmentName(apiKey)
	if err != nil {
		return nil
	}

	for _, batch := range batches {
		for _, ev := range batch.Events {
			event := &types.Event{
				Context:     ctx,
				APIHost:     apiHost,
				APIKey:      apiKey,
				Dataset:     batch.Dataset,
				Environment: environment,
				SampleRate:  uint(ev.SampleRate),
				Timestamp:   ev.Timestamp,
				Data:        ev.Attributes,
			}
			if err = r.processEvent(event, requestID); err != nil {
				r.Logger.Error().Logf("Error processing event: " + err.Error())
			}
		}
	}

	return nil
}

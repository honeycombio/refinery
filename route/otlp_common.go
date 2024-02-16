package route

import (
	"context"
	huskyotlp "github.com/honeycombio/husky/otlp"
	"github.com/honeycombio/refinery/types"
)

func processOTLPRequest(
	ctx context.Context,
	router *Router,
	batches []huskyotlp.Batch,
	apiKey string) error {

	var requestID types.RequestIDContextKey
	apiHost, err := router.Config.GetHoneycombAPI()
	if err != nil {
		router.Logger.Error().Logf("Unable to retrieve APIHost from config while processing OTLP batch")
		return err
	}

	// get environment name - will be empty for legacy keys
	environment, err := router.getEnvironmentName(apiKey)
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
			if err = router.processEvent(event, requestID); err != nil {
				router.Logger.Error().Logf("Error processing event: " + err.Error())
			}
		}
	}

	return nil
}

package route

import (
	"context"
	"errors"
	"net/http"

	huskyotlp "github.com/honeycombio/husky/otlp"
	"github.com/honeycombio/refinery/types"

	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

func (router *Router) postOTLP(w http.ResponseWriter, req *http.Request) {
	ri := huskyotlp.GetRequestInfoFromHttpHeaders(req.Header)
	if err := ri.ValidateTracesHeaders(); err != nil {
		if errors.Is(err, huskyotlp.ErrInvalidContentType) {
			router.handlerReturnWithError(w, ErrInvalidContentType, err)
		} else {
			router.handlerReturnWithError(w, ErrAuthNeeded, err)
		}
		return
	}

	result, err := huskyotlp.TranslateTraceRequestFromReader(req.Body, ri)
	if err != nil {
		router.handlerReturnWithError(w, ErrUpstreamFailed, err)
		return
	}

	if err := processTraceRequest(req.Context(), router, result.Batches, ri.ApiKey); err != nil {
		router.handlerReturnWithError(w, ErrUpstreamFailed, err)
	}
}

func (router *Router) Export(ctx context.Context, req *collectortrace.ExportTraceServiceRequest) (*collectortrace.ExportTraceServiceResponse, error) {
	ri := huskyotlp.GetRequestInfoFromGrpcMetadata(ctx)
	if err := ri.ValidateTracesHeaders(); err != nil {
		return nil, huskyotlp.AsGRPCError(err)
	}

	result, err := huskyotlp.TranslateTraceRequest(req, ri)
	if err != nil {
		return nil, huskyotlp.AsGRPCError(err)
	}

	if err := processTraceRequest(ctx, router, result.Batches, ri.ApiKey); err != nil {
		return nil, huskyotlp.AsGRPCError(err)
	}

	return &collectortrace.ExportTraceServiceResponse{}, nil
}

func processTraceRequest(
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

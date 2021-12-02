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
	if err := ri.ValidateHeaders(); err != nil {
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

	if err := processTraceRequest(req.Context(), router, result.Events, ri.ApiKey, ri.Dataset); err != nil {
		router.handlerReturnWithError(w, ErrUpstreamFailed, err)
	}
}

func (router *Router) Export(ctx context.Context, req *collectortrace.ExportTraceServiceRequest) (*collectortrace.ExportTraceServiceResponse, error) {
	ri := huskyotlp.GetRequestInfoFromGrpcMetadata(ctx)
	if err := ri.ValidateHeaders(); err != nil {
		return nil, huskyotlp.AsGRPCError(err)
	}

	result, err := huskyotlp.TranslateTraceRequest(req)
	if err != nil {
		return nil, huskyotlp.AsGRPCError(err)
	}

	if err := processTraceRequest(ctx, router, result.Events, ri.ApiKey, ri.Dataset); err != nil {
		return nil, huskyotlp.AsGRPCError(err)
	}

	return &collectortrace.ExportTraceServiceResponse{}, nil
}

func processTraceRequest(
	ctx context.Context,
	router *Router,
	batch []huskyotlp.Event,
	apiKey string,
	datasetName string) error {

	var requestID types.RequestIDContextKey
	apiHost, err := router.Config.GetHoneycombAPI()
	if err != nil {
		router.Logger.Error().Logf("Unable to retrieve APIHost from config while processing OTLP batch")
		return err
	}

	for _, ev := range batch {
		event := &types.Event{
			Context:    ctx,
			APIHost:    apiHost,
			APIKey:     apiKey,
			Dataset:    datasetName,
			SampleRate: uint(ev.SampleRate),
			Timestamp:  ev.Timestamp,
			Data:       ev.Attributes,
		}
		if err = router.processEvent(event, requestID); err != nil {
			router.Logger.Error().Logf("Error processing event: " + err.Error())
		}
	}

	return nil
}

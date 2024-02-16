package route

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	huskyotlp "github.com/honeycombio/husky/otlp"
	"github.com/honeycombio/refinery/types"

	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
)

func (r *Router) postOTLPLogs(w http.ResponseWriter, req *http.Request) {
	ri := huskyotlp.GetRequestInfoFromHttpHeaders(req.Header)

	if !r.Config.IsAPIKeyValid(ri.ApiKey) {
		err := fmt.Errorf("api key %s not found in list of authorized keys", ri.ApiKey)
		r.handlerReturnWithError(w, ErrAuthNeeded, err)
		return
	}

	if err := ri.ValidateLogsHeaders(); err != nil {
		if errors.Is(err, huskyotlp.ErrInvalidContentType) {
			r.handlerReturnWithError(w, ErrInvalidContentType, err)
		} else {
			r.handlerReturnWithError(w, ErrAuthNeeded, err)
		}
		return
	}

	result, err := huskyotlp.TranslateLogsRequestFromReader(req.Body, ri)
	if err != nil {
		r.handlerReturnWithError(w, ErrUpstreamFailed, err)
		return
	}

	if err := processLogsRequest(req.Context(), r, result.Batches, ri.ApiKey); err != nil {
		r.handlerReturnWithError(w, ErrUpstreamFailed, err)
	}
}

type LogsServer struct {
	router *Router
	collectorlogs.UnimplementedLogsServiceServer
}

func NewLogsServer(router *Router) *LogsServer {
	logsServer := LogsServer{router: router}
	return &logsServer
}

func (t *LogsServer) Export(ctx context.Context, req *collectorlogs.ExportLogsServiceRequest) (*collectorlogs.ExportLogsServiceResponse, error) {
	ri := huskyotlp.GetRequestInfoFromGrpcMetadata(ctx)
	if err := ri.ValidateLogsHeaders(); err != nil {
		return nil, huskyotlp.AsGRPCError(err)
	}

	result, err := huskyotlp.TranslateLogsRequest(req, ri)
	if err != nil {
		return nil, huskyotlp.AsGRPCError(err)
	}

	if err := processLogsRequest(ctx, t.router, result.Batches, ri.ApiKey); err != nil {
		return nil, huskyotlp.AsGRPCError(err)
	}

	return &collectorlogs.ExportLogsServiceResponse{}, nil
}

func processLogsRequest(
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

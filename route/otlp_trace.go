package route

import (
	"context"
	"net/http"
	"time"

	huskyotlp "github.com/honeycombio/husky/otlp"
	"github.com/honeycombio/refinery/types"

	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

func (router *Router) postOTLP(w http.ResponseWriter, req *http.Request) {
	ri := huskyotlp.GetRequestInfoFromHttpHeaders(req)
	if !ri.HasValidContentType() {
		router.handlerReturnWithError(w, ErrInvalidContentType, huskyotlp.ErrInvalidContentType)
		return
	}

	if err := ri.ValidateHeaders(); err != nil {
		router.handlerReturnWithError(w, ErrAuthNeeded, err)
		return
	}

	batch, err := huskyotlp.TranslateHttpTraceRequest(req)
	if err != nil {
		router.handlerReturnWithError(w, ErrUpstreamFailed, err)
		return
	}

	if err := processTraceRequest(req.Context(), router, batch, ri.ApiKey, ri.Dataset); err != nil {
		router.handlerReturnWithError(w, ErrUpstreamFailed, err)
	}
}

func (router *Router) Export(ctx context.Context, req *collectortrace.ExportTraceServiceRequest) (*collectortrace.ExportTraceServiceResponse, error) {
	ri := huskyotlp.GetRequestInfoFromGrpcMetadata(ctx)
	if err := ri.ValidateHeaders(); err != nil {
		return nil, huskyotlp.AsGRPCError(err)
	}

	batch, err := huskyotlp.TranslateGrpcTraceRequest(req)
	if err != nil {
		return nil, huskyotlp.AsGRPCError(err)
	}

	if err := processTraceRequest(ctx, router, batch, ri.ApiKey, ri.Dataset); err != nil {
		return nil, huskyotlp.AsGRPCError(err)
	}

	return &collectortrace.ExportTraceServiceResponse{}, nil
}

func processTraceRequest(
	ctx context.Context,
	router *Router,
	batch []map[string]interface{},
	apiKey string,
	datasetName string) error {

	var requestID types.RequestIDContextKey
	debugLog := router.iopLogger.Debug().WithField("request_id", requestID)

	apiHost, err := router.Config.GetHoneycombAPI()
	if err != nil {
		router.Logger.Error().Logf("Unable to retrieve APIHost from config while processing OTLP batch")
		return err
	}

	for _, ev := range batch {
		attrs := ev["data"].(map[string]interface{})
		timestamp := ev["time"].(time.Time)
		sampleRate, err := getSampleRateFromAttributes(attrs)
		if err != nil {
			debugLog.WithField("error", err.Error()).WithField("sampleRate", attrs["sampleRate"]).Logf("error parsing sampleRate")
		}

		event := &types.Event{
			Context:    ctx,
			APIHost:    apiHost,
			APIKey:     apiKey,
			Dataset:    datasetName,
			SampleRate: uint(sampleRate),
			Timestamp:  timestamp,
			Data:       attrs,
		}
		if err = router.processEvent(event, requestID); err != nil {
			router.Logger.Error().Logf("Error processing event: " + err.Error())
		}
	}

	return nil
}

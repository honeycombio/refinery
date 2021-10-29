package route

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net/http"
	"time"

	huskyotlp "github.com/honeycombio/husky/otlp"
	"github.com/honeycombio/refinery/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

func (router *Router) postOTLP(w http.ResponseWriter, req *http.Request) {
	ri := huskyotlp.GetRequestInfoFromHttpHeaders(req)
	if !ri.HasValidContentType() {
		router.handlerReturnWithError(w, ErrInvalidContentType, errors.New("invalid content-type"))
		return
	}
	if err := validateHeaders(ri); err != nil {
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
	if err := validateHeaders(ri); err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}

	batch, err := huskyotlp.TranslateGrpcTraceRequest(req)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}

	if err := processTraceRequest(ctx, router, batch, ri.ApiKey, ri.Dataset); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
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

func validateHeaders(ri huskyotlp.RequestInfo) error {
	if ri.ApiKey == "" {
		return errors.New("missing x-honeycomb-team header")
	}
	if ri.Dataset == "" {
		return errors.New("missing x-honeycomb-team header")
	}
	return nil
}

// bytesToTraceID returns an ID suitable for use for spans and traces. Before
// encoding the bytes as a hex string, we want to handle cases where we are
// given 128-bit IDs with zero padding, e.g. 0000000000000000f798a1e7f33c8af6.
// To do this, we borrow a strategy from Jaeger [1] wherein we split the byte
// sequence into two parts. The leftmost part could contain all zeros. We use
// that to determine whether to return a 64-bit hex encoded string or a 128-bit
// one.
//
// [1]: https://github.com/jaegertracing/jaeger/blob/cd19b64413eca0f06b61d92fe29bebce1321d0b0/model/ids.go#L81
func bytesToTraceID(traceID []byte) string {
	// binary.BigEndian.Uint64() does a bounds check on traceID which will
	// cause a panic if traceID is fewer than 8 bytes. In this case, we don't
	// need to check for zero padding on the high part anyway, so just return a
	// hex string.
	if len(traceID) < traceIDShortLength {
		return fmt.Sprintf("%x", traceID)
	}
	var low uint64
	if len(traceID) == traceIDLongLength {
		low = binary.BigEndian.Uint64(traceID[traceIDShortLength:])
		if high := binary.BigEndian.Uint64(traceID[:traceIDShortLength]); high != 0 {
			return fmt.Sprintf("%016x%016x", high, low)
		}
	} else {
		low = binary.BigEndian.Uint64(traceID)
	}

	return fmt.Sprintf("%016x", low)
}

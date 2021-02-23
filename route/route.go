package route

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/mux"
	jsoniter "github.com/json-iterator/go"
	"github.com/klauspost/compress/zstd"
	"github.com/vmihailenco/msgpack/v4"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"

	// grpc/gzip compressor, auto registers on import
	_ "google.golang.org/grpc/encoding/gzip"

	"github.com/honeycombio/refinery/collect"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/sharder"
	"github.com/honeycombio/refinery/transmit"
	"github.com/honeycombio/refinery/types"

	collectortrace "github.com/honeycombio/refinery/internal/opentelemetry-proto-gen/collector/trace/v1"
	common "github.com/honeycombio/refinery/internal/opentelemetry-proto-gen/common/v1"
	trace "github.com/honeycombio/refinery/internal/opentelemetry-proto-gen/trace/v1"
)

const (
	// numZstdDecoders is set statically here - we may make it into a config option
	// A normal practice might be to use some multiple of the CPUs, but that goes south
	// in kubernetes
	numZstdDecoders        = 4
	traceIDShortLength     = 8
	traceIDLongLength      = 16
	GRPCMessageSizeMax int = 5000000 // 5MB
	defaultSampleRate      = 1
)

type Router struct {
	Config               config.Config         `inject:""`
	Logger               logger.Logger         `inject:""`
	HTTPTransport        *http.Transport       `inject:"upstreamTransport"`
	UpstreamTransmission transmit.Transmission `inject:"upstreamTransmission"`
	PeerTransmission     transmit.Transmission `inject:"peerTransmission"`
	Sharder              sharder.Sharder       `inject:""`
	Collector            collect.Collector     `inject:""`
	Metrics              metrics.Metrics       `inject:""`

	// version is set on startup so that the router may answer HTTP requests for
	// the version
	versionStr string

	proxyClient *http.Client

	// type indicates whether this should listen for incoming events or content
	// redirected from a peer
	incomingOrPeer string

	// iopLogger is a logger that knows whether it's incoming or peer
	iopLogger iopLogger

	zstdDecoders chan *zstd.Decoder

	server     *http.Server
	grpcServer *grpc.Server
	doneWG     sync.WaitGroup
}

type BatchResponse struct {
	Status int    `json:"status"`
	Error  string `json:"error,omitempty"`
}

type iopLogger struct {
	logger.Logger
	incomingOrPeer string
}

func (i *iopLogger) Debug() logger.Entry {
	return i.Logger.Debug().WithField("router_iop", i.incomingOrPeer)
}

func (i *iopLogger) Info() logger.Entry {
	return i.Logger.Info().WithField("router_iop", i.incomingOrPeer)
}

func (i *iopLogger) Error() logger.Entry {
	return i.Logger.Error().WithField("router_iop", i.incomingOrPeer)
}

func (r *Router) SetVersion(ver string) {
	r.versionStr = ver
}

// LnS spins up the Listen and Serve portion of the router. A router is
// initialized as being for either incoming traffic from clients or traffic from
// a peer. They listen on different addresses so peer traffic can be
// prioritized.
func (r *Router) LnS(incomingOrPeer string) {
	r.incomingOrPeer = incomingOrPeer
	r.iopLogger = iopLogger{
		Logger:         r.Logger,
		incomingOrPeer: incomingOrPeer,
	}

	r.proxyClient = &http.Client{
		Timeout:   time.Second * 10,
		Transport: r.HTTPTransport,
	}

	var err error
	r.zstdDecoders, err = makeDecoders(numZstdDecoders)
	if err != nil {
		r.iopLogger.Error().Logf("couldn't start zstd decoders: %s", err.Error())
		return
	}

	r.Metrics.Register(r.incomingOrPeer+"_router_proxied", "counter")
	r.Metrics.Register(r.incomingOrPeer+"_router_event", "counter")
	r.Metrics.Register(r.incomingOrPeer+"_router_batch", "counter")
	r.Metrics.Register(r.incomingOrPeer+"_router_nonspan", "counter")
	r.Metrics.Register(r.incomingOrPeer+"_router_span", "counter")
	r.Metrics.Register(r.incomingOrPeer+"_router_peer", "counter")
	r.Metrics.Register(r.incomingOrPeer+"_router_dropped", "counter")

	muxxer := mux.NewRouter()

	muxxer.Use(r.setResponseHeaders)
	muxxer.Use(r.requestLogger)
	muxxer.Use(r.panicCatcher)

	// answer a basic health check locally
	muxxer.HandleFunc("/alive", r.alive).Name("local health")
	muxxer.HandleFunc("/panic", r.panic).Name("intentional panic")
	muxxer.HandleFunc("/version", r.version).Name("report version info")
	muxxer.HandleFunc("/debug/trace/{traceID}", r.debugTrace).Name("get debug information for given trace ID")

	// require an auth header for events and batches
	authedMuxxer := muxxer.PathPrefix("/1/").Methods("POST").Subrouter()
	authedMuxxer.Use(r.apiKeyChecker)

	// handle events and batches
	authedMuxxer.HandleFunc("/events/{datasetName}", r.event).Name("event")
	authedMuxxer.HandleFunc("/batch/{datasetName}", r.batch).Name("batch")

	// pass everything else through unmolested
	muxxer.PathPrefix("/").HandlerFunc(r.proxy).Name("proxy")

	var listenAddr, grpcAddr string
	if r.incomingOrPeer == "incoming" {
		listenAddr, err = r.Config.GetListenAddr()
		if err != nil {
			r.iopLogger.Error().Logf("failed to get listen addr config: %s", err)
			return
		}
		// GRPC listen addr is optional, err means addr was not empty and invalid
		grpcAddr, err = r.Config.GetGRPCListenAddr()
		if err != nil {
			r.iopLogger.Error().Logf("failed to get grpc listen addr config: %s", err)
			return
		}
	} else {
		listenAddr, err = r.Config.GetPeerListenAddr()
		if err != nil {
			r.iopLogger.Error().Logf("failed to get peer listen addr config: %s", err)
			return
		}
	}

	r.iopLogger.Info().Logf("Listening on %s", listenAddr)
	r.server = &http.Server{
		Addr:    listenAddr,
		Handler: muxxer,
	}

	if len(grpcAddr) > 0 {
		l, err := net.Listen("tcp", grpcAddr)
		if err != nil {
			r.iopLogger.Error().Logf("failed to listen to grpc addr: " + grpcAddr)
		}

		r.iopLogger.Info().Logf("gRPC listening on %s", grpcAddr)
		serverOpts := []grpc.ServerOption{
			grpc.MaxSendMsgSize(GRPCMessageSizeMax), // default is math.MaxInt32
			grpc.MaxRecvMsgSize(GRPCMessageSizeMax), // default is 4MB
			grpc.KeepaliveParams(keepalive.ServerParameters{
				Time:              10 * time.Second,
				Timeout:           2 * time.Second,
				MaxConnectionIdle: time.Minute,
			}),
		}
		r.grpcServer = grpc.NewServer(serverOpts...)
		collectortrace.RegisterTraceServiceServer(r.grpcServer, r)
		go r.grpcServer.Serve(l)
	}

	r.doneWG.Add(1)
	go func() {
		defer r.doneWG.Done()

		err = r.server.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			r.iopLogger.Error().Logf("failed to ListenAndServe: %s", err)
		}
	}()
}

func (r *Router) Stop() error {
	ctx, _ := context.WithTimeout(context.Background(), time.Minute)
	err := r.server.Shutdown(ctx)
	if err != nil {
		return err
	}
	if r.grpcServer != nil {
		r.grpcServer.GracefulStop()
	}
	r.doneWG.Wait()
	return nil
}

func (r *Router) alive(w http.ResponseWriter, req *http.Request) {
	r.iopLogger.Debug().Logf("answered /x/alive check")
	w.Write([]byte(`{"source":"refinery","alive":"yes"}`))
}

func (r *Router) panic(w http.ResponseWriter, req *http.Request) {
	panic("panic? never!")
}

func (r *Router) version(w http.ResponseWriter, req *http.Request) {
	w.Write([]byte(fmt.Sprintf(`{"source":"refinery","version":"%s"}`, r.versionStr)))
}

func (r *Router) debugTrace(w http.ResponseWriter, req *http.Request) {
	traceID := mux.Vars(req)["traceID"]
	shard := r.Sharder.WhichShard(traceID)
	w.Write([]byte(fmt.Sprintf(`{"traceID":"%s","node":"%s"}`, traceID, shard.GetAddress())))
}

// event is handler for /1/event/
func (r *Router) event(w http.ResponseWriter, req *http.Request) {
	r.Metrics.IncrementCounter(r.incomingOrPeer + "_router_event")
	defer req.Body.Close()

	bodyReader, err := r.getMaybeCompressedBody(req)
	if err != nil {
		r.handlerReturnWithError(w, ErrPostBody, err)
		return
	}

	reqBod, err := ioutil.ReadAll(bodyReader)
	if err != nil {
		r.handlerReturnWithError(w, ErrPostBody, err)
		return
	}

	ev, err := r.requestToEvent(req, reqBod)
	if err != nil {
		r.handlerReturnWithError(w, ErrReqToEvent, err)
		return
	}

	reqID := req.Context().Value(types.RequestIDContextKey{})
	err = r.processEvent(ev, reqID)
	if err != nil {
		r.handlerReturnWithError(w, ErrReqToEvent, err)
		return
	}
}

func (r *Router) requestToEvent(req *http.Request, reqBod []byte) (*types.Event, error) {
	// get necessary bits out of the incoming event
	apiKey := req.Header.Get(types.APIKeyHeader)
	if apiKey == "" {
		apiKey = req.Header.Get(types.APIKeyHeaderShort)
	}
	sampleRate, err := strconv.Atoi(req.Header.Get(types.SampleRateHeader))
	if err != nil {
		sampleRate = 1
	}
	eventTime := getEventTime(req.Header.Get(types.TimestampHeader))
	vars := mux.Vars(req)
	dataset := vars["datasetName"]

	apiHost, err := r.Config.GetHoneycombAPI()
	if err != nil {
		return nil, err
	}
	data := map[string]interface{}{}
	err = unmarshal(req, bytes.NewReader(reqBod), &data)
	if err != nil {
		return nil, err
	}

	return &types.Event{
		Context:    req.Context(),
		APIHost:    apiHost,
		APIKey:     apiKey,
		Dataset:    dataset,
		SampleRate: uint(sampleRate),
		Timestamp:  eventTime,
		Data:       data,
	}, nil
}

func (r *Router) batch(w http.ResponseWriter, req *http.Request) {
	r.Metrics.IncrementCounter(r.incomingOrPeer + "_router_batch")
	defer req.Body.Close()

	reqID := req.Context().Value(types.RequestIDContextKey{})
	debugLog := r.iopLogger.Debug().WithField("request_id", reqID)

	bodyReader, err := r.getMaybeCompressedBody(req)
	if err != nil {
		r.handlerReturnWithError(w, ErrPostBody, err)
		return
	}

	reqBod, err := ioutil.ReadAll(bodyReader)
	if err != nil {
		r.handlerReturnWithError(w, ErrPostBody, err)
		return
	}

	batchedEvents := make([]batchedEvent, 0)
	err = unmarshal(req, bytes.NewReader(reqBod), &batchedEvents)
	if err != nil {
		debugLog.WithField("error", err.Error()).WithField("request.url", req.URL).WithField("json_body", string(reqBod)).Logf("error parsing json")
		r.handlerReturnWithError(w, ErrJSONFailed, err)
		return
	}

	batchedResponses := make([]*BatchResponse, 0, len(batchedEvents))
	for _, bev := range batchedEvents {
		ev, err := r.batchedEventToEvent(req, bev)
		if err != nil {
			batchedResponses = append(
				batchedResponses,
				&BatchResponse{
					Status: http.StatusBadRequest,
					Error:  fmt.Sprintf("failed to convert to event: %s", err.Error()),
				},
			)
			debugLog.WithField("error", err).Logf("event from batch failed to process event")
			continue
		}

		err = r.processEvent(ev, reqID)

		var resp BatchResponse
		switch {
		case errors.Is(err, collect.ErrWouldBlock):
			resp.Status = http.StatusTooManyRequests
			resp.Error = err.Error()
		case err != nil:
			resp.Status = http.StatusBadRequest
			resp.Error = err.Error()
		default:
			resp.Status = http.StatusAccepted
		}
		batchedResponses = append(batchedResponses, &resp)
	}
	response, err := json.Marshal(batchedResponses)
	if err != nil {
		r.handlerReturnWithError(w, ErrJSONBuildFailed, err)
		return
	}
	w.Write(response)
}

func (r *Router) Export(ctx context.Context, req *collectortrace.ExportTraceServiceRequest) (*collectortrace.ExportTraceServiceResponse, error) {

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		r.Logger.Error().Logf("Unable to retreive metadata from OTLP request.")
		return &collectortrace.ExportTraceServiceResponse{}, nil
	}

	// requestID is used to track a requst as it moves between refinery nodes (peers)
	// the OTLP handler only receives incoming (not peer) requests for now so will be empty here
	var requestID types.RequestIDContextKey
	debugLog := r.iopLogger.Debug().WithField("request_id", requestID)

	apiKey, dataset := getAPIKeyAndDatasetFromMetadata(md)
	if apiKey == "" {
		r.Logger.Error().Logf("Received OTLP request without Honeycomb APIKey header")
		return &collectortrace.ExportTraceServiceResponse{}, nil
	}
	if dataset == "" {
		r.Logger.Error().Logf("Received OTLP request without Honeycomb dataset header")
		return &collectortrace.ExportTraceServiceResponse{}, nil
	}

	apiHost, err := r.Config.GetHoneycombAPI()
	if err != nil {
		r.Logger.Error().Logf("Unable to retrieve APIHost from config while processing OTLP batch")
		return &collectortrace.ExportTraceServiceResponse{}, nil
	}

	for _, resourceSpan := range req.ResourceSpans {
		resourceAttrs := make(map[string]interface{})

		if resourceSpan.Resource != nil {
			addAttributesToMap(resourceAttrs, resourceSpan.Resource.Attributes)
		}

		for _, librarySpan := range resourceSpan.InstrumentationLibrarySpans {
			library := librarySpan.InstrumentationLibrary
			if library != nil {
				if len(library.Name) > 0 {
					resourceAttrs["library.name"] = library.Name
				}
				if len(library.Version) > 0 {
					resourceAttrs["library.version"] = library.Version
				}
			}

			for _, span := range librarySpan.GetSpans() {
				traceID := bytesToTraceID(span.TraceId)
				spanID := hex.EncodeToString(span.SpanId)
				timestamp := time.Unix(0, int64(span.StartTimeUnixNano)).UTC()

				eventAttrs := map[string]interface{}{
					"trace.trace_id": traceID,
					"trace.span_id":  spanID,
					"type":           getSpanKind(span.Kind),
					"name":           span.Name,
					"duration_ms":    float64(span.EndTimeUnixNano-span.StartTimeUnixNano) / float64(time.Millisecond),
					"status_code":    int32(span.Status.Code),
				}
				if span.ParentSpanId != nil {
					eventAttrs["trace.parent_id"] = hex.EncodeToString(span.ParentSpanId)
				}
				if r.getSpanStatusCode(span.Status) == trace.Status_STATUS_CODE_ERROR {
					eventAttrs["error"] = true
				}
				if len(span.Status.Message) > 0 {
					eventAttrs["status_message"] = span.Status.Message
				}
				if span.Attributes != nil {
					addAttributesToMap(eventAttrs, span.Attributes)
				}

				sampleRate, err := getSampleRateFromAttributes(eventAttrs)
				if err != nil {
					debugLog.WithField("error", err.Error()).WithField("sampleRate", eventAttrs["sampleRate"]).Logf("error parsing sampleRate")
				}

				// copy resource attributes to event attributes
				for k, v := range resourceAttrs {
					eventAttrs[k] = v
				}

				event := &types.Event{
					Context:    ctx,
					APIHost:    apiHost,
					APIKey:     apiKey,
					Dataset:    dataset,
					SampleRate: uint(sampleRate),
					Timestamp:  timestamp,
					Data:       eventAttrs,
				}

				err = r.processEvent(event, requestID)
				if err != nil {
					r.Logger.Error().Logf("Error processing event: " + err.Error())
				}
			}
		}
	}

	return &collectortrace.ExportTraceServiceResponse{}, nil
}

func (r *Router) processEvent(ev *types.Event, reqID interface{}) error {
	debugLog := r.iopLogger.Debug().
		WithField("request_id", reqID).
		WithString("api_host", ev.APIHost).
		WithString("dataset", ev.Dataset)

	// extract trace ID, route to self or peer, pass on to collector
	// TODO make trace ID field configurable
	var traceID string
	if trID, ok := ev.Data["trace.trace_id"]; ok {
		traceID = trID.(string)
	} else if trID, ok := ev.Data["traceId"]; ok {
		traceID = trID.(string)
	}
	if traceID == "" {
		// not part of a trace. send along upstream
		r.Metrics.IncrementCounter(r.incomingOrPeer + "_router_nonspan")
		debugLog.WithString("api_host", ev.APIHost).
			WithString("dataset", ev.Dataset).
			Logf("sending non-trace event from batch")
		r.UpstreamTransmission.EnqueueEvent(ev)
		return nil
	}
	debugLog = debugLog.WithString("trace_id", traceID)

	// ok, we're a span. Figure out if we should handle locally or pass on to a peer
	targetShard := r.Sharder.WhichShard(traceID)
	if r.incomingOrPeer == "incoming" && !targetShard.Equals(r.Sharder.MyShard()) {
		r.Metrics.IncrementCounter(r.incomingOrPeer + "_router_peer")
		debugLog.WithString("peer", targetShard.GetAddress()).
			Logf("Sending span from batch to my peer")
		ev.APIHost = targetShard.GetAddress()

		// Unfortunately this doesn't tell us if the event was actually
		// enqueued; we need to watch the response channel to find out, at
		// which point it's too late to tell the client.
		r.PeerTransmission.EnqueueEvent(ev)
		return nil
	}

	// we're supposed to handle it
	var err error
	span := &types.Span{
		Event:   *ev,
		TraceID: traceID,
	}
	if r.incomingOrPeer == "incoming" {
		err = r.Collector.AddSpan(span)
	} else {
		err = r.Collector.AddSpanFromPeer(span)
	}
	if err != nil {
		r.Metrics.IncrementCounter(r.incomingOrPeer + "_router_dropped")
		debugLog.Logf("Dropping span from batch, channel full")
		return err
	}

	r.Metrics.IncrementCounter(r.incomingOrPeer + "_router_span")
	debugLog.Logf("Accepting span from batch for collection into a trace")
	return nil
}

func (r *Router) getMaybeCompressedBody(req *http.Request) (io.Reader, error) {
	var reader io.Reader
	switch req.Header.Get("Content-Encoding") {
	case "gzip":
		gzipReader, err := gzip.NewReader(req.Body)
		if err != nil {
			return nil, err
		}
		defer gzipReader.Close()

		buf := &bytes.Buffer{}
		if _, err := io.Copy(buf, gzipReader); err != nil {
			return nil, err
		}
		reader = buf
	case "zstd":
		zReader := <-r.zstdDecoders
		defer func(zReader *zstd.Decoder) {
			zReader.Reset(nil)
			r.zstdDecoders <- zReader
		}(zReader)

		err := zReader.Reset(req.Body)
		if err != nil {
			return nil, err
		}
		buf := &bytes.Buffer{}
		if _, err := io.Copy(buf, zReader); err != nil {
			return nil, err
		}

		reader = buf
	default:
		reader = req.Body
	}
	return reader, nil
}

func (r *Router) batchedEventToEvent(req *http.Request, bev batchedEvent) (*types.Event, error) {
	apiKey := req.Header.Get(types.APIKeyHeader)
	if apiKey == "" {
		apiKey = req.Header.Get(types.APIKeyHeaderShort)
	}

	sampleRate := bev.SampleRate
	if sampleRate == 0 {
		sampleRate = 1
	}
	eventTime := bev.getEventTime()
	// TODO move the following 3 lines outside of this loop; they could be done
	// once for the entire batch instead of in every event.
	vars := mux.Vars(req)
	dataset := vars["datasetName"]
	apiHost, err := r.Config.GetHoneycombAPI()
	if err != nil {
		return nil, err
	}
	return &types.Event{
		Context:    req.Context(),
		APIHost:    apiHost,
		APIKey:     apiKey,
		Dataset:    dataset,
		SampleRate: uint(sampleRate),
		Timestamp:  eventTime,
		Data:       bev.Data,
	}, nil
}

type batchedEvent struct {
	Timestamp        string                 `json:"time"`
	MsgPackTimestamp *time.Time             `msgpack:"time,omitempty"`
	SampleRate       int64                  `json:"samplerate" msgpack:"samplerate"`
	Data             map[string]interface{} `json:"data" msgpack:"data"`
}

func (b *batchedEvent) getEventTime() time.Time {
	if b.MsgPackTimestamp != nil {
		return b.MsgPackTimestamp.UTC()
	}

	return getEventTime(b.Timestamp)
}

// getEventTime tries to guess the time format in our time header!
// Allowable options are
// * RFC3339Nano
// * RFC3339
// * Unix Epoch time (integer seconds since 1970, eg 1535589382)
// * High resolution unix epoch time (eg 'unixmillis' 1535589382641)
// * High resolution unix epoch time as a float (eg 1535589382.641)
func getEventTime(etHeader string) time.Time {
	var eventTime time.Time
	if etHeader != "" {
		// Great, they sent us a time header. let's try and parse it.
		// RFC3339Nano is the default that we send from all our SDKs
		eventTime, _ = time.Parse(time.RFC3339Nano, etHeader)
		if eventTime.IsZero() {
			// the default didn't catch it, let's try a few other things
			// is it all numeric? then try unix epoch times
			epochInt, err := strconv.ParseInt(etHeader, 0, 64)
			if err == nil {
				// it might be seconds or it might be milliseconds! Who can know!
				// 10-digit numbers are seconds, 13-digit milliseconds, 16 microseconds
				if len(etHeader) == 10 {
					eventTime = time.Unix(epochInt, 0)
				} else if len(etHeader) > 10 {
					// turn it into seconds and fractional seconds
					fractionalTime := etHeader[:10] + "." + etHeader[10:]
					// then chop it into the int part and the fractional part
					if epochFloat, err := strconv.ParseFloat(fractionalTime, 64); err == nil {
						sec, dec := math.Modf(epochFloat)
						eventTime = time.Unix(int64(sec), int64(dec*(1e9)))
					}

				}
			} else {
				epochFloat, err := strconv.ParseFloat(etHeader, 64)
				if err == nil {
					sec, dec := math.Modf(epochFloat)
					eventTime = time.Unix(int64(sec), int64(dec*(1e9)))
				}
			}
		}
	}
	return eventTime.UTC()
}

func makeDecoders(num int) (chan *zstd.Decoder, error) {
	zstdDecoders := make(chan *zstd.Decoder, num)
	for i := 0; i < num; i++ {
		zReader, err := zstd.NewReader(
			nil,
			zstd.WithDecoderConcurrency(1),
			zstd.WithDecoderLowmem(true),
			zstd.WithDecoderMaxMemory(8*1024*1024),
		)
		if err != nil {
			return nil, err
		}
		zstdDecoders <- zReader
	}
	return zstdDecoders, nil
}

func unmarshal(r *http.Request, data io.Reader, v interface{}) error {
	switch r.Header.Get("Content-Type") {
	case "application/x-msgpack", "application/msgpack":
		return msgpack.NewDecoder(data).
			UseDecodeInterfaceLoose(true).
			Decode(v)
	default:
		return jsoniter.NewDecoder(data).Decode(v)
	}
}

func getAPIKeyAndDatasetFromMetadata(md metadata.MD) (apiKey string, dataset string) {
	apiKey = getFirstValueFromMetadata(types.APIKeyHeader, md)
	if apiKey == "" {
		apiKey = getFirstValueFromMetadata(types.APIKeyHeaderShort, md)
	}
	dataset = getFirstValueFromMetadata(types.DatasetHeader, md)

	return apiKey, dataset
}

// getFirstValueFromMetadata returns the first value of a metadata entry using a
// case-insensitive key
func getFirstValueFromMetadata(key string, md metadata.MD) string {
	if values := md.Get(key); len(values) > 0 {
		return values[0]
	}
	return ""
}

func addAttributesToMap(attrs map[string]interface{}, attributes []*common.KeyValue) {
	for _, attr := range attributes {
		if attr.Key == "" {
			continue
		}
		switch attr.Value.Value.(type) {
		case *common.AnyValue_StringValue:
			attrs[attr.Key] = attr.Value.GetStringValue()
		case *common.AnyValue_BoolValue:
			attrs[attr.Key] = attr.Value.GetBoolValue()
		case *common.AnyValue_DoubleValue:
			attrs[attr.Key] = attr.Value.GetDoubleValue()
		case *common.AnyValue_IntValue:
			attrs[attr.Key] = attr.Value.GetIntValue()
		}
	}
}

func getSpanKind(kind trace.Span_SpanKind) string {
	switch kind {
	case trace.Span_SPAN_KIND_CLIENT:
		return "client"
	case trace.Span_SPAN_KIND_SERVER:
		return "server"
	case trace.Span_SPAN_KIND_PRODUCER:
		return "producer"
	case trace.Span_SPAN_KIND_CONSUMER:
		return "consumer"
	case trace.Span_SPAN_KIND_INTERNAL:
		return "internal"
	case trace.Span_SPAN_KIND_UNSPECIFIED:
		fallthrough
	default:
		return "unspecified"
	}
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

// getSpanStatusCode checks the value of both the deprecated code and code fields
// on the span status and using the rules specified in the backward compatibility
// notes in the protobuf definitions. See:
//
// https://github.com/open-telemetry/opentelemetry-proto/blob/59c488bfb8fb6d0458ad6425758b70259ff4a2bd/opentelemetry/proto/trace/v1/trace.proto#L230
func (r *Router) getSpanStatusCode(status *trace.Status) trace.Status_StatusCode {
	if status.Code == trace.Status_STATUS_CODE_UNSET {
		if status.DeprecatedCode == trace.Status_DEPRECATED_STATUS_CODE_OK {
			return trace.Status_STATUS_CODE_UNSET
		}
		return trace.Status_STATUS_CODE_ERROR
	}
	return status.Code
}

func getSampleRateFromAttributes(attributes map[string]interface{}) (int, error) {
	var err error
	sampleRate := defaultSampleRate
	if attributes["sampleRate"] != nil {
		switch attributes["sampleRate"].(type) {
		case string:
			sampleRate, err = strconv.Atoi(attributes["sampleRate"].(string))
		case int:
			sampleRate = attributes["sampleRate"].(int)
		default:
			err = fmt.Errorf("Unrecognised sampleRate datatype - %T", attributes["sampleRate"])
		}
		// remove sampleRate from event fields
		delete(attributes, "sampleRate")
	}

	return sampleRate, err
}

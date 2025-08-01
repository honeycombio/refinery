package route

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	jsoniter "github.com/json-iterator/go"
	"github.com/klauspost/compress/zstd"
	"github.com/pelletier/go-toml/v2"
	"github.com/vmihailenco/msgpack/v5"
	"google.golang.org/grpc"
	healthserver "google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"gopkg.in/yaml.v3"

	// grpc/gzip compressor, auto registers on import
	_ "google.golang.org/grpc/encoding/gzip"

	huskyotlp "github.com/honeycombio/husky/otlp"

	"github.com/honeycombio/refinery/collect"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/health"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/sharder"
	"github.com/honeycombio/refinery/transmit"
	"github.com/honeycombio/refinery/types"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/trace"
	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

const (
	// numZstdDecoders is set statically here - we may make it into a config option
	// A normal practice might be to use some multiple of the CPUs, but that goes south
	// in kubernetes
	numZstdDecoders        = 4
	traceIDShortLength     = 8
	traceIDLongLength      = 16
	GRPCMessageSizeMax int = 5_000_000 // 5MB
	HTTPMessageSizeMax     = 5_000_000 // 5MB
	defaultSampleRate      = 1
)

type Router struct {
	Config               config.Config         `inject:""`
	Logger               logger.Logger         `inject:""`
	Health               health.Reporter       `inject:""`
	HTTPTransport        *http.Transport       `inject:"upstreamTransport"`
	UpstreamTransmission transmit.Transmission `inject:"upstreamTransmission"`
	PeerTransmission     transmit.Transmission `inject:"peerTransmission"`
	Sharder              sharder.Sharder       `inject:""`
	Collector            collect.Collector     `inject:""`
	Metrics              metrics.Metrics       `inject:"genericMetrics"`
	Tracer               trace.Tracer          `inject:"tracer"`

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
	donech     chan struct{}

	environmentCache *environmentCache
	hsrv             *healthserver.Server
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

var routerMetrics = []metrics.Metadata{
	{Name: "_router_proxied", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "the number of events proxied to another refinery"},
	{Name: "_router_event", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "the number of events received"},
	{Name: "_router_event_bytes", Type: metrics.Histogram, Unit: metrics.Bytes, Description: "the number of bytes per event received"},
	{Name: "_router_span", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "the number of spans received"},
	{Name: "_router_dropped", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "the number of events dropped because the channel was full"},
	{Name: "_router_nonspan", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "the number of non-span events received"},
	{Name: "_router_peer", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "the number of spans proxied to a peer"},
	{Name: "_router_batch", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "the number of batches of events received"},
	{Name: "_router_batch_events", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "the number of events received in batches"},
	{Name: "_router_otlp", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "the number of otlp requests received"},
	{Name: "_router_otlp_events", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "the number of events received in otlp requests"},
	{Name: "bytes_received_traces", Type: metrics.Counter, Unit: metrics.Bytes, Description: "the number of bytes received in trace events"},
	{Name: "bytes_received_logs", Type: metrics.Counter, Unit: metrics.Bytes, Description: "the number of bytes received in log events"},
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
	r.environmentCache = newEnvironmentCache(r.Config.GetEnvironmentCacheTTL(), r.lookupEnvironment)

	var err error
	r.zstdDecoders, err = makeDecoders(numZstdDecoders)
	if err != nil {
		r.iopLogger.Error().Logf("couldn't start zstd decoders: %s", err.Error())
		return
	}

	for _, metric := range routerMetrics {
		if strings.HasPrefix(metric.Name, "_") {
			metric.Name = r.incomingOrPeer + metric.Name
		}
		r.Metrics.Register(metric)
	}

	muxxer := mux.NewRouter()

	muxxer.Use(r.setResponseHeaders)
	muxxer.Use(r.requestLogger)
	muxxer.Use(r.panicCatcher)

	muxxer.HandleFunc("/alive", r.alive).Name("local health")
	muxxer.HandleFunc("/ready", r.ready).Name("local readiness")
	muxxer.HandleFunc("/panic", r.panic).Name("intentional panic")
	muxxer.HandleFunc("/version", r.version).Name("report version info")

	// require a local auth for query usage
	queryMuxxer := muxxer.PathPrefix("/query/").Methods("GET").Subrouter()
	queryMuxxer.Use(r.queryTokenChecker)

	queryMuxxer.HandleFunc("/trace/{traceID}", r.debugTrace).Name("get debug information for given trace ID")
	queryMuxxer.HandleFunc("/rules/{format}/{dataset}", r.getSamplerRules).Name("get formatted sampler rules for given dataset")
	queryMuxxer.HandleFunc("/allrules/{format}", r.getAllSamplerRules).Name("get formatted sampler rules for all datasets")
	queryMuxxer.HandleFunc("/configmetadata", r.getConfigMetadata).Name("get configuration metadata")

	// require an auth header for events and batches
	authedMuxxer := muxxer.PathPrefix("/1/").Methods("POST").Subrouter()
	authedMuxxer.UseEncodedPath()
	authedMuxxer.Use(r.apiKeyProcessor)

	// handle events and batches
	// Adds the OpenTelemetry instrumentation to the handler to enable tracing
	authedMuxxer.Handle("/events/{datasetName}", otelhttp.NewHandler(http.HandlerFunc(r.event), "handle_event")).Name("event")
	authedMuxxer.Handle("/batch/{datasetName}", otelhttp.NewHandler(http.HandlerFunc(r.batch), "handle_batch")).Name("batch")

	// require an auth header for OTLP requests
	r.AddOTLPMuxxer(muxxer)

	// pass everything else through unmolested
	muxxer.PathPrefix("/").HandlerFunc(r.proxy).Name("proxy")

	var listenAddr, grpcAddr string
	if r.incomingOrPeer == "incoming" {
		listenAddr = r.Config.GetListenAddr()
		// GRPC listen addr is optional
		grpcAddr = r.Config.GetGRPCListenAddr()
	} else {
		listenAddr = r.Config.GetPeerListenAddr()
	}

	r.iopLogger.Info().Logf("Listening on %s", listenAddr)
	r.server = &http.Server{
		Addr:        listenAddr,
		Handler:     muxxer,
		IdleTimeout: r.Config.GetHTTPIdleTimeout(),
	}

	r.donech = make(chan struct{})
	if r.Config.GetGRPCEnabled() && len(grpcAddr) > 0 {
		l, err := net.Listen("tcp", grpcAddr)
		if err != nil {
			r.iopLogger.Error().Logf("failed to listen to grpc addr: " + grpcAddr)
		}

		r.iopLogger.Info().Logf("gRPC listening on %s", grpcAddr)
		grpcConfig := r.Config.GetGRPCConfig()
		serverOpts := []grpc.ServerOption{
			grpc.MaxSendMsgSize(int(grpcConfig.MaxSendMsgSize)),
			grpc.MaxRecvMsgSize(int(grpcConfig.MaxRecvMsgSize)),
			grpc.KeepaliveParams(keepalive.ServerParameters{
				MaxConnectionIdle:     time.Duration(grpcConfig.MaxConnectionIdle),
				MaxConnectionAge:      time.Duration(grpcConfig.MaxConnectionAge),
				MaxConnectionAgeGrace: time.Duration(grpcConfig.MaxConnectionAgeGrace),
				Time:                  time.Duration(grpcConfig.KeepAlive),
				Timeout:               time.Duration(grpcConfig.KeepAliveTimeout),
			}),
			// Add the OpenTelemetry interceptor to the gRPC server to enable tracing
			grpc.StatsHandler(otelgrpc.NewServerHandler()),
		}
		r.grpcServer = grpc.NewServer(serverOpts...)

		traceServer := NewTraceServer(r)
		collectortrace.RegisterTraceServiceServer(r.grpcServer, traceServer)

		logsServer := NewLogsServer(r)
		collectorlogs.RegisterLogsServiceServer(r.grpcServer, logsServer)

		// health check -- manufactured by grpc health package
		r.hsrv = healthserver.NewServer()
		grpc_health_v1.RegisterHealthServer(r.grpcServer, r.hsrv)
		r.startGRPCHealthMonitor()

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
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	err := r.server.Shutdown(ctx)
	if err != nil {
		return err
	}
	if r.grpcServer != nil {
		r.grpcServer.GracefulStop()
	}
	close(r.donech)
	r.doneWG.Wait()
	return nil
}

func (r *Router) alive(w http.ResponseWriter, req *http.Request) {
	r.iopLogger.Debug().Logf("answered /alive check")

	alive := r.Health.IsAlive()
	r.Metrics.Gauge("is_alive", alive)
	if !alive {
		w.WriteHeader(http.StatusServiceUnavailable)
		r.marshalToFormat(w, map[string]interface{}{"source": "refinery", "alive": "no"}, "json")
		return
	}
	r.marshalToFormat(w, map[string]interface{}{"source": "refinery", "alive": "yes"}, "json")
}

func (r *Router) ready(w http.ResponseWriter, req *http.Request) {
	r.iopLogger.Debug().Logf("answered /ready check")

	ready := r.Health.IsReady()
	r.Metrics.Gauge("is_ready", ready)
	if !ready {
		w.WriteHeader(http.StatusServiceUnavailable)
		r.marshalToFormat(w, map[string]interface{}{"source": "refinery", "ready": "no"}, "json")
		return
	}
	r.marshalToFormat(w, map[string]interface{}{"source": "refinery", "ready": "yes"}, "json")
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
	w.Write([]byte(fmt.Sprintf(`{"traceID":"%s","node":"%s"}`, html.EscapeString(traceID), shard.GetAddress())))
}

func (r *Router) getSamplerRules(w http.ResponseWriter, req *http.Request) {
	format := strings.ToLower(mux.Vars(req)["format"])
	dataset := mux.Vars(req)["dataset"]
	cfg, name := r.Config.GetSamplerConfigForDestName(dataset)
	r.marshalToFormat(w, map[string]interface{}{name: cfg}, format)
}

func (r *Router) getAllSamplerRules(w http.ResponseWriter, req *http.Request) {
	format := strings.ToLower(mux.Vars(req)["format"])
	cfgs := r.Config.GetAllSamplerRules()
	r.marshalToFormat(w, cfgs, format)
}

func (r *Router) getConfigMetadata(w http.ResponseWriter, req *http.Request) {
	cm := r.Config.GetConfigMetadata()
	r.marshalToFormat(w, cm, "json")
}

func (r *Router) marshalToFormat(w http.ResponseWriter, obj interface{}, format string) {
	var body []byte
	var err error
	switch format {
	case "json":
		body, err = json.Marshal(obj)
		if err != nil {
			w.Write([]byte(fmt.Sprintf("got error %v trying to marshal to json\n", err)))
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	case "toml":
		body, err = toml.Marshal(obj)
		if err != nil {
			w.Write([]byte(fmt.Sprintf("got error %v trying to marshal to toml\n", err)))
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	case "yaml":
		body, err = yaml.Marshal(obj)
		if err != nil {
			w.Write([]byte(fmt.Sprintf("got error %v trying to marshal to toml\n", err)))
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	default:
		w.Write([]byte(fmt.Sprintf("invalid format '%s' when marshaling\n", format)))
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/"+format)
	w.Write(body)
}

// event is handler for /1/event/
func (r *Router) event(w http.ResponseWriter, req *http.Request) {
	r.Metrics.Increment(r.incomingOrPeer + "_router_event")
	defer req.Body.Close()

	ctx := req.Context()
	bodyReader, err := r.getMaybeCompressedBody(req)
	if err != nil {
		r.handlerReturnWithError(w, ErrPostBody, err)
		return
	}

	reqBod, err := io.ReadAll(bodyReader)
	if err != nil {
		r.handlerReturnWithError(w, ErrPostBody, err)
		return
	}

	ev, err := r.requestToEvent(ctx, req, reqBod)
	if err != nil {
		r.handlerReturnWithError(w, ErrReqToEvent, err)
		return
	}
	addIncomingUserAgent(ev, getUserAgentFromRequest(req))

	reqID := ctx.Value(types.RequestIDContextKey{})
	err = r.processEvent(ev, reqID)
	if err != nil {
		r.handlerReturnWithError(w, ErrReqToEvent, err)
		return
	}
}

func (r *Router) requestToEvent(ctx context.Context, req *http.Request, reqBod []byte) (*types.Event, error) {
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
	dataset, err := getDatasetFromRequest(req)
	if err != nil {
		return nil, err
	}

	apiHost := r.Config.GetHoneycombAPI()

	// get environment name - will be empty for legacy keys
	environment, err := r.getEnvironmentName(apiKey)
	if err != nil {
		return nil, err
	}

	data := map[string]interface{}{}
	err = unmarshal(req, bytes.NewReader(reqBod), &data)
	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		return nil, fmt.Errorf("empty event data")
	}

	return &types.Event{
		Context:     ctx,
		APIHost:     apiHost,
		APIKey:      apiKey,
		Dataset:     dataset,
		Environment: environment,
		SampleRate:  uint(sampleRate),
		Timestamp:   eventTime,
		Data:        data,
	}, nil
}

func (r *Router) batch(w http.ResponseWriter, req *http.Request) {
	r.Metrics.Increment(r.incomingOrPeer + "_router_batch")
	defer req.Body.Close()

	ctx := req.Context()
	reqID := ctx.Value(types.RequestIDContextKey{})
	debugLog := r.iopLogger.Debug().WithField("request_id", reqID)

	bodyReader, err := r.getMaybeCompressedBody(req)
	if err != nil {
		r.handlerReturnWithError(w, ErrPostBody, err)
		return
	}

	reqBod, err := io.ReadAll(bodyReader)
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
	r.Metrics.Count(r.incomingOrPeer+"_router_batch_events", len(batchedEvents))

	dataset, err := getDatasetFromRequest(req)
	if err != nil {
		r.handlerReturnWithError(w, ErrReqToEvent, err)
	}
	apiHost := r.Config.GetHoneycombAPI()

	apiKey := req.Header.Get(types.APIKeyHeader)
	if apiKey == "" {
		apiKey = req.Header.Get(types.APIKeyHeaderShort)
	}

	// get environment name - will be empty for legacy keys
	environment, err := r.getEnvironmentName(apiKey)
	if err != nil {
		r.handlerReturnWithError(w, ErrReqToEvent, err)
	}

	userAgent := getUserAgentFromRequest(req)
	batchedResponses := make([]*BatchResponse, 0, len(batchedEvents))
	for _, bev := range batchedEvents {
		if len(bev.Data) != 0 {
			ev := &types.Event{
				Context:     ctx,
				APIHost:     apiHost,
				APIKey:      apiKey,
				Dataset:     dataset,
				Environment: environment,
				SampleRate:  bev.getSampleRate(),
				Timestamp:   bev.getEventTime(),
				Data:        bev.Data,
			}

			addIncomingUserAgent(ev, userAgent)
			err = r.processEvent(ev, reqID)
		} else {
			// return an error for empty events
			err = fmt.Errorf("empty event data")
		}

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

func (router *Router) processOTLPRequest(
	ctx context.Context,
	batches []huskyotlp.Batch,
	apiKey string,
	incomingUserAgent string) error {

	var requestID types.RequestIDContextKey
	apiHost := router.Config.GetHoneycombAPI()

	router.Metrics.Increment(router.incomingOrPeer + "_router_otlp")

	// get environment name - will be empty for legacy keys
	environment, err := router.getEnvironmentName(apiKey)
	if err != nil {
		return nil
	}

	totalEvents := 0
	for _, batch := range batches {
		totalEvents += len(batch.Events)
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
			addIncomingUserAgent(event, incomingUserAgent)
			if err = router.processEvent(event, requestID); err != nil {
				router.Logger.Error().Logf("Error processing event: " + err.Error())
			}
		}
	}
	router.Metrics.Count(router.incomingOrPeer+"_router_otlp_events", totalEvents)

	return nil
}

func (r *Router) processEvent(ev *types.Event, reqID interface{}) error {
	debugLog := r.iopLogger.Debug().
		WithField("request_id", reqID).
		WithString("api_host", ev.APIHost).
		WithString("dataset", ev.Dataset).
		WithString("environment", ev.Environment)

	// record the event bytes size
	// we do this early so can include all event types (span, event, log, etc)
	r.Metrics.Histogram(r.incomingOrPeer+"_router_event_bytes", float64(ev.GetDataSize()))

	// check if this is a probe from another refinery; if so, we should drop it
	if ev.Data["meta.refinery.probe"] != nil {
		debugLog.Logf("dropping probe")
		return nil
	}

	traceID := extractTraceID(r.Config.GetTraceIdFieldNames(), ev)
	if traceID == "" {
		// not part of a trace. send along upstream
		r.Metrics.Increment(r.incomingOrPeer + "_router_nonspan")
		debugLog.WithString("api_host", ev.APIHost).
			WithString("dataset", ev.Dataset).
			Logf("sending non-trace event from batch")
		r.UpstreamTransmission.EnqueueEvent(ev)
		return nil
	}
	debugLog = debugLog.WithString("trace_id", traceID)

	span := &types.Span{
		Event:   *ev,
		TraceID: traceID,
		IsRoot:  isRootSpan(ev, r.Config),
	}

	// only record bytes received for incoming traffic when opamp is enabled and record usage is set to true
	if r.incomingOrPeer == "incoming" && r.Config.GetOpAMPConfig().Enabled && r.Config.GetOpAMPConfig().RecordUsage.Get() {
		if span.Data["meta.signal_type"] == "log" {
			r.Metrics.Count("bytes_received_logs", span.GetDataSize())
		} else {
			r.Metrics.Count("bytes_received_traces", span.GetDataSize())
		}
	}

	// we know we're a span, but we need to check if we're in Stress Relief mode;
	// if we are, then we want to make an immediate, deterministic trace decision
	// and either drop or send the trace without even trying to cache or forward it.
	isProbe := false
	if r.Collector.Stressed() {
		// only process spans that are not decision spans through stress relief
		if !span.IsDecisionSpan() {
			processed, kept := r.Collector.ProcessSpanImmediately(span)

			if processed {
				if !kept {
					return nil

				}

				// If the span was kept, we want to generate a probe that we'll forward
				// to a peer IF this span would have been forwarded.
				ev.Data["meta.refinery.probe"] = true
				isProbe = true
			}
		}
	}

	if r.Config.GetCollectionConfig().TraceLocalityEnabled() {
		// Figure out if we should handle this span locally or pass on to a peer
		targetShard := r.Sharder.WhichShard(traceID)
		if !targetShard.Equals(r.Sharder.MyShard()) {
			r.Metrics.Increment(r.incomingOrPeer + "_router_peer")
			debugLog.
				WithString("peer", targetShard.GetAddress()).
				WithField("isprobe", isProbe).
				Logf("Sending span from batch to peer")

			ev.APIHost = targetShard.GetAddress()

			// Unfortunately this doesn't tell us if the event was actually
			// enqueued; we need to watch the response channel to find out, at
			// which point it's too late to tell the client.
			r.PeerTransmission.EnqueueEvent(ev)
			return nil
		}
	}

	if isProbe {
		// If we got here it's because the span we were using for a probe was
		// intended for us, so just skip it.
		return nil
	}

	// we're supposed to handle it normally
	var err error
	if r.incomingOrPeer == "incoming" {
		err = r.Collector.AddSpan(span)
	} else {
		err = r.Collector.AddSpanFromPeer(span)
	}
	if err != nil {
		r.Metrics.Increment(r.incomingOrPeer + "_router_dropped")
		debugLog.Logf("Dropping span from batch, channel full")
		return err
	}

	r.Metrics.Increment(r.incomingOrPeer + "_router_span")

	debugLog.WithField("source", r.incomingOrPeer).Logf("Accepting span from batch for collection into a trace")
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
		if _, err := io.Copy(buf, io.LimitReader(gzipReader, HTTPMessageSizeMax)); err != nil {
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
		if _, err := io.Copy(buf, io.LimitReader(zReader, HTTPMessageSizeMax)); err != nil {
			return nil, err
		}

		reader = buf
	default:
		reader = req.Body
	}
	return reader, nil
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

func (b *batchedEvent) getSampleRate() uint {
	if b.SampleRate == 0 {
		return defaultSampleRate
	}
	return uint(b.SampleRate)
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
		decoder := msgpack.NewDecoder(data)
		decoder.UseLooseInterfaceDecoding(true)
		return decoder.Decode(v)
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

type environmentCache struct {
	mutex sync.RWMutex
	items map[string]*cacheItem
	ttl   time.Duration
	getFn func(string) (string, error)
}

func (r *Router) SetEnvironmentCache(ttl time.Duration, getFn func(string) (string, error)) {
	r.environmentCache = newEnvironmentCache(ttl, getFn)
}

func newEnvironmentCache(ttl time.Duration, getFn func(string) (string, error)) *environmentCache {
	return &environmentCache{
		items: make(map[string]*cacheItem),
		ttl:   ttl,
		getFn: getFn,
	}
}

type cacheItem struct {
	expiresAt time.Time
	value     string
}

// get queries the cached items, returning cache hits that have not expired.
// Cache missed use the configured getFn to populate the cache.
func (c *environmentCache) get(key string) (string, error) {
	var val string
	// get read lock so that we don't attempt to read from the map
	// while another routine has a write lock and is actively writing
	// to the map.
	c.mutex.RLock()
	if item, ok := c.items[key]; ok {
		if time.Now().Before(item.expiresAt) {
			val = item.value
		}
	}
	c.mutex.RUnlock()
	if val != "" {
		return val, nil
	}

	// get write lock early so we don't execute getFn in parallel so the
	// the result will be cached before the next lock is acquired to prevent
	// subsequent calls to getFn for the same key
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// check if the cache has been populated while waiting for a write lock
	if item, ok := c.items[key]; ok {
		if time.Now().Before(item.expiresAt) {
			return item.value, nil
		}
	}

	val, err := c.getFn(key)
	if err != nil {
		return "", err
	}

	c.addItem(key, val, c.ttl)
	return val, nil
}

// addItem create a new cache entry in the environment cache.
// This is not thread-safe, and should only be used in tests
func (c *environmentCache) addItem(key string, value string, ttl time.Duration) {
	c.items[key] = &cacheItem{
		expiresAt: time.Now().Add(ttl),
		value:     value,
	}
}

type TeamInfo struct {
	Slug string `json:"slug"`
}

type EnvironmentInfo struct {
	Slug string `json:"slug"`
	Name string `json:"name"`
}

type AuthInfo struct {
	APIKeyAccess map[string]bool `json:"api_key_access"`
	Team         TeamInfo        `json:"team"`
	Environment  EnvironmentInfo `json:"environment"`
}

func (r *Router) getEnvironmentName(apiKey string) (string, error) {
	if apiKey == "" || types.IsLegacyAPIKey(apiKey) {
		return "", nil
	}

	env, err := r.environmentCache.get(apiKey)
	if err != nil {
		return "", err
	}
	return env, nil
}

func (r *Router) lookupEnvironment(apiKey string) (string, error) {
	apiEndpoint := r.Config.GetHoneycombAPI()
	authURL, err := url.Parse(apiEndpoint)
	if err != nil {
		return "", fmt.Errorf("failed to parse Honeycomb API URL config value. %w", err)
	}

	authURL.Path = "/1/auth"
	req, err := http.NewRequest("GET", authURL.String(), nil)
	if err != nil {
		return "", fmt.Errorf("failed to create AuthInfo request. %w", err)
	}

	req.Header.Set("x-Honeycomb-team", apiKey)

	r.Logger.Debug().WithString("endpoint", authURL.String()).Logf("Attempting to get environment name using API key")
	resp, err := r.proxyClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed sending AuthInfo request to Honeycomb API. %w", err)
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusUnauthorized:
		return "", fmt.Errorf("received 401 response for AuthInfo request from Honeycomb API - check your API key")
	case resp.StatusCode > 299:
		return "", fmt.Errorf("received %d response for AuthInfo request from Honeycomb API", resp.StatusCode)
	}

	authinfo := AuthInfo{}
	if err := json.NewDecoder(resp.Body).Decode(&authinfo); err != nil {
		return "", fmt.Errorf("failed to JSON decode of AuthInfo response from Honeycomb API")
	}
	r.Logger.Debug().WithString("environment", authinfo.Environment.Name).Logf("Got environment")
	return authinfo.Environment.Name, nil
}

func (r *Router) Check(ctx context.Context, req *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	r.iopLogger.Debug().Logf("answered grpc_health_v1 check")
	return &grpc_health_v1.HealthCheckResponse{
		Status: grpc_health_v1.HealthCheckResponse_SERVING,
	}, nil
}

func (r *Router) Watch(req *grpc_health_v1.HealthCheckRequest, server grpc_health_v1.Health_WatchServer) error {
	r.iopLogger.Debug().Logf("serving grpc_health_v1 watch")
	return server.Send(&grpc_health_v1.HealthCheckResponse{
		Status: grpc_health_v1.HealthCheckResponse_SERVING,
	})
}

// startGRPCHealthMonitor starts a goroutine that periodically checks the health of the system and updates the grpc health server
func (r *Router) startGRPCHealthMonitor() {
	const (
		system      = "" // empty string represents the generic health of the whole system (corresponds to "ready")
		systemReady = "ready"
		systemAlive = "alive"
	)
	r.iopLogger.Debug().Logf("running grpc health monitor")

	setStatus := func(svc string, stat bool) {
		if stat {
			r.hsrv.SetServingStatus(svc, grpc_health_v1.HealthCheckResponse_SERVING)
		} else {
			r.hsrv.SetServingStatus(svc, grpc_health_v1.HealthCheckResponse_NOT_SERVING)
		}
	}

	r.doneWG.Add(1)
	go func() {
		defer r.doneWG.Done()
		// TODO: Does this time need to be configurable?
		watchticker := time.NewTicker(3 * time.Second)
		defer watchticker.Stop()
		for {
			select {
			case <-watchticker.C:
				alive := r.Health.IsAlive()
				ready := r.Health.IsReady()

				// we can just update everything because the grpc health server will only send updates if the status changes
				setStatus(systemReady, ready)
				setStatus(systemAlive, alive)
				setStatus(system, ready && alive)
			case <-r.donech:
				return
			}
		}
	}()
}

// AddOTLPMuxxer adds muxxer for OTLP requests
func (r *Router) AddOTLPMuxxer(muxxer *mux.Router) {
	// require an auth header for OTLP requests
	otlpMuxxer := muxxer.PathPrefix("/v1/").Methods("POST").Subrouter()
	otlpMuxxer.Use(r.apiKeyProcessor)

	// handle OTLP trace requests
	otlpMuxxer.HandleFunc("/traces", r.postOTLPTrace).Name("otlp_traces")
	otlpMuxxer.HandleFunc("/traces/", r.postOTLPTrace).Name("otlp_traces")

	// handle OTLP logs requests
	otlpMuxxer.HandleFunc("/logs", r.postOTLPLogs).Name("otlp_logs")
	otlpMuxxer.HandleFunc("/logs/", r.postOTLPLogs).Name("otlp_logs")
}

func getDatasetFromRequest(req *http.Request) (string, error) {
	dataset := mux.Vars(req)["datasetName"]
	if dataset == "" {
		return "", fmt.Errorf("missing dataset name")
	}
	dataset, err := url.PathUnescape(dataset)
	if err != nil {
		return "", err
	}
	return dataset, nil
}

func isRootSpan(ev *types.Event, cfg config.Config) bool {
	// log event should never be considered a root span, check for that first
	if signalType := ev.Data["meta.signal_type"]; signalType == "log" {
		return false
	}

	// check if the event has a root flag
	if isRoot, ok := ev.Data["meta.refinery.root"]; ok {
		v, ok := isRoot.(bool)
		if !ok {
			return false
		}

		return v
	}

	// check if the event has a parent id using the configured parent id field names
	for _, parentIdFieldName := range cfg.GetParentIdFieldNames() {
		parentId := ev.Data[parentIdFieldName]
		if _, ok := parentId.(string); ok && parentId != "" {
			return false
		}
	}
	return true
}

func extractTraceID(traceIdFieldNames []string, ev *types.Event) string {
	if trID, ok := ev.Data["meta.trace_id"]; ok {
		return trID.(string)
	}

	for _, traceIdFieldName := range traceIdFieldNames {
		if trID, ok := ev.Data[traceIdFieldName]; ok {
			return trID.(string)
		}
	}

	return ""
}

func getUserAgentFromRequest(req *http.Request) string {
	return req.Header.Get("User-Agent")
}

func addIncomingUserAgent(ev *types.Event, userAgent string) {
	if userAgent != "" && ev.Data["meta.refinery.incoming_user_agent"] == nil {
		ev.Data["meta.refinery.incoming_user_agent"] = userAgent
	}
}

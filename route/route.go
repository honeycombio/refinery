package route

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"net/url"
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

	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
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
	Metrics              metrics.Metrics       `inject:"metrics"`

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

	// used to identify Router as a OTLP TraceServer
	collectortrace.UnimplementedTraceServiceServer

	environmentCache *environmentCache
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
	r.environmentCache = newEnvironmentCache(r.Config.GetEnvironmentCacheTTL(), r.lookupEnvironment)

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

	// require an auth header for OTLP requests
	otlpMuxxer := muxxer.PathPrefix("/v1/").Methods("POST").Subrouter()
	otlpMuxxer.Use(r.apiKeyChecker)

	// handle OTLP trace requests
	otlpMuxxer.HandleFunc("/traces", r.postOTLP).Name("otlp")

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
	r.Metrics.Increment(r.incomingOrPeer + "_router_event")
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

	return &types.Event{
		Context:     req.Context(),
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

	apiKey := req.Header.Get(types.APIKeyHeader)
	if apiKey == "" {
		apiKey = req.Header.Get(types.APIKeyHeaderShort)
	}

	// get environment name - will be empty for legacy keys
	environment, err := r.getEnvironmentName(apiKey)
	if err != nil {
		r.handlerReturnWithError(w, ErrReqToEvent, err)
	}

	batchedResponses := make([]*BatchResponse, 0, len(batchedEvents))
	for _, bev := range batchedEvents {
		ev, err := r.batchedEventToEvent(req, bev, apiKey, environment)
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

func (r *Router) processEvent(ev *types.Event, reqID interface{}) error {
	debugLog := r.iopLogger.Debug().
		WithField("request_id", reqID).
		WithString("api_host", ev.APIHost).
		WithString("dataset", ev.Dataset).
		WithString("environment", ev.Environment)

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
		r.Metrics.Increment(r.incomingOrPeer + "_router_nonspan")
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
		r.Metrics.Increment(r.incomingOrPeer + "_router_peer")
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
		r.Metrics.Increment(r.incomingOrPeer + "_router_dropped")
		debugLog.Logf("Dropping span from batch, channel full")
		return err
	}

	r.Metrics.Increment(r.incomingOrPeer + "_router_span")
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

func (r *Router) batchedEventToEvent(req *http.Request, bev batchedEvent, apiKey string, environment string) (*types.Event, error) {
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
		Context:     req.Context(),
		APIHost:     apiHost,
		APIKey:      apiKey,
		Dataset:     dataset,
		Environment: environment,
		SampleRate:  uint(sampleRate),
		Timestamp:   eventTime,
		Data:        bev.Data,
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
	if item, ok := c.items[key]; ok {
		if time.Now().Before(item.expiresAt) {
			return item.value, nil
		}
	}

	// get write lock early so we don't execute getFn in parallel so the
	// the result will be cached before the next lock is aquired to prevent
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
	apiEndpoint, err := r.Config.GetHoneycombAPI()
	if err != nil {
		return "", fmt.Errorf("failed to read Honeycomb API config value. %w", err)
	}
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

	r.Logger.Debug().WithString("api_key", apiKey).WithString("endpoint", authURL.String()).Logf("Attempting to get environment name using API key")
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

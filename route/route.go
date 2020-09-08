package route

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/klauspost/compress/zstd"
	"github.com/tinylib/msgp/msgp"
	"github.com/vmihailenco/msgpack/v4"

	"github.com/honeycombio/samproxy/collect"
	"github.com/honeycombio/samproxy/config"
	"github.com/honeycombio/samproxy/logger"
	"github.com/honeycombio/samproxy/metrics"
	"github.com/honeycombio/samproxy/sharder"
	"github.com/honeycombio/samproxy/transmit"
	"github.com/honeycombio/samproxy/types"
)

const (
	// numZstdDecoders is set statically here - we may make it into a config option
	// A normal practice might be to use some multiple of the CPUs, but that goes south
	// in kubernetes
	numZstdDecoders = 4
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
	r.Metrics.Register(r.incomingOrPeer+"_router_span", "counter")
	r.Metrics.Register(r.incomingOrPeer+"_router_peer", "counter")

	muxxer := mux.NewRouter()

	muxxer.Use(r.setResponseHeaders)
	muxxer.Use(r.requestLogger)
	muxxer.Use(r.panicCatcher)

	// answer a basic health check locally
	muxxer.HandleFunc("/alive", r.alive).Name("local health")
	muxxer.HandleFunc("/panic", r.panic).Name("intentional panic")
	muxxer.HandleFunc("/version", r.version).Name("report version info")

	// require an auth header for events and batches
	authedMuxxer := muxxer.PathPrefix("/1/").Methods("POST").Subrouter()
	authedMuxxer.Use(r.apiKeyChecker)

	// handle events and batches
	authedMuxxer.HandleFunc("/events/{datasetName}", r.event).Name("event")
	authedMuxxer.HandleFunc("/batch/{datasetName}", r.batch).Name("batch")

	// pass everything else through unmolested
	muxxer.PathPrefix("/").HandlerFunc(r.proxy).Name("proxy")

	var listenAddr string
	if r.incomingOrPeer == "incoming" {
		listenAddr, err = r.Config.GetListenAddr()
		if err != nil {
			r.iopLogger.Error().Logf("failed to get listen addr config: %s", err)
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
	err = http.ListenAndServe(listenAddr, muxxer)
	if err != nil {
		r.iopLogger.Error().Logf("failed to ListenAndServe: %s", err)
	}
}

func (r *Router) alive(w http.ResponseWriter, req *http.Request) {
	r.iopLogger.Debug().Logf("answered /x/alive check")
	w.Write([]byte(`{"source":"samproxy","alive":true}`))
}

func (r *Router) panic(w http.ResponseWriter, req *http.Request) {
	panic("panic? never!")
}

func (r *Router) version(w http.ResponseWriter, req *http.Request) {
	w.Write([]byte(fmt.Sprintf(`{"source":"samproxy","version":"%s"}`, r.versionStr)))
}

// event is handler for /1/event/
func (r *Router) event(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()

	// get out request ID for logging
	reqID := req.Context().Value(types.RequestIDContextKey{})
	debugLog := r.iopLogger.Debug().WithField("request_id", reqID)

	reqBod, _ := ioutil.ReadAll(req.Body)
	var trEv eventWithTraceID
	// pull out just the trace ID for use in routing
	err := unmarshal(req, bytes.NewReader(reqBod), &trEv)
	if err != nil {
		debugLog.WithField("error", err.Error()).WithField("request.url", req.URL).WithField("json_body", string(reqBod)).Logf("error parsing json")
		r.handlerReturnWithError(w, ErrJSONFailed, err)
		return
	}

	// not part of a trace. send along upstream
	if trEv.TraceID == "" {
		r.Metrics.IncrementCounter(r.incomingOrPeer + "_router_event")
		ev, err := r.requestToEvent(req, reqBod)
		if err != nil {
			r.handlerReturnWithError(w, ErrReqToEvent, err)
			return
		}
		ev.Type = types.EventTypeEvent
		ev.Target = types.TargetUpstream
		debugLog.WithFields(map[string]interface{}{
			"api_host": ev.APIHost,
			"dataset":  ev.Dataset,
		}).Logf("sending non-trace event")
		r.UpstreamTransmission.EnqueueEvent(ev)
		return
	}

	// ok, we're a span. Figure out if we should handle locally or pass on to a
	// peer
	targetShard := r.Sharder.WhichShard(trEv.TraceID)
	if !targetShard.Equals(r.Sharder.MyShard()) {
		// it's not for us; send to the peer
		r.Metrics.IncrementCounter(r.incomingOrPeer + "_router_peer")
		ev, err := r.requestToEvent(req, reqBod)
		if err != nil {
			r.handlerReturnWithError(w, ErrReqToEvent, err)
			return
		}
		ev.Type = types.EventTypeSpan
		ev.Target = types.TargetPeer
		ev.APIHost = targetShard.GetAddress()
		debugLog.WithFields(map[string]interface{}{
			"api_host": ev.APIHost,
			"dataset":  ev.Dataset,
			"trace_id": trEv.TraceID,
			"peer":     targetShard.GetAddress(),
		}).Logf("Sending span to my peer")
		r.PeerTransmission.EnqueueEvent(ev)
		return
	}
	// we're supposed to handle it
	ev, err := r.requestToEvent(req, reqBod)
	if err != nil {
		r.handlerReturnWithError(w, ErrReqToEvent, err)
		return
	}
	ev.Type = types.EventTypeSpan
	ev.Target = types.TargetUpstream
	span := &types.Span{
		Event:   *ev,
		TraceID: trEv.TraceID,
	}
	r.Metrics.IncrementCounter(r.incomingOrPeer + "_router_span")
	debugLog.WithFields(map[string]interface{}{
		"api_host": ev.APIHost,
		"dataset":  ev.Dataset,
		"trace_id": trEv.TraceID,
	}).Logf("Accepting span for collection into a trace")
	if r.incomingOrPeer == "incoming" {
		r.Collector.AddSpan(span)
	} else {
		r.Collector.AddSpanFromPeer(span)
	}
}

type eventWithTraceID struct {
	TraceID string
}

func (ev *eventWithTraceID) UnmarshalJSON(b []byte) error {
	return ev.Unmarshal(b, json.Unmarshal)
}

func (ev *eventWithTraceID) UnmarshalMsgpack(b []byte) error {
	return ev.Unmarshal(b, msgpack.Unmarshal)
}

func (ev *eventWithTraceID) Unmarshal(b []byte, u func(d []byte, v interface{}) error) error {
	var e map[string]string

	err := u(b, &e)

	if err != nil {
		return err
	}

	if id := e["trace.trace_id"]; id != "" {
		ev.TraceID = id
	} else if id := e["traceId"]; id != "" {
		ev.TraceID = id
	}

	return nil
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
	batchedResponses := make([]*BatchResponse, 0)
	err = unmarshal(req, bytes.NewReader(reqBod), &batchedEvents)
	if err != nil {
		debugLog.WithField("error", err.Error()).WithField("request.url", req.URL).WithField("json_body", string(reqBod)).Logf("error parsing json")
		r.handlerReturnWithError(w, ErrJSONFailed, err)
		return
	}

	for _, bev := range batchedEvents {
		// extract trace ID, route to self or peer, pass on to collector
		// TODO make trace ID field configurable
		var traceID string
		if trID, ok := bev.Data["trace.trace_id"]; ok {
			traceID = trID.(string)
		} else if trID, ok := bev.Data["traceId"]; ok {
			traceID = trID.(string)
		}
		if traceID == "" {
			// not part of a trace. send along upstream
			r.Metrics.IncrementCounter(r.incomingOrPeer + "_router_event")
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
			batchedResponses = append(
				batchedResponses,
				&BatchResponse{Status: http.StatusAccepted},
			)
			debugLog.WithFields(map[string]interface{}{
				"api_host": ev.APIHost,
				"dataset":  ev.Dataset,
			}).Logf("sending non-trace event from batch")
			r.UpstreamTransmission.EnqueueEvent(ev)
			continue
		}
		// ok, we're a span. Figure out if we should handle locally or pass on to a peer
		targetShard := r.Sharder.WhichShard(traceID)
		if !targetShard.Equals(r.Sharder.MyShard()) {
			r.Metrics.IncrementCounter(r.incomingOrPeer + "_router_peer")
			ev, err := r.batchedEventToEvent(req, bev)
			if err != nil {
				batchedResponses = append(
					batchedResponses,
					&BatchResponse{
						Status: http.StatusBadRequest,
						Error:  fmt.Sprintf("failed to process event: %s", err.Error()),
					},
				)
				continue
			}
			debugLog.WithFields(map[string]interface{}{
				"api_host": ev.APIHost,
				"dataset":  ev.Dataset,
				"trace_id": traceID,
				"peer":     targetShard.GetAddress(),
			}).Logf("Sending span from batch to my peer")
			batchedResponses = append(
				batchedResponses,
				&BatchResponse{Status: http.StatusAccepted},
			)
			ev.APIHost = targetShard.GetAddress()
			r.PeerTransmission.EnqueueEvent(ev)
			continue
		}
		// we're supposed to handle it
		ev, err := r.batchedEventToEvent(req, bev)
		if err != nil {
			batchedResponses = append(
				batchedResponses,
				&BatchResponse{
					Status: http.StatusBadRequest,
					Error:  fmt.Sprintf("failed to process event: %s", err.Error()),
				},
			)
			continue
		}

		span := &types.Span{
			Event:   *ev,
			TraceID: traceID,
		}
		r.Metrics.IncrementCounter(r.incomingOrPeer + "_router_span")
		debugLog.WithFields(map[string]interface{}{
			"api_host": ev.APIHost,
			"dataset":  ev.Dataset,
			"trace_id": span.TraceID,
		}).Logf("Accepting span from batch for collection into a trace")
		batchedResponses = append(
			batchedResponses,
			&BatchResponse{Status: http.StatusAccepted},
		)
		if r.incomingOrPeer == "incoming" {
			r.Collector.AddSpan(span)
		} else {
			r.Collector.AddSpanFromPeer(span)
		}
	}
	response, err := json.Marshal(batchedResponses)
	if err != nil {
		r.handlerReturnWithError(w, ErrJSONBuildFailed, err)
		return
	}
	w.Write(response)
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
		return *b.MsgPackTimestamp
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
	return eventTime
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
		// First try to decode with the fast but fussy msgp. If that fails,
		// defer to the much more friendly msgpack.
		if decodable, ok := v.(msgp.Decodable); ok {
			err := msgp.Decode(data, decodable)

			if err == nil {
				return nil
			}
		}

		return msgpack.NewDecoder(data).
			UseDecodeInterfaceLoose(true).
			Decode(v)
	case "application/json":
		return json.NewDecoder(data).Decode(v)
	default:
		return errors.New("Bad Content-Type")
	}
}

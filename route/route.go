package route

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"

	"github.com/honeycombio/samproxy/collect"
	"github.com/honeycombio/samproxy/config"
	"github.com/honeycombio/samproxy/logger"
	"github.com/honeycombio/samproxy/metrics"
	"github.com/honeycombio/samproxy/sharder"
	"github.com/honeycombio/samproxy/transmit"
	"github.com/honeycombio/samproxy/types"
)

type Router struct {
	Config       config.Config         `inject:""`
	Logger       logger.Logger         `inject:""`
	HTTPClient   *http.Client          `inject:"upstreamClient"`
	Transmission transmit.Transmission `inject:""`
	Sharder      sharder.Sharder       `inject:""`
	Collector    collect.Collector     `inject:""`
	Metrics      metrics.Metrics       `inject:""`
}

func (r *Router) LnS() {

	r.Metrics.Register("router_proxied", "counter")
	r.Metrics.Register("router_event", "counter")
	r.Metrics.Register("router_batch", "counter")
	r.Metrics.Register("router_span", "counter")
	r.Metrics.Register("router_peer", "counter")

	muxxer := mux.NewRouter()

	muxxer.Use(r.setResponseHeaders)
	muxxer.Use(r.requestLogger)
	muxxer.Use(r.panicCatcher)

	// answer a basic health check locally
	muxxer.HandleFunc("/alive", r.alive).Name("local health")
	muxxer.HandleFunc("/panic", r.panic).Name("intentional panic")

	// require an auth header for events and batches
	authedMuxxer := muxxer.PathPrefix("/1/").Methods("POST").Subrouter()
	authedMuxxer.Use(r.apiKeyChecker)

	// handle events and batches
	authedMuxxer.HandleFunc("/events/{datasetName}", r.event).Name("event")
	authedMuxxer.HandleFunc("/batch/{datasetName}", r.batch).Name("batch")

	// pass everything else through unmolested
	muxxer.PathPrefix("/").HandlerFunc(r.proxy).Name("proxy")

	listenAddr, err := r.Config.GetListenAddr()
	if err != nil {
		r.Logger.Errorf("failed to get listen addr config: %s", err)
		return
	}

	r.Logger.Infof("Listening on %s", listenAddr)
	err = http.ListenAndServe(listenAddr, muxxer)
	if err != nil {
		r.Logger.Errorf("failed to ListenAndServe: %s", err)
	}
}

func (r *Router) alive(w http.ResponseWriter, req *http.Request) {
	w.Write([]byte(`{"source":"samproxy","alive":true}`))
}

func (r *Router) panic(w http.ResponseWriter, req *http.Request) {
	panic("panic? never!")
}

// event is handler for /1/event/
func (r *Router) event(w http.ResponseWriter, req *http.Request) {
	r.Logger.Debugf("handling POST to /1/event")
	defer req.Body.Close()
	reqBod, _ := ioutil.ReadAll(req.Body)
	var trEv eventWithTraceID
	// pull out just the trace ID for use in routing
	err := json.Unmarshal(reqBod, &trEv)
	if err != nil {
		r.handlerReturnWithError(w, ErrJSONFailed, err)
		return
	}

	// not part of a trace. send along upstream
	if trEv.TraceID == "" {
		r.Metrics.IncrementCounter("router_event")
		ev, err := r.requestToEvent(req, reqBod)
		if err != nil {
			r.handlerReturnWithError(w, ErrReqToEvent, err)
			return
		}
		r.Logger.Debugf("received a non-trace event. sending along")
		r.Transmission.EnqueueEvent(ev)
		return
	}

	// ok, we're a span. Figure out if we should handle locally or pass on to a
	// peer
	targetShard := r.Sharder.WhichShard(trEv.TraceID)
	r.Logger.Debugf("got target shard of %s and my shard is %s", targetShard, r.Sharder.MyShard())
	if !targetShard.Equals(r.Sharder.MyShard()) {
		r.Logger.Debugf("Sending span to my peer %s", targetShard)
		r.Metrics.IncrementCounter("router_peer")
		ev, err := r.requestToEvent(req, reqBod)
		if err != nil {
			r.handlerReturnWithError(w, ErrReqToEvent, err)
			return
		}
		ev.APIHost = targetShard.GetAddress()
		r.Transmission.EnqueueEvent(ev)
		return
	}
	// we're supposed to handle it
	ev, err := r.requestToEvent(req, reqBod)
	if err != nil {
		r.handlerReturnWithError(w, ErrReqToEvent, err)
		return
	}
	span := &types.Span{
		Event:   *ev,
		TraceID: trEv.TraceID,
	}
	r.Metrics.IncrementCounter("router_span")
	r.Collector.AddSpan(span)
}

type eventWithTraceID struct {
	TraceID string
}

func (ev *eventWithTraceID) UnmarshalJSON(b []byte) error {
	type bothTraceIDs struct {
		BeelineTraceID string `json:"trace.trace_id"`
		ZipkinTraceID  string `json:"traceId"`
	}
	var rawEv bothTraceIDs
	err := json.Unmarshal(b, &rawEv)
	if err != nil {
		return err
	}
	if rawEv.BeelineTraceID != "" {
		ev.TraceID = rawEv.BeelineTraceID
	} else if rawEv.ZipkinTraceID != "" {
		ev.TraceID = rawEv.ZipkinTraceID
	}
	return nil
}

func (r *Router) requestToEvent(req *http.Request, reqBod []byte) (*types.Event, error) {
	// get necessary bits out of the incoming event
	apiKey := req.Header.Get(types.APIKeyHeader)
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
	err = json.Unmarshal(reqBod, &data)
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
	r.Logger.Debugf("handling POST to /1/batch")
	r.Metrics.IncrementCounter("router_batch")
	defer req.Body.Close()
	bodyReader, err := getMaybeGzippedBody(req)
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
	batchedResponses := make([]int, 0)
	err = json.Unmarshal(reqBod, &batchedEvents)
	if err != nil {
		r.handlerReturnWithError(w, ErrJSONFailed, err)
		return
	}

	for _, bev := range batchedEvents {
		// extract trace ID, route to self or peer, pass on to collector
		var traceID string
		if trID, ok := bev.Data["trace.trace_id"]; ok {
			traceID = trID.(string)
		} else if trID, ok := bev.Data["traceId"]; ok {
			traceID = trID.(string)
		}
		if traceID == "" {
			// not part of a trace. send along upstream
			r.Metrics.IncrementCounter("router_event")
			ev, err := r.batchedEventToEvent(req, bev)
			if err != nil {
				batchedResponses = append(batchedResponses, http.StatusBadRequest)
				r.Logger.Debugf("batch element failed to convert to event: %s", err.Error())
				continue
			}
			r.Logger.Debugf("received a non-trace event. sending along")
			batchedResponses = append(batchedResponses, http.StatusAccepted)
			r.Transmission.EnqueueEvent(ev)
			continue
		}
		// ok, we're a span. Figure out if we should handle locally or pass on to a peer
		targetShard := r.Sharder.WhichShard(traceID)
		r.Logger.Debugf("got target shard of %s and my shard is %s", targetShard, r.Sharder.MyShard())
		if !targetShard.Equals(r.Sharder.MyShard()) {
			r.Logger.Debugf("Sending span to my peer %s", targetShard)
			r.Metrics.IncrementCounter("router_peer")
			ev, err := r.batchedEventToEvent(req, bev)
			if err != nil {
				batchedResponses = append(batchedResponses, http.StatusBadRequest)
				continue
			}
			ev.APIHost = targetShard.GetAddress()
			r.Transmission.EnqueueEvent(ev)
			continue
		}
		// we're supposed to handle it
		ev, err := r.batchedEventToEvent(req, bev)
		if err != nil {
			batchedResponses = append(batchedResponses, http.StatusBadRequest)
			continue
		}

		span := &types.Span{
			Event:   *ev,
			TraceID: traceID,
		}
		r.Metrics.IncrementCounter("router_span")
		r.Logger.Debugf("handing off span to collector: %+v", span.Data)
		batchedResponses = append(batchedResponses, http.StatusAccepted)
		r.Collector.AddSpan(span)
	}
	response, err := json.Marshal(batchedResponses)
	if err != nil {
		r.handlerReturnWithError(w, ErrJSONBuildFailed, err)
		return
	}
	w.Write(response)
}

func getMaybeGzippedBody(req *http.Request) (io.Reader, error) {
	var reader io.Reader
	switch req.Header.Get("Content-Encoding") {
	case "gzip":
		buf := bytes.Buffer{}
		if _, err := io.Copy(&buf, req.Body); err != nil {
			return nil, err
		}
		var err error
		reader, err = gzip.NewReader(&buf)
		if err != nil {
			return nil, err
		}
	default:
		reader = req.Body
	}
	return reader, nil
}

func (r *Router) batchedEventToEvent(req *http.Request, bev batchedEvent) (*types.Event, error) {
	apiKey := req.Header.Get(types.APIKeyHeader)
	sampleRate := bev.SampleRate
	if sampleRate == 0 {
		sampleRate = 1
	}
	eventTime := getEventTime(bev.Timestamp)
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
	Timestamp  string                 `json:"time"`
	SampleRate int64                  `json:"samplerate"`
	Data       map[string]interface{} `json:"data"`
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

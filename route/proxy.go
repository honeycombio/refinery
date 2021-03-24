package route

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
)

// proxy will pass the request through to Honeycomb unchanged and relay the
// response, blocking until it gets one. This is used for all non-event traffic
// (eg team api key verification, markers, etc.)
func (r *Router) proxy(w http.ResponseWriter, req *http.Request) {
	r.Metrics.Increment(r.incomingOrPeer + "_router_proxied")
	r.Logger.Debug().Logf("proxying request for %s", req.URL.Path)
	upstreamTarget, err := r.Config.GetHoneycombAPI()
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		io.WriteString(w, `{"error":"upstream target unavailable"}`)
		r.Logger.Error().Logf("error getting honeycomb API config: %s", err)
		return
	}
	forwarded := req.Header.Get("X-Forwarded-For")
	// let's copy the request over to a new one and
	// dispatch it upstream
	defer req.Body.Close()
	reqBod, _ := ioutil.ReadAll(req.Body)
	buf := bytes.NewBuffer(reqBod)
	upstreamReq, err := http.NewRequest(req.Method, upstreamTarget+req.URL.String(), buf)
	if err != nil {
		r.handlerReturnWithError(w, ErrUpstreamFailed, err)
		return
	}
	// add context to propagate timeouts and things
	upstreamReq = upstreamReq.WithContext(req.Context())
	// copy over headers from upstream to the upstream service
	for header, vals := range req.Header {
		upstreamReq.Header.Set(header, strings.Join(vals, ","))
	}
	if forwarded != "" {
		upstreamReq.Header.Set("X-Forwarded-For", forwarded+", "+req.RemoteAddr)
	} else {
		upstreamReq.Header.Set("X-Forwarded-For", req.RemoteAddr)
	}
	// call the upstream service
	resp, err := r.proxyClient.Do(upstreamReq)
	if err != nil {
		r.handlerReturnWithError(w, ErrUpstreamUnavailable, err)
		return
	}
	// ok, we got a response, let's pass it along
	defer resp.Body.Close()
	// copy over headers
	for header, vals := range resp.Header {
		w.Header().Set(header, strings.Join(vals, ","))
	}
	// copy over status code
	w.WriteHeader(resp.StatusCode)
	// copy over body
	io.Copy(w, resp.Body)
}

package route

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/honeycombio/samproxy/types"
)

func (r *Router) apiKeyChecker(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		apiKey := req.Header.Get(types.APIKeyHeader)
		if apiKey == "" {
			err := errors.New("no " + types.APIKeyHeader + " header found from within authing middleware")
			r.handlerReturnWithError(w, ErrAuthNeeded, err)
			return
		}
		allowedKeys, err := r.Config.GetAPIKeys()
		if err != nil {
			r.handlerReturnWithError(w, ErrConfigReadFailed, err)
			return
		}
		for _, key := range allowedKeys {
			if key == "*" {
				// all keys are allowed, it's all good
				next.ServeHTTP(w, req)
				return
			}
			if apiKey == key {
				// we're in the whitelist, it's all good
				next.ServeHTTP(w, req)
				return
			}
		}
		err = errors.New(fmt.Sprintf("api key %s not found in list of authed keys", apiKey))
		r.handlerReturnWithError(w, ErrAuthNeeded, err)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (rec *statusRecorder) WriteHeader(code int) {
	rec.status = code
	rec.ResponseWriter.WriteHeader(code)
}

// panicCatcher recovers any panics, sets a 500, and returns an obvious error
func (r *Router) panicCatcher(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		defer func() {
			if rcvr := recover(); rcvr != nil {
				r.handlerReturnWithError(w, ErrCaughtPanic, fmt.Errorf("caught panic: %v", rcvr))
			}
		}()
		next.ServeHTTP(w, req)
	})
}

// requestLogger logs one line debug per request that comes through samproxy
func (r *Router) requestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		arrivalTime := time.Now()
		remoteIP := req.RemoteAddr
		url := req.URL.String()
		method := req.Method

		route := mux.CurrentRoute(req)

		// go ahead and process the request
		wrapped := statusRecorder{w, 200}
		next.ServeHTTP(&wrapped, req)

		// calculate duration
		dur := float64(time.Since(arrivalTime)) / float64(time.Millisecond)

		// log that we did so TODO better formatted http log line
		r.Logger.Debugf("handled %s request %s %s %s %f %d", route.GetName(), remoteIP, method, url, dur, wrapped.status)
	})
}

func (r *Router) setResponseHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {

		// Set content type header early so it's before any calls to WriteHeader
		w.Header().Set("Content-Type", "application/json")

		// Allow cross-origin API operation from browser js
		w.Header().Set("Access-Control-Allow-Origin", "*")
		next.ServeHTTP(w, req)

	})
}

package route

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/honeycombio/refinery/types"
)

// for generating request IDs
func init() {
	rand.Seed(time.Now().UnixNano())
}

func (r *Router) apiKeyChecker(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		apiKey := req.Header.Get(types.APIKeyHeader)
		if apiKey == "" {
			apiKey = req.Header.Get(types.APIKeyHeaderShort)
		}
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
				// we're in the allowlist, it's all good
				next.ServeHTTP(w, req)
				return
			}
		}
		err = fmt.Errorf("api key %s not found in list of authed keys", apiKey)
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
				err, ok := rcvr.(error)

				if !ok {
					err = fmt.Errorf("caught panic: %v", rcvr)
				}

				r.handlerReturnWithError(w, ErrCaughtPanic, err)
			}
		}()
		next.ServeHTTP(w, req)
	})
}

// requestLogger logs one line debug per request that comes through Refinery
func (r *Router) requestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		arrivalTime := time.Now()
		remoteIP := req.RemoteAddr
		url := req.URL.String()
		method := req.Method
		route := mux.CurrentRoute(req)

		// generate a request ID and put it in the context for logging
		reqID := randStringBytes(8)
		req = req.WithContext(context.WithValue(req.Context(), types.RequestIDContextKey{}, reqID))

		// go ahead and process the request
		wrapped := statusRecorder{w, 200}
		next.ServeHTTP(&wrapped, req)

		// calculate duration
		dur := float64(time.Since(arrivalTime)) / float64(time.Millisecond)

		// log that we did so TODO better formatted http log line
		r.Logger.Debug().Logf("handled %s request %s %s %s %s %f %d", route.GetName(), reqID, remoteIP, method, url, dur, wrapped.status)
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

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// randStringBytes makes us a request ID for logging.
func randStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return string(b)
}

package route

import (
	"context"
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

func (r *Router) queryTokenChecker(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		requiredToken := r.Config.GetQueryAuthToken()
		if requiredToken == "" {
			err := fmt.Errorf("/query endpoint is not authorized for use (specify QueryAuthToken in config)")
			r.handlerReturnWithError(w, ErrAuthNeeded, err)
			return
		}

		token := req.Header.Get(types.QueryTokenHeader)
		if token == requiredToken {
			// if they're equal (including both blank) we're good
			next.ServeHTTP(w, req)
			return
		}

		err := fmt.Errorf("token %s found in %s not authorized for query", token, types.QueryTokenHeader)
		r.handlerReturnWithError(w, ErrAuthNeeded, err)
	})
}

func (r *Router) apiKeyProcessor(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		apiKey := req.Header.Get(types.APIKeyHeader)
		if apiKey == "" {
			apiKey = req.Header.Get(types.APIKeyHeaderShort)
		}

		keycfg := r.Config.GetAccessKeyConfig()

		overwriteWith, err := keycfg.CheckAndMaybeReplaceKey(apiKey)
		if err != nil {
			r.handlerReturnWithError(w, ErrAuthNeeded, err)
			return
		}
		if overwriteWith != apiKey {
			req.Header.Set(types.APIKeyHeader, overwriteWith)
		}
		next.ServeHTTP(w, req)
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

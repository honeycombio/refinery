package route

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"slices"
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

// truncate the key to 8 characters for logging
func (r *Router) sanitize(key string) string {
	return fmt.Sprintf("%.8s...", key)
}

func (r *Router) apiKeyProcessor(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		apiKey := req.Header.Get(types.APIKeyHeader)
		if apiKey == "" {
			apiKey = req.Header.Get(types.APIKeyHeaderShort)
		}

		keycfg := r.Config.GetAccessKeyConfig()

		// Apply AcceptOnlyListedKeys logic BEFORE we consider replacement
		if keycfg.AcceptOnlyListedKeys && !slices.Contains(keycfg.ReceiveKeys, apiKey) {
			err := fmt.Errorf("api key %s not found in list of authorized keys", r.sanitize(apiKey))
			r.handlerReturnWithError(w, ErrAuthNeeded, err)
			return
		}

		if keycfg.SendKey != "" {
			overwriteWith := ""
			switch keycfg.SendKeyMode {
			case "none":
				// don't replace keys at all
				// (SendKey is disabled)
			case "all":
				// overwrite all keys, even missing ones, with the configured one
				overwriteWith = keycfg.SendKey
			case "nonblank":
				// only replace nonblank keys with the configured one
				if apiKey != "" {
					overwriteWith = keycfg.SendKey
				}
			case "listedonly":
				// only replace keys that are listed in the `ReceiveKeys` list
				// reject everything else
				if slices.Contains(keycfg.ReceiveKeys, apiKey) {
					overwriteWith = keycfg.SendKey
				}
			case "missingonly":
				// only inject keys into telemetry that doesn't have a key at all
				if apiKey == "" {
					overwriteWith = keycfg.SendKey
				}
			case "unlisted":
				// only replace nonblank keys that are NOT listed in the `ReceiveKeys` list
				if apiKey != "" && !slices.Contains(keycfg.ReceiveKeys, apiKey) {
					overwriteWith = keycfg.SendKey
				}
			}
			req.Header.Set(types.APIKeyHeader, overwriteWith)
		}

		// we might still have a blank key, so check again
		if apiKey == "" {
			err := errors.New("no value for " + types.APIKeyHeader + " header found from within authing middleware")
			r.handlerReturnWithError(w, ErrAuthNeeded, err)
			return
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

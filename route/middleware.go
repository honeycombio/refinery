package route

import (
	"errors"
	"fmt"
	"net/http"

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
func (r *Router) setResponseHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {

		// Set content type header early so it's before any calls to WriteHeader
		w.Header().Set("Content-Type", "application/json")

		// Allow cross-origin API operation from browser js
		w.Header().Set("Access-Control-Allow-Origin", "*")
		next.ServeHTTP(w, req)

	})
}

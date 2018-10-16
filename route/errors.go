package route

import (
	"fmt"
	"net/http"
)

type handlerError struct {
	// err is the error that we're throwing
	err error
	// msg is the human-readable context with which we're throwing the error
	msg string
	// status is the HTTP status code we should return
	status int
	// detailed is whether the err itself should be included in the msg response
	detailed bool
	// friendly is whether the msg can be returned as is or if we should use a
	// generic error
	friendly bool
}

var ErrGenericMessage = "unexpected error!"

var (
	ErrJSONFailed          = handlerError{nil, "failed to parse JSON", http.StatusBadRequest, false, true}
	ErrJSONBuildFailed     = handlerError{nil, "failed to build JSON response", http.StatusInternalServerError, false, true}
	ErrPostBody            = handlerError{nil, "failed to read request body", http.StatusInternalServerError, false, false}
	ErrAuthNeeded          = handlerError{nil, "unknown API key - check your credentials", http.StatusBadRequest, true, true}
	ErrConfigReadFailed    = handlerError{nil, "failed to read config", http.StatusBadRequest, false, false}
	ErrUpstreamFailed      = handlerError{nil, "failed to create upstream request", http.StatusServiceUnavailable, true, true}
	ErrUpstreamUnavailable = handlerError{nil, "upstream target unavailable", http.StatusServiceUnavailable, true, true}
	ErrReqToEvent          = handlerError{nil, "failed to parse event", http.StatusBadRequest, false, true}
	ErrBatchToEvent        = handlerError{nil, "failed to parse event within batch", http.StatusBadRequest, false, true}
)

func (r *Router) handlerReturnWithError(w http.ResponseWriter, he handlerError, err error) {
	if err != nil {
		he.err = err
	}
	r.Logger.Errorf("returning error %+v, %s\n", he, he.err.Error())
	w.WriteHeader(he.status)
	errmsg := he.msg
	if he.detailed {
		errmsg = fmt.Sprintf(he.msg + ": " + he.err.Error())
	}
	if !he.friendly {
		errmsg = ErrGenericMessage
	}
	jsonErrMsg := []byte(`{"error":"` + errmsg + `"}`)
	w.Write(jsonErrMsg)
}

// +build all race

package route

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/honeycombio/refinery/logger"
)

func TestHandlerReturnWithError(t *testing.T) {
	var w *httptest.ResponseRecorder
	var l *logger.MockLogger
	var router *Router

	l = &logger.MockLogger{}
	router = &Router{
		Logger: l,
	}

	w = httptest.NewRecorder()
	http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		router.handlerReturnWithError(w, ErrCaughtPanic, errors.New("oh no"))
	}).ServeHTTP(w, &http.Request{})

	if len(l.Events) != 1 {
		t.Fail()
	}

	e := l.Events[0]

	if _, ok := e.Fields["error.stack_trace"]; !ok {
		t.Error("expected fields to contain error.stack_trace", e.Fields)
	}
}

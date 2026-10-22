package route

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	husky "github.com/honeycombio/husky/otlp"
	"github.com/honeycombio/refinery/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestHandleOTLPFailureResponseLogsCause(t *testing.T) {
	canonical := husky.ErrFailedParseBody

	tests := []struct {
		name        string
		cause       []error
		expectCause string
	}{
		{
			name:  "no cause keeps the log to the canonical message",
			cause: nil,
		},
		{
			name:        "wrapped cause is logged alongside it",
			cause:       []error{fmt.Errorf("%w: unexpected EOF", canonical)},
			expectCause: "failed to parse OTLP request body: unexpected EOF",
		},
		{
			name:  "explicit nil cause is ignored",
			cause: []error{nil},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &logger.MockLogger{}
			router := &Router{Logger: l}

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/v1/traces", nil)
			req.Header.Set("content-type", "application/protobuf")
			router.handleOTLPFailureResponse(w, req, canonical, tt.cause...)

			require.Len(t, l.Events, 1)
			fields := l.Events[0].Fields

			assert.Equal(t, canonical.Error(), fields["error"],
				"the logged message should stay the canonical one")

			if tt.expectCause == "" {
				assert.NotContains(t, fields, "cause")
			} else {
				assert.Equal(t, tt.expectCause, fields["cause"])
			}

			// The response is a public contract, so it carries the canonical
			// message whether or not a cause was logged.
			assert.Equal(t, canonical.HTTPStatusCode, w.Code)
			assert.Contains(t, w.Body.String(), canonical.Message)
		})
	}
}

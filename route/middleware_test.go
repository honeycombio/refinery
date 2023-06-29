package route

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/types"
)

type dummyHandler struct{}

func (d *dummyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("good"))
}

func TestRouter_queryTokenChecker(t *testing.T) {
	tests := []struct {
		name           string
		authtoken      string
		reqtoken       string
		want           int
		mustcontain    string
		mustnotcontain string
	}{
		{"both_empty", "", "", 400, "not authorized for use", "good"},
		{"auth_empty", "", "foo", 400, "not authorized for use", "good"},
		{"req_empty", "foo", "", 400, "not authorized for query", "good"},
		{"correct", "testtoken", "testtoken", 200, "good", "authorized"},
		{"incorrect", "testtoken", "wrongtoken", 400, "not authorized for query", "good"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := &Router{
				Logger: &logger.NullLogger{},
				Config: &config.MockConfig{QueryAuthToken: tt.authtoken},
			} // we're not using anything else on this router

			// Create a request to pass to our handler. We don't have any query parameters for now, so we'll
			// pass 'nil' as the third parameter.
			req, err := http.NewRequest("GET", "/query", nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set(types.QueryTokenHeader, tt.reqtoken)

			// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
			rr := httptest.NewRecorder()

			handler := router.queryTokenChecker(&dummyHandler{})
			handler.ServeHTTP(rr, req)

			// Check the status code is what we expect.
			if status := rr.Code; status != tt.want {
				t.Errorf("handler returned wrong status code: got %v want %v",
					status, tt.want)
			}

			// Check the response body is what we expect.
			if !strings.Contains(rr.Body.String(), tt.mustcontain) {
				t.Errorf("handler returned unexpected body: got %v should have contained %v",
					rr.Body.String(), tt.mustcontain)
			}
			// Check the response body is what we expect.
			if strings.Contains(rr.Body.String(), tt.mustnotcontain) {
				t.Errorf("handler returned unexpected body: got %v should NOT have contained %v",
					rr.Body.String(), tt.mustnotcontain)
			}
		})
	}
}

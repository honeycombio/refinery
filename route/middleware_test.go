package route

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/honeycombio/refinery/collect"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/types"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type dummyHandler struct{}

func (d *dummyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("good"))
}

type testRequest struct{}
type testResponse struct{}

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

func TestBackOffMiddleware(t *testing.T) {
	tests := []struct {
		name             string
		BackOffActivated bool
		httpStatusCode   int
		grpcStatusCode   codes.Code
		retryAfter       time.Duration
	}{
		{
			name:             "not in backoff returns 200",
			BackOffActivated: false,
			httpStatusCode:   http.StatusOK,
			grpcStatusCode:   codes.OK,
			retryAfter:       0,
		},
		{
			name:             "backoff enabled with no retry after set, return 429 without retry-after headers",
			BackOffActivated: true,
			httpStatusCode:   http.StatusTooManyRequests,
			grpcStatusCode:   codes.ResourceExhausted,
			retryAfter:       0,
		},
		{
			name:             "backoff enabled with retry after set, return 429 with retry-after headers",
			BackOffActivated: true,
			httpStatusCode:   http.StatusTooManyRequests,
			grpcStatusCode:   codes.ResourceExhausted,
			retryAfter:       1 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := &Router{
				Config: &config.MockConfig{
					StressRelief: config.StressReliefConfig{
						BackOffHTTPStatusCode: tt.httpStatusCode,
						BackOffGRPCStatusCode: tt.grpcStatusCode,
						BackOffRetryAfter:     config.Duration(tt.retryAfter),
					},
				},
				StressRelief: &collect.MockStressReliever{
					IsBackOffActivated: tt.BackOffActivated,
				},
			}

			// --- HTTP middleware ---

			// create an HTTP handler that will be wrapped by the middleware
			// this is a simple handler that just returns a 200 OK response
			next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			})
			// wrap the next handler with the backOffHTTPMiddleware
			handler := router.backOffHTTPMiddleware(next)

			// create a simple request and create a recorder to capture the response
			grpcReq, err := http.NewRequest("GET", "/test", nil)
			if err != nil {
				t.Fatal(err)
			}
			rr := httptest.NewRecorder()

			// call the handler with the request and recorder
			handler.ServeHTTP(rr, grpcReq)
			assert.Equal(t, tt.httpStatusCode, rr.Code)

			// if retryAfter is set, check the Retry-After header is set
			if tt.retryAfter > 0 {
				retry := rr.Header().Get("Retry-After")
				assert.Equal(t, strconv.Itoa(int(tt.retryAfter.Seconds())), retry)
			}

			// --- gRPC interceptor ---

			// create a test request and pass to the backOffGRPCInterceptor
			req := testRequest{}
			serverinfo := &grpc.UnaryServerInfo{}
			resp, err := router.backOffGRPCInterceptor(context.Background(), req, serverinfo, func(ctx context.Context, req any) (any, error) {
				// this is where the actual gRPC handler would be called
				// for this test, we just return a simple response and no error
				return testResponse{}, nil
			})

			if tt.BackOffActivated {
				assert.Equal(t, nil, resp)
				assert.NotNil(t, err)
				grpcErr, ok := status.FromError(err)
				assert.True(t, ok)
				assert.Equal(t, grpcErr.Code(), tt.grpcStatusCode)

				if tt.retryAfter > 0 {
					retry := grpcErr.Message()
					assert.Equal(t, "Retry-After: "+strconv.Itoa(int(tt.retryAfter.Seconds())), retry)
				}
			} else {
				assert.Equal(t, testResponse{}, resp)
				assert.NoError(t, err)
			}
		})
	}
}

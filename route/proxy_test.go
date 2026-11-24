package route

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/stretchr/testify/assert"
)

func TestProxyMaxBodySize(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	cfg := &config.MockConfig{
		GetHoneycombAPIVal: upstream.URL,
	}

	router := &Router{
		Config:      cfg,
		Logger:      &logger.NullLogger{},
		Metrics:     &metrics.NullMetrics{},
		proxyClient: upstream.Client(),
	}

	t.Run("body within limit succeeds", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/test", bytes.NewReader(make([]byte, 1024)))
		w := httptest.NewRecorder()

		router.proxy(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("body exceeding HTTPMessageSizeMax is rejected with 413", func(t *testing.T) {
		oversized := make([]byte, HTTPMessageSizeMax+100)
		req := httptest.NewRequest("POST", "/test", bytes.NewReader(oversized))
		w := httptest.NewRecorder()

		router.proxy(w, req)

		assert.Equal(t, http.StatusRequestEntityTooLarge, w.Code)
		assert.Contains(t, w.Body.String(), "request body exceeded maximum allowed size")
	})
}

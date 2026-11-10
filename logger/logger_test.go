package logger

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/honeycombio/refinery/config"

	"github.com/stretchr/testify/assert"
)

func TestHoneycombLoggerRespectsLogLevelAfterStart(t *testing.T) {
	cfg := &config.MockConfig{
		GetLoggerLevelVal:           config.WarnLevel,
		GetHoneycombLoggerConfigVal: config.HoneycombLoggerConfig{},
	}
	hcLogger := &HoneycombLogger{
		Config:       cfg,
		level:        config.WarnLevel,
		loggerConfig: config.HoneycombLoggerConfig{},
	}

	assert.Equal(t, config.WarnLevel, hcLogger.level)
	err := hcLogger.Start()
	assert.Nil(t, err)
	assert.Equal(t, config.WarnLevel, hcLogger.level)
}

// TestHoneycombLoggerReportsTransportErrors checks that a send which fails
// before any HTTP response (here, a dial failure from a fake dialer) is reported
// on stderr rather than being treated as a sampled-out line. No network is used.
func TestHoneycombLoggerReportsTransportErrors(t *testing.T) {
	cfg := &config.MockConfig{
		GetLoggerLevelVal: config.InfoLevel,
		GetHoneycombLoggerConfigVal: config.HoneycombLoggerConfig{
			APIHost: "http://honeycomb.invalid",
			APIKey:  "test-key",
			Dataset: "test",
		},
	}
	failingDialer := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return nil, errors.New("fake dialer: connection refused")
		},
	}
	hcLogger := &HoneycombLogger{
		Config:            cfg,
		UpstreamTransport: failingDialer,
		level:             config.InfoLevel,
	}
	assert.Nil(t, hcLogger.Start())

	// capture stderr while the response goroutine reports the failure.
	// os.Stderr is process-wide, so this test must not run in parallel.
	r, w, err := os.Pipe()
	assert.Nil(t, err)
	origStderr := os.Stderr
	os.Stderr = w
	defer func() { os.Stderr = origStderr }()

	// drain the pipe continuously so writes never block and output may arrive in several pieces
	var mu sync.Mutex
	var captured strings.Builder
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 4096)
		for {
			n, rerr := r.Read(buf)
			if n > 0 {
				mu.Lock()
				captured.Write(buf[:n])
				mu.Unlock()
			}
			if rerr != nil {
				return
			}
		}
	}()
	snapshot := func() string {
		mu.Lock()
		defer mu.Unlock()
		return captured.String()
	}

	hcLogger.Info().Logf("this log line cannot be delivered")

	// wait for the expected report, up to a deadline
	deadline := time.Now().Add(10 * time.Second)
	for !strings.Contains(snapshot(), "got an error back from Honeycomb") && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	os.Stderr = origStderr
	w.Close()
	<-done
	r.Close()

	got := snapshot()
	assert.Contains(t, got, "got an error back from Honeycomb", "expected transport error to be reported on stderr, got %q", got)
}

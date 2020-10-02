// +build all race

package transmit

import (
	"testing"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"

	libhoney "github.com/honeycombio/libhoney-go"
	"github.com/stretchr/testify/assert"
)

func TestDefaultTransmissionUpdatesUserAgentAdditionAfterStart(t *testing.T) {
	transmission := &DefaultTransmission{
		Config:     &config.MockConfig{},
		Logger:     &logger.NullLogger{},
		Metrics:    &metrics.NullMetrics{},
		LibhClient: &libhoney.Client{},
		Version:    "test",
	}

	assert.Equal(t, libhoney.UserAgentAddition, "")
	err := transmission.Start()
	assert.Nil(t, err)
	assert.Equal(t, libhoney.UserAgentAddition, "refinery/test")
}

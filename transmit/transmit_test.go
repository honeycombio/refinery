// +build all race

package transmit

import (
	"testing"

	"github.com/facebookgo/inject"
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

func TestDependencyInjection(t *testing.T) {
	var g inject.Graph
	err := g.Provide(
		&inject.Object{Value: &DefaultTransmission{}},

		&inject.Object{Value: &config.MockConfig{}},
		&inject.Object{Value: &logger.NullLogger{}},
		&inject.Object{Value: &metrics.NullMetrics{}, Name: "metrics"},
		&inject.Object{Value: "test", Name: "version"},
	)
	if err != nil {
		t.Error(err)
	}
	if err := g.Populate(); err != nil {
		t.Error(err)
	}
}

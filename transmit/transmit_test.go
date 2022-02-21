//go:build all || race
// +build all race

package transmit

import (
	"context"
	"testing"
	"time"

	"github.com/facebookgo/inject"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/types"

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

func TestConfigReloadRaciness(t *testing.T) {
	mockConfig := config.MockConfig{}
	transmission := &DefaultTransmission{
		Config:     &mockConfig,
		Logger:     &logger.NullLogger{},
		Metrics:    &metrics.NullMetrics{},
		LibhClient: &libhoney.Client{},
		Version:    "test",
	}

	err := transmission.Start()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(mockConfig.Callbacks))

	// imitate concurrent config reload
	for i := 0; i < 10; i++ {
		go func(j int) {
			mockConfig.Callbacks[0]()
		}(i)

		go func(j int) {
			mockConfig.Callbacks[0]()
		}(i)

		transmission.EnqueueEvent(&types.Event{
			Context:   context.Background(),
			Timestamp: time.Time{},
			Data:      map[string]interface{}{"foo": "bar"},
		})
	}
}

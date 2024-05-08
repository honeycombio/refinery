package sample

import (
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
)

type MockSampler struct {
	Config  *config.MockSamplerConfig
	Logger  logger.Logger
	Metrics metrics.Metrics

	// we make these public so we can test them
	SampleRate int
	FieldList  []string
	prefix     string
	key        *traceKey
}

func (d *MockSampler) Start() error {
	d.Logger.Debug().Logf("Starting MockSampler")
	defer func() { d.Logger.Debug().Logf("Finished starting MockSampler") }()
	d.SampleRate = d.Config.SampleRate
	d.FieldList = d.Config.FieldList
	d.key = newTraceKey(d.FieldList, false)
	d.prefix = "mock_"
	if d.Metrics == nil {
		d.Metrics = &metrics.NullMetrics{}
	}

	return nil
}

func (d *MockSampler) GetSampleRate(trace FieldsExtractor) (rate uint, keep bool, reason string, key string) {
	if d.SampleRate <= 1 {
		return 1, true, "mock/always", ""
	}
	// the mock sampler always samples, and the reason is the generated value of the key fields
	samplerKey := d.key.build(trace)

	return uint(d.SampleRate), true, "mock/sampler", samplerKey
}

func (d *MockSampler) GetKeyFields() []string {
	return d.Config.GetSamplingFields()
}

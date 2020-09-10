package sample

import (
	"io/ioutil"
	"math"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/honeycombio/samproxy/config"
	"github.com/honeycombio/samproxy/logger"
	"github.com/honeycombio/samproxy/types"
)

// TestInitialization tests that sample rates are consistently returned
func TestInitialization(t *testing.T) {
	mc := &config.MockConfig{
		GetOtherConfigVal: `{"SampleRate":10}`,
	}
	ds := &DeterministicSampler{
		Config: mc,
		Logger: &logger.NullLogger{},
	}

	err := ds.Start()
	assert.NoError(t, err, "starting deterministic sampler should not error")

	assert.Equal(t, 10, ds.sampleRate, "upper bound should be correctly calculated")
	assert.Equal(t, uint32(math.MaxUint32/10), ds.upperBound, "upper bound should be correctly calculated")
}

func TestInitializationFromConfigFile(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	assert.Equal(t, nil, err)
	defer os.RemoveAll(tmpDir)

	f, err := ioutil.TempFile(tmpDir, "*.toml")
	assert.Equal(t, nil, err)

	dummyConfig := []byte(`
		[InMemCollector]
			CacheCapacity=1000 

		[HoneycombMetrics]
			MetricsHoneycombAPI="http://honeycomb.io"
			MetricsAPIKey="1234"
			MetricsDataset="testDatasetName"
			MetricsReportingInterval=3
			
		[SamplerConfig._default]
			Sampler = "DeterministicSampler"
			SampleRate = 2

		[SamplerConfig.dataset1]
			Sampler = "DynamicSampler"
			SampleRate = 2
			FieldList = ["request.method","response.status_code"]
			UseTraceLength = true
			AddSampleRateKeyToTrace = true
			AddSampleRateKeyToTraceField = "meta.samproxy.dynsampler_key"

		[SamplerConfig.dataset2]

			Sampler = "DeterministicSampler"
			SampleRate = 10
	`)

	_, err = f.Write(dummyConfig)
	assert.Equal(t, nil, err)
	f.Close()

	c, err := config.NewConfig(f.Name(), f.Name())

	if err != nil {
		t.Error(err)
	}

	ds := &DeterministicSampler{
		Config:     c,
		Logger:     &logger.NullLogger{},
		configName: defaultConfigName,
	}
	ds.Start()

	assert.Equal(t, 2, ds.sampleRate)

	ds = &DeterministicSampler{
		Config:     c,
		Logger:     &logger.NullLogger{},
		configName: "dataset2",
	}
	ds.Start()
	// ensure we get the dataset value from "SamplerConfig.dataset2"
	assert.Equal(t, 10, ds.sampleRate)

}

// TestGetSampleRate verifies the same trace ID gets the same response
func TestGetSampleRate(t *testing.T) {
	mc := &config.MockConfig{
		GetOtherConfigVal: `{"SampleRate":10}`,
	}
	ds := &DeterministicSampler{
		Config: mc,
		Logger: &logger.NullLogger{},
	}

	tsts := []struct {
		trace   *types.Trace
		sampled bool
	}{
		{&types.Trace{TraceID: "abc123"}, false},
		{&types.Trace{TraceID: "def456"}, true},
		{&types.Trace{TraceID: "ghi789"}, false},
		{&types.Trace{TraceID: "zyx987"}, false},
		{&types.Trace{TraceID: "wvu654"}, false},
		{&types.Trace{TraceID: "tsr321"}, false},
	}
	ds.Start()

	for i, tst := range tsts {
		rate, keep := ds.GetSampleRate(tst.trace)
		assert.Equal(t, uint(10), rate, "sample rate should be fixed")
		assert.Equal(t, tst.sampled, keep, "%d: trace ID %s should be %v", i, tst.trace.TraceID, tst.sampled)
	}

}

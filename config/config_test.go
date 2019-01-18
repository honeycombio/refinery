package config

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetSamplerTypes(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	assert.Equal(t, nil, err)
	defer os.RemoveAll(tmpDir)

	f, err := ioutil.TempFile(tmpDir, "")
	assert.Equal(t, nil, err)

	dummyConfig := []byte(`
[[SamplerConfig]]
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

	var c Config
	fc := &FileConfig{Path: f.Name()}
	fc.Start()
	c = fc
	typ, err := c.GetDefaultSamplerType()
	assert.Equal(t, nil, err)
	assert.Equal(t, "DeterministicSampler", typ)

	typ, err = c.GetSamplerTypeForDataset("dataset1")
	assert.Equal(t, nil, err)
	assert.Equal(t, "DynamicSampler", typ)

	typ, err = c.GetSamplerTypeForDataset("dataset2")
	assert.Equal(t, nil, err)
	assert.Equal(t, "DeterministicSampler", typ)

	typ, err = c.GetSamplerTypeForDataset("dataset3")
	assert.Equal(t, "failed to find config tree for SamplerConfig.dataset3", err.Error())
}

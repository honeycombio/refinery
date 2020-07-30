package config

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestReadDefaultFiles(t *testing.T) {
	c := FileConfig{ConfigFile: "../config.toml", RulesFile: "../rules.toml"}
	err := c.Start()

	if err != nil {
		t.Error(err)
	}

	if d, _ := c.GetSendDelay(); d != 2*time.Second {
		t.Error("received", d, "expected", 2*time.Second)
	}

	if d, _ := c.GetTraceTimeout(); d != 60*time.Second {
		t.Error("received", d, "expected", 60*time.Second)
	}

	if d := c.GetSendTickerValue(); d != 100*time.Millisecond {
		t.Error("received", d, "expected", 100*time.Millisecond)
	}

	if d, _ := c.GetDefaultSamplerType(); d != "DeterministicSampler" {
		t.Error("received", d, "expected", "DeterministicSampler")
	}

	if d, _ := c.GetSamplerTypeForDataset("dataset1"); d != "DynamicSampler" {
		t.Error("received", d, "expected", "DynamicSampler")
	}
}

func TestGetSamplerTypes(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	assert.Equal(t, nil, err)
	defer os.RemoveAll(tmpDir)

	f, err := ioutil.TempFile(tmpDir, "*.toml")
	assert.Equal(t, nil, err)

	dummyConfig := []byte(`
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
		ClearFrequencySec = 60

	[SamplerConfig.dataset2]

		Sampler = "DeterministicSampler"
		SampleRate = 10

	[SamplerConfig.dataset3]

		Sampler = "EMADynamicSampler"
		GoalSampleRate = 10
`)

	_, err = f.Write(dummyConfig)
	assert.Equal(t, nil, err)
	f.Close()

	var c Config
	fc := &FileConfig{RulesFile: f.Name(), ConfigFile: f.Name()}
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
	assert.Equal(t, nil, err)
	assert.Equal(t, "EMADynamicSampler", typ)

	typ, err = c.GetSamplerTypeForDataset("dataset4")
	assert.Equal(t, "failed to find config tree for SamplerConfig.dataset4", err.Error())
}

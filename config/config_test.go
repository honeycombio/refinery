package config

import (
	"io/ioutil"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestReload(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	assert.Equal(t, nil, err)
	defer os.RemoveAll(tmpDir)

	f, err := ioutil.TempFile(tmpDir, "*.toml")
	assert.Equal(t, nil, err)

	dummy := []byte(`ListenAddr = "0.0.0.0:8080"`)

	_, err = f.Write(dummy)
	assert.Equal(t, nil, err)
	f.Close()

	c, err := NewConfig(f.Name(), f.Name())

	if err != nil {
		t.Error(err)
	}

	if d, _ := c.GetListenAddr(); d != "0.0.0.0:8080" {
		t.Error("received", d, "expected", "0.0.0.0:8080")
	}

	wg := &sync.WaitGroup{}

	ch := make(chan interface{}, 1)

	c.RegisterReloadCallback(func() {
		ch <- 1
	})

	wg.Add(1)

	go func() {
		defer wg.Done()
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Error("No callback")
		}
	}()

	if file, err := os.OpenFile(f.Name(), os.O_RDWR, 0644); err == nil {
		file.WriteString(`ListenAddr = "0.0.0.0:9000"`)
		file.Close()
	}

	wg.Wait()

	if d, _ := c.GetListenAddr(); d != "0.0.0.0:9000" {
		t.Error("received", d, "expected", "0.0.0.0:9000")
	}

}

func TestReadDefaults(t *testing.T) {
	c, err := NewConfig("../config_complete.toml", "../rules.toml")

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

	c, err := NewConfig(f.Name(), f.Name())

	if err != nil {
		t.Error(err)
	}

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

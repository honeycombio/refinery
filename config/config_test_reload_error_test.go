//go:build all || !race

package config

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestErrorReloading(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	rulesFile, err := os.CreateTemp(tmpDir, "*.toml")
	assert.NoError(t, err)

	configFile, err := os.CreateTemp(tmpDir, "*.toml")
	assert.NoError(t, err)

	dummy := []byte(`
	[InMemCollector]
		CacheCapacity=1000

	[HoneycombMetrics]
		MetricsHoneycombAPI="http://honeycomb.io"
		MetricsAPIKey="1234"
		MetricsDataset="testDatasetName"
		MetricsReportingInterval=3
	`)

	_, err = configFile.Write(dummy)
	assert.NoError(t, err)
	configFile.Close()

	dummy = []byte(`
	Sampler="DeterministicSampler"
	SampleRate=1
	`)

	_, err = rulesFile.Write(dummy)
	assert.NoError(t, err)
	rulesFile.Close()

	ch := make(chan interface{}, 1)

	c, err := NewConfig(configFile.Name(), rulesFile.Name(), func(err error) { ch <- 1 })

	if err != nil {
		t.Error(err)
	}

	d, name, _ := c.GetSamplerConfigForDataset("dataset5")
	if _, ok := d.(DeterministicSamplerConfig); ok {
		t.Error("type received", d, "expected", "DeterministicSampler")
	}
	if name != "DeterministicSampler" {
		t.Error("name received", d, "expected", "DeterministicSampler")
	}

	wg := &sync.WaitGroup{}

	wg.Add(1)

	go func() {
		defer wg.Done()
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Error("No error callback")
		}
	}()

	err = os.WriteFile(rulesFile.Name(), []byte(`Sampler="InvalidSampler"`), 0644)

	if err != nil {
		t.Error(err)
	}

	wg.Wait()

	// config should error and not update sampler to invalid type
	d, _, _ = c.GetSamplerConfigForDataset("dataset5")
	if _, ok := d.(DeterministicSamplerConfig); ok {
		t.Error("received", d, "expected", "DeterministicSampler")
	}
}

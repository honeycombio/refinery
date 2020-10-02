// +build all race

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
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	rulesFile, err := ioutil.TempFile(tmpDir, "*.toml")
	assert.NoError(t, err)

	configFile, err := ioutil.TempFile(tmpDir, "*.toml")
	assert.NoError(t, err)

	dummy := []byte(`
	ListenAddr="0.0.0.0:8080"

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

	c, err := NewConfig(configFile.Name(), rulesFile.Name(), func(err error) {})

	if err != nil {
		t.Error(err)
	}

	if d, _ := c.GetListenAddr(); d != "0.0.0.0:8080" {
		t.Error("received", d, "expected", "0.0.0.0:8080")
	}

	wg := &sync.WaitGroup{}

	ch := make(chan interface{}, 1)

	c.RegisterReloadCallback(func() {
		close(ch)
	})

	// Hey race detector, we're doing some concurrent config reads.
	// That's cool, right?
	go func() {
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for {
			c.GetListenAddr()
			select {
			case <-ch:
				return
			case <-tick.C:
			}
		}
	}()

	wg.Add(1)

	go func() {
		defer wg.Done()
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Error("No callback")
		}
	}()

	if file, err := os.OpenFile(configFile.Name(), os.O_RDWR, 0644); err == nil {
		file.WriteString(`ListenAddr = "0.0.0.0:9000"`)
		file.Close()
	}

	wg.Wait()

	if d, _ := c.GetListenAddr(); d != "0.0.0.0:9000" {
		t.Error("received", d, "expected", "0.0.0.0:9000")
	}

}

func TestReadDefaults(t *testing.T) {
	c, err := NewConfig("../config.toml", "../rules.toml", func(err error) {})

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

	if d, err := c.GetSamplerConfigForDataset("dataset-doesnt-exist"); err != nil {
		assert.IsType(t, &DeterministicSamplerConfig{}, d)
	}

	if d, err := c.GetSamplerConfigForDataset("dataset1"); err != nil {
		assert.IsType(t, &DynamicSamplerConfig{}, d)
	}

	if d, err := c.GetSamplerConfigForDataset("dataset4"); err != nil {
		switch r := d.(type) {
		case RulesBasedSamplerConfig:
			assert.Len(t, r.Rule, 3)

			var rule *RulesBasedSamplerRule

			rule = r.Rule[0]
			assert.Equal(t, 1, rule.SampleRate)
			assert.Equal(t, "500 errors", rule.Name)
			assert.Len(t, rule.Condition, 2)

			rule = r.Rule[1]
			assert.True(t, rule.Drop)
			assert.Equal(t, 0, rule.SampleRate)
			assert.Len(t, rule.Condition, 1)

		default:
			assert.Fail(t, "dataset4 should have a rules based sampler", d)
		}
	}

	if d, _ := c.GetPeers(); !(len(d) == 1 && d[0] == "http://127.0.0.1:8081") {
		t.Error("received", d, "expected", "[http://127.0.0.1:8081]")
	}

	if d, _ := c.GetPeerManagementType(); d != "file" {
		t.Error("received", d, "expected", "file")
	}

	if d, _ := c.GetUseIPV6Identifier(); d != false {
		t.Error("received", d, "expected", false)
	}

	if d := c.GetIsDryRun(); d != false {
		t.Error("received", d, "expected", false)
	}

	if d := c.GetDryRunFieldName(); d != "refinery_kept" {
		t.Error("received", d, "expected", "refinery_kept")
	}

	type imcConfig struct {
		CacheCapacity int
	}
	collectorConfig := &imcConfig{}
	err = c.GetOtherConfig("InMemCollector", collectorConfig)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, collectorConfig.CacheCapacity, 1000)
}

func TestPeerManagementType(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	configFile, err := ioutil.TempFile(tmpDir, "*.toml")
	assert.NoError(t, err)

	rulesFile, err := ioutil.TempFile(tmpDir, "*.toml")
	assert.NoError(t, err)

	_, err = configFile.Write([]byte(`
	[InMemCollector]
		CacheCapacity=1000

	[HoneycombMetrics]
		MetricsHoneycombAPI="http://honeycomb.io"
		MetricsAPIKey="1234"
		MetricsDataset="testDatasetName"
		MetricsReportingInterval=3

	[PeerManagement]
		Type = "redis"
		Peers = ["http://refinery-1231:8080"]
	`))

	c, err := NewConfig(configFile.Name(), rulesFile.Name(), func(err error) {})
	assert.NoError(t, err)

	if d, _ := c.GetPeerManagementType(); d != "redis" {
		t.Error("received", d, "expected", "redis")
	}
}

func TestDebugServiceAddr(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	configFile, err := ioutil.TempFile(tmpDir, "*.toml")
	assert.NoError(t, err)

	rulesFile, err := ioutil.TempFile(tmpDir, "*.toml")
	assert.NoError(t, err)

	_, err = configFile.Write([]byte(`
	DebugServiceAddr = "localhost:8085"

	[InMemCollector]
		CacheCapacity=1000

	[HoneycombMetrics]
		MetricsHoneycombAPI="http://honeycomb.io"
		MetricsAPIKey="1234"
		MetricsDataset="testDatasetName"
		MetricsReportingInterval=3
	`))

	c, err := NewConfig(configFile.Name(), rulesFile.Name(), func(err error) {})
	assert.NoError(t, err)

	if d, _ := c.GetDebugServiceAddr(); d != "localhost:8085" {
		t.Error("received", d, "expected", "localhost:8085")
	}
}

func TestDryRun(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	configFile, err := ioutil.TempFile(tmpDir, "*.toml")
	assert.NoError(t, err)

	_, err = configFile.Write([]byte(`
	[InMemCollector]
		CacheCapacity=1000

	[HoneycombMetrics]
		MetricsHoneycombAPI="http://honeycomb.io"
		MetricsAPIKey="1234"
		MetricsDataset="testDatasetName"
		MetricsReportingInterval=3
	`))

	rulesFile, err := ioutil.TempFile(tmpDir, "*.toml")
	assert.NoError(t, err)

	_, err = rulesFile.Write([]byte(`
	DryRun=true
	`))

	c, err := NewConfig(configFile.Name(), rulesFile.Name(), func(err error) {})
	assert.NoError(t, err)

	if d := c.GetIsDryRun(); d != true {
		t.Error("received", d, "expected", true)
	}
}

func TestMaxAlloc(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	configFile, err := ioutil.TempFile(tmpDir, "*.toml")
	assert.NoError(t, err)

	rulesFile, err := ioutil.TempFile(tmpDir, "*.toml")
	assert.NoError(t, err)

	_, err = configFile.Write([]byte(`
	[InMemCollector]
		CacheCapacity=1000
		MaxAlloc=17179869184

	[HoneycombMetrics]
		MetricsHoneycombAPI="http://honeycomb.io"
		MetricsAPIKey="1234"
		MetricsDataset="testDatasetName"
		MetricsReportingInterval=3
	`))

	c, err := NewConfig(configFile.Name(), rulesFile.Name(), func(err error) {})
	assert.NoError(t, err)

	expected := uint64(16 * 1024 * 1024 * 1024)
	inMemConfig, err := c.GetInMemCollectorCacheCapacity()
	assert.NoError(t, err)
	assert.Equal(t, expected, inMemConfig.MaxAlloc)
}

func TestGetSamplerTypes(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	configFile, err := ioutil.TempFile(tmpDir, "*.toml")
	assert.NoError(t, err)

	_, err = configFile.Write([]byte(`
	[InMemCollector]
		CacheCapacity=1000

	[HoneycombMetrics]
		MetricsHoneycombAPI="http://honeycomb.io"
		MetricsAPIKey="1234"
		MetricsDataset="testDatasetName"
		MetricsReportingInterval=3
	`))

	rulesFile, err := ioutil.TempFile(tmpDir, "*.toml")
	assert.NoError(t, err)

	dummyConfig := []byte(`
	Sampler = "DeterministicSampler"
	SampleRate = 2

	['dataset 1']
		Sampler = "DynamicSampler"
		SampleRate = 2
		FieldList = ["request.method","response.status_code"]
		UseTraceLength = true
		AddSampleRateKeyToTrace = true
		AddSampleRateKeyToTraceField = "meta.refinery.dynsampler_key"
		ClearFrequencySec = 60

	[dataset2]

		Sampler = "DeterministicSampler"
		SampleRate = 10

	[dataset3]

		Sampler = "EMADynamicSampler"
		GoalSampleRate = 10
		UseTraceLength = true
		AddSampleRateKeyToTrace = true
		FieldList = "[request.method]"
		Weight = 0.3
`)

	_, err = rulesFile.Write(dummyConfig)
	assert.NoError(t, err)
	rulesFile.Close()

	c, err := NewConfig(configFile.Name(), rulesFile.Name(), func(err error) {})

	if err != nil {
		t.Error(err)
	}

	if d, err := c.GetSamplerConfigForDataset("dataset-doesnt-exist"); assert.Equal(t, nil, err) {
		assert.IsType(t, &DeterministicSamplerConfig{}, d)
	}

	if d, err := c.GetSamplerConfigForDataset("dataset 1"); assert.Equal(t, nil, err) {
		assert.IsType(t, &DynamicSamplerConfig{}, d)
	}

	if d, err := c.GetSamplerConfigForDataset("dataset2"); assert.Equal(t, nil, err) {
		assert.IsType(t, &DeterministicSamplerConfig{}, d)
	}

	if d, err := c.GetSamplerConfigForDataset("dataset3"); assert.Equal(t, nil, err) {
		assert.IsType(t, &EMADynamicSamplerConfig{}, d)
	}
}

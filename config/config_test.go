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

	rulesFile, err := ioutil.TempFile(tmpDir, "*.toml")
	assert.Equal(t, nil, err)

	configFile, err := ioutil.TempFile(tmpDir, "*.toml")
	assert.Equal(t, nil, err)

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
	assert.Equal(t, nil, err)
	configFile.Close()

	c, err := NewConfig(configFile.Name(), rulesFile.Name())

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
	c, err := NewConfig("../config.toml", "../rules.toml")

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

	if d, _ := c.GetSamplerTypeForDataset("dataset-doesnt-exist"); d != "DeterministicSampler" {
		t.Error("received", d, "expected", "DeterministicSampler")
	}

	if d, _ := c.GetSamplerTypeForDataset("dataset1"); d != "DynamicSampler" {
		t.Error("received", d, "expected", "DynamicSampler")
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
	assert.Equal(t, nil, err)
	defer os.RemoveAll(tmpDir)

	configFile, err := ioutil.TempFile(tmpDir, "*.toml")
	assert.Equal(t, nil, err)

	rulesFile, err := ioutil.TempFile(tmpDir, "*.toml")
	assert.Equal(t, nil, err)

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
		Peers = ["http://samproxy-1231:8080"]
	`))

	c, err := NewConfig(configFile.Name(), rulesFile.Name())
	assert.Equal(t, nil, err)

	if d, _ := c.GetPeerManagementType(); d != "redis" {
		t.Error("received", d, "expected", "redis")
	}
}

func TestDebugServiceAddr(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	assert.Equal(t, nil, err)
	defer os.RemoveAll(tmpDir)

	configFile, err := ioutil.TempFile(tmpDir, "*.toml")
	assert.Equal(t, nil, err)

	rulesFile, err := ioutil.TempFile(tmpDir, "*.toml")
	assert.Equal(t, nil, err)

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

	c, err := NewConfig(configFile.Name(), rulesFile.Name())
	assert.Equal(t, nil, err)

	if d, _ := c.GetDebugServiceAddr(); d != "localhost:8085" {
		t.Error("received", d, "expected", "localhost:8085")
	}
}

func TestDryRun(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	assert.Equal(t, nil, err)
	defer os.RemoveAll(tmpDir)

	configFile, err := ioutil.TempFile(tmpDir, "*.toml")
	assert.Equal(t, nil, err)

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
	assert.Equal(t, nil, err)

	_, err = rulesFile.Write([]byte(`
	DryRun=true
	`))

	c, err := NewConfig(configFile.Name(), rulesFile.Name())
	assert.Equal(t, nil, err)

	if d := c.GetIsDryRun(); d != true {
		t.Error("received", d, "expected", true)
	}
}

func TestGetSamplerTypes(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	assert.Equal(t, nil, err)
	defer os.RemoveAll(tmpDir)

	configFile, err := ioutil.TempFile(tmpDir, "*.toml")
	assert.Equal(t, nil, err)

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
	assert.Equal(t, nil, err)

	dummyConfig := []byte(`
	[SamplerConfig._default]
		Sampler = "DeterministicSampler"
		SampleRate = 2

	[SamplerConfig.'dataset 1']
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

	_, err = rulesFile.Write(dummyConfig)
	assert.Equal(t, nil, err)
	rulesFile.Close()

	c, err := NewConfig(configFile.Name(), rulesFile.Name())

	if err != nil {
		t.Error(err)
	}

	typ, err := c.GetSamplerTypeForDataset("dataset-doesnt-exist")
	assert.Equal(t, nil, err)
	assert.Equal(t, "DeterministicSampler", typ)

	typ, err = c.GetSamplerTypeForDataset("dataset 1")
	assert.Equal(t, nil, err)
	assert.Equal(t, "DynamicSampler", typ)

	typ, err = c.GetSamplerTypeForDataset("dataset2")
	assert.Equal(t, nil, err)
	assert.Equal(t, "DeterministicSampler", typ)

	typ, err = c.GetSamplerTypeForDataset("dataset3")
	assert.Equal(t, nil, err)
	assert.Equal(t, "EMADynamicSampler", typ)
}

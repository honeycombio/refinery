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

func TestGRPCListenAddrEnvVar(t *testing.T) {
	const address = "127.0.0.1:4317"
	const envVarName = "REFINERY_GRPC_LISTEN_ADDRESS"
	os.Setenv(envVarName, address)
	defer os.Unsetenv(envVarName)

	c, err := NewConfig("../config.toml", "../rules.toml", func(err error) {})

	if err != nil {
		t.Error(err)
	}

	if a, _ := c.GetGRPCListenAddr(); a != address {
		t.Error("received", a, "expected", address)
	}
}

func TestRedisHostEnvVar(t *testing.T) {
	const host = "redis.magic:1337"
	const envVarName = "REFINERY_REDIS_HOST"
	os.Setenv(envVarName, host)
	defer os.Unsetenv(envVarName)

	c, err := NewConfig("../config.toml", "../rules.toml", func(err error) {})

	if err != nil {
		t.Error(err)
	}

	if d, _ := c.GetRedisHost(); d != host {
		t.Error("received", d, "expected", host)
	}
}

func TestRedisUsernameEnvVar(t *testing.T) {
	const username = "admin"
	const envVarName = "REFINERY_REDIS_USERNAME"
	os.Setenv(envVarName, username)
	defer os.Unsetenv(envVarName)

	c, err := NewConfig("../config.toml", "../rules.toml", func(err error) {})

	if err != nil {
		t.Error(err)
	}

	if d, _ := c.GetRedisUsername(); d != username {
		t.Error("received", d, "expected", username)
	}
}

func TestRedisPasswordEnvVar(t *testing.T) {
	const password = "admin1234"
	const envVarName = "REFINERY_REDIS_PASSWORD"
	os.Setenv(envVarName, password)
	defer os.Unsetenv(envVarName)

	c, err := NewConfig("../config.toml", "../rules.toml", func(err error) {})

	if err != nil {
		t.Error(err)
	}

	if d, _ := c.GetRedisPassword(); d != password {
		t.Error("received", d, "expected", password)
	}
}

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

	if d := c.GetAddHostMetadataToTrace(); d != false {
		t.Error("received", d, "expected", false)
	}

	if d := c.GetEnvironmentCacheTTL(); d != time.Hour {
		t.Error("received", d, "expected", time.Hour)
	}

	d, err := c.GetSamplerConfigForDataset("dataset-doesnt-exist")
	assert.NoError(t, err)
	assert.IsType(t, &DeterministicSamplerConfig{}, d)

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

func TestReadRulesConfig(t *testing.T) {
	c, err := NewConfig("../config.toml", "../rules_complete.toml", func(err error) {})

	if err != nil {
		t.Error(err)
	}

	d, err := c.GetSamplerConfigForDataset("dataset-doesnt-exist")
	assert.NoError(t, err)
	assert.IsType(t, &DeterministicSamplerConfig{}, d)

	d, err = c.GetSamplerConfigForDataset("dataset1")
	assert.NoError(t, err)
	assert.IsType(t, &DynamicSamplerConfig{}, d)

	d, err = c.GetSamplerConfigForDataset("dataset4")
	assert.NoError(t, err)
	switch r := d.(type) {
	case *RulesBasedSamplerConfig:
		assert.Len(t, r.Rule, 4)

		var rule *RulesBasedSamplerRule

		rule = r.Rule[0]
		assert.True(t, rule.Drop)
		assert.Equal(t, 0, rule.SampleRate)
		assert.Len(t, rule.Condition, 1)

		rule = r.Rule[1]
		assert.Equal(t, 1, rule.SampleRate)
		assert.Equal(t, "keep slow 500 errors", rule.Name)
		assert.Len(t, rule.Condition, 2)

	default:
		assert.Fail(t, "dataset4 should have a rules based sampler", d)
	}
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

func TestAbsentTraceKeyField(t *testing.T) {
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
	`))
	assert.NoError(t, err)

	_, err = rulesFile.Write([]byte(`
		[dataset1]
			Sampler = "EMADynamicSampler"
			GoalSampleRate = 10
			UseTraceLength = true
			AddSampleRateKeyToTrace = true
			FieldList = "[request.method]"
			Weight = 0.4
	`))

	rulesFile.Close()

	_, err = NewConfig(configFile.Name(), rulesFile.Name(), func(err error) {})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Error:Field validation for 'AddSampleRateKeyToTraceField'")
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
		AddSampleRateKeyToTraceField = "meta.refinery.dynsampler_key"
		FieldList = "[request.method]"
		Weight = 0.3

	[dataset4]

		Sampler = "TotalThroughputSampler"
		GoalThroughputPerSec = 100
		FieldList = "[request.method]"
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

	if d, err := c.GetSamplerConfigForDataset("dataset4"); assert.Equal(t, nil, err) {
		assert.IsType(t, &TotalThroughputSamplerConfig{}, d)
	}
}

func TestDefaultSampler(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	rulesFile, err := ioutil.TempFile(tmpDir, "*.toml")
	assert.NoError(t, err)

	configFile, err := ioutil.TempFile(tmpDir, "*.toml")
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

	c, err := NewConfig(configFile.Name(), rulesFile.Name(), func(err error) {})

	assert.NoError(t, err)

	s, err := c.GetSamplerConfigForDataset("nonexistent")

	assert.NoError(t, err)

	assert.IsType(t, &DeterministicSamplerConfig{}, s)
}

func TestHoneycombLoggerConfig(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	rulesFile, err := ioutil.TempFile(tmpDir, "*.toml")
	assert.NoError(t, err)

	configFile, err := ioutil.TempFile(tmpDir, "*.toml")
	assert.NoError(t, err)

	dummy := []byte(`
	[InMemCollector]
		CacheCapacity=1000

	[HoneycombMetrics]
		MetricsHoneycombAPI="http://honeycomb.io"
		MetricsAPIKey="1234"
		MetricsDataset="testDatasetName"
		MetricsReportingInterval=3

	[HoneycombLogger]
		LoggerHoneycombAPI="http://honeycomb.io"
		LoggerAPIKey="1234"
		LoggerDataset="loggerDataset"
		LoggerSamplerEnabled=true
		LoggerSamplerThroughput=10
	`)

	_, err = configFile.Write(dummy)
	assert.NoError(t, err)
	configFile.Close()

	c, err := NewConfig(configFile.Name(), rulesFile.Name(), func(err error) {})

	assert.NoError(t, err)

	loggerConfig, err := c.GetHoneycombLoggerConfig()

	assert.NoError(t, err)

	assert.Equal(t, "http://honeycomb.io", loggerConfig.LoggerHoneycombAPI)
	assert.Equal(t, "1234", loggerConfig.LoggerAPIKey)
	assert.Equal(t, "loggerDataset", loggerConfig.LoggerDataset)
	assert.Equal(t, true, loggerConfig.LoggerSamplerEnabled)
	assert.Equal(t, 10, loggerConfig.LoggerSamplerThroughput)
}

func TestHoneycombLoggerConfigDefaults(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	rulesFile, err := ioutil.TempFile(tmpDir, "*.toml")
	assert.NoError(t, err)

	configFile, err := ioutil.TempFile(tmpDir, "*.toml")
	assert.NoError(t, err)

	dummy := []byte(`
	[InMemCollector]
		CacheCapacity=1000

	[HoneycombMetrics]
		MetricsHoneycombAPI="http://honeycomb.io"
		MetricsAPIKey="1234"
		MetricsDataset="testDatasetName"
		MetricsReportingInterval=3

	[HoneycombLogger]
		LoggerHoneycombAPI="http://honeycomb.io"
		LoggerAPIKey="1234"
		LoggerDataset="loggerDataset"
	`)

	_, err = configFile.Write(dummy)
	assert.NoError(t, err)
	configFile.Close()

	c, err := NewConfig(configFile.Name(), rulesFile.Name(), func(err error) {})

	assert.NoError(t, err)

	loggerConfig, err := c.GetHoneycombLoggerConfig()

	assert.NoError(t, err)

	assert.Equal(t, false, loggerConfig.LoggerSamplerEnabled)
	assert.Equal(t, 5, loggerConfig.LoggerSamplerThroughput)
}

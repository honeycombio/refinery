package config

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func getConfig(args []string) (Config, error) {
	opts, err := NewCmdEnvOptions(args)
	if err != nil {
		return nil, err
	}
	return NewConfig(opts, func(err error) {})
}

func TestGRPCListenAddrEnvVar(t *testing.T) {
	const address = "127.0.0.1:4317"
	const envVarName = "REFINERY_GRPC_LISTEN_ADDR"
	os.Setenv(envVarName, address)
	defer os.Unsetenv(envVarName)

	c, err := getConfig([]string{"--config", "../config.toml", "--rules_config", "../rules.toml"})
	assert.NoError(t, err)

	if a, _ := c.GetGRPCListenAddr(); a != address {
		t.Error("received", a, "expected", address)
	}
}

func TestRedisHostEnvVar(t *testing.T) {
	const host = "redis.magic:1337"
	const envVarName = "REFINERY_REDIS_HOST"
	os.Setenv(envVarName, host)
	defer os.Unsetenv(envVarName)

	c, err := getConfig([]string{"--config", "../config.toml", "--rules_config", "../rules.toml"})
	assert.NoError(t, err)

	if d, _ := c.GetRedisHost(); d != host {
		t.Error("received", d, "expected", host)
	}
}

func TestRedisUsernameEnvVar(t *testing.T) {
	const username = "admin"
	const envVarName = "REFINERY_REDIS_USERNAME"
	os.Setenv(envVarName, username)
	defer os.Unsetenv(envVarName)

	c, err := getConfig([]string{"--config", "../config.toml", "--rules_config", "../rules.toml"})
	assert.NoError(t, err)

	if d, _ := c.GetRedisUsername(); d != username {
		t.Error("received", d, "expected", username)
	}
}

func TestRedisPasswordEnvVar(t *testing.T) {
	const password = "admin1234"
	const envVarName = "REFINERY_REDIS_PASSWORD"
	os.Setenv(envVarName, password)
	defer os.Unsetenv(envVarName)

	c, err := getConfig([]string{"--config", "../config.toml", "--rules_config", "../rules.toml"})
	assert.NoError(t, err)

	if d, _ := c.GetRedisPassword(); d != password {
		t.Error("received", d, "expected", password)
	}
}

func TestMetricsAPIKeyEnvVar(t *testing.T) {
	testCases := []struct {
		name   string
		envVar string
		key    string
	}{
		{
			name:   "Specific env var",
			envVar: "REFINERY_HONEYCOMB_METRICS_API_KEY",
			key:    "abc123",
		},
		{
			name:   "Fallback env var",
			envVar: "REFINERY_HONEYCOMB_API_KEY",
			key:    "321cba",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			os.Setenv(tc.envVar, tc.key)
			defer os.Unsetenv(tc.envVar)

			c, err := getConfig([]string{"--config", "../config.toml", "--rules_config", "../rules.toml"})
			if err != nil {
				t.Error(err)
			}

			if d, _ := c.GetHoneycombMetricsConfig(); d.MetricsAPIKey != tc.key {
				t.Error("received", d, "expected", tc.key)
			}
		})
	}
}

func TestMetricsAPIKeyMultipleEnvVar(t *testing.T) {
	const specificKey = "abc123"
	const specificEnvVarName = "REFINERY_HONEYCOMB_METRICS_API_KEY"
	const fallbackKey = "this should not be set in the config"
	const fallbackEnvVarName = "REFINERY_HONEYCOMB_API_KEY"

	os.Setenv(specificEnvVarName, specificKey)
	defer os.Unsetenv(specificEnvVarName)
	os.Setenv(fallbackEnvVarName, fallbackKey)
	defer os.Unsetenv(fallbackEnvVarName)

	c, err := getConfig([]string{"--config", "../config.toml", "--rules_config", "../rules.toml"})
	assert.NoError(t, err)

	if d, _ := c.GetHoneycombMetricsConfig(); d.MetricsAPIKey != specificKey {
		t.Error("received", d, "expected", specificKey)
	}
}

func TestMetricsAPIKeyFallbackEnvVar(t *testing.T) {
	const key = "abc1234"
	const envVarName = "REFINERY_HONEYCOMB_API_KEY"
	os.Setenv(envVarName, key)
	defer os.Unsetenv(envVarName)

	c, err := getConfig([]string{"--config", "../config.toml", "--rules_config", "../rules.toml"})
	assert.NoError(t, err)

	if d, _ := c.GetHoneycombMetricsConfig(); d.MetricsAPIKey != key {
		t.Error("received", d, "expected", key)
	}
}

// creates two temporary toml files from the strings passed in and returns their filenames
func createTempConfigs(t *testing.T, configBody string, rulesBody string) (string, string) {
	tmpDir, err := os.MkdirTemp("", "")
	assert.NoError(t, err)

	configFile, err := os.CreateTemp(tmpDir, "cfg_*.toml")
	assert.NoError(t, err)

	if configBody != "" {
		_, err = configFile.WriteString(configBody)
		assert.NoError(t, err)
	}
	configFile.Close()

	rulesFile, err := os.CreateTemp(tmpDir, "rules_*.toml")
	assert.NoError(t, err)

	if rulesBody != "" {
		_, err = rulesFile.WriteString(rulesBody)
		assert.NoError(t, err)
	}
	rulesFile.Close()

	return configFile.Name(), rulesFile.Name()
}

func TestReload(t *testing.T) {
	config, rules := createTempConfigs(t, `
	ListenAddr="0.0.0.0:8080"
	ConfigReloadInterval="1s"

	[InMemCollector]
		CacheCapacity=1000

	[HoneycombMetrics]
		MetricsHoneycombAPI="http://honeycomb.io"
		MetricsAPIKey="1234"
		MetricsDataset="testDatasetName"
		MetricsReportingInterval=3
	`, "")
	defer os.Remove(rules)
	defer os.Remove(config)
	c, err := getConfig([]string{"--config", config, "--rules_config", rules})
	assert.NoError(t, err)

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

	if file, err := os.OpenFile(config, os.O_RDWR, 0644); err == nil {
		file.WriteString(`ListenAddr = "0.0.0.0:9000"`)
		file.Close()
	}

	wg.Wait()

	if d, _ := c.GetListenAddr(); d != "0.0.0.0:9000" {
		t.Error("received", d, "expected", "0.0.0.0:9000")
	}

}

func TestReadDefaults(t *testing.T) {
	c, err := getConfig([]string{"--config", "../config.toml", "--rules_config", "../rules.toml"})
	assert.NoError(t, err)

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

	d, name, err := c.GetSamplerConfigForDestName("dataset-doesnt-exist")
	assert.NoError(t, err)
	assert.IsType(t, &DeterministicSamplerConfig{}, d)
	assert.Equal(t, "DeterministicSampler", name)
}

func TestReadRulesConfig(t *testing.T) {
	// TODO: convert these to YAML
	c, err := getConfig([]string{"--config", "../config.toml", "--rules_config", "../rules_complete.toml"})
	assert.NoError(t, err)

	d, name, err := c.GetSamplerConfigForDestName("dataset-doesnt-exist")
	assert.NoError(t, err)
	assert.IsType(t, &DeterministicSamplerConfig{}, d)
	assert.Equal(t, "DeterministicSampler", name)

	d, name, err = c.GetSamplerConfigForDestName("dataset1")
	assert.NoError(t, err)
	assert.IsType(t, &DynamicSamplerConfig{}, d)
	assert.Equal(t, "DynamicSampler", name)

	d, name, err = c.GetSamplerConfigForDestName("dataset4")
	assert.NoError(t, err)
	switch r := d.(type) {
	case *RulesBasedSamplerConfig:
		assert.Len(t, r.Rule, 6)

		var rule *RulesBasedSamplerRule

		rule = r.Rule[0]
		assert.True(t, rule.Drop)
		assert.Equal(t, 0, rule.SampleRate)
		assert.Len(t, rule.Condition, 1)

		rule = r.Rule[1]
		assert.Equal(t, 1, rule.SampleRate)
		assert.Equal(t, "keep slow 500 errors", rule.Name)
		assert.Len(t, rule.Condition, 2)

		rule = r.Rule[4]
		assert.Equal(t, 5, rule.SampleRate)
		assert.Equal(t, "span", rule.Scope)

		rule = r.Rule[5]
		assert.Equal(t, 10, rule.SampleRate)
		assert.Equal(t, "", rule.Scope)

		assert.Equal(t, "RulesBasedSampler", name)

	default:
		assert.Fail(t, "dataset4 should have a rules based sampler", d)
	}
}

func TestPeerManagementType(t *testing.T) {
	config, rules := createTempConfigs(t, `
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
		RedisPrefix = "testPrefix"
		RedisDatabase = 9
	`, "")
	defer os.Remove(rules)
	defer os.Remove(config)
	c, err := getConfig([]string{"--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	if d, _ := c.GetPeerManagementType(); d != "redis" {
		t.Error("received", d, "expected", "redis")
	}

	if s := c.GetRedisPrefix(); s != "testPrefix" {
		t.Error("received", s, "expected", "testPrefix")
	}

	if db := c.GetRedisDatabase(); db != 9 {
		t.Error("received", db, "expected", 9)
	}
}

func TestAbsentTraceKeyField(t *testing.T) {
	config, rules := createTempConfigs(t, `
	[InMemCollector]
		CacheCapacity=1000

	[HoneycombMetrics]
		MetricsHoneycombAPI="http://honeycomb.io"
		MetricsAPIKey="1234"
		MetricsDataset="testDatasetName"
		MetricsReportingInterval=3
	`, `
	[dataset1]
		Sampler = "EMADynamicSampler"
		GoalSampleRate = 10
		UseTraceLength = true
		AddSampleRateKeyToTrace = true
		FieldList = [ "request.method" ]
		Weight = 0.4
	`)
	defer os.Remove(rules)
	defer os.Remove(config)
	cfg, err := getConfig([]string{"--config", config, "--rules_config", rules})
	assert.NoError(t, err)
	_, samplerName, err := cfg.GetSamplerConfigForDestName("dataset1")
	assert.Error(t, err)
	assert.Equal(t, "EMADynamicSampler", samplerName)
	assert.Contains(t, err.Error(), "Error:Field validation for 'AddSampleRateKeyToTraceField'")
}

func TestDebugServiceAddr(t *testing.T) {
	config, rules := createTempConfigs(t, `
	DebugServiceAddr = "localhost:8085"

	[InMemCollector]
		CacheCapacity=1000

	[HoneycombMetrics]
		MetricsHoneycombAPI="http://honeycomb.io"
		MetricsAPIKey="1234"
		MetricsDataset="testDatasetName"
		MetricsReportingInterval=3
	`, "")
	defer os.Remove(rules)
	defer os.Remove(config)
	c, err := getConfig([]string{"--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	if d, _ := c.GetDebugServiceAddr(); d != "localhost:8085" {
		t.Error("received", d, "expected", "localhost:8085")
	}
}

func TestDryRun(t *testing.T) {
	config, rules := createTempConfigs(t, `
	[InMemCollector]
		CacheCapacity=1000

	[HoneycombMetrics]
		MetricsHoneycombAPI="http://honeycomb.io"
		MetricsAPIKey="1234"
		MetricsDataset="testDatasetName"
		MetricsReportingInterval=3
	`, `
	DryRun=true
	`)
	defer os.Remove(rules)
	defer os.Remove(config)
	c, err := getConfig([]string{"--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	if d := c.GetIsDryRun(); d != true {
		t.Error("received", d, "expected", true)
	}
}

func TestMaxAlloc(t *testing.T) {
	config, rules := createTempConfigs(t, `
	[InMemCollector]
		CacheCapacity=1000
		MaxAlloc=17179869184

	[HoneycombMetrics]
		MetricsHoneycombAPI="http://honeycomb.io"
		MetricsAPIKey="1234"
		MetricsDataset="testDatasetName"
		MetricsReportingInterval=3
	`, "")
	defer os.Remove(rules)
	defer os.Remove(config)
	c, err := getConfig([]string{"--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	expected := uint64(16 * 1024 * 1024 * 1024)
	inMemConfig, err := c.GetInMemCollectorCacheCapacity()
	assert.NoError(t, err)
	assert.Equal(t, expected, inMemConfig.MaxAlloc)
}

func TestGetSamplerTypes(t *testing.T) {
	config, rules := createTempConfigs(t, `
	[InMemCollector]
		CacheCapacity=1000

	[HoneycombMetrics]
		MetricsHoneycombAPI="http://honeycomb.io"
		MetricsAPIKey="1234"
		MetricsDataset="testDatasetName"
		MetricsReportingInterval=3
	`, `
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
		FieldList = ["request.method"]
		Weight = 0.3

	[dataset4]

		Sampler = "TotalThroughputSampler"
		GoalThroughputPerSec = 100
		FieldList = ["request.method"]
	`)
	defer os.Remove(rules)
	defer os.Remove(config)
	c, err := getConfig([]string{"--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	if d, name, err := c.GetSamplerConfigForDestName("dataset-doesnt-exist"); assert.Equal(t, nil, err) {
		assert.IsType(t, &DeterministicSamplerConfig{}, d)
		assert.Equal(t, "DeterministicSampler", name)
	}

	if d, name, err := c.GetSamplerConfigForDestName("dataset 1"); assert.Equal(t, nil, err) {
		assert.IsType(t, &DynamicSamplerConfig{}, d)
		assert.Equal(t, "DynamicSampler", name)
	}

	if d, name, err := c.GetSamplerConfigForDestName("dataset2"); assert.Equal(t, nil, err) {
		assert.IsType(t, &DeterministicSamplerConfig{}, d)
		assert.Equal(t, "DeterministicSampler", name)
	}

	if d, name, err := c.GetSamplerConfigForDestName("dataset3"); assert.Equal(t, nil, err) {
		assert.IsType(t, &EMADynamicSamplerConfig{}, d)
		assert.Equal(t, "EMADynamicSampler", name)
	}

	if d, name, err := c.GetSamplerConfigForDestName("dataset4"); assert.Equal(t, nil, err) {
		assert.IsType(t, &TotalThroughputSamplerConfig{}, d)
		assert.Equal(t, "TotalThroughputSampler", name)
	}
}

func TestDefaultSampler(t *testing.T) {
	config, rules := createTempConfigs(t, `
	[InMemCollector]
		CacheCapacity=1000

	[HoneycombMetrics]
		MetricsHoneycombAPI="http://honeycomb.io"
		MetricsAPIKey="1234"
		MetricsDataset="testDatasetName"
		MetricsReportingInterval=3
	`, "")
	defer os.Remove(rules)
	defer os.Remove(config)
	c, err := getConfig([]string{"--config", config, "--rules_config", rules})

	assert.NoError(t, err)

	s, name, err := c.GetSamplerConfigForDestName("nonexistent")

	assert.NoError(t, err)
	assert.Equal(t, "DeterministicSampler", name)

	assert.IsType(t, &DeterministicSamplerConfig{}, s)
}

func TestHoneycombLoggerConfig(t *testing.T) {
	config, rules := createTempConfigs(t, `
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
	`, "")
	defer os.Remove(rules)
	defer os.Remove(config)
	c, err := getConfig([]string{"--config", config, "--rules_config", rules})
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
	config, rules := createTempConfigs(t, `
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
	`, "")
	defer os.Remove(rules)
	defer os.Remove(config)
	c, err := getConfig([]string{"--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	loggerConfig, err := c.GetHoneycombLoggerConfig()

	assert.NoError(t, err)

	assert.Equal(t, false, loggerConfig.LoggerSamplerEnabled)
	assert.Equal(t, 5, loggerConfig.LoggerSamplerThroughput)
}

func TestDatasetPrefix(t *testing.T) {
	config, rules := createTempConfigs(t, `
	DatasetPrefix = "dataset"

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
	`, "")
	defer os.Remove(rules)
	defer os.Remove(config)
	c, err := getConfig([]string{"--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	assert.Equal(t, "dataset", c.GetDatasetPrefix())
}

func TestQueryAuthToken(t *testing.T) {
	config, rules := createTempConfigs(t, `
	QueryAuthToken = "MySeekretToken"

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
		LoggerDataset="loggerDataset"	`, "")
	defer os.Remove(rules)
	defer os.Remove(config)
	c, err := getConfig([]string{"--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	assert.Equal(t, "MySeekretToken", c.GetQueryAuthToken())
}

func TestGRPCServerParameters(t *testing.T) {
	config, rules := createTempConfigs(t, `
	[GRPCServerParameters]
		MaxConnectionIdle = "1m"
		MaxConnectionAge = "2m"
		MaxConnectionAgeGrace = "3m"
		Time = "4m"
		Timeout = "5m"

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
	`, "")
	defer os.Remove(rules)
	defer os.Remove(config)
	c, err := getConfig([]string{"--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	assert.Equal(t, 1*time.Minute, c.GetGRPCMaxConnectionIdle())
	assert.Equal(t, 2*time.Minute, c.GetGRPCMaxConnectionAge())
	assert.Equal(t, 3*time.Minute, c.GetGRPCMaxConnectionAgeGrace())
	assert.Equal(t, 4*time.Minute, c.GetGRPCTime())
	assert.Equal(t, 5*time.Minute, c.GetGRPCTimeout())
}

func TestHoneycombAdditionalErrorConfig(t *testing.T) {
	config, rules := createTempConfigs(t, `
	AdditionalErrorFields = [
		"first",
		"second"
	]

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
	`, "")
	defer os.Remove(rules)
	defer os.Remove(config)
	c, err := getConfig([]string{"--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	assert.Equal(t, []string{"first", "second"}, c.GetAdditionalErrorFields())
}

func TestHoneycombAdditionalErrorDefaults(t *testing.T) {
	config, rules := createTempConfigs(t, `
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
	`, "")
	defer os.Remove(rules)
	defer os.Remove(config)
	c, err := getConfig([]string{"--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	assert.Equal(t, []string{"trace.span_id"}, c.GetAdditionalErrorFields())
}

func TestSampleCacheParameters(t *testing.T) {
	config, rules := createTempConfigs(t, `
	[InMemCollector]
		CacheCapacity=1000

	[HoneycombMetrics]
		MetricsHoneycombAPI="http://honeycomb.io"
		MetricsAPIKey="1234"
		MetricsDataset="testDatasetName"
		MetricsReportingInterval=3

	`, "")
	defer os.Remove(rules)
	defer os.Remove(config)
	c, err := getConfig([]string{"--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	s := c.GetSampleCacheConfig()
	assert.Equal(t, "legacy", s.Type)
	assert.Equal(t, uint(10_000), s.KeptSize)
	assert.Equal(t, uint(1_000_000), s.DroppedSize)
	assert.Equal(t, 10*time.Second, time.Duration(s.SizeCheckInterval))
}

func TestSampleCacheParametersCuckoo(t *testing.T) {
	config, rules := createTempConfigs(t, `
	[InMemCollector]
		CacheCapacity=1000

	[HoneycombMetrics]
		MetricsHoneycombAPI="http://honeycomb.io"
		MetricsAPIKey="1234"
		MetricsDataset="testDatasetName"
		MetricsReportingInterval=3

	[SampleCache]
		Type="cuckoo"
		KeptSize=100_000
		DroppedSize=10_000_000
		SizeCheckInterval="60s"
	`, "")
	defer os.Remove(rules)
	defer os.Remove(config)
	c, err := getConfig([]string{"--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	s := c.GetSampleCacheConfig()
	assert.Equal(t, "cuckoo", s.Type)
	assert.Equal(t, uint(100_000), s.KeptSize)
	assert.Equal(t, uint(10_000_000), s.DroppedSize)
	assert.Equal(t, 1*time.Minute, time.Duration(s.SizeCheckInterval))
}

func TestAdditionalAttributes(t *testing.T) {
	config, rules := createTempConfigs(t, `
	[AdditionalAttributes]
		name="foo"
		other="bar"
		another="OneHundred"

	[InMemCollector]
		CacheCapacity=1000

	[HoneycombMetrics]
		MetricsHoneycombAPI="http://honeycomb.io"
		MetricsAPIKey="1234"
		MetricsDataset="testDatasetName"
		MetricsReportingInterval=3
	`, "")
	defer os.Remove(rules)
	defer os.Remove(config)
	c, err := getConfig([]string{"--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	assert.Equal(t, map[string]string{"name": "foo", "other": "bar", "another": "OneHundred"}, c.GetAdditionalAttributes())
}

func TestHoneycombIdFieldsConfig(t *testing.T) {
	config, rules := createTempConfigs(t, `
	TraceIdFieldNames = [
		"first",
		"second"
	]
	ParentIdFieldNames = [
		"zero",
		"one"
	]
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
	`, "")
	defer os.Remove(rules)
	defer os.Remove(config)
	c, err := getConfig([]string{"--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	assert.Equal(t, []string{"first", "second"}, c.GetTraceIdFieldNames())
	assert.Equal(t, []string{"zero", "one"}, c.GetParentIdFieldNames())
}

func TestHoneycombIdFieldsConfigDefault(t *testing.T) {
	config, rules := createTempConfigs(t, `
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
	`, "")
	defer os.Remove(rules)
	defer os.Remove(config)
	c, err := getConfig([]string{"--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	assert.Equal(t, []string{"trace.trace_id", "traceId"}, c.GetTraceIdFieldNames())
	assert.Equal(t, []string{"trace.parent_id", "parentId"}, c.GetParentIdFieldNames())
}

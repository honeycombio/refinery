package config

import (
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

func getConfig(args []string) (Config, error) {
	opts, err := NewCmdEnvOptions(args)
	if err != nil {
		return nil, err
	}
	return NewConfig(opts, func(err error) {})
}

// creates two temporary yaml files from the strings passed in and returns their filenames
func createTempConfigs(t *testing.T, configBody, rulesBody string) (string, string) {
	tmpDir, err := os.MkdirTemp("", "")
	assert.NoError(t, err)

	configFile, err := os.CreateTemp(tmpDir, "cfg_*.yaml")
	assert.NoError(t, err)

	_, err = configFile.WriteString(configBody)
	assert.NoError(t, err)
	configFile.Close()

	rulesFile, err := os.CreateTemp(tmpDir, "rules_*.yaml")
	assert.NoError(t, err)

	_, err = rulesFile.WriteString(rulesBody)
	assert.NoError(t, err)
	rulesFile.Close()

	return configFile.Name(), rulesFile.Name()
}

func setMap(m map[string]any, key string, value any) {
	if strings.Contains(key, ".") {
		parts := strings.Split(key, ".")
		if _, ok := m[parts[0]]; !ok {
			m[parts[0]] = make(map[string]any)
		}
		setMap(m[parts[0]].(map[string]any), strings.Join(parts[1:], "."), value)
		return
	}
	m[key] = value
}

func makeYAML(args ...interface{}) string {
	m := make(map[string]any)
	for i := 0; i < len(args); i += 2 {
		setMap(m, args[i].(string), args[i+1])
	}
	b, err := yaml.Marshal(m)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func TestGRPCListenAddrEnvVar(t *testing.T) {
	const address = "127.0.0.1:4317"
	const envVarName = "REFINERY_GRPC_LISTEN_ADDR"
	os.Setenv(envVarName, address)
	defer os.Unsetenv(envVarName)

	c, err := getConfig([]string{"--config", "../config.yaml", "--rules_config", "../rules.yaml"})
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

	c, err := getConfig([]string{"--config", "../config.yaml", "--rules_config", "../rules.yaml"})
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

	c, err := getConfig([]string{"--config", "../config.yaml", "--rules_config", "../rules.yaml"})
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

	c, err := getConfig([]string{"--config", "../config.yaml", "--rules_config", "../rules.yaml"})
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
			envVar: "REFINERY_LEGACY_METRICS_API_KEY",
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

			c, err := getConfig([]string{"--config", "../config.yaml", "--rules_config", "../rules.yaml"})
			if err != nil {
				t.Error(err)
			}

			if d, _ := c.GetHoneycombMetricsConfig(); d.APIKey != tc.key {
				t.Error("received", d, "expected", tc.key)
			}
		})
	}
}

func TestMetricsAPIKeyMultipleEnvVar(t *testing.T) {
	const specificKey = "abc123"
	const specificEnvVarName = "REFINERY_LEGACY_METRICS_API_KEY"
	const fallbackKey = "this should not be set in the config"
	const fallbackEnvVarName = "REFINERY_HONEYCOMB_API_KEY"

	os.Setenv(specificEnvVarName, specificKey)
	defer os.Unsetenv(specificEnvVarName)
	os.Setenv(fallbackEnvVarName, fallbackKey)
	defer os.Unsetenv(fallbackEnvVarName)

	c, err := getConfig([]string{"--config", "../config.yaml", "--rules_config", "../rules.yaml"})
	assert.NoError(t, err)

	if d, _ := c.GetHoneycombMetricsConfig(); d.APIKey != specificKey {
		t.Error("received", d, "expected", specificKey)
	}
}

func TestMetricsAPIKeyFallbackEnvVar(t *testing.T) {
	const key = "abc1234"
	const envVarName = "REFINERY_HONEYCOMB_API_KEY"
	os.Setenv(envVarName, key)
	defer os.Unsetenv(envVarName)

	c, err := getConfig([]string{"--config", "../config.yaml", "--rules_config", "../rules.yaml"})
	assert.NoError(t, err)

	if d, _ := c.GetHoneycombMetricsConfig(); d.APIKey != key {
		t.Error("received", d, "expected", key)
	}
}

func TestReload(t *testing.T) {
	cm := makeYAML("General.ConfigurationVersion", 2, "General.ConfigReloadInterval", Duration(1*time.Second), "Network.ListenAddr", "0.0.0.0:8080")
	rm := makeYAML("ConfigVersion", 2)
	config, rules := createTempConfigs(t, cm, rm)
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
		cm := makeYAML("General.ConfigurationVersion", 2, "General.ConfigReloadInterval", Duration(1*time.Second), "Network.ListenAddr", "0.0.0.0:9000")
		file.WriteString(cm)
		file.Close()
	}

	wg.Wait()

	if d, _ := c.GetListenAddr(); d != "0.0.0.0:9000" {
		t.Error("received", d, "expected", "0.0.0.0:9000")
	}

}

func TestReadDefaults(t *testing.T) {
	c, err := getConfig([]string{"--config", "../config.yaml", "--rules_config", "../rules.yaml"})
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

	if d := c.GetDryRunFieldName(); d != "meta.refinery.dryrun.kept" {
		t.Error("received", d, "expected", "meta.refinery.dryrun.kept")
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
	c, err := getConfig([]string{"--config", "../config.yaml", "--rules_config", "../rules_complete.yaml"})
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
		assert.Len(t, r.Rules, 6)

		var rule *RulesBasedSamplerRule

		rule = r.Rules[0]
		assert.True(t, rule.Drop)
		assert.Equal(t, 0, rule.SampleRate)
		assert.Len(t, rule.Conditions, 1)

		rule = r.Rules[1]
		assert.Equal(t, 1, rule.SampleRate)
		assert.Equal(t, "keep slow 500 errors", rule.Name)
		assert.Len(t, rule.Conditions, 2)

		rule = r.Rules[4]
		assert.Equal(t, 5, rule.SampleRate)
		assert.Equal(t, "span", rule.Scope)

		rule = r.Rules[5]
		assert.Equal(t, 10, rule.SampleRate)
		assert.Equal(t, "", rule.Scope)

		assert.Equal(t, "RulesBasedSampler", name)

	default:
		assert.Fail(t, "dataset4 should have a rules based sampler", d)
	}
}

func TestPeerManagementType(t *testing.T) {
	cm := makeYAML(
		"General.ConfigurationVersion", 2,
		"PeerManagement.Type", "redis",
		"PeerManagement.Peers", []string{"http://refinery-1231:8080"},
		"RedisPeerManagement.Prefix", "testPrefix",
		"RedisPeerManagement.Database", 9,
	)
	rm := makeYAML("ConfigVersion", 2)
	config, rules := createTempConfigs(t, cm, rm)
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

func TestDebugServiceAddr(t *testing.T) {
	cm := makeYAML("General.ConfigurationVersion", 2, "Debugging.DebugServiceAddr", "localhost:8085")
	rm := makeYAML("ConfigVersion", 2)
	config, rules := createTempConfigs(t, cm, rm)
	defer os.Remove(rules)
	defer os.Remove(config)
	c, err := getConfig([]string{"--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	if d, _ := c.GetDebugServiceAddr(); d != "localhost:8085" {
		t.Error("received", d, "expected", "localhost:8085")
	}
}

func TestDryRun(t *testing.T) {
	cm := makeYAML("General.ConfigurationVersion", 2, "Debugging.DryRun", true)
	rm := makeYAML("ConfigVersion", 2)
	config, rules := createTempConfigs(t, cm, rm)
	defer os.Remove(rules)
	defer os.Remove(config)
	c, err := getConfig([]string{"--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	if d := c.GetIsDryRun(); d != true {
		t.Error("received", d, "expected", true)
	}
}

func TestMaxAlloc(t *testing.T) {
	cm := makeYAML("General.ConfigurationVersion", 2, "Collection.CacheCapacity", 1000, "Collection.MaxAlloc", 17179869184)
	rm := makeYAML("ConfigVersion", 2)
	config, rules := createTempConfigs(t, cm, rm)
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
	cm := makeYAML("General.ConfigurationVersion", 2)
	rm := makeYAML(
		"ConfigVersion", 2,
		"Samplers.__default__.DeterministicSampler.SampleRate", 5,
		"Samplers.dataset 1.DynamicSampler.SampleRate", 2,
		"Samplers.dataset 1.DynamicSampler.FieldList", []string{"request.method", "response.status_code"},
		"Samplers.dataset 1.DynamicSampler.UseTraceLength", true,
		"Samplers.dataset 1.DynamicSampler.AddSampleRateKeyToTrace", true,
		"Samplers.dataset 1.DynamicSampler.AddSampleRateKeyToTraceField", "meta.refinery.dynsampler_key",
		"Samplers.dataset 1.DynamicSampler.ClearFrequencySec", 60,
		"Samplers.dataset2.DeterministicSampler.SampleRate", 10,
		"Samplers.dataset3.EMADynamicSampler.GoalSampleRate", 10,
		"Samplers.dataset3.EMADynamicSampler.UseTraceLength", true,
		"Samplers.dataset3.EMADynamicSampler.AddSampleRateKeyToTrace", true,
		"Samplers.dataset3.EMADynamicSampler.AddSampleRateKeyToTraceField", "meta.refinery.dynsampler_key",
		"Samplers.dataset3.EMADynamicSampler.FieldList", []string{"request.method"},
		"Samplers.dataset3.EMADynamicSampler.Weight", 0.3,
		"Samplers.dataset4.TotalThroughputSampler.GoalThroughputPerSec", 100,
		"Samplers.dataset4.TotalThroughputSampler.FieldList", []string{"request.method"},
	)
	config, rules := createTempConfigs(t, cm, rm)
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
	t.Skip("This tests for a default sampler, but we are currently not requiring explicit default samplers.")
	cm := makeYAML("General.ConfigurationVersion", 2)
	rm := makeYAML("ConfigVersion", 2)
	config, rules := createTempConfigs(t, cm, rm)
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
	cm := makeYAML(
		"General.ConfigurationVersion", 2,
		"Logger.Type", "honeycomb",
		"HoneycombLogger.APIHost", "http://honeycomb.io",
		"HoneycombLogger.APIKey", "1234",
		"HoneycombLogger.Dataset", "loggerDataset",
		"HoneycombLogger.SamplerEnabled", true,
		"HoneycombLogger.SamplerThroughput", 10,
	)
	rm := makeYAML("ConfigVersion", 2)
	config, rules := createTempConfigs(t, cm, rm)
	defer os.Remove(rules)
	defer os.Remove(config)
	c, err := getConfig([]string{"--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	loggerConfig, err := c.GetHoneycombLoggerConfig()

	assert.NoError(t, err)

	assert.Equal(t, "http://honeycomb.io", loggerConfig.APIHost)
	assert.Equal(t, "1234", loggerConfig.APIKey)
	assert.Equal(t, "loggerDataset", loggerConfig.Dataset)
	assert.Equal(t, true, loggerConfig.SamplerEnabled)
	assert.Equal(t, 10, loggerConfig.SamplerThroughput)
}

func TestHoneycombLoggerConfigDefaults(t *testing.T) {
	cm := makeYAML(
		"General.ConfigurationVersion", 2,
		"Logger.Type", "honeycomb",
		"HoneycombLogger.APIHost", "http://honeycomb.io",
		"HoneycombLogger.APIKey", "1234",
		"HoneycombLogger.Dataset", "loggerDataset",
	)
	rm := makeYAML("ConfigVersion", 2)
	config, rules := createTempConfigs(t, cm, rm)
	defer os.Remove(rules)
	defer os.Remove(config)
	c, err := getConfig([]string{"--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	loggerConfig, err := c.GetHoneycombLoggerConfig()

	assert.NoError(t, err)

	assert.Equal(t, false, loggerConfig.SamplerEnabled)
	assert.Equal(t, 5, loggerConfig.SamplerThroughput)
}

func TestDatasetPrefix(t *testing.T) {
	cm := makeYAML(
		"General.ConfigurationVersion", 2,
		"General.DatasetPrefix", "dataset",
	)
	rm := makeYAML("ConfigVersion", 2)
	config, rules := createTempConfigs(t, cm, rm)
	defer os.Remove(rules)
	defer os.Remove(config)
	c, err := getConfig([]string{"--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	assert.Equal(t, "dataset", c.GetDatasetPrefix())
}

func TestQueryAuthToken(t *testing.T) {
	cm := makeYAML(
		"General.ConfigurationVersion", 2,
		"Debugging.QueryAuthToken", "MySeekretToken",
	)
	rm := makeYAML("ConfigVersion", 2)
	config, rules := createTempConfigs(t, cm, rm)
	defer os.Remove(rules)
	defer os.Remove(config)
	c, err := getConfig([]string{"--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	assert.Equal(t, "MySeekretToken", c.GetQueryAuthToken())
}

func TestGRPCServerParameters(t *testing.T) {
	cm := makeYAML(
		"General.ConfigurationVersion", 2,
		"GRPCServerParameters.MaxConnectionIdle", "1m",
		"GRPCServerParameters.MaxConnectionAge", "2m",
		"GRPCServerParameters.MaxConnectionAgeGrace", "3m",
		"GRPCServerParameters.KeepAlive", "4m",
		"GRPCServerParameters.KeepAliveTimeout", "5m",
	)
	rm := makeYAML("ConfigVersion", 2)
	config, rules := createTempConfigs(t, cm, rm)
	defer os.Remove(rules)
	defer os.Remove(config)
	c, err := getConfig([]string{"--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	assert.Equal(t, 1*time.Minute, c.GetGRPCMaxConnectionIdle())
	assert.Equal(t, 2*time.Minute, c.GetGRPCMaxConnectionAge())
	assert.Equal(t, 3*time.Minute, c.GetGRPCMaxConnectionAgeGrace())
	assert.Equal(t, 4*time.Minute, c.GetGRPCKeepAlive())
	assert.Equal(t, 5*time.Minute, c.GetGRPCKeepAliveTimeout())
}

func TestHoneycombAdditionalErrorConfig(t *testing.T) {
	cm := makeYAML(
		"General.ConfigurationVersion", 2,
		"Debugging.AdditionalErrorFields", []string{"first", "second"},
	)
	rm := makeYAML("ConfigVersion", 2)
	config, rules := createTempConfigs(t, cm, rm)
	defer os.Remove(rules)
	defer os.Remove(config)
	c, err := getConfig([]string{"--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	assert.Equal(t, []string{"first", "second"}, c.GetAdditionalErrorFields())
}

func TestHoneycombAdditionalErrorDefaults(t *testing.T) {
	cm := makeYAML("General.ConfigurationVersion", 2)
	rm := makeYAML("ConfigVersion", 2)
	config, rules := createTempConfigs(t, cm, rm)
	defer os.Remove(rules)
	defer os.Remove(config)
	c, err := getConfig([]string{"--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	assert.Equal(t, []string{"trace.span_id"}, c.GetAdditionalErrorFields())
}

func TestSampleCacheParameters(t *testing.T) {
	cm := makeYAML("General.ConfigurationVersion", 2)
	rm := makeYAML("ConfigVersion", 2)
	config, rules := createTempConfigs(t, cm, rm)
	defer os.Remove(rules)
	defer os.Remove(config)
	c, err := getConfig([]string{"--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	s := c.GetSampleCacheConfig()
	assert.Equal(t, uint(10_000), s.KeptSize)
	assert.Equal(t, uint(1_000_000), s.DroppedSize)
	assert.Equal(t, 10*time.Second, time.Duration(s.SizeCheckInterval))
}

func TestSampleCacheParametersCuckoo(t *testing.T) {
	cm := makeYAML(
		"General.ConfigurationVersion", 2,
		"SampleCache.KeptSize", 100_000,
		"SampleCache.DroppedSize", 10_000_000,
		"SampleCache.SizeCheckInterval", "60s",
	)
	rm := makeYAML("ConfigVersion", 2)
	config, rules := createTempConfigs(t, cm, rm)
	defer os.Remove(rules)
	defer os.Remove(config)
	c, err := getConfig([]string{"--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	s := c.GetSampleCacheConfig()
	assert.Equal(t, uint(100_000), s.KeptSize)
	assert.Equal(t, uint(10_000_000), s.DroppedSize)
	assert.Equal(t, 1*time.Minute, time.Duration(s.SizeCheckInterval))
}

func TestAdditionalAttributes(t *testing.T) {
	cm := makeYAML(
		"General.ConfigurationVersion", 2,
		"Specialized.AdditionalAttributes", map[string]string{
			"name":    "foo",
			"other":   "bar",
			"another": "OneHundred",
		},
	)
	rm := makeYAML("ConfigVersion", 2)
	config, rules := createTempConfigs(t, cm, rm)
	defer os.Remove(rules)
	defer os.Remove(config)
	c, err := getConfig([]string{"--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	assert.Equal(t, map[string]string{"name": "foo", "other": "bar", "another": "OneHundred"}, c.GetAdditionalAttributes())
}

func TestHoneycombIdFieldsConfig(t *testing.T) {
	cm := makeYAML(
		"General.ConfigurationVersion", 2,
		"IDFieldNames.Trace", []string{"first", "second"},
		"IDFieldNames.Parent", []string{"zero", "one"},
	)
	rm := makeYAML("ConfigVersion", 2)
	config, rules := createTempConfigs(t, cm, rm)
	defer os.Remove(rules)
	defer os.Remove(config)
	c, err := getConfig([]string{"--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	assert.Equal(t, []string{"first", "second"}, c.GetTraceIdFieldNames())
	assert.Equal(t, []string{"zero", "one"}, c.GetParentIdFieldNames())
}

func TestHoneycombIdFieldsConfigDefault(t *testing.T) {
	cm := makeYAML("General.ConfigurationVersion", 2)
	rm := makeYAML("ConfigVersion", 2)
	config, rules := createTempConfigs(t, cm, rm)
	defer os.Remove(rules)
	defer os.Remove(config)
	c, err := getConfig([]string{"--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	assert.Equal(t, []string{"trace.trace_id", "traceId"}, c.GetTraceIdFieldNames())
	assert.Equal(t, []string{"trace.parent_id", "parentId"}, c.GetParentIdFieldNames())
}

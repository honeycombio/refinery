package config_test

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/configwatcher"
	"github.com/honeycombio/refinery/pubsub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func getConfig(args []string) (config.Config, error) {
	opts, err := config.NewCmdEnvOptions(args)
	if err != nil {
		return nil, err
	}
	return config.NewConfig(opts, func(err error) {})
}

// creates two temporary yaml files from the strings passed in and returns their filenames
func createTempConfigs(t *testing.T, configBody, rulesBody string) (string, string) {
	tmpDir := t.TempDir()

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
	const envVarName = "REFINERY_GRPC_LISTEN_ADDRESS"
	t.Setenv(envVarName, address)

	c, err := getConfig([]string{"--no-validate", "--config", "../config.yaml", "--rules_config", "../rules.yaml"})
	assert.NoError(t, err)

	if a := c.GetGRPCListenAddr(); a != address {
		t.Error("received", a, "expected", address)
	}
}

func TestRedisHostEnvVar(t *testing.T) {
	const host = "redis.magic:1337"
	const envVarName = "REFINERY_REDIS_HOST"
	t.Setenv(envVarName, host)

	c, err := getConfig([]string{"--no-validate", "--config", "../config.yaml", "--rules_config", "../rules.yaml"})
	assert.NoError(t, err)

	if d := c.GetRedisPeerManagement().Host; d != host {
		t.Error("received", d, "expected", host)
	}
}

func TestRedisUsernameEnvVar(t *testing.T) {
	const username = "admin"
	const envVarName = "REFINERY_REDIS_USERNAME"
	t.Setenv(envVarName, username)

	c, err := getConfig([]string{"--no-validate", "--config", "../config.yaml", "--rules_config", "../rules.yaml"})
	assert.NoError(t, err)

	if d := c.GetRedisPeerManagement().Username; d != username {
		t.Error("received", d, "expected", username)
	}
}

func TestRedisPasswordEnvVar(t *testing.T) {
	const password = "admin1234"
	const envVarName = "REFINERY_REDIS_PASSWORD"
	t.Setenv(envVarName, password)

	c, err := getConfig([]string{"--no-validate", "--config", "../config.yaml", "--rules_config", "../rules.yaml"})
	assert.NoError(t, err)

	if d := c.GetRedisPeerManagement().Password; d != password {
		t.Error("received", d, "expected", password)
	}
}

func TestRedisAuthCodeEnvVar(t *testing.T) {
	const authCode = "A:LKNGSDKLSHOE&SDLFKN"
	const envVarName = "REFINERY_REDIS_AUTH_CODE"
	t.Setenv(envVarName, authCode)

	c, err := getConfig([]string{"--no-validate", "--config", "../config.yaml", "--rules_config", "../rules.yaml"})
	assert.NoError(t, err)

	if d := c.GetRedisPeerManagement().AuthCode; d != authCode {
		t.Error("received", d, "expected", authCode)
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
			t.Setenv(tc.envVar, tc.key)

			c, err := getConfig([]string{"--no-validate", "--config", "../config.yaml", "--rules_config", "../rules.yaml"})
			if err != nil {
				t.Error(err)
			}

			if d := c.GetLegacyMetricsConfig(); d.APIKey != tc.key {
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

	t.Setenv(specificEnvVarName, specificKey)
	t.Setenv(fallbackEnvVarName, fallbackKey)

	c, err := getConfig([]string{"--no-validate", "--config", "../config.yaml", "--rules_config", "../rules.yaml"})
	assert.NoError(t, err)

	if d := c.GetLegacyMetricsConfig(); d.APIKey != specificKey {
		t.Error("received", d, "expected", specificKey)
	}
}

func TestMetricsAPIKeyFallbackEnvVar(t *testing.T) {
	const key = "abc1234"
	const envVarName = "REFINERY_HONEYCOMB_API_KEY"
	t.Setenv(envVarName, key)

	c, err := getConfig([]string{"--no-validate", "--config", "../config.yaml", "--rules_config", "../rules.yaml"})
	assert.NoError(t, err)

	if d := c.GetLegacyMetricsConfig(); d.APIKey != key {
		t.Error("received", d, "expected", key)
	}
}

func TestReload(t *testing.T) {
	cm := makeYAML("General.ConfigurationVersion", 2, "General.ConfigReloadInterval", config.Duration(1*time.Second), "Network.ListenAddr", "0.0.0.0:8080")
	rm := makeYAML("ConfigVersion", 2)
	cfg, rules := createTempConfigs(t, cm, rm)
	c, err := getConfig([]string{"--no-validate", "--config", cfg, "--rules_config", rules})
	assert.NoError(t, err)

	pubsub := &pubsub.LocalPubSub{
		Config: c,
	}
	pubsub.Start()
	defer pubsub.Stop()
	watcher := &configwatcher.ConfigWatcher{
		Config: c,
		PubSub: pubsub,
	}
	watcher.Start()
	defer watcher.Stop()

	if d := c.GetListenAddr(); d != "0.0.0.0:8080" {
		t.Error("received", d, "expected", "0.0.0.0:8080")
	}

	wg := &sync.WaitGroup{}

	ch := make(chan interface{}, 1)

	c.RegisterReloadCallback(func(cfgHash, ruleHash string) {
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
			close(ch)
		}
	}()

	if file, err := os.OpenFile(cfg, os.O_RDWR, 0644); err == nil {
		cm := makeYAML("General.ConfigurationVersion", 2, "General.ConfigReloadInterval", config.Duration(1*time.Second), "Network.ListenAddr", "0.0.0.0:9000")
		file.WriteString(cm)
		file.Close()
	}

	wg.Wait()

	if d := c.GetListenAddr(); d != "0.0.0.0:9000" {
		t.Error("received", d, "expected", "0.0.0.0:9000")
	}

}

func TestReloadDisabled(t *testing.T) {
	cm := makeYAML("General.ConfigurationVersion", 2, "General.ConfigReloadInterval", config.Duration(0*time.Second), "Network.ListenAddr", "0.0.0.0:8080")
	rm := makeYAML("ConfigVersion", 2)
	cfg, rules := createTempConfigs(t, cm, rm)
	c, err := getConfig([]string{"--no-validate", "--config", cfg, "--rules_config", rules})
	assert.NoError(t, err)

	if d := c.GetListenAddr(); d != "0.0.0.0:8080" {
		t.Error("received", d, "expected", "0.0.0.0:8080")
	}

	if file, err := os.OpenFile(cfg, os.O_RDWR, 0644); err == nil {
		// Since we disabled reload checking this should not change anything
		cm := makeYAML("General.ConfigurationVersion", 2, "General.ConfigReloadInterval", config.Duration(0*time.Second), "Network.ListenAddr", "0.0.0.0:9000")
		file.WriteString(cm)
		file.Close()
	}

	time.Sleep(5 * time.Second)

	if d := c.GetListenAddr(); d != "0.0.0.0:8080" {
		t.Error("received", d, "expected", "0.0.0.0:8080")
	}
}

func TestReadDefaults(t *testing.T) {
	c, err := getConfig([]string{"--no-validate", "--config", "../config.yaml", "--rules_config", "../rules.yaml"})
	assert.NoError(t, err)

	if d := c.GetTracesConfig().GetSendDelay(); d != 2*time.Second {
		t.Error("received", d, "expected", 2*time.Second)
	}

	if d := c.GetTracesConfig().GetTraceTimeout(); d != 60*time.Second {
		t.Error("received", d, "expected", 60*time.Second)
	}

	if d := c.GetTracesConfig().GetSendTickerValue(); d != 100*time.Millisecond {
		t.Error("received", d, "expected", 100*time.Millisecond)
	}

	if d := c.GetPeerManagementType(); d != "file" {
		t.Error("received", d, "expected", "file")
	}

	if d := c.GetUseIPV6Identifier(); d != false {
		t.Error("received", d, "expected", false)
	}

	if d := c.GetIsDryRun(); d != false {
		t.Error("received", d, "expected", false)
	}

	if d := c.GetAddHostMetadataToTrace(); d != true {
		t.Error("received", d, "expected", true)
	}

	if d := c.GetEnvironmentCacheTTL(); d != time.Hour {
		t.Error("received", d, "expected", time.Hour)
	}

	d, name := c.GetSamplerConfigForDestName("dataset-doesnt-exist")
	assert.IsType(t, &config.DeterministicSamplerConfig{}, d)
	assert.Equal(t, "DeterministicSampler", name)
}

func TestReadRulesConfig(t *testing.T) {
	c, err := getConfig([]string{"--no-validate", "--config", "../config.yaml", "--rules_config", "../rules_complete.yaml"})
	assert.NoError(t, err)

	d, name := c.GetSamplerConfigForDestName("doesnt-exist")
	assert.IsType(t, &config.DeterministicSamplerConfig{}, d)
	assert.Equal(t, "DeterministicSampler", name)

	d, name = c.GetSamplerConfigForDestName("env1")
	assert.IsType(t, &config.DynamicSamplerConfig{}, d)
	assert.Equal(t, "DynamicSampler", name)

	d, name = c.GetSamplerConfigForDestName("env4")
	switch r := d.(type) {
	case *config.RulesBasedSamplerConfig:
		assert.Len(t, r.Rules, 7)

		var rule *config.RulesBasedSamplerRule

		rule = r.Rules[0]
		assert.True(t, rule.Drop)
		assert.Equal(t, 0, rule.SampleRate)
		assert.Len(t, rule.Conditions, 1)

		rule = r.Rules[2]
		assert.Equal(t, 1, rule.SampleRate)
		assert.Equal(t, "keep slow 500 errors across semantic conventions", rule.Name)
		assert.Len(t, rule.Conditions, 2)

		rule = r.Rules[4]
		assert.Equal(t, 5, rule.SampleRate)
		assert.Equal(t, "span", rule.Scope)

		rule = r.Rules[6]
		assert.Equal(t, 10, rule.SampleRate)
		assert.Equal(t, "", rule.Scope)

		assert.Equal(t, "RulesBasedSampler", name)

	default:
		assert.Fail(t, "env4 should have a rules based sampler", d)
	}
}

func TestPeerManagementType(t *testing.T) {
	cm := makeYAML(
		"General.ConfigurationVersion", 2,
		"PeerManagement.Type", "redis",
		"PeerManagement.Peers", []string{"refinery-1231:8080"},
		"RedisPeerManagement.Prefix", "testPrefix",
		"RedisPeerManagement.Database", 9,
	)
	rm := makeYAML("ConfigVersion", 2)
	config, rules := createTempConfigs(t, cm, rm)
	c, err := getConfig([]string{"--no-validate", "--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	if d := c.GetPeerManagementType(); d != "redis" {
		t.Error("received", d, "expected", "redis")
	}

	if s := c.GetRedisPeerManagement().Prefix; s != "testPrefix" {
		t.Error("received", s, "expected", "testPrefix")
	}

	if db := c.GetRedisPeerManagement().Database; db != 9 {
		t.Error("received", db, "expected", 9)
	}
}

func TestDebugServiceAddr(t *testing.T) {
	cm := makeYAML("General.ConfigurationVersion", 2, "Debugging.DebugServiceAddr", "localhost:8085")
	rm := makeYAML("ConfigVersion", 2)
	config, rules := createTempConfigs(t, cm, rm)
	c, err := getConfig([]string{"--no-validate", "--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	if d := c.GetDebugServiceAddr(); d != "localhost:8085" {
		t.Error("received", d, "expected", "localhost:8085")
	}
}

func TestHTTPIdleTimeout(t *testing.T) {
	cm := makeYAML("General.ConfigurationVersion", 2, "Network.HTTPIdleTimeout", "60s")
	rm := makeYAML("ConfigVersion", 2)
	config, rules := createTempConfigs(t, cm, rm)
	c, err := getConfig([]string{"--no-validate", "--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	if d := c.GetHTTPIdleTimeout(); d != time.Minute {
		t.Error("received", d, "expected", time.Minute)
	}
}

func TestDryRun(t *testing.T) {
	cm := makeYAML("General.ConfigurationVersion", 2, "Debugging.DryRun", true)
	rm := makeYAML("ConfigVersion", 2)
	config, rules := createTempConfigs(t, cm, rm)
	c, err := getConfig([]string{"--no-validate", "--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	if d := c.GetIsDryRun(); d != true {
		t.Error("received", d, "expected", true)
	}
}

func TestRedisClusterHosts(t *testing.T) {
	clusterHosts := []string{"localhost:7001", "localhost:7002"}
	cm := makeYAML(
		"General.ConfigurationVersion", 2,
		"PeerManagement.Type", "redis",
		"RedisPeerManagement.ClusterHosts", clusterHosts,
		"RedisPeerManagement.Prefix", "test",
		"RedisPeerManagement.Database", 9,
	)
	rm := makeYAML("ConfigVersion", 2)
	config, rules := createTempConfigs(t, cm, rm)
	c, err := getConfig([]string{"--no-validate", "--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	d := c.GetRedisPeerManagement().ClusterHosts
	require.NotNil(t, d)
	require.EqualValues(t, clusterHosts, d)
}

func TestMaxAlloc(t *testing.T) {
	cm := makeYAML("General.ConfigurationVersion", 2, "Collection.CacheCapacity", 1000, "Collection.MaxAlloc", 17179869184)
	rm := makeYAML("ConfigVersion", 2)
	cfg, rules := createTempConfigs(t, cm, rm)
	c, err := getConfig([]string{"--no-validate", "--config", cfg, "--rules_config", rules})
	assert.NoError(t, err)

	expected := config.MemorySize(16 * 1024 * 1024 * 1024)
	inMemConfig := c.GetCollectionConfig()
	assert.Equal(t, expected, inMemConfig.MaxAlloc)
}

func TestPeerAndIncomingQueueSize(t *testing.T) {
	testcases := []struct {
		name                string
		configYAML          string
		expectedForPeer     int
		expectedForIncoming int
	}{
		{
			name:                "default",
			configYAML:          makeYAML("General.ConfigurationVersion", 2, "Collection.CacheCapacity", 1000),
			expectedForPeer:     3000,
			expectedForIncoming: 3000,
		},
		{
			name:                "PeerInMemoryCapacity is set",
			configYAML:          makeYAML("General.ConfigurationVersion", 2, "Collection.CacheCapacity", 1000, "Collection.PeerQueueSize", 4000),
			expectedForPeer:     4000,
			expectedForIncoming: 3000,
		},
		{
			name:                "IncomingInMemoryCapacity is set",
			configYAML:          makeYAML("General.ConfigurationVersion", 2, "Collection.CacheCapacity", 1000, "Collection.IncomingQueueSize", 4000),
			expectedForPeer:     3000,
			expectedForIncoming: 4000,
		},
		{
			name:                "below the minimum",
			configYAML:          makeYAML("General.ConfigurationVersion", 2, "Collection.CacheCapacity", 1000, "Collection.PeerQueueSize", 2000, "Collection.IncomingQueueSize", 2000),
			expectedForPeer:     3000,
			expectedForIncoming: 3000,
		},
	}

	for _, tc := range testcases {
		rm := makeYAML("ConfigVersion", 2)
		config, rules := createTempConfigs(t, tc.configYAML, rm)
		c, err := getConfig([]string{"--no-validate", "--config", config, "--rules_config", rules})
		assert.NoError(t, err)

		inMemConfig := c.GetCollectionConfig()
		assert.Equal(t, tc.expectedForPeer, inMemConfig.GetPeerQueueSize())
		assert.Equal(t, tc.expectedForIncoming, inMemConfig.GetIncomingQueueSize())
	}
}

func TestAvailableMemoryCmdLine(t *testing.T) {
	cm := makeYAML("General.ConfigurationVersion", 2, "Collection.CacheCapacity", 1000, "Collection.AvailableMemory", 2_000_000_000)
	rm := makeYAML("ConfigVersion", 2)
	cfg, rules := createTempConfigs(t, cm, rm)
	c, err := getConfig([]string{"--no-validate", "--config", cfg, "--rules_config", rules, "--available-memory", "2.5Gib"})
	assert.NoError(t, err)

	expected := config.MemorySize(2*1024*1024*1024 + 512*1024*1024)
	inMemConfig := c.GetCollectionConfig()
	assert.NoError(t, err)
	assert.Equal(t, expected, inMemConfig.AvailableMemory)
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
	cfg, rules := createTempConfigs(t, cm, rm)
	c, err := getConfig([]string{"--no-validate", "--config", cfg, "--rules_config", rules})
	assert.NoError(t, err)

	if d, name := c.GetSamplerConfigForDestName("dataset-doesnt-exist"); assert.Equal(t, nil, err) {
		assert.IsType(t, &config.DeterministicSamplerConfig{}, d)
		assert.Equal(t, "DeterministicSampler", name)
	}

	if d, name := c.GetSamplerConfigForDestName("dataset 1"); assert.Equal(t, nil, err) {
		assert.IsType(t, &config.DynamicSamplerConfig{}, d)
		assert.Equal(t, "DynamicSampler", name)
	}

	if d, name := c.GetSamplerConfigForDestName("dataset2"); assert.Equal(t, nil, err) {
		assert.IsType(t, &config.DeterministicSamplerConfig{}, d)
		assert.Equal(t, "DeterministicSampler", name)
	}

	if d, name := c.GetSamplerConfigForDestName("dataset3"); assert.Equal(t, nil, err) {
		assert.IsType(t, &config.EMADynamicSamplerConfig{}, d)
		assert.Equal(t, "EMADynamicSampler", name)
	}

	if d, name := c.GetSamplerConfigForDestName("dataset4"); assert.Equal(t, nil, err) {
		assert.IsType(t, &config.TotalThroughputSamplerConfig{}, d)
		assert.Equal(t, "TotalThroughputSampler", name)
	}
}

func TestDefaultSampler(t *testing.T) {
	t.Skip("This tests for a default sampler, but we are currently not requiring explicit default samplers.")
	cm := makeYAML("General.ConfigurationVersion", 2)
	rm := makeYAML("ConfigVersion", 2)
	cfg, rules := createTempConfigs(t, cm, rm)
	c, err := getConfig([]string{"--no-validate", "--config", cfg, "--rules_config", rules})

	assert.NoError(t, err)

	s, name := c.GetSamplerConfigForDestName("nonexistent")

	assert.Equal(t, "DeterministicSampler", name)
	assert.IsType(t, &config.DeterministicSamplerConfig{}, s)
}

func TestHoneycombLoggerConfig(t *testing.T) {
	cm := makeYAML(
		"General.ConfigurationVersion", 2,
		"Logger.Type", "honeycomb",
		"HoneycombLogger.APIHost", "http://honeycomb.io",
		"HoneycombLogger.APIKey", "1234",
		"HoneycombLogger.Dataset", "loggerDataset",
		"HoneycombLogger.SamplerEnabled", true,
		"HoneycombLogger.SamplerThroughput", 5,
	)
	rm := makeYAML("ConfigVersion", 2)
	config, rules := createTempConfigs(t, cm, rm)
	// Set the environment variable to test that it overrides the config
	oldenv := os.Getenv("REFINERY_HONEYCOMB_API_KEY")
	os.Setenv("REFINERY_HONEYCOMB_API_KEY", "321cba")
	defer os.Setenv("REFINERY_HONEYCOMB_API_KEY", oldenv)
	c, err := getConfig([]string{"--no-validate", "--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	loggerConfig := c.GetHoneycombLoggerConfig()

	assert.Equal(t, "http://honeycomb.io", loggerConfig.APIHost)
	assert.Equal(t, "321cba", loggerConfig.APIKey)
	assert.Equal(t, "loggerDataset", loggerConfig.Dataset)
	assert.Equal(t, true, loggerConfig.GetSamplerEnabled())
	assert.Equal(t, 5, loggerConfig.SamplerThroughput)
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
	c, err := getConfig([]string{"--no-validate", "--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	loggerConfig := c.GetHoneycombLoggerConfig()

	assert.Equal(t, true, loggerConfig.GetSamplerEnabled())
	assert.Equal(t, 10, loggerConfig.SamplerThroughput)
}

func TestHoneycombGRPCConfigDefaults(t *testing.T) {
	cm := makeYAML(
		"General.ConfigurationVersion", 2,
		"GRPCServerParameters.Enabled", true,
		"GRPCServerParameters.ListenAddr", "localhost:4343",
	)
	rm := makeYAML("ConfigVersion", 2)
	cfg, rules := createTempConfigs(t, cm, rm)
	c, err := getConfig([]string{"--no-validate", "--config", cfg, "--rules_config", rules})
	assert.NoError(t, err)

	assert.Equal(t, true, c.GetGRPCEnabled())

	a := c.GetGRPCListenAddr()
	assert.Equal(t, "localhost:4343", a)

	grpcConfig := c.GetGRPCConfig()
	assert.Equal(t, config.DefaultTrue(true), *grpcConfig.Enabled)
	assert.Equal(t, "localhost:4343", grpcConfig.ListenAddr)
	assert.Equal(t, 1*time.Minute, time.Duration(grpcConfig.MaxConnectionIdle))
	assert.Equal(t, 3*time.Minute, time.Duration(grpcConfig.MaxConnectionAge))
	assert.Equal(t, 1*time.Minute, time.Duration(grpcConfig.MaxConnectionAgeGrace))
	assert.Equal(t, 1*time.Minute, time.Duration(grpcConfig.KeepAlive))
	assert.Equal(t, 20*time.Second, time.Duration(grpcConfig.KeepAliveTimeout))
	assert.Equal(t, config.MemorySize(15*1_000_000), grpcConfig.MaxSendMsgSize)
	assert.Equal(t, config.MemorySize(15*1_000_000), grpcConfig.MaxRecvMsgSize)
}

func TestStdoutLoggerConfig(t *testing.T) {
	cm := makeYAML(
		"General.ConfigurationVersion", 2,
		"Logger.Type", "stdout",
		"StdoutLogger.Structured", true,
		"StdoutLogger.SamplerThroughput", 5,
		"StdoutLogger.SamplerEnabled", true,
	)
	rm := makeYAML("ConfigVersion", 2)
	config, rules := createTempConfigs(t, cm, rm)
	fmt.Println(config)
	c, err := getConfig([]string{"--no-validate", "--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	loggerConfig := c.GetStdoutLoggerConfig()

	assert.True(t, loggerConfig.Structured)
	assert.True(t, loggerConfig.SamplerEnabled)
	assert.Equal(t, 5, loggerConfig.SamplerThroughput)
}

func TestStdoutLoggerConfigDefaults(t *testing.T) {
	cm := makeYAML(
		"General.ConfigurationVersion", 2,
	)
	rm := makeYAML("ConfigVersion", 2)
	config, rules := createTempConfigs(t, cm, rm)
	c, err := getConfig([]string{"--no-validate", "--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	loggerConfig := c.GetStdoutLoggerConfig()

	assert.False(t, loggerConfig.Structured)
	assert.False(t, loggerConfig.SamplerEnabled)
	assert.Equal(t, 10, loggerConfig.SamplerThroughput)
}
func TestDatasetPrefix(t *testing.T) {
	cm := makeYAML(
		"General.ConfigurationVersion", 2,
		"General.DatasetPrefix", "dataset",
	)
	rm := makeYAML("ConfigVersion", 2)
	config, rules := createTempConfigs(t, cm, rm)
	c, err := getConfig([]string{"--no-validate", "--config", config, "--rules_config", rules})
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
	c, err := getConfig([]string{"--no-validate", "--config", config, "--rules_config", rules})
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
		"GRPCServerParameters.ListenAddr", "localhost:4317",
		"GRPCServerParameters.Enabled", true,
	)
	rm := makeYAML("ConfigVersion", 2)
	config, rules := createTempConfigs(t, cm, rm)
	c, err := getConfig([]string{"--no-validate", "--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	gc := c.GetGRPCConfig()

	assert.Equal(t, 1*time.Minute, time.Duration(gc.MaxConnectionIdle))
	assert.Equal(t, 2*time.Minute, time.Duration(gc.MaxConnectionAge))
	assert.Equal(t, 3*time.Minute, time.Duration(gc.MaxConnectionAgeGrace))
	assert.Equal(t, 4*time.Minute, time.Duration(gc.KeepAlive))
	assert.Equal(t, 5*time.Minute, time.Duration(gc.KeepAliveTimeout))
	assert.Equal(t, true, c.GetGRPCEnabled())
	addr := c.GetGRPCListenAddr()
	assert.Equal(t, "localhost:4317", addr)
}

func TestHoneycombAdditionalErrorConfig(t *testing.T) {
	cm := makeYAML(
		"General.ConfigurationVersion", 2,
		"Debugging.AdditionalErrorFields", []string{"first", "second"},
	)
	rm := makeYAML("ConfigVersion", 2)
	config, rules := createTempConfigs(t, cm, rm)
	c, err := getConfig([]string{"--no-validate", "--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	assert.Equal(t, []string{"first", "second"}, c.GetAdditionalErrorFields())
}

func TestHoneycombAdditionalErrorDefaults(t *testing.T) {
	cm := makeYAML("General.ConfigurationVersion", 2)
	rm := makeYAML("ConfigVersion", 2)
	config, rules := createTempConfigs(t, cm, rm)
	c, err := getConfig([]string{"--no-validate", "--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	assert.Equal(t, []string{"trace.span_id"}, c.GetAdditionalErrorFields())
}

func TestSampleCacheParameters(t *testing.T) {
	cm := makeYAML("General.ConfigurationVersion", 2)
	rm := makeYAML("ConfigVersion", 2)
	config, rules := createTempConfigs(t, cm, rm)
	c, err := getConfig([]string{"--no-validate", "--config", config, "--rules_config", rules})
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
	c, err := getConfig([]string{"--no-validate", "--config", config, "--rules_config", rules})
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
	c, err := getConfig([]string{"--no-validate", "--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	assert.Equal(t, map[string]string{"name": "foo", "other": "bar", "another": "OneHundred"}, c.GetAdditionalAttributes())
}

func TestHoneycombIdFieldsConfig(t *testing.T) {
	cm := makeYAML(
		"General.ConfigurationVersion", 2,
		"IDFields.TraceNames", []string{"first", "second"},
		"IDFields.ParentNames", []string{"zero", "one"},
	)
	rm := makeYAML("ConfigVersion", 2)
	config, rules := createTempConfigs(t, cm, rm)
	c, err := getConfig([]string{"--no-validate", "--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	assert.Equal(t, []string{"first", "second"}, c.GetTraceIdFieldNames())
	assert.Equal(t, []string{"zero", "one"}, c.GetParentIdFieldNames())
}

func TestHoneycombIdFieldsConfigDefault(t *testing.T) {
	cm := makeYAML("General.ConfigurationVersion", 2)
	rm := makeYAML("ConfigVersion", 2)
	config, rules := createTempConfigs(t, cm, rm)
	c, err := getConfig([]string{"--no-validate", "--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	assert.Equal(t, []string{"trace.trace_id", "traceId"}, c.GetTraceIdFieldNames())
	assert.Equal(t, []string{"trace.parent_id", "parentId"}, c.GetParentIdFieldNames())
}

func TestOverrideConfigDefaults(t *testing.T) {
	/// Check that fields that default to true can be set to false
	cm := makeYAML(
		"General.ConfigurationVersion", 2,
		"RefineryTelemetry.AddSpanCountToRoot", false,
		"RefineryTelemetry.AddHostMetadataToTrace", false,
		"HoneycombLogger.SamplerEnabled", false,
		"Specialized.CompressPeerCommunication", false,
		"GRPCServerParameters.Enabled", false,
	)
	rm := makeYAML("ConfigVersion", 2)
	config, rules := createTempConfigs(t, cm, rm)
	c, err := getConfig([]string{"--no-validate", "--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	assert.Equal(t, false, c.GetAddSpanCountToRoot())
	assert.Equal(t, false, c.GetAddHostMetadataToTrace())
	loggerConfig := c.GetHoneycombLoggerConfig()
	assert.Equal(t, false, loggerConfig.GetSamplerEnabled())
	assert.Equal(t, false, c.GetCompressPeerCommunication())
	assert.Equal(t, false, c.GetGRPCEnabled())
}

func TestMemorySizeUnmarshal(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected config.MemorySize
	}{
		{
			name:     "single letter",
			input:    "1G",
			expected: 1000 * 1000 * 1000,
		},
		{
			name:     "B included",
			input:    "1GB",
			expected: 1000 * 1000 * 1000,
		},
		{
			name:     "iB included",
			input:    "1GiB",
			expected: 1024 * 1024 * 1024,
		},
		{
			name:     "k8s format",
			input:    "1Gi",
			expected: 1024 * 1024 * 1024,
		},
		{
			name:     "single letter lowercase",
			input:    "1g",
			expected: 1000 * 1000 * 1000,
		},
		{
			name:     "b included lowercase",
			input:    "1gb",
			expected: 1000 * 1000 * 1000,
		},
		{
			name:     "ib included  lowercase",
			input:    "1gib",
			expected: 1024 * 1024 * 1024,
		},
		{
			name:     "k8s format lowercase",
			input:    "1gi",
			expected: 1024 * 1024 * 1024,
		},
		{
			name:     "bytes",
			input:    "100000",
			expected: 100000,
		},
		{
			name:     "b",
			input:    "1b",
			expected: 1,
		},
		{
			name:     "bi",
			input:    "1Bi",
			expected: 1,
		},
		{
			name:     "k",
			input:    "1K",
			expected: 1000,
		},
		{
			name:     "ki",
			input:    "1Ki",
			expected: 1024,
		},
		{
			name:     "m",
			input:    "1M",
			expected: 1000 * 1000,
		},
		{
			name:     "mi",
			input:    "1Mi",
			expected: 1024 * 1024,
		},
		{
			name:     "t",
			input:    "1T",
			expected: 1000 * 1000 * 1000 * 1000,
		},
		{
			name:     "ti",
			input:    "1Ti",
			expected: 1024 * 1024 * 1024 * 1024,
		},
		{
			name:     "p",
			input:    "1p",
			expected: 1000 * 1000 * 1000 * 1000 * 1000,
		},
		{
			name:     "pi",
			input:    "1pi",
			expected: 1024 * 1024 * 1024 * 1024 * 1024,
		},
		{
			name:     "e",
			input:    "1e",
			expected: 1000 * 1000 * 1000 * 1000 * 1000 * 1000,
		},
		{
			name:     "ei",
			input:    "1ei",
			expected: 1024 * 1024 * 1024 * 1024 * 1024 * 1024,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var m config.MemorySize
			err := m.UnmarshalText([]byte(tt.input))
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, m)
		})
	}
}

func TestMemorySizeUnmarshalInvalid(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "no number",
			input: "G",
		},
		{
			name:  "invalid unit",
			input: "1A",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var m config.MemorySize
			err := m.UnmarshalText([]byte(tt.input))
			assert.Contains(t, err.Error(), fmt.Sprintf(config.InvalidSizeError, tt.input))
		})
	}
}

func TestMemorySizeMarshal(t *testing.T) {
	tests := []struct {
		name     string
		input    config.MemorySize
		expected string
	}{
		{
			name:     "zero",
			input:    0,
			expected: "0",
		},
		{
			name:     "ei",
			input:    config.MemorySize(3 * config.Ei),
			expected: "3Ei",
		},
		{
			name:     "e",
			input:    config.MemorySize(3 * config.E),
			expected: "3E",
		},
		{
			name:     "pi",
			input:    config.MemorySize(3 * config.Pi),
			expected: "3Pi",
		},
		{
			name:     "p",
			input:    config.MemorySize(3 * config.P),
			expected: "3P",
		},
		{
			name:     "gi",
			input:    config.MemorySize(3 * config.Gi),
			expected: "3Gi",
		},
		{
			name:     "g",
			input:    config.MemorySize(3 * config.G),
			expected: "3G",
		},
		{
			name:     "mi",
			input:    config.MemorySize(3 * config.Mi),
			expected: "3Mi",
		},
		{
			name:     "m",
			input:    config.MemorySize(3 * config.M),
			expected: "3M",
		},
		{
			name:     "ki",
			input:    config.MemorySize(3 * config.Ki),
			expected: "3Ki",
		},
		{
			name:     "k",
			input:    config.MemorySize(3 * config.K),
			expected: "3K",
		},
		{
			name:     "b",
			input:    config.MemorySize(3),
			expected: "3",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.input.MarshalText()
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, string(result))
		})
	}
}

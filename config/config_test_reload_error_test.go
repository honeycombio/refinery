//go:build all || !race

package config_test

import (
	"os"
	"testing"
	"time"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/configwatcher"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/pubsub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrorReloading(t *testing.T) {
	cm := makeYAML(
		"General.ConfigurationVersion", 2,
		"General.ConfigReloadInterval", config.Duration(1*time.Second),
		"Network.ListenAddr", "0.0.0.0:8080",
		"HoneycombLogger.APIKey", "SetThisToAHoneycombKey",
	)
	rm := makeYAML(
		"RulesVersion", 2,
		"Samplers.__default__.DeterministicSampler.SampleRate", 5,
	)
	cfg, rules := createTempConfigs(t, cm, rm)
	defer os.Remove(rules)
	defer os.Remove(cfg)

	opts, err := config.NewCmdEnvOptions([]string{"--config", cfg, "--rules_config", rules})
	assert.NoError(t, err)

	c, err := config.NewConfig(opts)
	assert.NoError(t, err)

	pubsub := &pubsub.LocalPubSub{
		Config: c,
	}
	pubsub.Start()
	defer pubsub.Stop()
	mockLogger := &logger.MockLogger{}
	watcher := &configwatcher.ConfigWatcher{
		Config: c,
		PubSub: pubsub,
		Logger: mockLogger,
	}
	watcher.Start()
	defer watcher.Stop()

	d, name := c.GetSamplerConfigForDestName("dataset5")
	if _, ok := d.(config.DeterministicSamplerConfig); ok {
		t.Error("type received", d, "expected", "DeterministicSampler")
	}
	if name != "DeterministicSampler" {
		t.Error("name received", d, "expected", "DeterministicSampler")
	}

	// This is valid YAML, but invalid config
	rm2 := makeYAML(
		"RulesVersion", 2,
		"Samplers.__default__.InvalidSampler.SampleRate", 50,
	)
	err = os.WriteFile(rules, []byte(rm2), 0644)
	assert.NoError(t, err)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		require.Len(c, mockLogger.Events, 1)
		assert.Contains(c, mockLogger.Events[0].Fields["error"], "error reloading config")
	}, 5*time.Second, 100*time.Millisecond)

	// config should error and not update sampler to invalid type
	d, _ = c.GetSamplerConfigForDestName("dataset5")
	if _, ok := d.(config.DeterministicSamplerConfig); ok {
		t.Error("received", d, "expected", "DeterministicSampler")
	}
}

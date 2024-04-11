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
	t.Skip()
	cm := makeYAML(
		"General.ConfigurationVersion", 2,
		"General.ConfigReloadInterval", Duration(1*time.Second),
		"Network.ListenAddr", "0.0.0.0:8080",
		"HoneycombLogger.APIKey", "SetThisToAHoneycombKey",
	)
	rm := makeYAML(
		"RulesVersion", 2,
		"Samplers.__default__.DeterministicSampler.SampleRate", 5,
	)
	config, rules := createTempConfigs(t, cm, rm)
	defer os.Remove(rules)
	defer os.Remove(config)

	opts, err := NewCmdEnvOptions([]string{"--config", config, "--rules_config", rules})
	assert.NoError(t, err)

	ch := make(chan interface{}, 1)
	c, err := NewConfig(opts, func(err error) { ch <- 1 })
	assert.NoError(t, err)

	d, name, _ := c.GetSamplerConfigForDestName("dataset5")
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

	// This is valid YAML, but invalid config
	rm2 := makeYAML(
		"RulesVersion", 2,
		"Samplers.__default__.InvalidSampler.SampleRate", 50,
	)
	err = os.WriteFile(rules, []byte(rm2), 0644)

	assert.NoError(t, err)

	wg.Wait()

	// config should error and not update sampler to invalid type
	d, _, _ = c.GetSamplerConfigForDestName("dataset5")
	if _, ok := d.(DeterministicSamplerConfig); ok {
		t.Error("received", d, "expected", "DeterministicSampler")
	}
}

package sample

import (
	"fmt"
	"testing"
	"time"

	dynsampler "github.com/honeycombio/dynsampler-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
)

func newKeyTestFactory(t testing.TB) *SamplerFactory {
	factory := &SamplerFactory{
		Logger:  &logger.NullLogger{},
		Metrics: &metrics.NullMetrics{},
	}
	require.NoError(t, factory.Start())
	t.Cleanup(factory.Stop)
	return factory
}

// dynsamplerFor returns the engine a sampler was built with, whatever its type.
func dynsamplerFor(t testing.TB, s Sampler) any {
	t.Helper()
	switch s := s.(type) {
	case *DynamicSampler:
		return s.dynsampler
	case *EMADynamicSampler:
		return s.dynsampler
	case *EMAThroughputSampler:
		return s.dynsampler
	case *TotalThroughputSampler:
		return s.dynsampler
	case *WindowedThroughputSampler:
		return s.dynsampler
	default:
		t.Fatalf("no engine for sampler type %T", s)
		return nil
	}
}

// TestOmittedFieldsShareWithWrittenDefaults checks that a config which leaves a
// field out lands on the same engine as one that writes that field's default.
// Before the key was built from the defaulted config, the two produced separate
// engines with identical parameters, so a goal throughput was served twice.
func TestOmittedFieldsShareWithWrittenDefaults(t *testing.T) {
	fields := []string{"service.name"}

	tests := []struct {
		name     string
		omitted  any
		defaults any
	}{
		{
			name:     "DynamicSampler",
			omitted:  &config.DynamicSamplerConfig{SampleRate: 10, FieldList: fields},
			defaults: &config.DynamicSamplerConfig{SampleRate: 10, FieldList: fields, MaxKeys: 500, ClearFrequency: config.Duration(30 * time.Second)},
		},
		{
			name:    "EMADynamicSampler",
			omitted: &config.EMADynamicSamplerConfig{GoalSampleRate: 10, FieldList: fields},
			defaults: &config.EMADynamicSamplerConfig{
				GoalSampleRate: 10, FieldList: fields, MaxKeys: 500,
				AdjustmentInterval: config.Duration(15 * time.Second),
				Weight:             0.5, AgeOutValue: 0.5, BurstMultiple: 2, BurstDetectionDelay: 3,
			},
		},
		{
			name:    "EMAThroughputSampler",
			omitted: &config.EMAThroughputSamplerConfig{GoalThroughputPerSec: 100, FieldList: fields},
			defaults: &config.EMAThroughputSamplerConfig{
				GoalThroughputPerSec: 100, FieldList: fields, MaxKeys: 500, InitialSampleRate: 10,
				AdjustmentInterval: config.Duration(15 * time.Second),
				Weight:             0.5, AgeOutValue: 0.5, BurstMultiple: 2, BurstDetectionDelay: 3,
			},
		},
		{
			name:     "TotalThroughputSampler",
			omitted:  &config.TotalThroughputSamplerConfig{GoalThroughputPerSec: 100, FieldList: fields},
			defaults: &config.TotalThroughputSamplerConfig{GoalThroughputPerSec: 100, FieldList: fields, MaxKeys: 500, ClearFrequency: config.Duration(30 * time.Second)},
		},
		{
			name:    "WindowedThroughputSampler",
			omitted: &config.WindowedThroughputSamplerConfig{GoalThroughputPerSec: 100, FieldList: fields},
			defaults: &config.WindowedThroughputSamplerConfig{
				GoalThroughputPerSec: 100, FieldList: fields, MaxKeys: 500,
				UpdateFrequency:   config.Duration(time.Second),
				LookbackFrequency: config.Duration(30 * time.Second),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newKeyTestFactory(t)
			s1 := f.createSampler(tt.omitted, "env")
			s2 := f.createSampler(tt.defaults, "env")
			require.NotNil(t, s1)
			require.NotNil(t, s2)
			assert.Same(t, dynsamplerFor(t, s1), dynsamplerFor(t, s2))
			assert.Len(t, f.sharedDynsamplers, 1)
		})
	}
}

// TestEffectiveConfigMatchesEngine checks that each effective config reports the
// values the engine actually runs with, so the key describes the engine it names.
// Since every value is now passed explicitly, dynsampler-go's own defaults no
// longer apply: Refinery pins them, and a later library change will not move
// behaviour without these functions moving with it.
func TestEffectiveConfigMatchesEngine(t *testing.T) {
	fields := []string{"service.name"}

	t.Run("DynamicSampler", func(t *testing.T) {
		c := &config.DynamicSamplerConfig{FieldList: fields}
		e := effectiveDynamicSamplerConfig(c)
		d := createDynForDynamicSampler(c).(*dynsampler.AvgSampleRate)
		defer d.Stop()
		assert.Equal(t, int(e.SampleRate), d.GoalSampleRate)
		assert.Equal(t, time.Duration(e.ClearFrequency), d.ClearFrequencyDuration)
		assert.Equal(t, e.MaxKeys, d.MaxKeys)
	})

	t.Run("EMADynamicSampler", func(t *testing.T) {
		c := &config.EMADynamicSamplerConfig{FieldList: fields}
		e := effectiveEMADynamicSamplerConfig(c)
		d := createDynForEMADynamicSampler(c)
		defer d.Stop()
		assert.Equal(t, e.GoalSampleRate, d.GoalSampleRate)
		assert.Equal(t, time.Duration(e.AdjustmentInterval), d.AdjustmentIntervalDuration)
		assert.Equal(t, e.Weight, d.Weight)
		assert.Equal(t, e.AgeOutValue, d.AgeOutValue)
		assert.Equal(t, e.BurstMultiple, d.BurstMultiple)
		assert.Equal(t, e.BurstDetectionDelay, d.BurstDetectionDelay)
		assert.Equal(t, e.MaxKeys, d.MaxKeys)
	})

	t.Run("EMAThroughputSampler", func(t *testing.T) {
		c := &config.EMAThroughputSamplerConfig{FieldList: fields}
		e := effectiveEMAThroughputSamplerConfig(c)
		d := createDynForEMAThroughputSampler(c)
		defer d.Stop()
		assert.Equal(t, e.GoalThroughputPerSec, d.GoalThroughputPerSec)
		assert.Equal(t, e.InitialSampleRate, d.InitialSampleRate)
		assert.Equal(t, time.Duration(e.AdjustmentInterval), d.AdjustmentInterval)
		assert.Equal(t, e.Weight, d.Weight)
		assert.Equal(t, e.AgeOutValue, d.AgeOutValue)
		assert.Equal(t, e.BurstMultiple, d.BurstMultiple)
		assert.Equal(t, e.BurstDetectionDelay, d.BurstDetectionDelay)
		assert.Equal(t, e.MaxKeys, d.MaxKeys)
	})

	t.Run("TotalThroughputSampler", func(t *testing.T) {
		c := &config.TotalThroughputSamplerConfig{FieldList: fields}
		e := effectiveTotalThroughputSamplerConfig(c)
		d := createDynForTotalThroughputSampler(c)
		defer d.Stop()
		assert.Equal(t, e.GoalThroughputPerSec, d.GoalThroughputPerSec)
		assert.Equal(t, time.Duration(e.ClearFrequency), d.ClearFrequencyDuration)
		assert.Equal(t, e.MaxKeys, d.MaxKeys)
	})

	t.Run("WindowedThroughputSampler", func(t *testing.T) {
		// An uneven lookback exercises the flooring the engine applies.
		c := &config.WindowedThroughputSamplerConfig{
			FieldList:         fields,
			UpdateFrequency:   config.Duration(2 * time.Second),
			LookbackFrequency: config.Duration(95 * time.Second),
		}
		e := effectiveWindowedThroughputSamplerConfig(c)
		d := createDynForWindowedThroughputSampler(c)
		defer d.Stop()
		assert.Equal(t, float64(e.GoalThroughputPerSec), d.GoalThroughputPerSec)
		assert.Equal(t, time.Duration(e.UpdateFrequency), d.UpdateFrequencyDuration)
		assert.Equal(t, time.Duration(e.LookbackFrequency), d.LookbackFrequencyDuration)
		assert.Equal(t, e.MaxKeys, d.MaxKeys)
	})
}

// TestLookbackFlooringSharesEngine checks that two lookbacks the engine floors to
// the same window share one engine.
func TestLookbackFlooringSharesEngine(t *testing.T) {
	f := newKeyTestFactory(t)
	mk := func(lookback time.Duration) *config.WindowedThroughputSamplerConfig {
		return &config.WindowedThroughputSamplerConfig{
			GoalThroughputPerSec: 100,
			FieldList:            []string{"service.name"},
			UpdateFrequency:      config.Duration(2 * time.Second),
			LookbackFrequency:    config.Duration(lookback),
		}
	}
	s1 := f.createSampler(mk(90*time.Second), "env")
	s2 := f.createSampler(mk(91*time.Second), "env")
	require.NotNil(t, s1)
	require.NotNil(t, s2)
	assert.Same(t, dynsamplerFor(t, s1), dynsamplerFor(t, s2))
	assert.Len(t, f.sharedDynsamplers, 1)
}

// TestFieldListSeparatorIsUnambiguous checks that a field name containing the
// separator the key format used to rely on does not collide with two fields.
func TestFieldListSeparatorIsUnambiguous(t *testing.T) {
	f := newKeyTestFactory(t)
	s1 := f.createSampler(&config.DynamicSamplerConfig{SampleRate: 10, FieldList: []string{"app.user name"}}, "env")
	s2 := f.createSampler(&config.DynamicSamplerConfig{SampleRate: 10, FieldList: []string{"app.user", "name"}}, "env")
	require.NotNil(t, s1)
	require.NotNil(t, s2)
	assert.NotSame(t, dynsamplerFor(t, s1), dynsamplerFor(t, s2))
	assert.Len(t, f.sharedDynsamplers, 2)
}

// TestRuleIdentityIsInTheKey checks that identically configured rules in one
// rules file each get their own engine, so each gets its own throughput budget.
func TestRuleIdentityIsInTheKey(t *testing.T) {
	const ruleCount = 4

	rules := make([]map[string]any, 0, ruleCount)
	for i := range ruleCount {
		rules = append(rules, map[string]any{
			"Name": fmt.Sprintf("rule-%d", i),
			"Conditions": []map[string]any{
				{"Field": "service.name", "Operator": "=", "Value": fmt.Sprintf("svc-%d", i)},
			},
			"Sampler": map[string]any{
				"EMAThroughputSampler": map[string]any{
					"GoalThroughputPerSec": 1000,
					"FieldList":            []string{"http.status_code"},
				},
			},
		})
	}

	cm := makeYAML("General/ConfigurationVersion", 2)
	rm := makeYAML(
		"RulesVersion", 2,
		"Samplers/__default__/DeterministicSampler/SampleRate", 1,
		"Samplers/production/RulesBasedSampler/Rules", rules,
	)
	cfg, rulesFile := createTempConfigs(t, cm, rm)
	c, err := getConfig([]string{"--no-validate", "--config", cfg, "--rules_config", rulesFile})
	require.NoError(t, err)

	factory := SamplerFactory{
		Config:  c,
		Logger:  &logger.NullLogger{},
		Metrics: &metrics.NullMetrics{},
	}
	require.NoError(t, factory.Start())
	defer factory.Stop()

	require.NotNil(t, factory.GetSamplerImplementationForKey("production"))
	assert.Len(t, factory.sharedDynsamplers, ruleCount)

	// Rules sharing a name still get their own engine, since the index is what
	// makes the identity unique.
	assert.NotEqual(t, makeRuleID(0, "same"), makeRuleID(1, "same"))
}

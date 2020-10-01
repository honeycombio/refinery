// +build all race

package sample

import (
	"testing"

	"github.com/honeycombio/samproxy/config"
	"github.com/honeycombio/samproxy/logger"
	"github.com/honeycombio/samproxy/metrics"
	"github.com/honeycombio/samproxy/types"
	"github.com/stretchr/testify/assert"
)

type TestRulesData struct {
	Rules        *config.RulesBasedSamplerConfig
	Spans        []*types.Span
	ExpectedRate uint
	ExpectedKeep bool
}

func TestRules(t *testing.T) {
	data := []TestRulesData{
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rule: []*config.RulesBasedSamplerRule{
					{
						Name:       "int64equals",
						SampleRate: 10,
						Condition: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: "=",
								Value:    int64(1),
							},
						},
					},
				},
			},
			Spans: []*types.Span{
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"test": int64(1),
						},
					},
				},
			},
			ExpectedKeep: true,
			ExpectedRate: 10,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rule: []*config.RulesBasedSamplerRule{
					{
						Name:       "int64greaterthan",
						SampleRate: 10,
						Condition: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: ">",
								Value:    int64(1),
							},
						},
					},
				},
			},
			Spans: []*types.Span{
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"test": int64(2),
						},
					},
				},
			},
			ExpectedKeep: true,
			ExpectedRate: 10,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rule: []*config.RulesBasedSamplerRule{
					{
						Name:       "int64lessthan",
						SampleRate: 10,
						Condition: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: "<",
								Value:    int64(2),
							},
						},
					},
				},
			},
			Spans: []*types.Span{
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"test": int64(1),
						},
					},
				},
			},
			ExpectedKeep: true,
			ExpectedRate: 10,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rule: []*config.RulesBasedSamplerRule{
					{
						Name:       "int64float64lessthan",
						SampleRate: 10,
						Condition: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: "<",
								Value:    2.2,
							},
						},
					},
				},
			},
			Spans: []*types.Span{
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"test": int64(1),
						},
					},
				},
			},
			ExpectedKeep: true,
			ExpectedRate: 10,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rule: []*config.RulesBasedSamplerRule{
					{
						Name:       "rule that wont be hit",
						SampleRate: 0,
						Condition: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: ">",
								Value:    2.2,
							},
						},
					},
					{
						Name:       "fallback",
						SampleRate: 10,
					},
				},
			},
			Spans: []*types.Span{
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"test": int64(1),
						},
					},
				},
			},
			ExpectedKeep: true,
			ExpectedRate: 10,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rule: []*config.RulesBasedSamplerRule{
					{
						Name:       "multiple matches",
						SampleRate: 10,
						Condition: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: "<=",
								Value:    2.2,
							},
							{
								Field:    "test",
								Operator: ">=",
								Value:    2.2,
							},
							{
								Field:    "test_two",
								Operator: "=",
								Value:    true,
							},
						},
					},
				},
			},
			Spans: []*types.Span{
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"test":     2.2,
							"test_two": false,
						},
					},
				},
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"test_two": true,
						},
					},
				},
			},
			ExpectedKeep: true,
			ExpectedRate: 10,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rule: []*config.RulesBasedSamplerRule{
					{
						Name: "drop",
						Drop: true,
						Condition: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: ">",
								Value:    int64(2),
							},
						},
					},
				},
			},
			Spans: []*types.Span{
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"test": float64(3),
						},
					},
				},
			},
			ExpectedKeep: false,
			ExpectedRate: 0,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rule: []*config.RulesBasedSamplerRule{
					{
						Name: "drop everything",
						Drop: true,
					},
				},
			},
			Spans: []*types.Span{
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"test": int64(1),
						},
					},
				},
			},
			ExpectedKeep: false,
			ExpectedRate: 0,
		},
	}

	for _, d := range data {
		sampler := &RulesBasedSampler{
			Config:  d.Rules,
			Logger:  &logger.NullLogger{},
			Metrics: &metrics.NullMetrics{},
		}

		trace := &types.Trace{}

		for _, span := range d.Spans {
			trace.AddSpan(span)
		}

		rate, keep := sampler.GetSampleRate(trace)

		assert.Equal(t, d.ExpectedRate, rate)

		// we can only test when we don't expect to keep the trace
		if !d.ExpectedKeep {
			assert.Equal(t, d.ExpectedKeep, keep, d.Rules)
		}
	}
}

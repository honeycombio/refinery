package sample

import (
	"testing"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/types"
	"github.com/stretchr/testify/assert"
)

type TestRulesData struct {
	Rules        *config.RulesBasedSamplerConfig
	Spans        []*types.Span
	ExpectedRate uint
	ExpectedKeep bool
	ExpectedName string
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
			ExpectedName: "fallback",
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
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rule: []*config.RulesBasedSamplerRule{
					{
						Name:       "test multiple rules must all be matched",
						SampleRate: 4,
						Condition: []*config.RulesBasedSamplerCondition{
							{
								Field:    "first",
								Operator: "=",
								Value:    int64(1),
							},
							{
								Field:    "second",
								Operator: "=",
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
							"first": int64(1),
						},
					},
				},
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"first": int64(1),
						},
					},
				},
			},
			ExpectedKeep: true,
			// the trace does not match all the rules so we expect the default sample rate
			ExpectedRate: 1,
			ExpectedName: "no rule matched",
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rule: []*config.RulesBasedSamplerRule{
					{
						Name:       "not equal test",
						SampleRate: 4,
						Condition: []*config.RulesBasedSamplerCondition{
							{
								Field:    "first",
								Operator: "!=",
								Value:    int64(10),
							},
						},
					},
				},
			},
			Spans: []*types.Span{
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"first": int64(9),
						},
					},
				},
			},
			ExpectedKeep: true,
			ExpectedRate: 4,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rule: []*config.RulesBasedSamplerRule{
					{
						Name:       "exists test",
						SampleRate: 4,
						Condition: []*config.RulesBasedSamplerCondition{
							{
								Field:    "first",
								Operator: "exists",
							},
						},
					},
				},
			},
			Spans: []*types.Span{
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"first": int64(9),
						},
					},
				},
			},
			ExpectedKeep: true,
			ExpectedRate: 4,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rule: []*config.RulesBasedSamplerRule{
					{
						Name:       "not exists test",
						SampleRate: 4,
						Condition: []*config.RulesBasedSamplerCondition{
							{
								Field:    "first",
								Operator: "not-exists",
							},
						},
					},
				},
			},
			Spans: []*types.Span{
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"second": int64(9),
						},
					},
				},
			},
			ExpectedKeep: true,
			ExpectedRate: 4,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rule: []*config.RulesBasedSamplerRule{
					{
						Name:       "starts with test",
						SampleRate: 4,
						Condition: []*config.RulesBasedSamplerCondition{
							{
								Field:    "first",
								Operator: "starts-with",
								Value:    "honey",
							},
						},
					},
				},
			},
			Spans: []*types.Span{
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"first": "honeycomb",
						},
					},
				},
			},
			ExpectedKeep: true,
			ExpectedRate: 4,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rule: []*config.RulesBasedSamplerRule{
					{
						Name:       "contains test",
						SampleRate: 4,
						Condition: []*config.RulesBasedSamplerCondition{
							{
								Field:    "first",
								Operator: "contains",
								Value:    "eyco",
							},
						},
					},
				},
			},
			Spans: []*types.Span{
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"first": "honeycomb",
						},
					},
				},
			},
			ExpectedKeep: true,
			ExpectedRate: 4,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rule: []*config.RulesBasedSamplerRule{
					{
						Name:       "does not contain test",
						SampleRate: 4,
						Condition: []*config.RulesBasedSamplerCondition{
							{
								Field:    "first",
								Operator: "does-not-contain",
								Value:    "noteyco",
							},
						},
					},
				},
			},
			Spans: []*types.Span{
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"first": "honeycomb",
						},
					},
				},
			},
			ExpectedKeep: true,
			ExpectedRate: 4,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rule: []*config.RulesBasedSamplerRule{
					{
						Name:       "YAMLintgeaterthan",
						SampleRate: 10,
						Condition: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: ">",
								Value:    int(1),
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
						Name:       "Check root span for span count",
						SampleRate: 1,
						Condition: []*config.RulesBasedSamplerCondition{
							{
								Field:    "meta.span_count",
								Operator: "=",
								Value:    int(2),
							},
						},
					},
				},
			},
			Spans: []*types.Span{
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"trace.trace_id":  "12345",
							"trace.span_id":   "54321",
							"meta.span_count": int64(2),
							"test":            int64(2),
						},
					},
				},
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"trace.trace_id":  "12345",
							"trace.span_id":   "654321",
							"trace.parent_id": "54321",
							"test":            int64(2),
						},
					},
				},
			},
			ExpectedName: "Check root span for span count",
			ExpectedKeep: true,
			ExpectedRate: 1,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rule: []*config.RulesBasedSamplerRule{
					{
						Name:       "Check root span for span count",
						Drop:       true,
						SampleRate: 0,
						Condition: []*config.RulesBasedSamplerCondition{
							{
								Field:    "meta.span_count",
								Operator: ">=",
								Value:    int(2),
							},
						},
					},
				},
			},
			Spans: []*types.Span{
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"trace.trace_id":  "12345",
							"trace.span_id":   "54321",
							"meta.span_count": int64(2),
							"test":            int64(2),
						},
					},
				},
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"trace.trace_id":  "12345",
							"trace.span_id":   "654321",
							"trace.parent_id": "54321",
							"test":            int64(2),
						},
					},
				},
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"trace.trace_id":  "12345",
							"trace.span_id":   "754321",
							"trace.parent_id": "54321",
							"test":            int64(3),
						},
					},
				},
			},
			ExpectedName: "Check root span for span count",
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

		rate, keep, reason := sampler.GetSampleRate(trace)

		assert.Equal(t, d.ExpectedRate, rate, d.Rules)
		name := d.ExpectedName
		if name == "" {
			name = d.Rules.Rule[0].Name
		}
		assert.Contains(t, reason, name)

		// we can only test when we don't expect to keep the trace
		if !d.ExpectedKeep {
			assert.Equal(t, d.ExpectedKeep, keep, d.Rules)
		}
	}
}

func TestRulesWithNestedFields(t *testing.T) {
	data := []TestRulesData{
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rule: []*config.RulesBasedSamplerRule{
					{
						Name:       "nested field",
						SampleRate: 10,
						Condition: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test.test1",
								Operator: "=",
								Value:    "a",
							},
						},
					},
				},
				CheckNestedFields: true,
			},
			Spans: []*types.Span{
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"test": map[string]interface{}{
								"test1": "a",
							},
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
						Name:       "field not nested",
						SampleRate: 10,
						Condition: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test.test1",
								Operator: "=",
								Value:    "a",
							},
						},
					},
				},
				CheckNestedFields: true,
			},
			Spans: []*types.Span{
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"test.test1": "a",
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
						Name:       "not exists test",
						SampleRate: 4,
						Condition: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test.test1",
								Operator: "not-exists",
							},
						},
					},
				},
				CheckNestedFields: true,
			},
			Spans: []*types.Span{
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"test": map[string]interface{}{
								"test2": "b",
							},
						},
					},
				},
			},
			ExpectedKeep: true,
			ExpectedRate: 4,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rule: []*config.RulesBasedSamplerRule{
					{
						Name:       "do not check nested",
						SampleRate: 4,
						Condition: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test.test1",
								Operator: "exists",
							},
						},
					},
				},
				CheckNestedFields: false,
			},
			Spans: []*types.Span{
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"test": map[string]interface{}{
								"test1": "a",
							},
						},
					},
				},
			},
			ExpectedKeep: true,
			ExpectedRate: 1,
			ExpectedName: "no rule matched",
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

		rate, keep, reason := sampler.GetSampleRate(trace)

		assert.Equal(t, d.ExpectedRate, rate, d.Rules)
		name := d.ExpectedName
		if name == "" {
			name = d.Rules.Rule[0].Name
		}
		assert.Contains(t, reason, name)

		// we can only test when we don't expect to keep the trace
		if !d.ExpectedKeep {
			assert.Equal(t, d.ExpectedKeep, keep, d.Rules)
		}
	}
}

func TestRulesWithDynamicSampler(t *testing.T) {
	data := []TestRulesData{
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rule: []*config.RulesBasedSamplerRule{
					{
						Name: "downstream-dynamic",
						Condition: []*config.RulesBasedSamplerCondition{
							{
								Field:    "rule_test",
								Operator: "=",
								Value:    int64(1),
							},
						},
						Sampler: &config.RulesBasedDownstreamSampler{
							DynamicSampler: &config.DynamicSamplerConfig{
								SampleRate:                   10,
								FieldList:                    []string{"http.status_code"},
								AddSampleRateKeyToTrace:      true,
								AddSampleRateKeyToTraceField: "meta.key",
							},
						},
					},
				},
			},
			Spans: []*types.Span{
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"rule_test":        int64(1),
							"http.status_code": "200",
						},
					},
				},
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"rule_test":        int64(1),
							"http.status_code": "200",
						},
					},
				},
			},
			ExpectedKeep: true,
			ExpectedRate: 10,
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

		sampler.Start()
		rate, keep, reason := sampler.GetSampleRate(trace)

		assert.Equal(t, d.ExpectedRate, rate, d.Rules)
		name := d.ExpectedName
		if name == "" {
			name = d.Rules.Rule[0].Name
		}
		assert.Contains(t, reason, name)

		// we can only test when we don't expect to keep the trace
		if !d.ExpectedKeep {
			assert.Equal(t, d.ExpectedKeep, keep, d.Rules)
		}

		spans := trace.GetSpans()
		assert.Len(t, spans, len(d.Spans), "should have the same number of spans as input")
		for _, span := range spans {
			assert.Equal(t, span.Event.Data, map[string]interface{}{
				"rule_test":        int64(1),
				"http.status_code": "200",
				"meta.key":         "200•,",
			}, "should add the sampling key to all spans in the trace")
		}
	}
}

func TestRulesWithEMADynamicSampler(t *testing.T) {
	data := []TestRulesData{
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rule: []*config.RulesBasedSamplerRule{
					{
						Name: "downstream-dynamic",
						Condition: []*config.RulesBasedSamplerCondition{
							{
								Field:    "rule_test",
								Operator: "=",
								Value:    int64(1),
							},
						},
						Sampler: &config.RulesBasedDownstreamSampler{
							EMADynamicSampler: &config.EMADynamicSamplerConfig{
								GoalSampleRate:               10,
								FieldList:                    []string{"http.status_code"},
								AddSampleRateKeyToTrace:      true,
								AddSampleRateKeyToTraceField: "meta.key",
							},
						},
					},
				},
			},
			Spans: []*types.Span{
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"rule_test":        int64(1),
							"http.status_code": "200",
						},
					},
				},
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"rule_test":        int64(1),
							"http.status_code": "200",
						},
					},
				},
			},
			ExpectedKeep: true,
			ExpectedRate: 10,
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

		sampler.Start()
		rate, keep, reason := sampler.GetSampleRate(trace)

		assert.Equal(t, d.ExpectedRate, rate, d.Rules)
		name := d.ExpectedName
		if name == "" {
			name = d.Rules.Rule[0].Name
		}
		assert.Contains(t, reason, name)

		// we can only test when we don't expect to keep the trace
		if !d.ExpectedKeep {
			assert.Equal(t, d.ExpectedKeep, keep, d.Rules)
		}

		spans := trace.GetSpans()
		assert.Len(t, spans, len(d.Spans), "should have the same number of spans as input")
		for _, span := range spans {
			assert.Equal(t, span.Event.Data, map[string]interface{}{
				"rule_test":        int64(1),
				"http.status_code": "200",
				"meta.key":         "200•,",
			}, "should add the sampling key to all spans in the trace")
		}
	}
}

func TestRuleMatchesSpanMatchingSpan(t *testing.T) {
	testCases := []struct {
		name           string
		spans          []*types.Span
		keepSpanScope  bool
		keepTraceScope bool
	}{
		{
			name:           "all conditions match single span",
			keepSpanScope:  true,
			keepTraceScope: true,
			spans: []*types.Span{
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"rule_test":        int64(1),
							"http.status_code": "200",
						},
					},
				},
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"rule_test":        int64(5),
							"http.status_code": "500",
						},
					},
				},
			},
		},
		{
			name:           "all conditions do not match single span",
			keepSpanScope:  false,
			keepTraceScope: true,
			spans: []*types.Span{
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"rule_test":        int64(1),
							"http.status_code": "500",
						},
					},
				},
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"rule_test":        int64(5),
							"http.status_code": "200",
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, scope := range []string{"span", "trace"} {
				sampler := &RulesBasedSampler{
					Config: &config.RulesBasedSamplerConfig{
						Rule: []*config.RulesBasedSamplerRule{
							{
								Name:       "Rule to match span",
								Scope:      scope,
								SampleRate: 1,
								Condition: []*config.RulesBasedSamplerCondition{
									{
										Field:    "rule_test",
										Operator: "=",
										Value:    int64(1),
									},
									{
										Field:    "http.status_code",
										Operator: "=",
										Value:    "200",
									},
								},
							},
							{
								Name:       "Default rule",
								Drop:       true,
								SampleRate: 1,
							},
						},
					},
					Logger:  &logger.NullLogger{},
					Metrics: &metrics.NullMetrics{},
				}

				trace := &types.Trace{}

				for _, span := range tc.spans {
					trace.AddSpan(span)
				}

				sampler.Start()
				rate, keep, _ := sampler.GetSampleRate(trace)

				assert.Equal(t, uint(1), rate, rate)
				if scope == "span" {
					assert.Equal(t, tc.keepSpanScope, keep, keep)
				} else {
					assert.Equal(t, tc.keepTraceScope, keep, keep)
				}
			}
		})
	}
}

func TestRulesDatatypes(t *testing.T) {
	data := []TestRulesData{
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rule: []*config.RulesBasedSamplerRule{
					{
						Name:       "int64Unchanged",
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
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rule: []*config.RulesBasedSamplerRule{
					{
						Name:       "floatUnchanged",
						SampleRate: 10,
						Condition: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: "=",
								Value:    float64(1.01),
							},
						},
					},
				},
			},
			Spans: []*types.Span{
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"test": float64(1.01),
						},
					},
				},
			},
			ExpectedKeep: true,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rule: []*config.RulesBasedSamplerRule{
					{
						Name:       "stringUnchanged",
						SampleRate: 10,
						Condition: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: "=",
								Value:    "foo",
							},
						},
					},
				},
			},
			Spans: []*types.Span{
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"test": "foo",
						},
					},
				},
			},
			ExpectedKeep: true,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rule: []*config.RulesBasedSamplerRule{
					{
						Name:       "boolUnchanged",
						SampleRate: 10,
						Condition: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: "=",
								Value:    "true",
							},
						},
					},
				},
			},
			Spans: []*types.Span{
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"test": true,
						},
					},
				},
			},
			ExpectedKeep: true,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rule: []*config.RulesBasedSamplerRule{
					{
						Name:       "boolShouldChangeToFalse",
						SampleRate: 10,
						Condition: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: "=",
								Value:    "blaaahhhh",
							},
						},
					},
				},
			},
			Spans: []*types.Span{
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"test": false,
						},
					},
				},
			},
			ExpectedKeep: true,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rule: []*config.RulesBasedSamplerRule{
					{
						Name:       "intToFloat",
						SampleRate: 10,
						Condition: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: "=",
								Value:    int64(10),
							},
						},
					},
				},
			},
			Spans: []*types.Span{
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"test": 10.,
						},
					},
				},
			},
			ExpectedKeep: true,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rule: []*config.RulesBasedSamplerRule{
					{
						Name:       "floatToInt",
						SampleRate: 10,
						Condition: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: "=",
								Value:    float64(100.01),
							},
						},
					},
				},
			},
			Spans: []*types.Span{
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"test": 100,
						},
					},
				},
			},
			ExpectedKeep: true,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rule: []*config.RulesBasedSamplerRule{
					{
						Name:       "invalidConfigComparesStringWithInt",
						SampleRate: 10,
						Condition: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: "=",
								Value:    "500",
							},
						},
					},
				},
			},
			Spans: []*types.Span{
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"test": "500",
						},
					},
				},
			},
			ExpectedKeep: true,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rule: []*config.RulesBasedSamplerRule{
					{
						Name:       "stringToInt",
						SampleRate: 10,
						Condition: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: "=",
								Value:    500,
							},
						},
					},
				},
			},
			Spans: []*types.Span{
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"test": "500",
						},
					},
				},
			},
			ExpectedKeep: true,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rule: []*config.RulesBasedSamplerRule{
					{
						Name:       "intToString",
						SampleRate: 10,
						Condition: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: ">",
								Value:    "1",
							},
						},
					},
				},
			},
			Spans: []*types.Span{
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"test": 2,
						},
					},
				},
			},
			ExpectedKeep: true,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rule: []*config.RulesBasedSamplerRule{
					{
						Name:       "floatToString",
						SampleRate: 10,
						Condition: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: "<",
								Value:    "10.3",
							},
						},
					},
				},
			},
			Spans: []*types.Span{
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"test": 9.3,
						},
					},
				},
			},
			ExpectedKeep: true,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rule: []*config.RulesBasedSamplerRule{
					{
						Name:       "stringToFloat",
						SampleRate: 10,
						Condition: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: "<=",
								Value:    4.13,
							},
						},
					},
				},
			},
			Spans: []*types.Span{
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"test": "4.13",
						},
					},
				},
			},
			ExpectedKeep: true,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rule: []*config.RulesBasedSamplerRule{
					{
						Name:       "stringToFloatInvalid",
						SampleRate: 10,
						Condition: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: ">=",
								Value:    4.13,
							},
						},
					},
				},
			},
			Spans: []*types.Span{
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"test": "fourPointOneThree",
						},
					},
				},
			},
			ExpectedKeep: true,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rule: []*config.RulesBasedSamplerRule{
					{
						Name:       "stringNotEqual",
						SampleRate: 10,
						Condition: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: "!=",
								Value:    "notRightValue",
							},
						},
					},
				},
			},
			Spans: []*types.Span{
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"test": "rightValue",
						},
					},
				},
			},
			ExpectedKeep: true,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rule: []*config.RulesBasedSamplerRule{
					{
						Name:       "toStringNotEqual",
						SampleRate: 10,
						Condition: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: "!=",
								Value:    "667",
							},
						},
					},
				},
			},
			Spans: []*types.Span{
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"test": 777,
						},
					},
				},
			},
			ExpectedKeep: false,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rule: []*config.RulesBasedSamplerRule{
					{
						Name:       "shouldFail",
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
			ExpectedKeep: false,
		},
	}

	for _, d := range data {
		sampler := &RulesBasedSampler{
			Config:  d.Rules,
			Logger:  &logger.NullLogger{},
			Metrics: &metrics.NullMetrics{},
		}

		sampler.Start()

		trace := &types.Trace{}

		for _, span := range d.Spans {
			trace.AddSpan(span)
		}

		_, keep, _ := sampler.GetSampleRate(trace)

		// // we can only test when we don't expect to keep the trace
		if !d.ExpectedKeep {
			assert.Equal(t, d.ExpectedKeep, keep, d.Rules)
		}
	}
}

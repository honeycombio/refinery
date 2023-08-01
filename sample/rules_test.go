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
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "int64equals",
						SampleRate: 10,
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: config.EQ,
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
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "int64greaterthan",
						SampleRate: 10,
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: config.GT,
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
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "int64lessthan",
						SampleRate: 10,
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: config.LT,
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
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "int64float64lessthan",
						SampleRate: 10,
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: config.LT,
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
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "rule that wont be hit",
						SampleRate: 0,
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: config.GT,
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
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "multiple matches",
						SampleRate: 10,
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: config.LTE,
								Value:    2.2,
							},
							{
								Field:    "test",
								Operator: config.GTE,
								Value:    2.2,
							},
							{
								Field:    "test_two",
								Operator: config.EQ,
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
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name: "drop",
						Drop: true,
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: config.GT,
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
				Rules: []*config.RulesBasedSamplerRule{
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
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "test multiple rules must all be matched",
						SampleRate: 4,
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Field:    "first",
								Operator: config.EQ,
								Value:    int64(1),
							},
							{
								Field:    "second",
								Operator: config.EQ,
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
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "not equal test",
						SampleRate: 4,
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Field:    "first",
								Operator: config.NEQ,
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
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "exists test",
						SampleRate: 4,
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Field:    "first",
								Operator: config.Exists,
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
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "not exists test",
						SampleRate: 4,
						Conditions: []*config.RulesBasedSamplerCondition{
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
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "starts with test",
						SampleRate: 4,
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Field:    "first",
								Operator: config.StartsWith,
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
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "contains test",
						SampleRate: 4,
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Field:    "first",
								Operator: config.Contains,
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
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "does not contain test",
						SampleRate: 4,
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Field:    "first",
								Operator: config.DoesNotContain,
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
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "YAMLintgeaterthan",
						SampleRate: 10,
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: config.GT,
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
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "Check root span for span count",
						SampleRate: 1,
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Field:    "meta.span_count",
								Operator: config.EQ,
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
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "Check root span for span count",
						Drop:       true,
						SampleRate: 0,
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Field:    "meta.span_count",
								Operator: config.GTE,
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
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "Check that root span is missing",
						Drop:       true,
						SampleRate: 0,
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Operator: config.HasRootSpan,
								Value:    false,
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
			ExpectedName: "Check that root span is missing",
			ExpectedKeep: false,
			ExpectedRate: 0,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "Check that root span is present",
						SampleRate: 99,
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Operator: config.HasRootSpan,
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
			ExpectedName: "Check that root span is present",
			ExpectedKeep: true,
			ExpectedRate: 99,
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
			if _, ok := span.Data["trace.parent_id"]; ok == false {
				trace.RootSpan = span
			}
		}

		rate, keep, reason, key := sampler.GetSampleRate(trace)

		assert.Equal(t, d.ExpectedRate, rate, d.Rules)
		name := d.ExpectedName
		if name == "" {
			name = d.Rules.Rules[0].Name
		}
		assert.Contains(t, reason, name)
		assert.Equal(t, "", key)

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
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "nested field",
						SampleRate: 10,
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test.test1",
								Operator: config.EQ,
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
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "field not nested",
						SampleRate: 10,
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test.test1",
								Operator: config.EQ,
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
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "not exists test",
						SampleRate: 4,
						Conditions: []*config.RulesBasedSamplerCondition{
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
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "do not check nested",
						SampleRate: 4,
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test.test1",
								Operator: config.Exists,
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

		rate, keep, reason, key := sampler.GetSampleRate(trace)

		assert.Equal(t, d.ExpectedRate, rate, d.Rules)
		name := d.ExpectedName
		if name == "" {
			name = d.Rules.Rules[0].Name
		}
		assert.Contains(t, reason, name)
		assert.Equal(t, "", key)

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
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name: "downstream-dynamic",
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Field:    "rule_test",
								Operator: config.EQ,
								Value:    int64(1),
							},
						},
						Sampler: &config.RulesBasedDownstreamSampler{
							DynamicSampler: &config.DynamicSamplerConfig{
								SampleRate: 10,
								FieldList:  []string{"http.status_code"},
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
		rate, keep, reason, key := sampler.GetSampleRate(trace)

		assert.Equal(t, d.ExpectedRate, rate, d.Rules)
		name := d.ExpectedName
		if name == "" {
			name = d.Rules.Rules[0].Name
		}
		assert.Contains(t, reason, name)
		assert.Equal(t, "200•,", key)

		// we can only test when we don't expect to keep the trace
		if !d.ExpectedKeep {
			assert.Equal(t, d.ExpectedKeep, keep, d.Rules)
		}

		spans := trace.GetSpans()
		assert.Len(t, spans, len(d.Spans), "should have the same number of spans as input")
	}
}

func TestRulesWithEMADynamicSampler(t *testing.T) {
	data := []TestRulesData{
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name: "downstream-dynamic",
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Field:    "rule_test",
								Operator: config.EQ,
								Value:    int64(1),
							},
						},
						Sampler: &config.RulesBasedDownstreamSampler{
							EMADynamicSampler: &config.EMADynamicSamplerConfig{
								GoalSampleRate: 10,
								FieldList:      []string{"http.status_code"},
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
		rate, keep, reason, key := sampler.GetSampleRate(trace)

		assert.Equal(t, d.ExpectedRate, rate, d.Rules)
		name := d.ExpectedName
		if name == "" {
			name = d.Rules.Rules[0].Name
		}
		assert.Contains(t, reason, name)
		assert.Equal(t, "200•,", key)

		// we can only test when we don't expect to keep the trace
		if !d.ExpectedKeep {
			assert.Equal(t, d.ExpectedKeep, keep, d.Rules)
		}

		spans := trace.GetSpans()
		assert.Len(t, spans, len(d.Spans), "should have the same number of spans as input")
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
						Rules: []*config.RulesBasedSamplerRule{
							{
								Name:       "Rule to match span",
								Scope:      scope,
								SampleRate: 1,
								Conditions: []*config.RulesBasedSamplerCondition{
									{
										Field:    "rule_test",
										Operator: config.EQ,
										Value:    int64(1),
									},
									{
										Field:    "http.status_code",
										Operator: config.EQ,
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
				rate, keep, _, _ := sampler.GetSampleRate(trace)

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
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "int64Unchanged",
						SampleRate: 10,
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: config.EQ,
								Value:    int64(1),
								Datatype: "int",
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
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "floatUnchanged",
						SampleRate: 10,
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: config.EQ,
								Value:    float64(1.01),
								Datatype: "float",
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
			ExpectedRate: 10,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "stringUnchanged",
						SampleRate: 10,
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: config.EQ,
								Value:    "foo",
								Datatype: "string",
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
			ExpectedRate: 10,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "boolUnchanged",
						SampleRate: 10,
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: config.EQ,
								Value:    "true",
								Datatype: "string",
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
			ExpectedRate: 10,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "bool",
						SampleRate: 10,
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: config.EQ,
								Value:    true,
								Datatype: "bool",
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
			ExpectedRate: 10,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "boolShouldChangeToFalse",
						SampleRate: 10,
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: config.Contains,
								Value:    "ru",
								Datatype: "string",
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
			ExpectedRate: 10,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "intToFloat",
						SampleRate: 10,
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: config.EQ,
								Value:    int64(10),
								Datatype: "int",
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
			ExpectedRate: 10,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "floatToInt",
						SampleRate: 10,
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: config.LT,
								Value:    float64(100.01),
								Datatype: "float",
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
			ExpectedRate: 10,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "invalidConfigComparesStringWithInt",
						SampleRate: 10,
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: config.EQ,
								Value:    "500",
								Datatype: "string",
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
			ExpectedRate: 10,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "stringToInt",
						SampleRate: 10,
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: config.EQ,
								Value:    500,
								Datatype: "int",
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
			ExpectedRate: 10,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "intToString",
						SampleRate: 10,
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: config.GT,
								Value:    "1",
								Datatype: "string",
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
			ExpectedRate: 10,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "floatToString",
						SampleRate: 10,
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: config.GT,
								Value:    "10.3",
								Datatype: "string",
							},
						},
					},
				},
			},
			Spans: []*types.Span{
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"test": 9.3, // "9.3" is greater than "10.3" when compared as a string
						},
					},
				},
			},
			ExpectedKeep: true,
			ExpectedRate: 10,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "stringToFloat",
						SampleRate: 10,
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: config.LTE,
								Value:    4.13,
								Datatype: "float",
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
			ExpectedRate: 10,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "stringToFloatInvalid",
						SampleRate: 10,
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: config.GTE,
								Value:    4.13,
								Datatype: "float",
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
			ExpectedRate: 1,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "stringNotEqual",
						SampleRate: 10,
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: config.NEQ,
								Value:    "notRightValue",
								Datatype: "string",
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
			ExpectedRate: 10,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "toStringNotEqual",
						SampleRate: 10,
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: config.NEQ,
								Value:    "667",
								Datatype: "string",
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
			ExpectedKeep: true,
			ExpectedRate: 10,
		},
		{
			Rules: &config.RulesBasedSamplerConfig{
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "intsNotEqual",
						SampleRate: 10,
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Field:    "test",
								Operator: config.NEQ,
								Value:    int64(1),
								Datatype: "int",
							},
						},
					},
				},
			},
			Spans: []*types.Span{
				{
					Event: types.Event{
						Data: map[string]interface{}{
							"test": int64(11),
						},
					},
				},
			},
			ExpectedKeep: true,
			ExpectedRate: 10,
		},
	}

	for _, d := range data {
		t.Run(d.Rules.Rules[0].Name, func(t *testing.T) {
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

			rate, keep, _, _ := sampler.GetSampleRate(trace)
			assert.Equal(t, d.ExpectedRate, rate, d.Rules)
			// because keep depends on sampling rate, we can only test expectedKeep when it should be false
			if !d.ExpectedKeep {
				assert.Equal(t, d.ExpectedKeep, keep, d.Rules)
			}
		})
	}
}

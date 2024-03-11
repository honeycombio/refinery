package sample

import (
	"encoding/json"
	"math/rand"
	"strings"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/types"
	"github.com/tidwall/gjson"
)

type RulesBasedSampler struct {
	Config   *config.RulesBasedSamplerConfig
	Logger   logger.Logger
	Metrics  metrics.Metrics
	samplers map[string]Sampler
	prefix   string
}

const RootPrefix = "root."

func (s *RulesBasedSampler) Start() error {
	s.Logger.Debug().Logf("Starting RulesBasedSampler")
	defer func() { s.Logger.Debug().Logf("Finished starting RulesBasedSampler") }()
	s.prefix = "rulesbased_"

	s.Metrics.Register(s.prefix+"num_dropped", "counter")
	s.Metrics.Register(s.prefix+"num_dropped_by_drop_rule", "counter")
	s.Metrics.Register(s.prefix+"num_kept", "counter")
	s.Metrics.Register(s.prefix+"sample_rate", "histogram")

	s.samplers = make(map[string]Sampler)

	for _, rule := range s.Config.Rules {
		for _, cond := range rule.Conditions {
			if err := cond.Init(); err != nil {
				s.Logger.Debug().WithFields(map[string]interface{}{
					"rule_name": rule.Name,
					"condition": cond.String(),
				}).Logf("error creating rule evaluation function: %s", err)
				continue
			}
		}
		// Check if any rule has a downstream sampler and create it
		if rule.Sampler != nil {
			var sampler Sampler
			if rule.Sampler.DynamicSampler != nil {
				sampler = &DynamicSampler{Config: rule.Sampler.DynamicSampler, Logger: s.Logger, Metrics: s.Metrics}
			} else if rule.Sampler.EMADynamicSampler != nil {
				sampler = &EMADynamicSampler{Config: rule.Sampler.EMADynamicSampler, Logger: s.Logger, Metrics: s.Metrics}
			} else if rule.Sampler.TotalThroughputSampler != nil {
				sampler = &TotalThroughputSampler{Config: rule.Sampler.TotalThroughputSampler, Logger: s.Logger, Metrics: s.Metrics}
			} else if rule.Sampler.EMAThroughputSampler != nil {
				sampler = &EMAThroughputSampler{Config: rule.Sampler.EMAThroughputSampler, Logger: s.Logger, Metrics: s.Metrics}
			} else if rule.Sampler.WindowedThroughputSampler != nil {
				sampler = &WindowedThroughputSampler{Config: rule.Sampler.WindowedThroughputSampler, Logger: s.Logger, Metrics: s.Metrics}
			} else if rule.Sampler.DeterministicSampler != nil {
				sampler = &DeterministicSampler{Config: rule.Sampler.DeterministicSampler, Logger: s.Logger, Metrics: s.Metrics}
			} else {
				s.Logger.Debug().WithFields(map[string]interface{}{
					"rule_name": rule.Name,
				}).Logf("invalid or missing downstream sampler")
				continue
			}

			err := sampler.Start()
			if err != nil {
				s.Logger.Debug().WithFields(map[string]interface{}{
					"rule_name": rule.Name,
				}).Logf("error creating downstream sampler: %s", err)
				continue
			}
			s.samplers[rule.String()] = sampler
		}
	}
	return nil
}

func (s *RulesBasedSampler) GetSampleRate(trace *types.Trace) (rate uint, keep bool, reason string, key string) {
	logger := s.Logger.Debug().WithFields(map[string]interface{}{
		"trace_id": trace.TraceID,
	})

	for _, rule := range s.Config.Rules {
		var matched bool
		var reason string

		switch rule.Scope {
		case "span":
			matched = ruleMatchesSpanInTrace(trace, rule, s.Config.CheckNestedFields)
			reason = "rules/span/"
		case "trace", "":
			matched = ruleMatchesTrace(trace, rule, s.Config.CheckNestedFields)
			reason = "rules/trace/"
		default:
			logger.WithFields(map[string]interface{}{
				"rule_name": rule.Name,
				"scope":     rule.Scope,
			}).Logf("invalid scope %s given for rule: %s", rule.Scope, rule.Name)
			matched = true
			reason = "rules/invalid scope/"
		}

		if matched {
			var rate uint
			var keep bool
			var samplerReason string
			var key string

			if rule.Sampler != nil {
				var sampler Sampler
				var found bool
				if sampler, found = s.samplers[rule.String()]; !found {
					logger.WithFields(map[string]interface{}{
						"rule_name": rule.Name,
					}).Logf("could not find downstream sampler for rule: %s", rule.Name)
					return 1, true, reason + "bad_rule:" + rule.Name, ""
				}
				rate, keep, samplerReason, key = sampler.GetSampleRate(trace)
				reason += rule.Name + ":" + samplerReason
			} else {
				rate = uint(rule.SampleRate)
				keep = !rule.Drop && rule.SampleRate > 0 && rand.Intn(rule.SampleRate) == 0
				reason += rule.Name
				s.Metrics.Histogram(s.prefix+"sample_rate", float64(rate))
			}

			if keep {
				s.Metrics.Increment(s.prefix + "num_kept")
			} else {
				s.Metrics.Increment(s.prefix + "num_dropped")
				if rule.Drop {
					// If we dropped because of an explicit drop rule, then increment that too.
					s.Metrics.Increment(s.prefix + "num_dropped_by_drop_rule")
				}
			}
			logger.WithFields(map[string]interface{}{
				"rate":      rate,
				"keep":      keep,
				"drop_rule": rule.Drop,
			}).Logf("got sample rate and decision")
			return rate, keep, reason, key
		}
	}

	return 1, true, "no rule matched", ""
}

func ruleMatchesTrace(t *types.Trace, rule *config.RulesBasedSamplerRule, checkNestedFields bool) bool {
	// We treat a rule with no conditions as a match.
	if rule.Conditions == nil {
		return true
	}

	var matched int

	for _, condition := range rule.Conditions {
		// This condition is evaluated for the trace as a whole.
		// If RootSpan is nil, it means the trace timer has fired or the trace has been
		// ejected from the cache before the root span has arrived.
		if condition.Operator == config.HasRootSpan {
			if (t.RootSpan != nil) == config.TryConvertToBool(condition.Value) {
				matched++
				continue
			} else {
				// if HasRootSpan is one of the conditions, and it didn't match,
				// there's no need to check the rest of the conditions.
				return false
			}

		}

	span:
		for _, span := range t.GetSpans() {
			value, exists, checkedOnlyRoot := extractValueFromSpan(t, span, condition, checkNestedFields)
			if condition.Matches == nil {
				if conditionMatchesValue(condition, value, exists) {
					matched++
					break span
				}
			} else if condition.Matches(value, exists) {
				matched++
				break span
			}
			if checkedOnlyRoot {
				// if we only checked the root span and it didn't match,
				// there's no need to check the rest of the spans;
				// they can't possibly match and we can end early.
				break span
			}
		}
	}
	return matched == len(rule.Conditions)
}

func ruleMatchesSpanInTrace(trace *types.Trace, rule *config.RulesBasedSamplerRule, checkNestedFields bool) bool {
	// We treat a rule with no conditions as a match.
	if rule.Conditions == nil {
		return true
	}

	for _, span := range trace.GetSpans() {
		ruleMatched := true
		for _, condition := range rule.Conditions {
			// whether this condition is matched by this span.
			value, exists, checkedOnlyRoot := extractValueFromSpan(trace, span, condition, checkNestedFields)
			if condition.Matches == nil {
				if !conditionMatchesValue(condition, value, exists) {
					ruleMatched = false
					if checkedOnlyRoot {
						// if we only checked the root span and it didn't match,
						// there's no need to check the rest of the spans;
						// they can't possibly match and we can end early.
						return false
					}
					// if any condition fails, we can't possibly succeed,
					// so exit inner loop early
					break
				}
			}

			if condition.Matches != nil {
				if !condition.Matches(value, exists) {
					ruleMatched = false
					if checkedOnlyRoot {
						// if we only checked the root span and it didn't match,
						// there's no need to check the rest of the spans;
						// they can't possibly match and we can end early.
						return false
					}
					// if any condition fails, we can't possibly succeed,
					// so exit inner loop early
					break
				}
			}
		}
		// If this span was matched by every condition, then the rule as a whole
		// matches (and we can return)
		if ruleMatched {
			return true
		}
	}

	// if the rule didn't match above, then it doesn't match the trace.
	return false
}

// extractValueFromSpan extracts the `value` found at the first of the given condition's fields found on the input `span`.
// It returns the extracted `value` and an `exists` boolean indicating whether any of the condition's fields are present
// on the input span.
//
// We need to check the fields in order; if we find a match using 'root.' we
// can short-circuit the rest of the spans because they'll all return the same
// value. But if we check a non-root value first, we need to keep checking all
// the spans to see if any of them match.
func extractValueFromSpan(
	trace *types.Trace,
	span *types.Span,
	condition *config.RulesBasedSamplerCondition,
	checkNestedFields bool) (value interface{}, exists bool, checkedOnlyRoot bool) {
	// start with the assumption that we only checked the root span
	checkedOnlyRoot = true

	// If the condition is a descendant count, we extract the count from trace and return it.
	// Note that this is the equivalent of checking the root span's descendant count, so
	// we don't need to check the other spans.
	if f, ok := condition.GetComputedField(); ok {
		switch f {
		case config.NUM_DESCENDANTS:
			return int64(trace.DescendantCount()), true, true
		}
	}

	// we need to preserve which span we're actually using, since
	// we might need to use the root span instead of the current span.
	original := span
	// whether this condition is matched by this span.
	for _, field := range condition.Fields {
		// always start with the original span
		span = original
		// check if rule uses root span context
		if strings.HasPrefix(field, RootPrefix) {
			// make sure root span exists
			if trace.RootSpan != nil {
				field = field[len(RootPrefix):]
				// now we're using the root span
				span = trace.RootSpan
			} else {
				// we wanted root span but this trace doesn't have one, so just skip it
				continue
			}
		} else {
			checkedOnlyRoot = false
		}

		value, exists = span.Data[field]
		if exists {
			return value, exists, checkedOnlyRoot
		}
	}
	if !exists && checkNestedFields {
		jsonStr, err := json.Marshal(span.Data)
		if err == nil {
			for _, field := range condition.Fields {
				result := gjson.Get(string(jsonStr), field)
				if result.Exists() {
					return result.String(), true, false
				}
			}
		}
	}
	return nil, false, false
}

// This only gets called when we're using one of the basic operators, and
// there is no datatype specified (meaning that the Matches function has not
// been set). In this case, we need to do some type conversion and comparison
// to determine whether the condition matches the value.
func conditionMatchesValue(condition *config.RulesBasedSamplerCondition, value interface{}, exists bool) bool {
	var match bool
	switch exists {
	case true:
		switch condition.Operator {
		case config.Exists:
			match = exists
		case config.NEQ:
			if comparison, ok := compare(value, condition.Value); ok {
				match = comparison != equal
			}
		case config.EQ:
			if comparison, ok := compare(value, condition.Value); ok {
				match = comparison == equal
			}
		case config.GT:
			if comparison, ok := compare(value, condition.Value); ok {
				match = comparison == more
			}
		case config.GTE:
			if comparison, ok := compare(value, condition.Value); ok {
				match = comparison == more || comparison == equal
			}
		case config.LT:
			if comparison, ok := compare(value, condition.Value); ok {
				match = comparison == less
			}
		case config.LTE:
			if comparison, ok := compare(value, condition.Value); ok {
				match = comparison == less || comparison == equal
			}
		}
	case false:
		switch condition.Operator {
		case config.NotExists:
			match = !exists
		}
	}
	return match
}

const (
	less  = -1
	equal = 0
	more  = 1
)

func compare(a, b interface{}) (int, bool) {
	// a is the tracing data field value. This can be: float64, int64, bool, or string
	// b is the Rule condition value. This can be: float64, int64, int, bool, or string
	// Note: in YAML config parsing, the Value may be returned as int
	// When comparing numeric values, we need to check across the 3 types: float64, int64, and int

	if a == nil {
		if b == nil {
			return equal, true
		}

		return less, true
	}

	if b == nil {
		return more, true
	}

	switch at := a.(type) {
	case int64:
		switch bt := b.(type) {
		case int:
			i := int(at)
			switch {
			case i < bt:
				return less, true
			case i > bt:
				return more, true
			default:
				return equal, true
			}
		case int64:
			switch {
			case at < bt:
				return less, true
			case at > bt:
				return more, true
			default:
				return equal, true
			}
		case float64:
			f := float64(at)
			switch {
			case f < bt:
				return less, true
			case f > bt:
				return more, true
			default:
				return equal, true
			}
		}
	case float64:
		switch bt := b.(type) {
		case int:
			f := float64(bt)
			switch {
			case at < f:
				return less, true
			case at > f:
				return more, true
			default:
				return equal, true
			}
		case int64:
			f := float64(bt)
			switch {
			case at < f:
				return less, true
			case at > f:
				return more, true
			default:
				return equal, true
			}
		case float64:
			switch {
			case at < bt:
				return less, true
			case at > bt:
				return more, true
			default:
				return equal, true
			}
		}
	case bool:
		switch bt := b.(type) {
		case bool:
			switch {
			case !at && bt:
				return less, true
			case at && !bt:
				return more, true
			default:
				return equal, true
			}
		}
	case string:
		switch bt := b.(type) {
		case string:
			return strings.Compare(at, bt), true
		}
	}

	return equal, false
}

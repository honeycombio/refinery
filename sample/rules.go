package sample

import (
	"math/rand"
	"strings"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/types"
)

type RulesBasedSampler struct {
	Config  *config.RulesBasedSamplerConfig
	Logger  logger.Logger
	Metrics metrics.Metrics
}

func (s *RulesBasedSampler) Start() error {
	s.Logger.Debug().Logf("Starting RulesBasedSampler")
	defer func() { s.Logger.Debug().Logf("Finished starting RulesBasedSampler") }()

	s.Metrics.Register("rulessampler_num_dropped", "counter")
	s.Metrics.Register("rulessampler_num_kept", "counter")
	s.Metrics.Register("rulessampler_sample_rate", "histogram")

	return nil
}

func (s *RulesBasedSampler) GetSampleRate(trace *types.Trace) (rate uint, keep bool) {
	logger := s.Logger.Debug().WithFields(map[string]interface{}{
		"trace_id": trace.TraceID,
	})

	for _, rule := range s.Config.Rule {
		var matched int
		rate := uint(rule.SampleRate)
		keep := !rule.Drop && rule.SampleRate > 0 && rand.Intn(rule.SampleRate) == 0

		// no condition signifies the default
		if rule.Condition == nil {
			s.Metrics.Histogram("rulessampler_sample_rate", float64(rule.SampleRate))
			if keep {
				s.Metrics.Increment("rulessampler_num_kept")
			} else {
				s.Metrics.Increment("rulessampler_num_dropped")
			}
			logger.WithFields(map[string]interface{}{
				"rate":      rate,
				"keep":      keep,
				"drop_rule": rule.Drop,
			}).Logf("got sample rate and decision")
			return rate, keep
		}

		for _, condition := range rule.Condition {
		span:
			for _, span := range trace.GetSpans() {
				var match bool
				value, exists := span.Data[condition.Field]

				switch exists {
				case true:
					switch condition.Operator {
					case "exists":
						match = exists
					case "!=":
						if comparison, ok := compare(value, condition.Value); ok {
							match = comparison != equal
						}
					case "=":
						if comparison, ok := compare(value, condition.Value); ok {
							match = comparison == equal
						}
					case ">":
						if comparison, ok := compare(value, condition.Value); ok {
							match = comparison == more
						}
					case ">=":
						if comparison, ok := compare(value, condition.Value); ok {
							match = comparison == more || comparison == equal
						}
					case "<":
						if comparison, ok := compare(value, condition.Value); ok {
							match = comparison == less
						}
					case "<=":
						if comparison, ok := compare(value, condition.Value); ok {
							match = comparison == less || comparison == equal
						}
					case "starts-with":
						switch a := value.(type) {
						case string:
							switch b := condition.Value.(type) {
							case string:
								match = strings.HasPrefix(a, b)
							}
						}
					case "contains":
						switch a := value.(type) {
						case string:
							switch b := condition.Value.(type) {
							case string:
								match = strings.Contains(a, b)
							}
						}
					case "does-not-contain":
						switch a := value.(type) {
						case string:
							switch b := condition.Value.(type) {
							case string:
								match = !strings.Contains(a, b)
							}
						}
					}
				case false:
					switch condition.Operator {
					case "not-exists":
						match = !exists
					}
				}

				if match {
					matched++
					break span
				}
			}
		}

		if matched == len(rule.Condition) {
			s.Metrics.Histogram("rulessampler_sample_rate", float64(rule.SampleRate))
			if keep {
				s.Metrics.Increment("rulessampler_num_kept")
			} else {
				s.Metrics.Increment("rulessampler_num_dropped")
			}
			logger.WithFields(map[string]interface{}{
				"rate":      rate,
				"keep":      keep,
				"drop_rule": rule.Drop,
				"rule_name": rule.Name,
			}).Logf("got sample rate and decision")
			return rate, keep
		}
	}

	return 1, true
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

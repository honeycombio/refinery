package sample

import (
	"math/rand"
	"strings"

	"github.com/honeycombio/samproxy/config"
	"github.com/honeycombio/samproxy/logger"
	"github.com/honeycombio/samproxy/metrics"
	"github.com/honeycombio/samproxy/types"
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
				s.Metrics.IncrementCounter("rulessampler_num_kept")
			} else {
				s.Metrics.IncrementCounter("dynsampler_num_dropped")
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

				if d, ok := span.Data[condition.Field]; ok {
					if c, ok := compare(d, condition.Value); ok {
						switch condition.Operator {
						case "!=":
							match = c != equal
						case "=":
							match = c == equal
						case ">":
							match = c == more
						case ">=":
							match = c == more || c == equal
						case "<":
							match = c == less
						case "<=":
							match = c == less || c == equal
						}
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
				s.Metrics.IncrementCounter("rulessampler_num_kept")
			} else {
				s.Metrics.IncrementCounter("dynsampler_num_dropped")
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

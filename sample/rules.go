package sample

import (
	"math/rand"
	"strings"

	"github.com/honeycombio/samproxy/config"
	"github.com/honeycombio/samproxy/logger"
	"github.com/honeycombio/samproxy/types"
)

type RulesBasedSampler struct {
	Logger logger.Logger
	Config *config.RulesBasedSamplerConfig
}

func (s *RulesBasedSampler) Start() error {
	s.Logger.Debug().Logf("Starting RulesBasedSampler")
	defer func() { s.Logger.Debug().Logf("Finished starting RulesBasedSampler") }()

	return nil
}

func (s *RulesBasedSampler) GetSampleRate(trace *types.Trace) (rate uint, keep bool) {
	for _, rule := range s.Config.Rule {
		var matched int
		keep := rule.SampleRate > 0 && rand.Intn(rule.SampleRate) == 0

		// no condition signifies the default
		if rule.Condition == nil {
			return uint(rule.SampleRate), keep
		}

		for _, condition := range rule.Condition {
			for _, span := range trace.GetSpans() {
				if d, ok := span.Data[condition.Field]; ok {
					if c, ok := compare(d, condition.Value); ok {
						switch condition.Operator {
						case "=":
							if c == equal {
								matched++
							}
						case ">":
							if c == more {
								matched++
							}
						case ">=":
							if c == more || c == equal {
								matched++
							}
						case "<":
							if c == less {
								matched++
							}
						case "<=":
							if c == less || c == equal {
								matched++
							}
						}
					}
				}
			}

			if matched == len(rule.Condition) {
				// we have a match
				return uint(rule.SampleRate), keep
			}
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

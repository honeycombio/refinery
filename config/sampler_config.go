package config

import (
	"fmt"
	"strconv"
	"strings"
)

type DeterministicSamplerConfig struct {
	SampleRate int `validate:"required,gte=1"`
}

type DynamicSamplerConfig struct {
	SampleRate                   int64 `validate:"required,gte=1"`
	ClearFrequencySec            int64
	FieldList                    []string `validate:"required"`
	UseTraceLength               bool
	AddSampleRateKeyToTrace      bool
	AddSampleRateKeyToTraceField string `validate:"required_with=AddSampleRateKeyToTrace"`
}

type EMADynamicSamplerConfig struct {
	GoalSampleRate      int `validate:"gte=1"`
	AdjustmentInterval  int
	Weight              float64 `validate:"gt=0,lt=1"`
	AgeOutValue         float64
	BurstMultiple       float64
	BurstDetectionDelay uint
	MaxKeys             int

	FieldList                    []string `validate:"required"`
	UseTraceLength               bool
	AddSampleRateKeyToTrace      bool
	AddSampleRateKeyToTraceField string `validate:"required_with=AddSampleRateKeyToTrace"`
}

type TotalThroughputSamplerConfig struct {
	GoalThroughputPerSec         int64 `validate:"gte=1"`
	ClearFrequencySec            int64
	FieldList                    []string `validate:"required"`
	UseTraceLength               bool
	AddSampleRateKeyToTrace      bool
	AddSampleRateKeyToTraceField string `validate:"required_with=AddSampleRateKeyToTrace"`
}

type RulesBasedSamplerCondition struct {
	Field    string
	Operator string
	Value    interface{}
	Datatype string
	Matches  func(value any, exists bool) bool
}

func (r *RulesBasedSamplerCondition) Init() error {
	return r.setMatchesFunction()
}

func (r *RulesBasedSamplerCondition) String() string {
	return fmt.Sprintf("%+v", *r)
}

func (r *RulesBasedSamplerCondition) setMatchesFunction() error {
	switch r.Operator {
	case "exists":
		r.Matches = func(value any, exists bool) bool {
			return exists
		}
	case "not-exists":
		r.Matches = func(value any, exists bool) bool {
			return !exists
		}
	case "!=":
		return setCompareOperators(r, "!=")
	case "=":
		return setCompareOperators(r, "=")
	case ">":
		return setCompareOperators(r, ">")
	case ">=":
		return setCompareOperators(r, ">=")
	case "<":
		return setCompareOperators(r, "<")
	case "<=":
		return setCompareOperators(r, "<=") //which format is better?
	case "starts-with":
		err := setMatchStringBasedOperators(r, "starts-with")
		if err != nil {
			return err
		}
	case "contains":
		err := setMatchStringBasedOperators(r, "contains")
		if err != nil {
			return err
		}
	case "does-not-contain":
		err := setMatchStringBasedOperators(r, "does-not-contain")
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown operator '%s'", r.Operator)
	}
	return nil
}

func tryConvertToInt(v any) (int, bool) {
	switch value := v.(type) {
	case int:
		return value, true
	case int64:
		return int(value), true
	case float64:
		return int(value), true
	case bool:
		return 0, false
	case string:
		n, err := strconv.Atoi(value)
		if err == nil {
			return n, true
		}
		return 0, false
	default:
		return 0, false
	}
}

func tryConvertToInt64(v any) (int64, bool) {
	switch value := v.(type) {
	case int:
		return int64(value), true
	case int64:
		return value, true
	case float64:
		return int64(value), true
	case bool:
		return 0, false
	case string:
		n, err := strconv.ParseInt(value, 10, 64)
		if err == nil {
			return n, true
		}
		return 0, false
	default:
		return 0, false
	}
}

func tryConvertToFloat(v any) (float64, bool) {
	switch value := v.(type) {
	case float64:
		return value, true
	case int:
		return float64(value), true
	case int64:
		return float64(value), true
	case bool:
		return 0, false
	case string:
		n, err := strconv.ParseFloat(value, 64)
		return n, err == nil
	default:
		return 0, false
	}
}

func tryConvertToString(v any) (string, bool) {
	switch value := v.(type) {
	case string:
		return value, true
	case int:
		return strconv.Itoa(value), true
	case int64:
		return strconv.FormatInt(value, 10), true
	case float64:
		return strconv.FormatFloat(value, 'E', -1, 64), false
	case bool:
		return strconv.FormatBool(value), true
	default:
		return "", false
	}
}

func tryConvertToBool(v any) bool {
	switch value := v.(type) {
	case string:
		if value == "true" {
			return true
		}
		return false
	default:
		return false
	}
}

func setCompareOperators(r *RulesBasedSamplerCondition, condition string) error {
	switch conditionValue := r.Value.(type) {
	case string:
		// check if conditionValue and spanValue are not equal
		switch condition {
		case "!=":
			r.Matches = func(spanValue any, exists bool) bool {
				if n, ok := tryConvertToString(spanValue); exists && ok {
					return n != conditionValue
				}
				return false
			}
			return nil
		case "=":
			r.Matches = func(spanValue any, exists bool) bool {
				if n, ok := tryConvertToString(spanValue); exists && ok {
					return n == conditionValue
				}
				return false
			}
			return nil
		}
	case int:
		// check if conditionValue and spanValue are not equal
		switch condition {
		case "!=":
			r.Matches = func(spanValue any, exists bool) bool {
				if n, ok := tryConvertToInt(spanValue); exists && ok {
					return n != conditionValue
				}
				return false
			}
			return nil
		case "=":
			r.Matches = func(spanValue any, exists bool) bool {
				if n, ok := tryConvertToInt(spanValue); exists && ok {
					return n == conditionValue
				}
				return false
			}
			return nil
		case ">":
			r.Matches = func(spanValue any, exists bool) bool {
				if n, ok := tryConvertToInt(spanValue); exists && ok {
					return n > conditionValue
				}
				return false
			}
			return nil
		case ">=":
			r.Matches = func(spanValue any, exists bool) bool {
				if n, ok := tryConvertToInt(spanValue); exists && ok {
					return n >= conditionValue
				}
				return false
			}
			return nil
		case "<":
			r.Matches = func(spanValue any, exists bool) bool {
				if n, ok := tryConvertToInt(spanValue); exists && ok {
					return n < conditionValue
				}
				return false
			}
			return nil
		case "<=":
			r.Matches = func(spanValue any, exists bool) bool {
				if n, ok := tryConvertToInt(spanValue); exists && ok {
					return n <= conditionValue
				}
				return false
			}
			return nil
		}
	case int64:
		// check if conditionValue and spanValue are not equal
		switch condition {
		case "!=":
			r.Matches = func(spanValue any, exists bool) bool {
				if n, ok := tryConvertToInt64(spanValue); exists && ok {
					return n != conditionValue
				}
				return false
			}
			return nil
		case "=":
			r.Matches = func(spanValue any, exists bool) bool {
				if n, ok := tryConvertToInt64(spanValue); exists && ok {
					return n == conditionValue
				}
				return false
			}
			return nil
		case ">":
			r.Matches = func(spanValue any, exists bool) bool {
				if n, ok := tryConvertToInt64(spanValue); exists && ok {
					return n > conditionValue
				}
				return false
			}
			return nil
		case ">=":
			r.Matches = func(spanValue any, exists bool) bool {
				if n, ok := tryConvertToInt64(spanValue); exists && ok {
					return n >= conditionValue
				}
				return false
			}
			return nil
		case "<":
			r.Matches = func(spanValue any, exists bool) bool {
				if n, ok := tryConvertToInt64(spanValue); exists && ok {
					return n < conditionValue
				}
				return false
			}
			return nil
		case "<=":
			r.Matches = func(spanValue any, exists bool) bool {
				if n, ok := tryConvertToInt64(spanValue); exists && ok {
					return n <= conditionValue
				}
				return false
			}
			return nil
		}
	case float64:
		// check if conditionValue and spanValue are not equal
		switch condition {
		case "!=":
			r.Matches = func(spanValue any, exists bool) bool {
				if n, ok := tryConvertToFloat(spanValue); exists && ok {
					return n != conditionValue
				}
				return false
			}
			return nil
		case "=":
			r.Matches = func(spanValue any, exists bool) bool {
				if n, ok := tryConvertToFloat(spanValue); exists && ok {
					return n == conditionValue
				}
				return false
			}
			return nil
		case ">":
			r.Matches = func(spanValue any, exists bool) bool {
				if n, ok := tryConvertToFloat(spanValue); exists && ok {
					return n > conditionValue
				}
				return false
			}
			return nil
		case ">=":
			r.Matches = func(spanValue any, exists bool) bool {
				if n, ok := tryConvertToFloat(spanValue); exists && ok {
					return n >= conditionValue
				}
				return false
			}
			return nil
		case "<":
			r.Matches = func(spanValue any, exists bool) bool {
				if n, ok := tryConvertToFloat(spanValue); exists && ok {
					return n < conditionValue
				}
				return false
			}
			return nil
		case "<=":
			r.Matches = func(spanValue any, exists bool) bool {
				if n, ok := tryConvertToFloat(spanValue); exists && ok {
					return n <= conditionValue
				}
				return false
			}
			return nil
		}
	case bool:
		// check if conditionValue and spanValue are not equal
		switch condition {
		case "!=":
			r.Matches = func(spanValue any, exists bool) bool {
				if n := tryConvertToBool(spanValue); exists && n {
					return n != conditionValue
				}
				return false
			}
			return nil
		case "=":
			r.Matches = func(spanValue any, exists bool) bool {
				if n := tryConvertToBool(spanValue); exists && n {
					return n == conditionValue
				}
				return false
			}
			return nil
		}
	default:
		return fmt.Errorf("%s value's type must be either string, int, float or bool, but was '%T'", conditionValue, conditionValue)
	}
	return nil // what return was missing?
}

func setMatchStringBasedOperators(r *RulesBasedSamplerCondition, condition string) error {
	conditionValue, ok := r.Value.(string)
	if !ok {
		return fmt.Errorf("%s value must be a string, but was '%s'", condition, r.Value)
	}

	r.Matches = func(spanValue any, exists bool) bool {
		s, ok := spanValue.(string)
		if !ok {
			return false
		}

		switch condition {
		case "starts-with":
			return strings.HasPrefix(s, conditionValue)
		case "contains":
			return strings.Contains(s, conditionValue)
		case "does-not-contain":
			return !strings.Contains(s, conditionValue)
		}
		return false
	}
	return nil
}

type RulesBasedDownstreamSampler struct {
	DynamicSampler         *DynamicSamplerConfig
	EMADynamicSampler      *EMADynamicSamplerConfig
	TotalThroughputSampler *TotalThroughputSamplerConfig
}

type RulesBasedSamplerRule struct {
	Name       string
	SampleRate int
	Sampler    *RulesBasedDownstreamSampler
	Drop       bool
	Scope      string `validate:"oneof=span trace"`
	Conditions []*RulesBasedSamplerCondition
}

func (r *RulesBasedSamplerRule) String() string {
	return fmt.Sprintf("%+v", *r)
}

type RulesBasedSamplerConfig struct {
	Rule              []*RulesBasedSamplerRule
	CheckNestedFields bool
}

func (r *RulesBasedSamplerConfig) String() string {
	return fmt.Sprintf("%+v", *r)
}

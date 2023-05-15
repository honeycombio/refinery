package config

import (
	"fmt"
	"strconv"
	"strings"
)

type V2SamplerChoice struct {
	Name                   string                        `yaml:"name,omitempty"`
	DeterministicSampler   *DeterministicSamplerConfig   `yaml:"deterministicSampler,omitempty"`
	RulesBasedSampler      *RulesBasedSamplerConfig      `yaml:"rulesBasedSampler,omitempty"`
	DynamicSampler         *DynamicSamplerConfig         `yaml:"dynamicSampler,omitempty"`
	EMADynamicSampler      *EMADynamicSamplerConfig      `yaml:"emaDynamicSampler,omitempty"`
	TotalThroughputSampler *TotalThroughputSamplerConfig `yaml:"totalThroughputSampler,omitempty"`
}

type V2SamplerConfig struct {
	ConfigVersion int `yaml:"configVersion" validate:"required,ge=2"`
	Samplers      map[string]*V2SamplerChoice
}

type DeterministicSamplerConfig struct {
	SampleRate int `default:"1" validate:"required,gte=1"`
}

type DynamicSamplerConfig struct {
	SampleRate                   int64    `yaml:",omitempty" validate:"required,gte=1"`
	ClearFrequencySec            int64    `yaml:",omitempty"`
	FieldList                    []string `yaml:",omitempty" validate:"required"`
	UseTraceLength               bool     `yaml:",omitempty"`
	AddSampleRateKeyToTrace      bool     `yaml:",omitempty"`
	AddSampleRateKeyToTraceField string   `yaml:",omitempty" validate:"required_with=AddSampleRateKeyToTrace"`
}

type EMADynamicSamplerConfig struct {
	GoalSampleRate               int      `yaml:",omitempty" validate:"gte=1"`
	AdjustmentInterval           int      `yaml:",omitempty"`
	Weight                       float64  `yaml:",omitempty" validate:"gt=0,lt=1"`
	AgeOutValue                  float64  `yaml:",omitempty"`
	BurstMultiple                float64  `yaml:",omitempty"`
	BurstDetectionDelay          uint     `yaml:",omitempty"`
	MaxKeys                      int      `yaml:",omitempty"`
	FieldList                    []string `yaml:",omitempty" validate:"required"`
	UseTraceLength               bool     `yaml:",omitempty"`
	AddSampleRateKeyToTrace      bool     `yaml:",omitempty"`
	AddSampleRateKeyToTraceField string   `yaml:",omitempty" validate:"required_with=AddSampleRateKeyToTrace"`
}

type TotalThroughputSamplerConfig struct {
	GoalThroughputPerSec         int64    `yaml:",omitempty" validate:"gte=1"`
	ClearFrequencySec            int64    `yaml:",omitempty"`
	FieldList                    []string `yaml:",omitempty" validate:"required"`
	UseTraceLength               bool     `yaml:",omitempty"`
	AddSampleRateKeyToTrace      bool     `yaml:",omitempty"`
	AddSampleRateKeyToTraceField string   `yaml:",omitempty" validate:"required_with=AddSampleRateKeyToTrace"`
}

type RulesBasedSamplerConfig struct {
	// Rules has deliberately different names for json and yaml for conversion from old to new format
	Rules             []*RulesBasedSamplerRule `json:"rule" yaml:"rules,omitempty"`
	CheckNestedFields bool                     `yaml:"checkNestedFields,omitempty"`
}

type RulesBasedDownstreamSampler struct {
	DynamicSampler         *DynamicSamplerConfig         `yaml:"dynamicSampler,omitempty"`
	EMADynamicSampler      *EMADynamicSamplerConfig      `yaml:"emaDynamicSampler,omitempty"`
	TotalThroughputSampler *TotalThroughputSamplerConfig `yaml:"totalThroughputSampler,omitempty"`
}

type RulesBasedSamplerRule struct {
	// Conditions has deliberately different names for json and yaml for conversion from old to new format
	Name       string                        `yaml:"name,omitempty"`
	SampleRate int                           `yaml:"samplerate,omitempty"`
	Drop       bool                          `yaml:"drop,omitempty"`
	Scope      string                        `yaml:"scope,omitempty" validate:"oneof=span trace"`
	Conditions []*RulesBasedSamplerCondition `json:"condition" yaml:"conditions,omitempty"`
	Sampler    *RulesBasedDownstreamSampler  `yaml:"sampler,omitempty"`
}

func (r *RulesBasedSamplerRule) String() string {
	return fmt.Sprintf("%+v", *r)
}

type RulesBasedSamplerCondition struct {
	Field    string                            `validate:"required"`
	Operator string                            `validate:"required"`
	Value    interface{}                       ``
	Datatype string                            `yaml:"datatype,omitempty"`
	Matches  func(value any, exists bool) bool `json:"-" yaml:"-"`
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
		return nil
	case "not-exists":
		r.Matches = func(value any, exists bool) bool {
			return !exists
		}
		return nil
	case "!=", "=", ">", "<", "<=", ">=":
		return setCompareOperators(r, r.Operator)
	case "starts-with", "contains", "does-not-contain":
		err := setMatchStringBasedOperators(r, r.Operator)
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

// In the case of strings, we want to stringize everything we get through a
// "standard" format, which we are defining as whatever Go does with the %v
// operator to sprintf. This will make sure that no matter how people encode
// their values, they compare on an equal footing.
func tryConvertToString(v any) (string, bool) {
	return fmt.Sprintf("%v", v), true
}

func tryConvertToBool(v any) bool {
	value, ok := tryConvertToString(v)
	if !ok {
		return false
	}
	str, err := strconv.ParseBool(value)
	if err != nil {
		return false
	}
	if str {
		return true
	} else {
		return false
	}
}

func setCompareOperators(r *RulesBasedSamplerCondition, condition string) error {
	switch r.Datatype {
	case "string":
		conditionValue, ok := tryConvertToString(r.Value)
		if !ok {
			return fmt.Errorf("could not convert %v to string", r.Value)
		}

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
		case ">":
			r.Matches = func(spanValue any, exists bool) bool {
				if n, ok := tryConvertToString(spanValue); exists && ok {
					return n > conditionValue
				}
				return false
			}
			return nil
		case "<":
			r.Matches = func(spanValue any, exists bool) bool {
				if n, ok := tryConvertToString(spanValue); exists && ok {
					return n < conditionValue
				}
				return false
			}
			return nil
		case "<=":
			r.Matches = func(spanValue any, exists bool) bool {
				if n, ok := tryConvertToString(spanValue); exists && ok {
					return n <= conditionValue
				}
				return false
			}
			return nil
		}
	case "int":
		// check if conditionValue and spanValue are not equal
		conditionValue, ok := tryConvertToInt(r.Value)
		if !ok {
			return fmt.Errorf("could not convert %v to string", r.Value)
		}
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
	case "float":
		conditionValue, ok := tryConvertToFloat(r.Value)
		if !ok {
			return fmt.Errorf("could not convert %v to string", r.Value)
		}
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
	case "bool":
		conditionValue := tryConvertToBool(r.Value)

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
	case "":
		// user did not specify datatype, so do not specify matches function
	default:
		return fmt.Errorf("%s must be either string, int, float or bool", r.Datatype)
	}
	return nil
}

func setMatchStringBasedOperators(r *RulesBasedSamplerCondition, condition string) error {
	conditionValue, ok := tryConvertToString(r.Value)
	if !ok {
		return fmt.Errorf("%s value must be a string, but was '%s'", condition, r.Value)
	}

	switch condition {
	case "starts-with":
		r.Matches = func(spanValue any, exists bool) bool {
			s, ok := tryConvertToString(spanValue)
			if ok {
				return strings.HasPrefix(s, conditionValue)
			}
			return false
		}
	case "contains":
		r.Matches = func(spanValue any, exists bool) bool {
			s, ok := tryConvertToString(spanValue)
			if ok {
				return strings.Contains(s, conditionValue)
			}
			return false
		}
	case "does-not-contain":
		r.Matches = func(spanValue any, exists bool) bool {
			s, ok := tryConvertToString(spanValue)
			if ok {
				return !strings.Contains(s, conditionValue)
			}
			return false
		}
	}

	return nil
}

func (r *RulesBasedSamplerConfig) String() string {
	return fmt.Sprintf("%+v", *r)
}

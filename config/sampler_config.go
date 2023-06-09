package config

import (
	"fmt"
	"strconv"
	"strings"
)

// The json tags in this file are used for conversion from the old format (see tools/convert for details).
// They are deliberately all lowercase.
// The yaml tags are used for the new format and are PascalCase.

type V2SamplerChoice struct {
	DeterministicSampler   *DeterministicSamplerConfig   `json:"deterministicsampler" yaml:"DeterministicSampler,omitempty"`
	RulesBasedSampler      *RulesBasedSamplerConfig      `json:"rulesbasedsampler" yaml:"RulesBasedSampler,omitempty"`
	DynamicSampler         *DynamicSamplerConfig         `json:"dynamicsampler" yaml:"DynamicSampler,omitempty"`
	EMADynamicSampler      *EMADynamicSamplerConfig      `json:"emadynamicsampler" yaml:"EMADynamicSampler,omitempty"`
	EMAThroughputSampler   *EMAThroughputSamplerConfig   `json:"emathroughputsampler" yaml:"EMAThroughputSampler,omitempty"`
	TotalThroughputSampler *TotalThroughputSamplerConfig `json:"totalthroughputsampler" yaml:"TotalThroughputSampler,omitempty"`
}

func (v *V2SamplerChoice) Sampler() (any, string) {
	switch {
	case v.DeterministicSampler != nil:
		return v.DeterministicSampler, "DeterministicSampler"
	case v.RulesBasedSampler != nil:
		return v.RulesBasedSampler, "RulesBasedSampler"
	case v.DynamicSampler != nil:
		return v.DynamicSampler, "DynamicSampler"
	case v.EMADynamicSampler != nil:
		return v.EMADynamicSampler, "EMADynamicSampler"
	case v.EMAThroughputSampler != nil:
		return v.EMAThroughputSampler, "EMAThroughputSampler"
	case v.TotalThroughputSampler != nil:
		return v.TotalThroughputSampler, "TotalThroughputSampler"
	default:
		return nil, ""
	}
}

type V2SamplerConfig struct {
	RulesVersion int                         `json:"rulesversion" yaml:"RulesVersion" validate:"required,ge=2"`
	Samplers     map[string]*V2SamplerChoice `json:"samplers" yaml:"Samplers,omitempty" validate:"required"`
}

type DeterministicSamplerConfig struct {
	SampleRate int `json:"samplerate" yaml:"SampleRate,omitempty" default:"1" validate:"required,gte=1"`
}

type DynamicSamplerConfig struct {
	SampleRate     int64    `json:"samplerate" yaml:"SampleRate,omitempty" validate:"required,gte=1"`
	ClearFrequency Duration `json:"clearfrequency" yaml:"ClearFrequency,omitempty"`
	FieldList      []string `json:"fieldlist" yaml:"FieldList,omitempty" validate:"required"`
	MaxKeys        int      `json:"maxkeys" yaml:"MaxKeys,omitempty"`
	UseTraceLength bool     `json:"usetracelength" yaml:"UseTraceLength,omitempty"`
}

type EMADynamicSamplerConfig struct {
	GoalSampleRate      int      `json:"goalsamplerate" yaml:"GoalSampleRate,omitempty" validate:"gte=1"`
	AdjustmentInterval  Duration `json:"adjustmentinterval" yaml:"AdjustmentInterval,omitempty"`
	Weight              float64  `json:"weight" yaml:"Weight,omitempty" validate:"gt=0,lt=1"`
	AgeOutValue         float64  `json:"ageoutvalue" yaml:"AgeOutValue,omitempty"`
	BurstMultiple       float64  `json:"burstmultiple" yaml:"BurstMultiple,omitempty"`
	BurstDetectionDelay uint     `json:"burstdetectiondelay" yaml:"BurstDetectionDelay,omitempty"`
	FieldList           []string `json:"fieldlist" yaml:"FieldList,omitempty" validate:"required"`
	MaxKeys             int      `json:"maxkeys" yaml:"MaxKeys,omitempty"`
	UseTraceLength      bool     `json:"usetracelength" yaml:"UseTraceLength,omitempty"`
}

type EMAThroughputSamplerConfig struct {
	GoalThroughputPerSec int      `json:"goalthroughputpersec" yaml:"GoalThroughputPerSec,omitempty"`
	InitialSampleRate    int      `json:"initialsamplerate" yaml:"InitialSampleRate,omitempty"`
	AdjustmentInterval   Duration `json:"adjustmentinterval" yaml:"AdjustmentInterval,omitempty"`
	Weight               float64  `json:"weight" yaml:"Weight,omitempty"`
	AgeOutValue          float64  `json:"ageoutvalue" yaml:"AgeOutValue,omitempty"`
	BurstMultiple        float64  `json:"burstmultiple" yaml:"BurstMultiple,omitempty"`
	BurstDetectionDelay  uint     `json:"burstdetectiondelay" yaml:"BurstDetectionDelay,omitempty"`
	FieldList            []string `json:"fieldlist" yaml:"FieldList,omitempty"`
	MaxKeys              int      `json:"maxkeys" yaml:"MaxKeys,omitempty"`
	UseTraceLength       bool     `json:"usetracelength" yaml:"UseTraceLength,omitempty"`
}

type TotalThroughputSamplerConfig struct {
	GoalThroughputPerSec int64    `json:"goalthroughputpersec" yaml:"GoalThroughputPerSec,omitempty" validate:"gte=1"`
	ClearFrequency       Duration `json:"clearfrequency" yaml:"ClearFrequency,omitempty"`
	FieldList            []string `json:"fieldlist" yaml:"FieldList,omitempty" validate:"required"`
	MaxKeys              int      `json:"maxkeys" yaml:"MaxKeys,omitempty"`
	UseTraceLength       bool     `json:"usetracelength" yaml:"UseTraceLength,omitempty"`
}

type RulesBasedSamplerConfig struct {
	// Rules has deliberately different names for json and yaml for conversion from old to new format
	Rules             []*RulesBasedSamplerRule `json:"rule" yaml:"Rules,omitempty"`
	CheckNestedFields bool                     `json:"checknestedfields" yaml:"CheckNestedFields,omitempty"`
}

type RulesBasedDownstreamSampler struct {
	DynamicSampler         *DynamicSamplerConfig         `json:"dynamicsampler" yaml:"DynamicSampler,omitempty"`
	EMADynamicSampler      *EMADynamicSamplerConfig      `json:"emadynamicsampler" yaml:"EMADynamicSampler,omitempty"`
	EMAThroughputSampler   *EMAThroughputSamplerConfig   `json:"emathroughputsampler" yaml:"EMAThroughputSampler,omitempty"`
	TotalThroughputSampler *TotalThroughputSamplerConfig `json:"totalthroughputsampler" yaml:"TotalThroughputSampler,omitempty"`
}

type RulesBasedSamplerRule struct {
	// Conditions has deliberately different names for json and yaml for conversion from old to new format
	Name       string                        `json:"name" yaml:"Name,omitempty"`
	SampleRate int                           `json:"samplerate" yaml:"SampleRate,omitempty"`
	Drop       bool                          `json:"drop" yaml:"Drop,omitempty"`
	Scope      string                        `json:"scope" yaml:"Scope,omitempty" validate:"oneof=span trace"`
	Conditions []*RulesBasedSamplerCondition `json:"condition" yaml:"Conditions,omitempty"`
	Sampler    *RulesBasedDownstreamSampler  `json:"sampler" yaml:"Sampler,omitempty"`
}

func (r *RulesBasedSamplerRule) String() string {
	return fmt.Sprintf("%+v", *r)
}

type RulesBasedSamplerCondition struct {
	Field    string                            `json:"field" yaml:"Field" validate:"required"`
	Operator string                            `json:"operator" yaml:"Operator" validate:"required"`
	Value    interface{}                       `json:"value" yaml:"Value" `
	Datatype string                            `json:"datatype" yaml:"Datatype,omitempty"`
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

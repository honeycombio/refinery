package config

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/honeycombio/refinery/generics"
)

// Define some constants for rule comparison operators
const (
	NEQ = "!="
	EQ  = "="
	GT  = ">"
	LT  = "<"
	GTE = ">="
	LTE = "<="
)

// and also the rule keyword operators
const (
	Contains       = "contains"
	DoesNotContain = "does-not-contain"
	StartsWith     = "starts-with"
	Exists         = "exists"
	NotExists      = "not-exists"
	HasRootSpan    = "has-root-span"
	MatchesRegexp  = "matches"
	In             = "in"
	NotIn          = "not-in"
)

// ComputedField is a virtual field. It's value is calculated during rule evaluation.
// We use the `?.` prefix to distinguish computed fields from regular fields.
type ComputedField string

const (
	// ComputedFieldPrefix is the prefix for computed fields.
	ComputedFieldPrefix               = "?."
	NUM_DESCENDANTS     ComputedField = ComputedFieldPrefix + "NUM_DESCENDANTS"
)

// The json tags in this file are used for conversion from the old format (see tools/convert for details).
// They are deliberately all lowercase.
// The yaml tags are used for the new format and are PascalCase.

type V2SamplerChoice struct {
	DeterministicSampler      *DeterministicSamplerConfig      `json:"deterministicsampler" yaml:"DeterministicSampler,omitempty"`
	RulesBasedSampler         *RulesBasedSamplerConfig         `json:"rulesbasedsampler" yaml:"RulesBasedSampler,omitempty"`
	DynamicSampler            *DynamicSamplerConfig            `json:"dynamicsampler" yaml:"DynamicSampler,omitempty"`
	EMADynamicSampler         *EMADynamicSamplerConfig         `json:"emadynamicsampler" yaml:"EMADynamicSampler,omitempty"`
	EMAThroughputSampler      *EMAThroughputSamplerConfig      `json:"emathroughputsampler" yaml:"EMAThroughputSampler,omitempty"`
	WindowedThroughputSampler *WindowedThroughputSamplerConfig `json:"windowedthroughputsampler" yaml:"WindowedThroughputSampler,omitempty"`
	TotalThroughputSampler    *TotalThroughputSamplerConfig    `json:"totalthroughputsampler" yaml:"TotalThroughputSampler,omitempty"`
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
	case v.WindowedThroughputSampler != nil:
		return v.WindowedThroughputSampler, "WindowedThroughputSampler"
	case v.TotalThroughputSampler != nil:
		return v.TotalThroughputSampler, "TotalThroughputSampler"
	default:
		return nil, ""
	}
}

// This looks through the list of samplers and finds samplers that have
// sample rates that are nontrivial. The point of this is to determine which
// samplers are meaningful to report as ones the user might want to adjust
// because the way that sample rates are computed has changed. It's used
// by the rules converter.
func (v *V2SamplerChoice) NameMeaningfulSamplers() []string {
	names := generics.NewSet[string]()
	switch {
	case v.DeterministicSampler != nil:
		names.Add("DeterministicSampler")
	case v.RulesBasedSampler != nil:
		for _, rule := range v.RulesBasedSampler.Rules {
			if rule.Sampler != nil {
				if rule.SampleRate > 1 {
					names.Add(fmt.Sprintf("RulesBasedSampler(%s)", rule.Name))
				} else if rule.Sampler.NameMeaningfulRate() != "" {
					names.Add(fmt.Sprintf("RulesBasedSampler(%s, downstream Sampler %s)", rule.Name, rule.Sampler.NameMeaningfulRate()))
				}
			}
		}
	case v.DynamicSampler != nil:
		names.Add("DynamicSampler")
	case v.EMADynamicSampler != nil:
		names.Add("EMADynamicSampler")
	case v.EMAThroughputSampler != nil:
		names.Add("EMAThroughputSampler")
	case v.WindowedThroughputSampler != nil:
		names.Add("WindowedThroughputSampler")
	case v.TotalThroughputSampler != nil:
		names.Add("TotalThroughputSampler")
	default:
		return nil
	}

	return names.Members()
}

func (v *RulesBasedDownstreamSampler) NameMeaningfulRate() string {
	switch {
	case v.DynamicSampler != nil:
		if v.DynamicSampler.SampleRate > 1 {
			return "DynamicSampler"
		}
	case v.EMADynamicSampler != nil:
		if v.EMADynamicSampler.GoalSampleRate > 1 {
			return "EMADynamicSampler"
		}
	case v.EMAThroughputSampler != nil:
		if v.EMAThroughputSampler.GoalThroughputPerSec > 1 {
			return "EMAThroughputSampler"
		}
	case v.WindowedThroughputSampler != nil:
		if v.WindowedThroughputSampler.GoalThroughputPerSec > 0 {
			return "WindowedThroughputSampler"
		}
	case v.TotalThroughputSampler != nil:
		if v.TotalThroughputSampler.GoalThroughputPerSec > 0 {
			return "TotalThroughputSampler"
		}
	}
	return ""
}

type V2SamplerConfig struct {
	RulesVersion         int                         `json:"rulesversion" yaml:"RulesVersion" validate:"required,ge=2"`
	Samplers             map[string]*V2SamplerChoice `json:"samplers" yaml:"Samplers,omitempty" validate:"required"`
	uniqueSamplingFields generics.Set[string]
}

func (v *V2SamplerConfig) UniqueSamplingFields() []string {
	return v.uniqueSamplingFields.Members()
}

func (v *V2SamplerConfig) ExtractUniqueSamplingFields() {
	v.uniqueSamplingFields = generics.NewSet[string]()

	for _, sampler := range v.Samplers {
		if sampler == nil {
			continue
		}

		s, _ := sampler.Sampler()
		if s == nil {
			continue
		}

		if c, ok := s.(GetSamplingFielder); ok {
			for _, field := range c.GetSamplingFields() {
				v.uniqueSamplingFields.Add(field)
			}
		}
	}
}

type GetSamplingFielder interface {
	GetSamplingFields() []string
}

var _ GetSamplingFielder = (*DeterministicSamplerConfig)(nil)

type DeterministicSamplerConfig struct {
	SampleRate int `json:"samplerate" yaml:"SampleRate,omitempty" default:"1" validate:"required,gte=1"`
}

func (d *DeterministicSamplerConfig) GetSamplingFields() []string {
	return nil
}

var _ GetSamplingFielder = (*DynamicSamplerConfig)(nil)

type DynamicSamplerConfig struct {
	SampleRate     int64    `json:"samplerate" yaml:"SampleRate,omitempty" validate:"required,gte=1"`
	ClearFrequency Duration `json:"clearfrequency" yaml:"ClearFrequency,omitempty"`
	FieldList      []string `json:"fieldlist" yaml:"FieldList,omitempty" validate:"required"`
	MaxKeys        int      `json:"maxkeys" yaml:"MaxKeys,omitempty"`
	UseTraceLength bool     `json:"usetracelength" yaml:"UseTraceLength,omitempty"`
}

func (d *DynamicSamplerConfig) GetSamplingFields() []string {
	return d.FieldList
}

var _ GetSamplingFielder = (*EMADynamicSamplerConfig)(nil)

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

func (d *EMADynamicSamplerConfig) GetSamplingFields() []string {
	return d.FieldList
}

var _ GetSamplingFielder = (*EMAThroughputSamplerConfig)(nil)

type EMAThroughputSamplerConfig struct {
	GoalThroughputPerSec int      `json:"goalthroughputpersec" yaml:"GoalThroughputPerSec,omitempty"`
	UseClusterSize       bool     `json:"useclustersize" yaml:"UseClusterSize,omitempty"`
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

func (d *EMAThroughputSamplerConfig) GetSamplingFields() []string {
	return d.FieldList
}

var _ GetSamplingFielder = (*WindowedThroughputSamplerConfig)(nil)

type WindowedThroughputSamplerConfig struct {
	UpdateFrequency      Duration `json:"updatefrequency" yaml:"UpdateFrequency,omitempty"`
	LookbackFrequency    Duration `json:"lookbackfrequency" yaml:"LookbackFrequency,omitempty"`
	GoalThroughputPerSec int      `json:"goalthroughputpersec" yaml:"GoalThroughputPerSec,omitempty"`
	UseClusterSize       bool     `json:"useclustersize" yaml:"UseClusterSize,omitempty"`
	FieldList            []string `json:"fieldlist" yaml:"FieldList,omitempty"`
	MaxKeys              int      `json:"maxkeys" yaml:"MaxKeys,omitempty"`
	UseTraceLength       bool     `json:"usetracelength" yaml:"UseTraceLength,omitempty"`
}

func (d *WindowedThroughputSamplerConfig) GetSamplingFields() []string {
	return d.FieldList
}

var _ GetSamplingFielder = (*TotalThroughputSamplerConfig)(nil)

type TotalThroughputSamplerConfig struct {
	GoalThroughputPerSec int      `json:"goalthroughputpersec" yaml:"GoalThroughputPerSec,omitempty" validate:"gte=1"`
	UseClusterSize       bool     `json:"useclustersize" yaml:"UseClusterSize,omitempty"`
	ClearFrequency       Duration `json:"clearfrequency" yaml:"ClearFrequency,omitempty"`
	FieldList            []string `json:"fieldlist" yaml:"FieldList,omitempty" validate:"required"`
	MaxKeys              int      `json:"maxkeys" yaml:"MaxKeys,omitempty"`
	UseTraceLength       bool     `json:"usetracelength" yaml:"UseTraceLength,omitempty"`
}

func (d *TotalThroughputSamplerConfig) GetSamplingFields() []string {
	return d.FieldList
}

var _ GetSamplingFielder = (*RulesBasedSamplerConfig)(nil)

type RulesBasedSamplerConfig struct {
	// Rules has deliberately different names for json and yaml for conversion from old to new format
	Rules             []*RulesBasedSamplerRule `json:"rule" yaml:"Rules,omitempty"`
	CheckNestedFields bool                     `json:"checknestedfields" yaml:"CheckNestedFields,omitempty"`
}

func (r *RulesBasedSamplerConfig) GetSamplingFields() []string {
	fields := make([]string, 0)

	for _, rule := range r.Rules {
		if rule == nil {
			continue
		}

		for _, condition := range rule.Conditions {
			fields = append(fields, condition.Fields...)

			if condition.Field != "" {
				fields = append(fields, condition.Field)
			}
		}

		if rule.Sampler != nil {
			fields = append(fields, rule.Sampler.GetSamplingFields()...)
		}
	}

	return fields
}

var _ GetSamplingFielder = (*RulesBasedDownstreamSampler)(nil)

type RulesBasedDownstreamSampler struct {
	DynamicSampler            *DynamicSamplerConfig            `json:"dynamicsampler" yaml:"DynamicSampler,omitempty"`
	EMADynamicSampler         *EMADynamicSamplerConfig         `json:"emadynamicsampler" yaml:"EMADynamicSampler,omitempty"`
	EMAThroughputSampler      *EMAThroughputSamplerConfig      `json:"emathroughputsampler" yaml:"EMAThroughputSampler,omitempty"`
	WindowedThroughputSampler *WindowedThroughputSamplerConfig `json:"windowedthroughputsampler" yaml:"WindowedThroughputSampler,omitempty"`
	TotalThroughputSampler    *TotalThroughputSamplerConfig    `json:"totalthroughputsampler" yaml:"TotalThroughputSampler,omitempty"`
	DeterministicSampler      *DeterministicSamplerConfig      `json:"deterministicsampler" yaml:"DeterministicSampler,omitempty"`
}

func (r *RulesBasedDownstreamSampler) GetSamplingFields() []string {
	fields := make([]string, 0)

	if r.DeterministicSampler != nil {
		fields = append(fields, r.DeterministicSampler.GetSamplingFields()...)
	}

	if r.DynamicSampler != nil {
		fields = append(fields, r.DynamicSampler.GetSamplingFields()...)
	}

	if r.EMADynamicSampler != nil {
		fields = append(fields, r.EMADynamicSampler.GetSamplingFields()...)
	}

	if r.EMAThroughputSampler != nil {
		fields = append(fields, r.EMAThroughputSampler.GetSamplingFields()...)
	}

	if r.WindowedThroughputSampler != nil {
		fields = append(fields, r.WindowedThroughputSampler.GetSamplingFields()...)
	}

	if r.TotalThroughputSampler != nil {
		fields = append(fields, r.TotalThroughputSampler.GetSamplingFields()...)
	}

	return fields
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
	Field    string                            `json:"field" yaml:"Field"`
	Fields   []string                          `json:"fields" yaml:"Fields,omitempty"`
	Operator string                            `json:"operator" yaml:"Operator" validate:"required"`
	Value    any                               `json:"value" yaml:"Value" `
	Datatype string                            `json:"datatype" yaml:"Datatype,omitempty"`
	Matches  func(value any, exists bool) bool `json:"-" yaml:"-"`
}

func (r *RulesBasedSamplerCondition) Init() error {
	// if Field is specified, we move it into Fields so that we don't have to deal with checking both.
	if r.Field != "" {
		// we're going to check that both aren't defined -- this should have been caught by validation
		// but we'll also check here just in case.
		if len(r.Fields) > 0 {
			return fmt.Errorf("both Field and Fields are defined in a single condition")
		}
		// now we know it's safe to move Field into Fields
		r.Fields = []string{r.Field}
	}
	return r.setMatchesFunction()
}

func (r *RulesBasedSamplerCondition) String() string {
	return fmt.Sprintf("%+v", *r)
}

func (r *RulesBasedSamplerCondition) GetComputedField() (ComputedField, bool) {
	if strings.HasPrefix(r.Field, ComputedFieldPrefix) {
		return ComputedField(r.Field), true
	}
	return "", false

}

func (r *RulesBasedSamplerCondition) setMatchesFunction() error {
	switch r.Operator {
	case Exists:
		r.Matches = func(value any, exists bool) bool {
			return exists
		}
		return nil
	case NotExists:
		r.Matches = func(value any, exists bool) bool {
			return !exists
		}
		return nil
	case NEQ, EQ, GT, LT, LTE, GTE:
		return setCompareOperators(r, r.Operator)
	case StartsWith, Contains, DoesNotContain:
		err := setMatchStringBasedOperators(r, r.Operator)
		if err != nil {
			return err
		}
	case MatchesRegexp:
		err := setRegexStringMatchOperator(r)
		if err != nil {
			return err
		}
	case HasRootSpan:
		// this is evaluated at the trace level, so we don't need to do anything here
		return nil
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

func TryConvertToBool(v any) bool {
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

		switch condition {
		case NEQ:
			r.Matches = func(spanValue any, exists bool) bool {
				if n, ok := tryConvertToString(spanValue); exists && ok {
					return n != conditionValue
				}
				return false
			}
			return nil
		case EQ:
			r.Matches = func(spanValue any, exists bool) bool {
				if n, ok := tryConvertToString(spanValue); exists && ok {
					return n == conditionValue
				}
				return false
			}
			return nil
		case GT:
			r.Matches = func(spanValue any, exists bool) bool {
				if n, ok := tryConvertToString(spanValue); exists && ok {
					return n > conditionValue
				}
				return false
			}
			return nil
		case GTE:
			r.Matches = func(spanValue any, exists bool) bool {
				if n, ok := tryConvertToString(spanValue); exists && ok {
					return n >= conditionValue
				}
				return false
			}
			return nil
		case LT:
			r.Matches = func(spanValue any, exists bool) bool {
				if n, ok := tryConvertToString(spanValue); exists && ok {
					return n < conditionValue
				}
				return false
			}
			return nil
		case LTE:
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
		case NEQ:
			r.Matches = func(spanValue any, exists bool) bool {
				if n, ok := tryConvertToInt(spanValue); exists && ok {
					return n != conditionValue
				}
				return false
			}
			return nil
		case EQ:
			r.Matches = func(spanValue any, exists bool) bool {
				if n, ok := tryConvertToInt(spanValue); exists && ok {
					return n == conditionValue
				}
				return false
			}
			return nil
		case GT:
			r.Matches = func(spanValue any, exists bool) bool {
				if n, ok := tryConvertToInt(spanValue); exists && ok {
					return n > conditionValue
				}
				return false
			}
			return nil
		case GTE:
			r.Matches = func(spanValue any, exists bool) bool {
				if n, ok := tryConvertToInt(spanValue); exists && ok {
					return n >= conditionValue
				}
				return false
			}
			return nil
		case LT:
			r.Matches = func(spanValue any, exists bool) bool {
				if n, ok := tryConvertToInt(spanValue); exists && ok {
					return n < conditionValue
				}
				return false
			}
			return nil
		case LTE:
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
		case NEQ:
			r.Matches = func(spanValue any, exists bool) bool {
				if n, ok := tryConvertToFloat(spanValue); exists && ok {
					return n != conditionValue
				}
				return false
			}
			return nil
		case EQ:
			r.Matches = func(spanValue any, exists bool) bool {
				if n, ok := tryConvertToFloat(spanValue); exists && ok {
					return n == conditionValue
				}
				return false
			}
			return nil
		case GT:
			r.Matches = func(spanValue any, exists bool) bool {
				if n, ok := tryConvertToFloat(spanValue); exists && ok {
					return n > conditionValue
				}
				return false
			}
			return nil
		case GTE:
			r.Matches = func(spanValue any, exists bool) bool {
				if n, ok := tryConvertToFloat(spanValue); exists && ok {
					return n >= conditionValue
				}
				return false
			}
			return nil
		case LT:
			r.Matches = func(spanValue any, exists bool) bool {
				if n, ok := tryConvertToFloat(spanValue); exists && ok {
					return n < conditionValue
				}
				return false
			}
			return nil
		case LTE:
			r.Matches = func(spanValue any, exists bool) bool {
				if n, ok := tryConvertToFloat(spanValue); exists && ok {
					return n <= conditionValue
				}
				return false
			}
			return nil
		}
	case "bool":
		conditionValue := TryConvertToBool(r.Value)

		switch condition {
		case NEQ:
			r.Matches = func(spanValue any, exists bool) bool {
				if n := TryConvertToBool(spanValue); exists {
					return n != conditionValue
				}
				return false
			}
			return nil
		case EQ:
			r.Matches = func(spanValue any, exists bool) bool {
				if n := TryConvertToBool(spanValue); exists {
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
	case StartsWith:
		r.Matches = func(spanValue any, exists bool) bool {
			s, ok := tryConvertToString(spanValue)
			if ok {
				return strings.HasPrefix(s, conditionValue)
			}
			return false
		}
	case Contains:
		r.Matches = func(spanValue any, exists bool) bool {
			s, ok := tryConvertToString(spanValue)
			if ok {
				return strings.Contains(s, conditionValue)
			}
			return false
		}
	case DoesNotContain:
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

func setRegexStringMatchOperator(r *RulesBasedSamplerCondition) error {
	conditionValue, ok := tryConvertToString(r.Value)
	if !ok {
		return fmt.Errorf("regex value must be a string, but was '%s'", r.Value)
	}

	regex, err := regexp.Compile(conditionValue)
	if err != nil {
		return fmt.Errorf("'matches' pattern must be a valid Go regexp, but was '%s'", r.Value)
	}

	r.Matches = func(spanValue any, exists bool) bool {
		s, ok := tryConvertToString(spanValue)
		if ok {
			return regex.MatchString(s)
		}
		return false
	}

	return nil
}

func (r *RulesBasedSamplerConfig) String() string {
	return fmt.Sprintf("%+v", *r)
}

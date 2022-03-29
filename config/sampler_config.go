package config

import (
	"fmt"
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
}

func (r *RulesBasedSamplerCondition) String() string {
	return fmt.Sprintf("%+v", *r)
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
	Condition  []*RulesBasedSamplerCondition
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

package config

type DeterministicSamplerConfig struct {
	SampleRate int `validate:"required,gte=1"`
}

type DynamicSamplerConfig struct {
	SampleRate                   int64 `validate:"required,gte=1"`
	ClearFrequencySec            int64
	FieldList                    []string `validate:"required"`
	UseTraceLength               bool
	AddSampleRateKeyToTrace      bool
	AddSampleRateKeyToTraceField string
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
	AddSampleRateKeyToTraceField string
}

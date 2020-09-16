package config

type DeterministicSamplerConfig struct {
	SampleRate int
}

type DynamicSamplerConfig struct {
	SampleRate                   int64
	ClearFrequencySec            int64
	FieldList                    []string
	UseTraceLength               bool
	AddSampleRateKeyToTrace      bool
	AddSampleRateKeyToTraceField string
}

type EMADynamicSamplerConfig struct {
	GoalSampleRate      int
	AdjustmentInterval  int
	Weight              float64
	AgeOutValue         float64
	BurstMultiple       float64
	BurstDetectionDelay uint
	MaxKeys             int

	FieldList                    []string
	UseTraceLength               bool
	AddSampleRateKeyToTrace      bool
	AddSampleRateKeyToTraceField string
}

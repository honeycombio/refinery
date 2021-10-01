package sample

import (
	"os"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/types"
)

type Sampler interface {
	GetSampleRate(trace *types.Trace) (rate uint, keep bool)
	Start() error
}

// SamplerFactory is used to create new samplers with common (injected) resources
type SamplerFactory struct {
	Config  config.Config   `inject:""`
	Logger  logger.Logger   `inject:""`
	Metrics metrics.Metrics `inject:"metrics"`
}

// GetSamplerImplementationForDataset returns the sampler implementation for the dataset,
// or nil if it is not defined
func (s *SamplerFactory) GetSamplerImplementationForDataset(dataset string) Sampler {
	c, err := s.Config.GetSamplerConfigForDataset(dataset)
	if err != nil {
		return nil
	}

	var sampler Sampler

	switch c := c.(type) {
	case *config.DeterministicSamplerConfig:
		sampler = &DeterministicSampler{Config: c, Logger: s.Logger}
	case *config.DynamicSamplerConfig:
		sampler = &DynamicSampler{Config: c, Logger: s.Logger, Metrics: s.Metrics}
	case *config.EMADynamicSamplerConfig:
		sampler = &EMADynamicSampler{Config: c, Logger: s.Logger, Metrics: s.Metrics}
	case *config.RulesBasedSamplerConfig:
		sampler = &RulesBasedSampler{Config: c, Logger: s.Logger, Metrics: s.Metrics}
	case *config.TotalThroughputSamplerConfig:
		sampler = &TotalThroughputSampler{Config: c, Logger: s.Logger, Metrics: s.Metrics}
	default:
		s.Logger.Error().Logf("unknown sampler type %T. Exiting.", c)
		os.Exit(1)
	}

	err = sampler.Start()
	if err != nil {
		s.Logger.Debug().WithField("dataset", dataset).Logf("failed to start sampler")
		return nil
	}

	s.Logger.Debug().WithField("dataset", dataset).Logf("created implementation for sampler type %T", c)

	return sampler
}

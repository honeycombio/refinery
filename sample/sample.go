package sample

import (
	"os"

	"github.com/honeycombio/samproxy/config"
	"github.com/honeycombio/samproxy/logger"
	"github.com/honeycombio/samproxy/metrics"
	"github.com/honeycombio/samproxy/types"
)

type Sampler interface {
	GetSampleRate(trace *types.Trace) (rate uint, keep bool)
}

// SamplerFactory is used to create new samplers with common (injected) resources
type SamplerFactory struct {
	Config  config.Config   `inject:""`
	Logger  logger.Logger   `inject:""`
	Metrics metrics.Metrics `inject:""`
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
		s.Logger.Debug().WithField("dataset", dataset).Logf("creating deterministic sampler implementation")
		ds := &DeterministicSampler{Config: c, Logger: s.Logger}
		ds.Start()
		sampler = ds
	case *config.DynamicSamplerConfig:
		s.Logger.Debug().WithField("dataset", dataset).Logf("creating dynamic sampler implementation")
		ds := &DynamicSampler{Config: c, Logger: s.Logger, Metrics: s.Metrics}
		ds.Start()
		sampler = ds
	case *config.EMADynamicSamplerConfig:
		s.Logger.Debug().WithField("dataset", dataset).Logf("creating EMA dynamic sampler implementation")
		ds := &EMADynamicSampler{Config: c, Logger: s.Logger, Metrics: s.Metrics}
		ds.Start()
		sampler = ds
	case *config.RulesBasedSamplerConfig:
		s.Logger.Debug().WithField("dataset", dataset).Logf("creating rules based sampler implementation")
		ds := &RulesBasedSampler{Config: c, Logger: s.Logger, Metrics: s.Metrics}
		ds.Start()
		sampler = ds
	default:
		s.Logger.Error().Logf("unknown sampler type %T. Exiting.", c)
		os.Exit(1)
	}

	return sampler
}

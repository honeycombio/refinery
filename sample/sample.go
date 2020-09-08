package sample

import (
	"os"

	"github.com/honeycombio/samproxy/config"
	"github.com/honeycombio/samproxy/logger"
	"github.com/honeycombio/samproxy/metrics"
	"github.com/honeycombio/samproxy/types"
)

const defaultConfigName = "_default"

type Sampler interface {
	GetSampleRate(trace *types.Trace) (rate uint, keep bool)
}

// SamplerFactory is used to create new samplers with common (injected) resources
type SamplerFactory struct {
	Config  config.Config   `inject:""`
	Logger  logger.Logger   `inject:""`
	Metrics metrics.Metrics `inject:""`
}

// GetDefaultSamplerImplementation returns the default sampler implementation
// or exits fatally if not defined
func (s *SamplerFactory) GetDefaultSamplerImplementation() Sampler {
	samplerType, err := s.Config.GetDefaultSamplerType()
	if err != nil {
		s.Logger.Error().WithField("error", err).Logf("unable to get default sampler type from config")
		os.Exit(1)
	}
	s.Logger.Debug().Logf("creating default sampler implementation")
	return s.getSamplerForType(samplerType, defaultConfigName)
}

// GetSamplerImplementationForDataset returns the sampler implementation for the dataset,
// or nil if it is not defined
func (s *SamplerFactory) GetSamplerImplementationForDataset(dataset string) Sampler {
	samplerType, err := s.Config.GetSamplerTypeForDataset(dataset)
	if err != nil {
		return nil
	}
	s.Logger.Debug().WithField("dataset", dataset).Logf("creating sampler implementation")
	return s.getSamplerForType(samplerType, dataset)
}

func (s *SamplerFactory) getSamplerForType(samplerType, configName string) Sampler {
	var sampler Sampler
	switch samplerType {
	case "DeterministicSampler":
		ds := &DeterministicSampler{configName: configName, Config: s.Config, Logger: s.Logger}
		ds.Start()
		sampler = ds
	case "DynamicSampler":
		ds := &DynamicSampler{configName: configName, Config: s.Config, Logger: s.Logger, Metrics: s.Metrics}
		ds.Start()
		sampler = ds
	case "EMADynamicSampler":
		ds := &EMADynamicSampler{configName: configName, Config: s.Config, Logger: s.Logger, Metrics: s.Metrics}
		ds.Start()
		sampler = ds
	default:
		s.Logger.Error().Logf("unknown sampler type %s. Exiting.", samplerType)
		os.Exit(1)
	}

	return sampler
}

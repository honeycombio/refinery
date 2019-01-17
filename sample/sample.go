package sample

import (
	"fmt"
	"os"

	"github.com/honeycombio/samproxy/config"
	"github.com/honeycombio/samproxy/types"
)

const defaultConfigName = "_default"

type Sampler interface {
	GetSampleRate(trace *types.Trace) (rate uint, keep bool)
}

// GetDefaultSamplerImplementation returns the default sampler implementation
// or exits fatally if not defined
func GetDefaultSamplerImplementation(c config.Config) Sampler {
	samplerType, err := c.GetDefaultSamplerType()
	if err != nil {
		fmt.Printf("unable to get default sampler type from config: %v\n", err)
		os.Exit(1)
	}

	return getSamplerForType(samplerType, defaultConfigName)
}

// GetSamplerImplementationForDataset returns the sampler implementation for the dataset,
// or nil if it is not defined
func GetSamplerImplementationForDataset(c config.Config, dataset string) Sampler {
	samplerType, err := c.GetSamplerTypeForDataset(dataset)
	if err != nil {
		return nil
	}
	return getSamplerForType(samplerType, dataset)
}

func getSamplerForType(samplerType, configName string) Sampler {
	var sampler Sampler
	switch samplerType {
	case "DeterministicSampler":
		sampler = &DeterministicSampler{configName: configName}
	case "DynamicSampler":
		sampler = &DynamicSampler{configName: configName}
	default:
		fmt.Printf("unknown sampler type %s. Exiting.\n", samplerType)
		os.Exit(1)
	}
	return sampler
}

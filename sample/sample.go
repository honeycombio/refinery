package sample

import (
	"fmt"
	"os"

	"github.com/honeycombio/samproxy/config"
	"github.com/honeycombio/samproxy/types"
)

type Sampler interface {
	GetSampleRate(trace *types.Trace) (rate uint, keep bool)
}

func GetSamplerImplementation(c config.Config) Sampler {
	var sampler Sampler
	samplerType, err := c.GetSamplerType()
	if err != nil {
		fmt.Printf("unable to get sampler type from config: %v\n", err)
		os.Exit(1)
	}
	switch samplerType {
	case "DeterministicSampler":
		sampler = &DeterministicSampler{}
	case "DynamicSampler":
		sampler = &DynamicSampler{}
	default:
		fmt.Printf("unknown sampler type %s. Exiting.\n", samplerType)
		os.Exit(1)
	}
	return sampler
}

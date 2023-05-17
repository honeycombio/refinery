package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/creasty/defaults"
	"github.com/honeycombio/refinery/config"
	"gopkg.in/yaml.v3"
)

func _keysToLowercase(m map[string]any) map[string]any {
	newmap := make(map[string]any)
	for k, v := range m {
		switch val := v.(type) {
		case map[string]any:
			v = _keysToLowercase(val)
		}
		newmap[strings.ToLower(k)] = v
	}
	return newmap
}

func readV1RulesIntoV2Sampler(samplerType string, rulesmap map[string]any) (*config.V2SamplerChoice, string, error) {
	// construct a sampler of the appropriate type that we can treat as "any" for unmarshalling
	var sampler any
	switch samplerType {
	case "DeterministicSampler":
		sampler = &config.DeterministicSamplerConfig{}
	case "DynamicSampler":
		sampler = &config.DynamicSamplerConfig{}
	case "EMADynamicSampler":
		sampler = &config.EMADynamicSamplerConfig{}
	case "RulesBasedSampler":
		sampler = &config.RulesBasedSamplerConfig{}
	case "TotalThroughputSampler":
		sampler = &config.TotalThroughputSamplerConfig{}
	default:
		return nil, "not found", errors.New("no sampler found")
	}

	// We use a little trick here -- we have read the rules into a generic map.
	// First we convert the generic map into all lowercase keys, then marshal
	// them into a bytestream using JSON, then finally unmarshal them into their
	// final form. This lets us use the JSON tags to do the mapping of old field
	// names onto new names, while we use the YAML tags to render the new names
	// in the final output. So it's real important to have both tags on any
	// field that gets renamed!

	// convert all the keys to lowercase
	lowermap := _keysToLowercase(rulesmap)

	// marshal the rules into a bytestream
	b, err := json.Marshal(lowermap)
	if err != nil {
		return nil, "", fmt.Errorf("getV1RulesForSampler unable to marshal config: %w", err)
	}

	// and unmarshal them back into the sampler
	err = json.Unmarshal(b, sampler)
	if err != nil {
		return nil, "", fmt.Errorf("getV1RulesForSampler unable to unmarshal config: %w", err)
	}
	// now we've got the config, apply defaults to zero values
	if err := defaults.Set(sampler); err != nil {
		return nil, "", fmt.Errorf("getV1RulesForSampler unable to apply defaults: %w", err)
	}

	// and now put it into the V2 sampler config
	newSampler := &config.V2SamplerChoice{}
	switch samplerType {
	case "DeterministicSampler":
		newSampler.DeterministicSampler = sampler.(*config.DeterministicSamplerConfig)
	case "DynamicSampler":
		newSampler.DynamicSampler = sampler.(*config.DynamicSamplerConfig)
	case "EMADynamicSampler":
		newSampler.EMADynamicSampler = sampler.(*config.EMADynamicSamplerConfig)
	case "RulesBasedSampler":
		newSampler.RulesBasedSampler = sampler.(*config.RulesBasedSamplerConfig)
	case "TotalThroughputSampler":
		newSampler.TotalThroughputSampler = sampler.(*config.TotalThroughputSamplerConfig)
	}

	return newSampler, samplerType, nil
}

func ConvertRules(rules map[string]any, w io.Writer) {
	// this writes the rules to w as a YAML file for debugging
	// yaml.NewEncoder(w).Encode(rules)

	// get the sampler type for the default rule
	defaultSamplerType, _ := config.GetValueForCaseInsensitiveKey(rules, "sampler", "DeterministicSampler")

	newConfig := &config.V2SamplerConfig{
		ConfigVersion: 2,
		Samplers:      make(map[string]*config.V2SamplerChoice),
	}
	sampler, _, err := readV1RulesIntoV2Sampler(defaultSamplerType, rules)
	if err != nil {
		panic(err)
	}

	newConfig.Samplers["__default__"] = sampler

	for k, v := range rules {
		// if it's not a map, skip it
		if _, ok := v.(map[string]any); !ok {
			continue
		}
		sub := v.(map[string]any)

		// make sure it's a sampler destination key by checking for the presence
		// of a "sampler" key or a "samplerate" key; having either one is good
		// enough
		if _, ok := config.GetValueForCaseInsensitiveKey(sub, "sampler", ""); !ok {
			if _, ok := config.GetValueForCaseInsensitiveKey(sub, "samplerate", ""); !ok {
				continue
			}
		}

		// get the sampler type for the rule
		samplerType, _ := config.GetValueForCaseInsensitiveKey(sub, "sampler", "DeterministicSampler")
		sampler, _, err := readV1RulesIntoV2Sampler(samplerType, sub)
		if err != nil {
			panic(err)
		}

		newConfig.Samplers[k] = sampler
	}

	yaml.NewEncoder(w).Encode(newConfig)
}

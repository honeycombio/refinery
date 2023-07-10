package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/creasty/defaults"
	"github.com/honeycombio/refinery/config"
	"gopkg.in/yaml.v3"
)

func transformSamplerMap(m map[string]any) map[string]any {
	newmap := make(map[string]any)
	for k, v := range m {
		switch val := v.(type) {
		case map[string]any:
			v = transformSamplerMap(val)
		case []any:
			for i, x := range val {
				if m, ok := x.(map[string]any); ok {
					m = transformSamplerMap(m)
					val[i] = m
				}
			}
			v = val
		}

		k = strings.ToLower(k)

		// there are some fields that are named differently in v2 than in v1
		// and some data types that are different
		// this fixes those up
		switch k {
		case "clearfrequencysec":
			k = "clearfrequency"
			v = config.Duration(v.(int64)) * config.Duration(time.Second)
		case "adjustmentinterval":
			if _, ok := v.(config.Duration); !ok {
				var i64 int64
				switch i := v.(type) {
				case int64:
					i64 = i
				case int:
					i64 = int64(i)
				}
				v = config.Duration(i64) * config.Duration(time.Second)
			}
		}
		newmap[k] = v
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
	// First we convert the generic map into all lowercase keys (and do some
	// data cleanup on the way by), then marshal them into a bytestream using
	// JSON, then finally unmarshal them into their final form. This lets us use
	// the JSON tags to do the mapping of old field names onto new names, while
	// we use the YAML tags to render the new names in the final output. So it's
	// real important to have both tags on any field that gets renamed!

	// convert all the keys to lowercase
	lowermap := transformSamplerMap(rulesmap)

	// marshal the rules into a JSON bytestream that has the right shape to unmarshal into the sampler
	b, err := json.Marshal(lowermap)
	if err != nil {
		return nil, "", fmt.Errorf("readV1RulesIntoV2Sampler unable to marshal config: %w", err)
	}

	// and unmarshal them back into the sampler
	err = json.Unmarshal(b, sampler)
	if err != nil {
		return nil, "", fmt.Errorf("readV1RulesIntoV2Sampler unable to unmarshal config: %w", err)
	}
	// now we've got the config, apply defaults to zero values
	if err := defaults.Set(sampler); err != nil {
		return nil, "", fmt.Errorf("readV1RulesIntoV2Sampler unable to apply defaults: %w", err)
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

// getValueForCaseInsensitiveKey is a generic helper function that returns the
// value from a map[string]any for the given key, ignoring case of the key. It
// returns ok=true only if the key was found and could be converted to the
// required type. Otherwise it returns the default value and ok=false.
func getValueForCaseInsensitiveKey[T any](m map[string]any, key string, def T) (T, bool) {
	for k, v := range m {
		if strings.EqualFold(k, key) {
			if t, ok := v.(T); ok {
				return t, true
			}
		}
	}
	return def, false
}

func CheckConfigForSampleRateChanges(cfg *config.V2SamplerConfig) map[string][]string {
	warnings := make(map[string][]string)
	for name, v := range cfg.Samplers {
		meaningfuls := v.NameMeaningfulSamplers()
		if len(meaningfuls) > 0 {
			warnings[name] = meaningfuls
		}
	}
	return warnings
}

func convertRulesToNewConfig(rules map[string]any) *config.V2SamplerConfig {
	// get the sampler type for the default rule
	defaultSamplerType, _ := getValueForCaseInsensitiveKey(rules, "sampler", "DeterministicSampler")

	newConfig := &config.V2SamplerConfig{
		RulesVersion: 2,
		Samplers:     make(map[string]*config.V2SamplerChoice),
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
		if _, ok := getValueForCaseInsensitiveKey(sub, "sampler", ""); !ok {
			if _, ok := getValueForCaseInsensitiveKey(sub, "samplerate", ""); !ok {
				continue
			}
		}

		// get the sampler type for the rule
		samplerType, _ := getValueForCaseInsensitiveKey(sub, "sampler", "DeterministicSampler")
		sampler, _, err := readV1RulesIntoV2Sampler(samplerType, sub)
		if err != nil {
			panic(err)
		}

		newConfig.Samplers[k] = sampler
	}
	return newConfig
}

func ConvertRules(rules map[string]any, w io.Writer) {
	newConfig := convertRulesToNewConfig(rules)
	w.Write([]byte(fmt.Sprintf("# Automatically generated on %s\n", time.Now().Format(time.RFC3339))))
	yaml.NewEncoder(w).Encode(newConfig)

	warningText := `
WARNING: Version 2 of Refinery has changed the way that sample rates are calculated for
the dynamic and throughput samplers. Refinery v1.x was documented as counting the number
of spans, but in fact it was counting traces. Version 2 samplers correctly count the
number of spans when doing these calculations.

This means that you may need to adjust the target throughput or sample rates to get the
same behavior as before.

When using v1, if you have had difficulty reaching your target throughput or sample rates,
or if different parts of your key space vary widely in their span counts, then there is a
good chance you should lower the target values in v2.

The following parts of your configuration should be checked:`

	thingsToWarnAbout := CheckConfigForSampleRateChanges(newConfig)
	if len(thingsToWarnAbout) > 0 {
		fmt.Println(warningText)
		for k, v := range thingsToWarnAbout {
			for _, s := range v {
				fmt.Printf(" *  %s: %s\n", k, s)
			}
		}
	}
}

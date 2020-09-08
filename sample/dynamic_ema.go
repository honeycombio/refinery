package sample

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"

	dynsampler "github.com/honeycombio/dynsampler-go"

	"github.com/honeycombio/samproxy/config"
	"github.com/honeycombio/samproxy/logger"
	"github.com/honeycombio/samproxy/metrics"
	"github.com/honeycombio/samproxy/types"
)

type EMADynamicSampler struct {
	Config  config.Config
	Logger  logger.Logger
	Metrics metrics.Metrics

	goalSampleRate      int
	adjustmentInterval  int
	weight              float64
	ageOutValue         float64
	burstMultiple       float64
	burstDetectionDelay uint
	maxKeys             int
	fieldList           []string
	useTraceLength      bool
	addDynsampleKey     bool
	addDynsampleField   string
	configName          string

	dynsampler dynsampler.Sampler
}

type EMADynSamplerConfig struct {
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

func (d *EMADynamicSampler) Start() error {
	d.Logger.Debug().Logf("Starting EMADynamicSampler")
	defer func() { d.Logger.Debug().Logf("Finished starting EMADynamicSampler") }()
	dsConfig := EMADynSamplerConfig{}
	configKey := fmt.Sprintf("SamplerConfig.%s", d.configName)
	err := d.Config.GetOtherConfig(configKey, &dsConfig)
	if err != nil {
		return err
	}
	if dsConfig.GoalSampleRate < 1 {
		d.Logger.Debug().Logf("configured sample rate for dynamic sampler was %d; forcing to 1", dsConfig.GoalSampleRate)
		dsConfig.GoalSampleRate = 1
	}
	d.goalSampleRate = dsConfig.GoalSampleRate
	d.adjustmentInterval = dsConfig.AdjustmentInterval
	d.weight = dsConfig.Weight
	d.ageOutValue = dsConfig.AgeOutValue
	d.burstMultiple = dsConfig.BurstMultiple
	d.burstDetectionDelay = dsConfig.BurstDetectionDelay
	d.maxKeys = dsConfig.MaxKeys

	// get list of fields to use when constructing the dynsampler key
	fieldList := dsConfig.FieldList

	// always put the field list in sorted order for easier comparison
	sort.Strings(fieldList)
	d.fieldList = fieldList

	d.useTraceLength = dsConfig.UseTraceLength

	d.addDynsampleKey = dsConfig.AddSampleRateKeyToTrace
	d.addDynsampleField = dsConfig.AddSampleRateKeyToTraceField

	// spin up the actual dynamic sampler
	d.dynsampler = &dynsampler.EMASampleRate{
		GoalSampleRate:      d.goalSampleRate,
		AdjustmentInterval:  d.adjustmentInterval,
		Weight:              d.weight,
		AgeOutValue:         d.ageOutValue,
		BurstDetectionDelay: d.burstDetectionDelay,
		BurstMultiple:       d.burstMultiple,
		MaxKeys:             d.maxKeys,
	}
	d.dynsampler.Start()

	// Register stastics this package will produce
	d.Metrics.Register("dynsampler_num_dropped", "counter")
	d.Metrics.Register("dynsampler_num_kept", "counter")
	d.Metrics.Register("dynsampler_sample_rate", "histogram")

	// listen for config reloads
	d.Config.RegisterReloadCallback(d.reloadConfigs)
	return nil
}

func (d *EMADynamicSampler) reloadConfigs() {
	d.Logger.Debug().Logf("reloading config for dynamic sampler")
	// only actually reload the dynsampler if the config changed.
	var configChanged bool

	dsConfig := EMADynSamplerConfig{}
	configKey := fmt.Sprintf("SamplerConfig.%s", d.configName)
	err := d.Config.GetOtherConfig(configKey, &dsConfig)
	if err != nil {
		d.Logger.Error().Logf("Failed to get dynsampler settings when reloading configs:", err)
	}
	if dsConfig.GoalSampleRate < 1 {
		d.Logger.Debug().Logf("configured sample rate for dynamic sampler was %d; forcing to 1", dsConfig.GoalSampleRate)
		dsConfig.GoalSampleRate = 1
	}
	if d.goalSampleRate != dsConfig.GoalSampleRate {
		configChanged = true
		d.goalSampleRate = dsConfig.GoalSampleRate
	}
	if d.adjustmentInterval != dsConfig.AdjustmentInterval {
		configChanged = true
		d.adjustmentInterval = dsConfig.AdjustmentInterval
	}
	if d.weight != dsConfig.Weight {
		configChanged = true
		d.weight = dsConfig.Weight
	}
	if d.maxKeys != dsConfig.MaxKeys {
		configChanged = true
		d.maxKeys = dsConfig.MaxKeys
	}
	if d.burstMultiple != dsConfig.BurstMultiple {
		configChanged = true
		d.burstMultiple = dsConfig.BurstMultiple
	}
	if d.burstDetectionDelay != dsConfig.BurstDetectionDelay {
		configChanged = true
		d.burstDetectionDelay = dsConfig.BurstDetectionDelay
	}
	if d.ageOutValue != dsConfig.AgeOutValue {
		configChanged = true
		d.ageOutValue = dsConfig.AgeOutValue
	}

	// get list of fields to use when constructing the dynsampler key
	fieldList := dsConfig.FieldList
	sort.Strings(fieldList)
	// find out if the field list changed by checking that length is the same
	// and if it is that the sorted list of fields are the same
	if len(d.fieldList) != len(fieldList) {
		configChanged = true
		d.fieldList = fieldList
	} else {
		for i, field := range fieldList {
			if d.fieldList[i] != field {
				configChanged = true
				d.fieldList = fieldList
				break
			}
		}
	}

	if d.useTraceLength != dsConfig.UseTraceLength {
		configChanged = true
		d.useTraceLength = dsConfig.UseTraceLength
	}

	if d.addDynsampleKey != dsConfig.AddSampleRateKeyToTrace {
		configChanged = true
		d.addDynsampleKey = dsConfig.AddSampleRateKeyToTrace
	}
	if d.addDynsampleField != dsConfig.AddSampleRateKeyToTraceField {
		configChanged = true
		d.addDynsampleField = dsConfig.AddSampleRateKeyToTraceField
	}

	if configChanged {
		newSampler := &dynsampler.EMASampleRate{
			GoalSampleRate:      d.goalSampleRate,
			AdjustmentInterval:  d.adjustmentInterval,
			Weight:              d.weight,
			AgeOutValue:         d.ageOutValue,
			BurstDetectionDelay: d.burstDetectionDelay,
			BurstMultiple:       d.burstMultiple,
			MaxKeys:             d.maxKeys,
		}
		newSampler.Start()

		d.Logger.Debug().Logf("reloaded dynsampler configs with values %+v", dsConfig)

		d.dynsampler = newSampler
	} else {
		d.Logger.Debug().Logf("skipping dynsampler reload because the config of %+v is unchanged from the previous state", dsConfig)
	}
}

func (d *EMADynamicSampler) GetSampleRate(trace *types.Trace) (uint, bool) {
	key := d.buildKey(trace)
	rate := d.dynsampler.GetSampleRate(key)
	if rate < 1 { // protect against dynsampler being broken even though it shouldn't be
		rate = 1
	}
	shouldKeep := rand.Intn(int(rate)) == 0
	d.Logger.Debug().WithFields(map[string]interface{}{
		"sample_key":  key,
		"sample_rate": rate,
		"sample_keep": shouldKeep,
		"trace_id":    trace.TraceID,
	}).Logf("got sample rate and decision")
	if shouldKeep {
		d.Metrics.IncrementCounter("dynsampler_num_kept")
	} else {
		d.Metrics.IncrementCounter("dynsampler_num_dropped")
	}
	d.Metrics.Histogram("dynsampler_sample_rate", float64(rate))
	return uint(rate), shouldKeep
}

// buildKey takes a trace and returns the key to use for the dynsampler.
func (d *EMADynamicSampler) buildKey(trace *types.Trace) string {
	// fieldCollector gets all values from the fields listed in the config, even
	// if they happen multiple times.
	fieldCollector := map[string][]string{}

	// for each field, for each span, get the value of that field
	spans := trace.GetSpans()
	for _, field := range d.fieldList {
		for _, span := range spans {
			if val, ok := span.Data[field]; ok {
				switch val := val.(type) {
				case string:
					fieldCollector[field] = append(fieldCollector[field], val)
				case float64:
					valStr := strconv.FormatFloat(val, 'f', -1, 64)
					fieldCollector[field] = append(fieldCollector[field], valStr)
				case bool:
					valStr := strconv.FormatBool(val)
					fieldCollector[field] = append(fieldCollector[field], valStr)
				}
			}
		}
	}
	// ok, now we have a map of fields to a list of all values for that field.

	var key string
	for _, field := range d.fieldList {
		// sort and collapse list
		sort.Strings(fieldCollector[field])
		var prevStr string
		for _, str := range fieldCollector[field] {
			if str != prevStr {
				key += str + "•"
			}
			prevStr = str
		}
		// get ready for the next element
		key += ","
	}
	if d.useTraceLength {
		key += strconv.FormatInt(int64(len(spans)), 10)
	}

	if d.addDynsampleKey {
		for _, span := range trace.GetSpans() {
			span.Data[d.addDynsampleField] = key
		}
	}

	return key
}

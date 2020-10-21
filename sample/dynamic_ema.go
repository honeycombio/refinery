package sample

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"

	dynsampler "github.com/honeycombio/dynsampler-go"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/types"
)

type EMADynamicSampler struct {
	Config  *config.EMADynamicSamplerConfig
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

func (d *EMADynamicSampler) Start() error {
	d.Logger.Debug().Logf("Starting EMADynamicSampler")
	defer func() { d.Logger.Debug().Logf("Finished starting EMADynamicSampler") }()
	d.goalSampleRate = d.Config.GoalSampleRate
	d.adjustmentInterval = d.Config.AdjustmentInterval
	d.weight = d.Config.Weight
	d.ageOutValue = d.Config.AgeOutValue
	d.burstMultiple = d.Config.BurstMultiple
	d.burstDetectionDelay = d.Config.BurstDetectionDelay
	d.maxKeys = d.Config.MaxKeys

	// get list of fields to use when constructing the dynsampler key
	fieldList := d.Config.FieldList

	// always put the field list in sorted order for easier comparison
	sort.Strings(fieldList)
	d.fieldList = fieldList

	d.useTraceLength = d.Config.UseTraceLength

	d.addDynsampleKey = d.Config.AddSampleRateKeyToTrace
	d.addDynsampleField = d.Config.AddSampleRateKeyToTraceField

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

	return nil
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
				fieldCollector[field] = append(fieldCollector[field], fmt.Sprintf("%v", val))
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

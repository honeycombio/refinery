package sample

import (
	"math/rand"
	"sort"
	"strconv"

	dynsampler "github.com/honeycombio/dynsampler-go"

	"github.com/honeycombio/samproxy/config"
	"github.com/honeycombio/samproxy/logger"
	"github.com/honeycombio/samproxy/metrics"
	"github.com/honeycombio/samproxy/types"
)

type DynamicSampler struct {
	Config  *config.DynamicSamplerConfig
	Logger  logger.Logger
	Metrics metrics.Metrics

	sampleRate        int64
	clearFrequencySec int64
	fieldList         []string
	useTraceLength    bool
	addDynsampleKey   bool
	addDynsampleField string
	configName        string

	dynsampler dynsampler.Sampler
}

func (d *DynamicSampler) Start() error {
	d.Logger.Debug().Logf("Starting DynamicSampler")
	defer func() { d.Logger.Debug().Logf("Finished starting DynamicSampler") }()
	d.sampleRate = d.Config.SampleRate
	if d.Config.ClearFrequencySec == 0 {
		d.Config.ClearFrequencySec = 30
	}
	d.clearFrequencySec = d.Config.ClearFrequencySec

	// get list of fields to use when constructing the dynsampler key
	fieldList := d.Config.FieldList

	// always put the field list in sorted order for easier comparison
	sort.Strings(fieldList)
	d.fieldList = fieldList

	d.useTraceLength = d.Config.UseTraceLength

	d.addDynsampleKey = d.Config.AddSampleRateKeyToTrace
	d.addDynsampleField = d.Config.AddSampleRateKeyToTraceField

	// spin up the actual dynamic sampler
	d.dynsampler = &dynsampler.AvgSampleRate{
		GoalSampleRate:    int(d.sampleRate),
		ClearFrequencySec: int(d.clearFrequencySec),
	}
	d.dynsampler.Start()

	// Register stastics this package will produce
	d.Metrics.Register("dynsampler_num_dropped", "counter")
	d.Metrics.Register("dynsampler_num_kept", "counter")
	d.Metrics.Register("dynsampler_sample_rate", "histogram")

	return nil
}

func (d *DynamicSampler) GetSampleRate(trace *types.Trace) (uint, bool) {
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
func (d *DynamicSampler) buildKey(trace *types.Trace) string {
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

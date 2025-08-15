package sample

import (
	"math/rand"
	"sync"
	"time"

	dynsampler "github.com/honeycombio/dynsampler-go"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/types"
)

type WindowedThroughputSampler struct {
	Config  *config.WindowedThroughputSamplerConfig
	Logger  logger.Logger
	Metrics metrics.Metrics

	updatefrequency      config.Duration
	lookbackfrequency    config.Duration
	goalThroughputPerSec int
	clusterSize          int
	useClusterSize       bool
	maxKeys              int
	prefix               string

	key                      *traceKey
	keyMu                    sync.Mutex
	clusterMu                sync.Mutex
	dynsamplerMu             sync.Mutex
	keyFields, nonRootFields []string

	dynsampler      *dynsampler.WindowedThroughput
	metricsRecorder *dynsamplerMetricsRecorder
}

func (d *WindowedThroughputSampler) Start() error {
	d.Logger.Debug().Logf("Starting WindowedThroughputSampler")
	defer func() { d.Logger.Debug().Logf("Finished starting WindowedThroughputSampler") }()
	d.goalThroughputPerSec = d.Config.GoalThroughputPerSec
	d.useClusterSize = d.Config.UseClusterSize
	if d.clusterSize == 0 {
		d.clusterSize = 1
	}
	d.updatefrequency = d.Config.UpdateFrequency
	d.lookbackfrequency = d.Config.LookbackFrequency
	d.key = newTraceKey(d.Config.FieldList, d.Config.UseTraceLength)
	d.maxKeys = d.Config.MaxKeys
	if d.maxKeys == 0 {
		d.maxKeys = 500
	}
	d.prefix = "windowedthroughput"
	d.keyFields, d.nonRootFields = config.GetKeyFields(d.Config.GetSamplingFields())

	// spin up the actual dynamic sampler
	d.dynsampler = &dynsampler.WindowedThroughput{
		GoalThroughputPerSec:      float64(d.goalThroughputPerSec) / float64(d.clusterSize),
		UpdateFrequencyDuration:   time.Duration(d.updatefrequency),
		LookbackFrequencyDuration: time.Duration(d.lookbackfrequency),
		MaxKeys:                   d.maxKeys,
	}
	d.dynsampler.Start()

	// Register statistics this package will produce
	d.metricsRecorder = &dynsamplerMetricsRecorder{
		prefix: d.prefix,
		met:    d.Metrics,
	}
	d.metricsRecorder.RegisterMetrics(d.dynsampler)
	return nil
}

func (d *WindowedThroughputSampler) Stop() {
	d.dynsampler.Stop()
}

func (d *WindowedThroughputSampler) SetClusterSize(size int) {
	if d.useClusterSize {
		d.clusterMu.Lock()
		d.dynsamplerMu.Lock()
		d.clusterSize = size
		d.dynsampler.GoalThroughputPerSec = float64(d.goalThroughputPerSec) / float64(size)
		d.dynsamplerMu.Unlock()
		d.clusterMu.Unlock()
	}
}

func (d *WindowedThroughputSampler) GetSampleRate(trace *types.Trace) (rate uint, keep bool, reason string, key string) {
	d.keyMu.Lock()
	key, n := d.key.build(trace)
	d.keyMu.Unlock()

	if n == maxKeyLength {
		d.Logger.Debug().Logf("trace key hit max length of %d, truncating", maxKeyLength)
	}
	count := int(trace.DescendantCount())
	d.dynsamplerMu.Lock()
	rate = uint(d.dynsampler.GetSampleRateMulti(key, count))
	d.dynsamplerMu.Unlock()
	if rate < 1 { // protect against dynsampler being broken even though it shouldn't be
		rate = 1
	}
	shouldKeep := rand.Intn(int(rate)) == 0
	d.Logger.Debug().WithFields(map[string]interface{}{
		"sample_key":  key,
		"sample_rate": rate,
		"sample_keep": shouldKeep,
		"trace_id":    trace.TraceID,
		"span_count":  count,
	}).Logf("got sample rate and decision")
	d.dynsamplerMu.Lock()
	d.metricsRecorder.RecordMetrics(d.dynsampler, shouldKeep, rate, n)
	d.dynsamplerMu.Unlock()

	return rate, shouldKeep, d.prefix, key
}

func (d *WindowedThroughputSampler) GetKeyFields() ([]string, []string) {
	return d.keyFields, d.nonRootFields
}

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

type TotalThroughputSampler struct {
	Config  *config.TotalThroughputSamplerConfig
	Logger  logger.Logger
	Metrics metrics.Metrics

	goalThroughputPerSec int
	clusterSize          int
	useClusterSize       bool
	clearFrequency       config.Duration
	maxKeys              int
	prefix               string

	key                      *traceKey
	keyMu                    sync.Mutex
	clusterMu                sync.Mutex
	dynsamplerMu             sync.Mutex
	keyFields, nonRootFields []string

	dynsampler      *dynsampler.TotalThroughput
	metricsRecorder *dynsamplerMetricsRecorder
}

func (d *TotalThroughputSampler) Start() error {
	d.Logger.Debug().Logf("Starting TotalThroughputSampler")
	defer func() { d.Logger.Debug().Logf("Finished starting TotalThroughputSampler") }()
	if d.Config.GoalThroughputPerSec < 1 {
		d.Logger.Debug().Logf("configured sample rate for dynamic sampler was %d; forcing to 100", d.Config.GoalThroughputPerSec)
		d.Config.GoalThroughputPerSec = 100
	}
	d.goalThroughputPerSec = d.Config.GoalThroughputPerSec
	d.useClusterSize = d.Config.UseClusterSize
	if d.clusterSize == 0 {
		d.clusterSize = 1
	}
	if d.Config.ClearFrequency == 0 {
		d.Config.ClearFrequency = config.Duration(30 * time.Second)
	}
	d.clearFrequency = d.Config.ClearFrequency
	d.key = newTraceKey(d.Config.FieldList, d.Config.UseTraceLength)
	d.maxKeys = d.Config.MaxKeys
	if d.maxKeys == 0 {
		d.maxKeys = 500
	}
	d.prefix = "totalthroughput"
	d.keyFields, d.nonRootFields = config.GetKeyFields(d.Config.GetSamplingFields())

	// spin up the actual dynamic sampler
	d.dynsampler = &dynsampler.TotalThroughput{
		GoalThroughputPerSec:   d.goalThroughputPerSec / d.clusterSize,
		ClearFrequencyDuration: time.Duration(d.clearFrequency),
		MaxKeys:                d.maxKeys,
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

func (d *TotalThroughputSampler) Stop() {
	d.dynsampler.Stop()
}

func (d *TotalThroughputSampler) SetClusterSize(size int) {
	if d.useClusterSize {
		d.clusterMu.Lock()
		d.dynsamplerMu.Lock()
		d.clusterSize = size
		d.dynsampler.GoalThroughputPerSec = d.goalThroughputPerSec / size
		d.dynsamplerMu.Unlock()
		d.clusterMu.Unlock()
	}
}

func (d *TotalThroughputSampler) GetSampleRate(trace *types.Trace) (rate uint, keep bool, reason string, key string) {
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

func (d *TotalThroughputSampler) GetKeyFields() ([]string, []string) {
	return d.keyFields, d.nonRootFields
}

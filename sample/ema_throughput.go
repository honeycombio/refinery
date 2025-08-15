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

type EMAThroughputSampler struct {
	Config  *config.EMAThroughputSamplerConfig
	Logger  logger.Logger
	Metrics metrics.Metrics

	adjustmentInterval   config.Duration
	weight               float64
	initialSampleRate    int
	goalThroughputPerSec int
	clusterSize          int
	useClusterSize       bool
	ageOutValue          float64
	burstMultiple        float64
	burstDetectionDelay  uint
	maxKeys              int
	prefix               string

	key                      *traceKey
	keyMu                    sync.Mutex
	clusterMu                sync.Mutex
	keyFields, nonRootFields []string

	dynsampler      *dynsampler.EMAThroughput
	metricsRecorder *dynsamplerMetricsRecorder
}

func (d *EMAThroughputSampler) Start() error {
	d.Logger.Debug().Logf("Starting EMAThroughputSampler")
	defer func() { d.Logger.Debug().Logf("Finished starting EMAThroughputSampler") }()
	d.initialSampleRate = d.Config.InitialSampleRate
	d.goalThroughputPerSec = d.Config.GoalThroughputPerSec
	d.useClusterSize = d.Config.UseClusterSize
	if d.clusterSize == 0 {
		d.clusterSize = 1
	}
	d.adjustmentInterval = d.Config.AdjustmentInterval
	d.weight = d.Config.Weight
	d.ageOutValue = d.Config.AgeOutValue
	d.burstMultiple = d.Config.BurstMultiple
	d.burstDetectionDelay = d.Config.BurstDetectionDelay
	d.key = newTraceKey(d.Config.FieldList, d.Config.UseTraceLength)
	d.maxKeys = d.Config.MaxKeys
	if d.maxKeys == 0 {
		d.maxKeys = 500
	}
	d.prefix = "emathroughput"

	d.keyFields, d.nonRootFields = config.GetKeyFields(d.Config.GetSamplingFields())
	// spin up the actual dynamic sampler
	d.dynsampler = &dynsampler.EMAThroughput{
		GoalThroughputPerSec: d.goalThroughputPerSec / d.clusterSize,
		InitialSampleRate:    d.initialSampleRate,
		AdjustmentInterval:   time.Duration(d.adjustmentInterval),
		Weight:               d.weight,
		AgeOutValue:          d.ageOutValue,
		BurstDetectionDelay:  d.burstDetectionDelay,
		BurstMultiple:        d.burstMultiple,
		MaxKeys:              d.maxKeys,
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

func (d *EMAThroughputSampler) Stop() {
	d.dynsampler.Stop()
}

func (d *EMAThroughputSampler) SetClusterSize(size int) {
	if d.useClusterSize {
		d.clusterMu.Lock()
		d.clusterSize = size
		d.dynsampler.GoalThroughputPerSec = d.goalThroughputPerSec / size
		d.clusterMu.Unlock()
	}
}

func (d *EMAThroughputSampler) GetSampleRate(trace *types.Trace) (rate uint, keep bool, reason string, key string) {
	d.keyMu.Lock()
	key, n := d.key.build(trace)
	d.keyMu.Unlock()
	
	if n == maxKeyLength {
		d.Logger.Debug().Logf("trace key hit max length of %d, truncating", maxKeyLength)
	}
	count := int(trace.DescendantCount())
	rate = uint(d.dynsampler.GetSampleRateMulti(key, count))
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
	d.metricsRecorder.RecordMetrics(d.dynsampler, shouldKeep, rate, n)
	return rate, shouldKeep, d.prefix, key
}

func (d *EMAThroughputSampler) GetKeyFields() ([]string, []string) {
	return d.keyFields, d.nonRootFields
}

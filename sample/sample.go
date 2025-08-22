package sample

import (
	"fmt"
	"os"
	"strings"
	"sync"

	dynsampler "github.com/honeycombio/dynsampler-go"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/peer"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/types"
)

type Sampler interface {
	GetSampleRate(trace *types.Trace) (rate uint, keep bool, reason string, key string)
	GetKeyFields() ([]string, []string)
	Start() error
}

type CanSetGoalThroughputPerSec interface {
	SetGoalThroughputPerSec(int)
}

// SamplerFactory is used to create new samplers with common (injected) resources
type SamplerFactory struct {
	Config    config.Config   `inject:""`
	Logger    logger.Logger   `inject:""`
	Metrics   metrics.Metrics `inject:"metrics"`
	Peers     peer.Peers      `inject:""`
	peerCount int
	mutex     sync.Mutex

	// Shared dynsampler instances to maintain global throughput tracking
	sharedDynsamplers map[string]any

	// Store original GoalThroughputPerSec values for cluster size calculations.
	// We need this to recalculate goal throughput values when the cluster size
	// changes, which does not trigger a full config reload.
	goalThroughputConfigs map[string]int
}

func (s *SamplerFactory) updatePeerCounts() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.Peers != nil {
		peers, err := s.Peers.GetPeers()
		// Only update the stored count if there were no errors
		if err == nil && len(peers) > 0 {
			s.peerCount = len(peers)
		}
	}

	// Update goal throughput for all throughput-based dynsamplers
	for dynsamplerKey, dynsamplerInstance := range s.sharedDynsamplers {
		if hasThroughput, ok := dynsamplerInstance.(CanSetGoalThroughputPerSec); ok {
			// Calculate new throughput based on cluster size
			newThroughput := max(s.goalThroughputConfigs[dynsamplerKey]/s.peerCount, 1)
			hasThroughput.SetGoalThroughputPerSec(newThroughput)
		}
	}
}

func (s *SamplerFactory) Start() error {
	s.peerCount = 1
	s.sharedDynsamplers = make(map[string]any)
	s.goalThroughputConfigs = make(map[string]int)
	if s.Peers != nil {
		s.Peers.RegisterUpdatedPeersCallback(s.updatePeerCounts)
	}
	return nil
}

func getSharedDynsampler[ST any, CT any](
	s *SamplerFactory,
	dynsamplerKey string,
	config CT,
	create func(config CT) ST,
) ST {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	var ok bool
	var dynsamplerInstance ST
	if dynsamplerInstance, ok = s.sharedDynsamplers[dynsamplerKey].(ST); !ok {
		dynsamplerInstance = create(config)
		s.sharedDynsamplers[dynsamplerKey] = dynsamplerInstance
	}
	return dynsamplerInstance
}

// createSampler creates a sampler with shared dynsamplers based on the config type
func (s *SamplerFactory) createSampler(c any, keyPrefix string) Sampler {
	var sampler Sampler

	switch c := c.(type) {
	case *config.DeterministicSamplerConfig:
		sampler = &DeterministicSampler{Config: c, Logger: s.Logger, Metrics: s.Metrics}
	case *config.DynamicSamplerConfig:
		dynsamplerKey := fmt.Sprintf("%s:dynamic:%d:%v", keyPrefix, c.SampleRate, c.FieldList)
		dynsamplerInstance := getSharedDynsampler(s, dynsamplerKey, c, createDynForDynamicSampler)
		sampler = &DynamicSampler{Config: c, Logger: s.Logger, Metrics: s.Metrics, dynsampler: dynsamplerInstance}
	case *config.EMADynamicSamplerConfig:
		dynsamplerKey := fmt.Sprintf("%s:emadynamic:%d:%v", keyPrefix, c.GoalSampleRate, c.FieldList)
		dynsamplerInstance := getSharedDynsampler(s, dynsamplerKey, c, createDynForEMADynamicSampler)
		sampler = &EMADynamicSampler{Config: c, Logger: s.Logger, Metrics: s.Metrics, dynsampler: dynsamplerInstance}
	case *config.RulesBasedSamplerConfig:
		sampler = &RulesBasedSampler{Config: c, Logger: s.Logger, Metrics: s.Metrics, SamplerFactory: s}
	case *config.TotalThroughputSamplerConfig:
		dynsamplerKey := fmt.Sprintf("%s:totalthroughput:%d:%v", keyPrefix, c.GoalThroughputPerSec, c.FieldList)
		dynsamplerInstance := getSharedDynsampler(s, dynsamplerKey, c, createDynForTotalThroughputSampler)
		// Store goal throughput config under mutex protection
		s.mutex.Lock()
		s.goalThroughputConfigs[dynsamplerKey] = c.GoalThroughputPerSec
		s.mutex.Unlock()
		sampler = &TotalThroughputSampler{Config: c, Logger: s.Logger, Metrics: s.Metrics, dynsampler: dynsamplerInstance}
	case *config.EMAThroughputSamplerConfig:
		dynsamplerKey := fmt.Sprintf("%s:emathroughput:%d:%v", keyPrefix, c.GoalThroughputPerSec, c.FieldList)
		dynsamplerInstance := getSharedDynsampler(s, dynsamplerKey, c, createDynForEMAThroughputSampler)
		s.mutex.Lock()
		s.goalThroughputConfigs[dynsamplerKey] = c.GoalThroughputPerSec
		s.mutex.Unlock()
		sampler = &EMAThroughputSampler{Config: c, Logger: s.Logger, Metrics: s.Metrics, dynsampler: dynsamplerInstance}
	case *config.WindowedThroughputSamplerConfig:
		dynsamplerKey := fmt.Sprintf("%s:windowedthroughput:%d:%v", keyPrefix, c.GoalThroughputPerSec, c.FieldList)
		dynsamplerInstance := getSharedDynsampler(s, dynsamplerKey, c, createDynForWindowedThroughputSampler)
		s.mutex.Lock()
		s.goalThroughputConfigs[dynsamplerKey] = c.GoalThroughputPerSec
		s.mutex.Unlock()
		sampler = &WindowedThroughputSampler{Config: c, Logger: s.Logger, Metrics: s.Metrics, dynsampler: dynsamplerInstance}
	default:
		s.Logger.Error().Logf("unknown sampler type %T. Exiting.", c)
		os.Exit(1)
		return nil
	}

	err := sampler.Start()
	if err != nil {
		s.Logger.Debug().WithField("dataset", keyPrefix).Logf("failed to start sampler")
		return nil
	}

	s.Logger.Debug().WithField("dataset", keyPrefix).Logf("created implementation for sampler type %T", c)
	// Update peer counts after creating a sampler
	s.updatePeerCounts()

	return sampler
}

// GetSamplerImplementationForKey returns the sampler implementation for the given
// samplerKey (dataset for legacy keys, environment otherwise), or nil if it is not defined
func (s *SamplerFactory) GetSamplerImplementationForKey(samplerKey string) Sampler {
	c, _ := s.Config.GetSamplerConfigForDestName(samplerKey)

	return s.createSampler(c, "")
}

// GetDownstreamSampler creates a downstream sampler for use in rules-based sampling,
// ensuring it participates in the shared dynsampler system
func (s *SamplerFactory) GetDownstreamSampler(
	parentSamplerKey string,
	downstreamConfig *config.RulesBasedDownstreamSampler,
) Sampler {
	keyPrefix := fmt.Sprintf("rules:%s:", parentSamplerKey)

	// Extract the actual config from the downstream wrapper
	var actualConfig any
	switch {
	case downstreamConfig.DynamicSampler != nil:
		actualConfig = downstreamConfig.DynamicSampler
	case downstreamConfig.EMADynamicSampler != nil:
		actualConfig = downstreamConfig.EMADynamicSampler
	case downstreamConfig.TotalThroughputSampler != nil:
		actualConfig = downstreamConfig.TotalThroughputSampler
	case downstreamConfig.EMAThroughputSampler != nil:
		actualConfig = downstreamConfig.EMAThroughputSampler
	case downstreamConfig.WindowedThroughputSampler != nil:
		actualConfig = downstreamConfig.WindowedThroughputSampler
	case downstreamConfig.DeterministicSampler != nil:
		actualConfig = downstreamConfig.DeterministicSampler
	default:
		return nil
	}

	return s.createSampler(actualConfig, keyPrefix)
}

// When the config changes, all our shared dynsamplers are invalid. This stops and clears
// them so they can be re-created when the samplers are rebuilt.
func (s *SamplerFactory) ClearDynsamplers() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Stop all shared dynsamplers
	for _, dynsampler := range s.sharedDynsamplers {
		if stopper, ok := dynsampler.(interface{ Stop() }); ok {
			stopper.Stop()
		}
	}

	clear(s.sharedDynsamplers)
	clear(s.goalThroughputConfigs)
}

// Stop cleans up all shared dynsamplers
func (s *SamplerFactory) Stop() {
	s.ClearDynsamplers()
}

var samplerMetrics = []metrics.Metadata{
	{Name: "_num_dropped", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "Number of traces dropped by configured sampler"},
	{Name: "_num_kept", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "Number of traces kept by configured sampler"},
	{Name: "_sample_rate", Type: metrics.Histogram, Unit: metrics.Dimensionless, Description: "Sample rate for traces"},
	{Name: "_sampler_key_cardinality", Type: metrics.Histogram, Unit: metrics.Dimensionless, Description: "Number of unique keys being tracked by the sampler"},
	{Name: "rulebased_num_dropped_by_drop_rule", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "Number of traces dropped by the drop rule"},
}

func getMetricType(name string) metrics.MetricType {
	if strings.HasSuffix(name, "_count") {
		return metrics.Counter
	}
	return metrics.Gauge
}

type internalDysamplerMetric struct {
	metricType metrics.MetricType
	val        int64 // Value for counters, or gauge value
}

type dynsamplerMetricsRecorder struct {
	prefix    string
	dynPrefix string // Used for accessing metrics from dynsampler-go
	// Stores the last recorded internal metrics produced by dynsampler-go
	lastMetrics map[string]internalDysamplerMetric
	mu          sync.Mutex
	met         metrics.Metrics
	metricNames samplerMetricNames
}

// RegisterMetrics registers the metrics that will be recorded by this package.
// It initializes the necessary metrics and prepares them for recording.
// It MUST be called before any calls to RecordMetrics.
func (d *dynsamplerMetricsRecorder) RegisterMetrics(sampler dynsampler.Sampler) {
	// Register statistics this package will produce
	d.dynPrefix = d.prefix + "_"
	d.lastMetrics = make(map[string]internalDysamplerMetric)
	dynInternalMetrics := sampler.GetMetrics(d.dynPrefix)
	for name, val := range dynInternalMetrics {
		metricType := getMetricType(name)
		d.lastMetrics[name] = internalDysamplerMetric{
			metricType: metricType,
			val:        val,
		}
	}
	d.metricNames = newSamplerMetricNames(d.prefix, d.met)
}

func (d *dynsamplerMetricsRecorder) RecordMetrics(sampler dynsampler.Sampler, kept bool, rate uint, numTraceKey int) {
	d.mu.Lock()
	defer d.mu.Unlock()

	for name, val := range sampler.GetMetrics(d.dynPrefix) {
		m := d.lastMetrics[name]
		switch m.metricType {
		case metrics.Counter:
			delta := val - m.val
			d.met.Count(name, delta)
			m.val = val
			d.lastMetrics[name] = m
		case metrics.Gauge:
			d.met.Gauge(name, float64(val))
		}
	}

	if kept {
		d.met.Increment(d.metricNames.numKept)
	} else {
		d.met.Increment(d.metricNames.numDropped)
	}
	d.met.Histogram(d.metricNames.samplerKeyCardinality, float64(numTraceKey))
	d.met.Histogram(d.metricNames.sampleRate, float64(rate))
}

// samplerMetricNames is a struct that holds the names of metrics used by the
// sampler implementations. This is used to avoid allocation from string concatenation
// in the hot path of sampling.
type samplerMetricNames struct {
	prefix                string
	numKept               string
	numDropped            string
	sampleRate            string
	samplerKeyCardinality string
	numDroppedByDropRule  string
}

func newSamplerMetricNames(prefix string, met metrics.Metrics) samplerMetricNames {
	sm := samplerMetricNames{}
	for _, metric := range samplerMetrics {
		fullname := prefix + metric.Name
		switch metric.Name {
		case "_num_kept":
			sm.numKept = fullname
		case "_num_dropped":
			sm.numDropped = fullname
		case "_sample_rate":
			sm.sampleRate = fullname
		case "_sampler_key_cardinality":
			sm.samplerKeyCardinality = fullname
		case "rulebased_num_dropped_by_drop_rule":
			sm.numDroppedByDropRule = metric.Name
		}

		metric.Name = fullname
		met.Register(metric)
	}

	return sm
}

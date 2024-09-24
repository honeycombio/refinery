package sample

import (
	"fmt"
	"os"
	"strings"

	dynsampler "github.com/honeycombio/dynsampler-go"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/peer"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/types"
)

type Sampler interface {
	GetSampleRate(trace *types.Trace) (rate uint, keep bool, reason string, key string)
	GetKeyFields() []string
	Start() error
}

type ClusterSizer interface {
	SetClusterSize(size int)
}

// SamplerFactory is used to create new samplers with common (injected) resources
type SamplerFactory struct {
	Config    config.Config   `inject:""`
	Logger    logger.Logger   `inject:""`
	Metrics   metrics.Metrics `inject:"genericMetrics"`
	Peers     peer.Peers      `inject:""`
	peerCount int
	samplers  []Sampler
}

func (s *SamplerFactory) updatePeerCounts() {
	if s.Peers != nil {
		peers, err := s.Peers.GetPeers()
		// Only update the stored count if there were no errors
		if err == nil && len(peers) > 0 {
			s.peerCount = len(peers)
		}
	}

	// all the samplers who want it should use the stored count
	for _, sampler := range s.samplers {
		if clusterSizer, ok := sampler.(ClusterSizer); ok {
			s.Logger.Warn().Logf("set cluster size to %d", s.peerCount)
			clusterSizer.SetClusterSize(s.peerCount)
		} else {
			s.Logger.Warn().Logf("sampler does not implement ClusterSizer")
		}
	}
}

func (s *SamplerFactory) Start() error {
	s.peerCount = 1
	if s.Peers != nil {
		s.Peers.RegisterUpdatedPeersCallback(s.updatePeerCounts)
	}
	return nil
}

// GetSamplerImplementationForKey returns the sampler implementation for the given
// samplerKey (dataset for legacy keys, environment otherwise), or nil if it is not defined
func (s *SamplerFactory) GetSamplerImplementationForKey(samplerKey string, isLegacyKey bool) Sampler {
	if isLegacyKey {
		if prefix := s.Config.GetDatasetPrefix(); prefix != "" {
			samplerKey = fmt.Sprintf("%s.%s", prefix, samplerKey)
		}
	}

	c, _ := s.Config.GetSamplerConfigForDestName(samplerKey)

	var sampler Sampler

	switch c := c.(type) {
	case *config.DeterministicSamplerConfig:
		sampler = &DeterministicSampler{Config: c, Logger: s.Logger, Metrics: s.Metrics}
	case *config.DynamicSamplerConfig:
		sampler = &DynamicSampler{Config: c, Logger: s.Logger, Metrics: s.Metrics}
	case *config.EMADynamicSamplerConfig:
		sampler = &EMADynamicSampler{Config: c, Logger: s.Logger, Metrics: s.Metrics}
	case *config.RulesBasedSamplerConfig:
		sampler = &RulesBasedSampler{Config: c, Logger: s.Logger, Metrics: s.Metrics}
	case *config.TotalThroughputSamplerConfig:
		sampler = &TotalThroughputSampler{Config: c, Logger: s.Logger, Metrics: s.Metrics}
	case *config.EMAThroughputSamplerConfig:
		sampler = &EMAThroughputSampler{Config: c, Logger: s.Logger, Metrics: s.Metrics}
	case *config.WindowedThroughputSamplerConfig:
		sampler = &WindowedThroughputSampler{Config: c, Logger: s.Logger, Metrics: s.Metrics}
	default:
		s.Logger.Error().Logf("unknown sampler type %T. Exiting.", c)
		os.Exit(1)
	}

	err := sampler.Start()
	if err != nil {
		s.Logger.Debug().WithField("dataset", samplerKey).Logf("failed to start sampler")
		return nil
	}

	s.Logger.Debug().WithField("dataset", samplerKey).Logf("created implementation for sampler type %T", c)
	// call this every time we add a sampler
	s.samplers = append(s.samplers, sampler)
	s.updatePeerCounts()

	return sampler
}

var samplerMetrics = []metrics.Metadata{
	{Name: "_num_dropped", Type: metrics.Counter, Unit: "count", Description: "Number of traces dropped by configured sampler"},
	{Name: "_num_kept", Type: metrics.Counter, Unit: "count", Description: "Number of traces kept by configured sampler"},
	{Name: "_sample_rate", Type: metrics.Histogram, Unit: "count", Description: "Sample rate for traces"},
}

func getMetricType(name string) metrics.MetricType {
	if strings.HasSuffix(name, "_count") {
		return metrics.Counter
	}
	return metrics.Gauge
}

type dynsamplerMetricsRecorder struct {
	prefix      string
	lastMetrics map[string]int64
	met         metrics.Metrics
}

func (d *dynsamplerMetricsRecorder) RegisterMetrics(sampler dynsampler.Sampler) {
	// Register statistics this package will produce
	d.lastMetrics = sampler.GetMetrics(d.prefix + "_")
	for name := range d.lastMetrics {
		d.met.Register(metrics.Metadata{
			Name: name,
			Type: getMetricType(name),
		})
	}

	for _, metric := range samplerMetrics {
		metric.Name = d.prefix + metric.Name
		d.met.Register(metric)
	}

}

func (d *dynsamplerMetricsRecorder) RecordMetrics(sampler dynsampler.Sampler, kept bool, rate uint) {
	for name, val := range sampler.GetMetrics(d.prefix + "_") {
		switch getMetricType(name) {
		case metrics.Counter:
			delta := val - d.lastMetrics[name]
			d.met.Count(name, delta)
			d.lastMetrics[name] = val
		case metrics.Gauge:
			d.met.Gauge(name, val)
		}
	}

	if kept {
		d.met.Increment(d.prefix + "_num_kept")
	} else {
		d.met.Increment(d.prefix + "_num_dropped")
	}
	d.met.Histogram(d.prefix+"_sample_rate", float64(rate))
}

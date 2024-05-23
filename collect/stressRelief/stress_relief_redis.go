package stressRelief

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/dgryski/go-wyhash"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/health"
	"github.com/honeycombio/refinery/internal/peer"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/jonboulle/clockwork"
	"golang.org/x/sync/errgroup"
)

var _ StressReliever = &StressRelief{}

const stressReliefHealthSource = "stress_relief"

var calculationInterval = 100 * time.Millisecond

type StressRelief struct {
	RefineryMetrics metrics.Metrics `inject:"genericMetrics"`
	Logger          logger.Logger   `inject:""`
	Peer            peer.Peers      `inject:""`
	Clock           clockwork.Clock `inject:""`
	Health          health.Recorder `inject:""`
	done            chan struct{}

	mode               StressReliefMode
	activateLevel      uint
	deactivateLevel    uint
	overallStressLevel uint
	sampleRate         uint64
	upperBound         uint64
	reason             string
	formula            string
	stressed           bool
	stayOnUntil        time.Time
	minDuration        time.Duration
	peerChan           <-chan peer.PeerInfo

	eg *errgroup.Group

	algorithms map[string]func(string, string) float64
	calcs      []StressReliefCalculation

	lock         sync.RWMutex
	stressLevels map[string]stressReport
}

func (s *StressRelief) Start() error {
	s.Logger.Debug().Logf("Starting StressRelief system")
	defer func() { s.Logger.Debug().Logf("Finished starting StressRelief system") }()

	// We use an algorithms map so that we can name these algorithms, which makes it easier for several things:
	// - change our mind about which algorithm to use
	// - logging the algorithm actually used
	// - making it easier to make them configurable
	// At the moment, we are not permitting these to be configurable, but we might change our minds on this.
	// Thus, we're also including a couple of algorithms we don't currently use for convenience.
	algorithms := stressReliefAlgorithm{
		data:   s.RefineryMetrics,
		logger: s.Logger,
	}
	s.algorithms = map[string]func(string, string) float64{
		"linear":  algorithms.linear,  // just use the ratio
		"sqrt":    algorithms.sqrt,    // small values are inflated
		"square":  algorithms.square,  // big values are deflated
		"sigmoid": algorithms.sigmoid, // don't worry about small stuff, but if we cross the midline, start worrying quickly
	}

	// All of the numerator metrics are gauges. The denominator metrics are constants.
	s.calcs = []StressReliefCalculation{
		{Numerator: "collector_incoming_queue_length", Denominator: "INCOMING_CAP", Algorithm: "sqrt", Reason: "CacheCapacity (incoming)"},
		{Numerator: "libhoney_upstream_queue_length", Denominator: "UPSTREAM_BUFFER_SIZE", Algorithm: "sqrt", Reason: "UpstreamBufferSize"},
		{Numerator: "memory_heap_allocation", Denominator: "MEMORY_MAX_ALLOC", Algorithm: "sigmoid", Reason: "MaxAlloc"},
		{Numerator: "smartstore_span_queue_length", Denominator: "SPAN_CHANNEL_CAP", Algorithm: "sqrt", Reason: "SpanChannelCapacity"},
		// TODO: add metrics for stress relief calculation

		// users need to tell us what's their redis memory limit
		//{Numerator: "redisstore_memory_used_total", }
	}

	s.stressLevels = make(map[string]stressReport)
	s.done = make(chan struct{})

	s.Health.Register(stressReliefHealthSource, 5*calculationInterval)

	s.RefineryMetrics.Register("cluster_stress_level", "gauge")
	s.RefineryMetrics.Register("individual_stress_level", "gauge")
	s.RefineryMetrics.Register("stress_relief_activated", "gauge")

	s.peerChan = s.Peer.Subscribe()
	s.eg = &errgroup.Group{}
	s.eg.Go(s.monitor)

	return nil
}

func (s *StressRelief) monitor() error {
	tick := time.NewTicker(calculationInterval)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			currentLevel := s.Recalc()
			// publish the stress level to the rest of the cluster
			err := s.Peer.PublishPeerInfo(peer.PeerInfo{
				Data: peerMessageToBytes(currentLevel),
			})
			if err != nil {
				s.Logger.Error().Logf("error publishing stress level: %s", err)
			} else {
				s.Health.Ready(stressReliefHealthSource, true)
			}

		case msg := <-s.peerChan:
			level, err := newMessageFromBytes(msg.Data)
			if err != nil {
				s.Logger.Error().Logf("error parsing stress level message: %s", err)
				continue
			}

			s.lock.Lock()
			s.stressLevels[msg.ID()] = stressReport{
				key:       msg.ID(),
				level:     level,
				timestamp: s.Clock.Now(),
			}
			s.lock.Unlock()
		case <-s.done:
			s.Logger.Debug().Logf("Stopping StressRelief system")
			return nil
		}
	}

}

func (s *StressRelief) Stop() error {
	s.Health.Ready(stressReliefHealthSource, false)
	close(s.done)
	return s.eg.Wait()
}

func (s *StressRelief) UpdateFromConfig(cfg config.StressReliefConfig) {
	s.lock.Lock()
	defer s.lock.Unlock()

	switch cfg.Mode {
	case "never", "":
		s.mode = Never
	case "monitor":
		s.mode = Monitor
	case "always":
		s.mode = Always
	default: // validation shouldn't let this happen but we'll be safe...
		s.mode = Never
		s.Logger.Error().Logf("StressRelief mode is '%s' which shouldn't happen", cfg.Mode)
	}
	s.Logger.Debug().WithField("mode", s.mode).Logf("setting StressRelief mode")

	s.activateLevel = cfg.ActivationLevel
	s.deactivateLevel = cfg.DeactivationLevel
	s.sampleRate = cfg.SamplingRate
	if s.sampleRate == 0 {
		s.sampleRate = 1
	}
	s.minDuration = time.Duration(cfg.MinimumActivationDuration)

	s.Logger.Debug().
		WithField("activation_level", s.activateLevel).
		WithField("deactivation_level", s.deactivateLevel).
		WithField("sampling_rate", s.sampleRate).
		WithField("min_duration", s.minDuration).
		WithField("startup_duration", cfg.MinimumActivationDuration).
		Logf("StressRelief parameters")

	// Get the actual upper bound - the largest possible 64-bit value divided by
	// the sample rate. This is used because the hash with which we sample is a
	// uint64. In the case where the sample rate is 1, this should sample every
	// value.
	s.upperBound = math.MaxUint64 / s.sampleRate
}

// We want to calculate the stress from various values around the system. Each key value
// can be reported as a key-value.
// This should be called periodically.
func (s *StressRelief) Recalc() uint {
	// we have multiple queues to watch, and for each we calculate a stress level for that queue, which is
	// 100 * the fraction of its capacity in use. Our overall stress level is the max of those values.
	// We track the config value that is under stress as "reason".

	var maximumLevel float64
	var reason string
	var formula string
	for _, c := range s.calcs {
		stress := 100 * s.algorithms[c.Algorithm](c.Numerator, c.Denominator)
		if stress > maximumLevel {
			maximumLevel = stress
			reason = c.Reason
			formula = fmt.Sprintf("%s(%v/%v)=%v", c.Algorithm, c.Numerator, c.Denominator, stress)
		}
	}
	level := uint(maximumLevel)
	s.RefineryMetrics.Gauge("individual_stress_level", level)

	s.Logger.Debug().WithField("stress_level", level).WithField("stress_formula", s.formula).WithField("reason", reason).Logf("calculated stress level")

	clusterStressLevel := s.clusterStressLevel(level)
	s.RefineryMetrics.Gauge("cluster_stress_level", clusterStressLevel)

	s.lock.Lock()
	defer s.lock.Unlock()

	s.overallStressLevel = clusterStressLevel
	s.reason = reason
	s.formula = formula

	switch s.mode {
	case Never:
		s.stressed = false
	case Always:
		s.stressed = true
	case Monitor:
		// If it's off, should we activate it?
		if !s.stressed && s.overallStressLevel >= s.activateLevel {
			s.stressed = true
			s.Logger.Warn().WithFields(map[string]interface{}{
				"overall_stress_level":  s.overallStressLevel,
				"instance_stress_level": level,
				"stress_formula":        s.formula,
				"reason":                s.reason,
			}).Logf("StressRelief has been activated")
		}

		// We want make sure that stress relief is below the deactivate level
		// for a minimum time after the last time we said it should be, so
		// whenever it's above that value we push the time out.
		if s.stressed && s.overallStressLevel >= s.deactivateLevel {
			s.stayOnUntil = s.Clock.Now().Add(s.minDuration)
		}
		// If it's on, should we deactivate it?
		if s.stressed && s.overallStressLevel < s.deactivateLevel && s.Clock.Now().After(s.stayOnUntil) {
			s.stressed = false
			s.Logger.Warn().WithFields(map[string]interface{}{
				"overall_stress_level":  s.overallStressLevel,
				"instance_stress_level": level,
			}).Logf("StressRelief has been deactivated")
		}
	}

	if s.stressed {
		s.RefineryMetrics.Gauge("stress_relief_activated", 1)
	} else {
		s.RefineryMetrics.Gauge("stress_relief_activated", 0)
	}

	return uint(level)
}

// Stressed() indicates whether the system should act as if it's stressed.
func (s *StressRelief) Stressed() bool {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.stressed
}

func (s *StressRelief) GetSampleRate(traceID string) (rate uint, keep bool, reason string) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	if s.sampleRate <= 1 {
		return 1, true, "stress_relief/always"
	}
	hash := wyhash.Hash([]byte(traceID), hashSeed)
	return uint(s.sampleRate), hash <= s.upperBound, "stress_relief/deterministic/" + s.reason
}

// ShouldSampleDeterministically returns true if the trace should be deterministically sampled.
// It uses the traceID to calculate a hash and then divides it by the maximum possible value
// to get a percentage. If the percentage is less than the deterministic fraction, it returns true.
func (s *StressRelief) ShouldSampleDeterministically(traceID string) bool {
	samplePercentage := s.deterministicFraction()
	hash := wyhash.Hash([]byte(traceID), hashSeed)

	return float64(hash)/float64(math.MaxUint64)*100 < float64(samplePercentage)
}

// deterministicFraction returns the fraction of traces that should be deterministic sampled
// It calculates the result by using the stress level as the fraction between the activation
// level and 100%. The result is rounded to the nearest integer.
//
// for example:
// - if the stress level is 90 and the activation level is 80, the result will be 50
// - meaning that 50% of the traces should be deterministic sampled
func (s *StressRelief) deterministicFraction() uint {
	if s.overallStressLevel < s.activateLevel {
		return 0
	}

	// round to the nearest integer
	return uint(float64(s.overallStressLevel-s.activateLevel)/float64(100-s.activateLevel)*100 + 0.5)
}

type stressReport struct {
	key   string
	level uint
	// we need to expire these reports after a certain amount of time
	timestamp time.Time
}

// clusterStressLevel calculates the overall stress level for the cluster
// by using the stress levels reported by each node.
// It uses the geometric mean of the stress levels reported by each node to
// calculate the overall stress level for the cluster.
func (s *StressRelief) clusterStressLevel(level uint) uint {
	// we need to calculate the stress level from the levels we've been given
	// and then publish it to the cluster
	report := stressReport{
		key:       s.Peer.HostID(),
		level:     level,
		timestamp: s.Clock.Now(),
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	s.stressLevels[report.key] = report
	var total float64
	availablePeers := 0
	for _, report := range s.stressLevels {
		// TODO: maybe make the expiration time configurable
		if s.Clock.Since(report.timestamp) > 5*time.Second {
			delete(s.stressLevels, report.key)
			continue
		}
		// we don't want to include peers that are just starting up
		if report.level == 0 {
			continue
		}
		availablePeers++
		total += float64(report.level * report.level)
	}

	if availablePeers == 0 {
		availablePeers = 1
	}

	return uint(math.Sqrt(total / float64(availablePeers)))
}

func peerMessageToBytes(level uint) []byte {
	return []byte(fmt.Sprintf("stress_level/%d", level))
}

func newMessageFromBytes(b []byte) (uint, error) {
	parts := bytes.SplitN(b, []byte("/"), 2)
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid message format: %s", b)
	}

	if string(parts[0]) != "stress_level" {
		return 0, errors.New("invalid message type")
	}

	level, err := strconv.Atoi(string(parts[1]))
	if err != nil {
		return 0, fmt.Errorf("invalid level: %s", parts[1])
	}
	return uint(level), nil
}

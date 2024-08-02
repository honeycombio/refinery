package collect

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dgryski/go-wyhash"
	"github.com/facebookgo/startstop"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/health"
	"github.com/honeycombio/refinery/internal/peer"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/pubsub"
	"github.com/jonboulle/clockwork"
)

const stressReliefTopic = "refinery-stress-relief"

type StressReliever interface {
	UpdateFromConfig(cfg config.StressReliefConfig)
	Recalc() uint
	Stressed() bool
	GetSampleRate(traceID string) (rate uint, keep bool, reason string)
	ShouldSampleDeterministically(traceID string) bool

	startstop.Starter
}

var _ StressReliever = &MockStressReliever{}

type MockStressReliever struct {
	IsStressed              bool
	SampleDeterministically bool
	ShouldKeep              bool
	SampleRate              uint
}

func (m *MockStressReliever) Start() error                                   { return nil }
func (m *MockStressReliever) UpdateFromConfig(cfg config.StressReliefConfig) {}
func (m *MockStressReliever) Recalc() uint                                   { return 0 }
func (m *MockStressReliever) Stressed() bool                                 { return m.IsStressed }
func (m *MockStressReliever) GetSampleRate(traceID string) (rate uint, keep bool, reason string) {
	return m.SampleRate, m.ShouldKeep, "mock"
}
func (m *MockStressReliever) ShouldSampleDeterministically(traceID string) bool {
	return m.SampleDeterministically
}

// hashSeed is a random value to seed the hash generator for the sampler.
// We want it to be a constant that's the same across all nodes so that they
// all make the same sampling decisions during stress relief.
const hashSeed = 34527861234

type StressReliefMode int

const (
	Never StressReliefMode = iota
	Monitor
	Always
)

type stressReport struct {
	key   string
	level uint
	// we need to expire these reports after a certain amount of time
	timestamp time.Time
}

var _ StressReliever = &StressRelief{}

type StressRelief struct {
	RefineryMetrics metrics.Metrics `inject:"metrics"`
	Logger          logger.Logger   `inject:""`
	Health          health.Recorder `inject:""`
	PubSub          pubsub.PubSub   `inject:""`
	Peer            peer.Peers      `inject:""`
	Clock           clockwork.Clock `inject:""`
	Done            chan struct{}

	mode               StressReliefMode
	hostID             string
	activateLevel      uint
	deactivateLevel    uint
	sampleRate         uint64
	upperBound         uint64
	overallStressLevel uint
	reason             string
	formula            string
	stressed           bool
	stayOnUntil        time.Time
	minDuration        time.Duration

	algorithms map[string]func(string, string) float64
	calcs      []StressReliefCalculation

	lock         sync.RWMutex
	stressLevels map[string]stressReport
	// only used in tests
	disableStressLevelReport bool
}

const StressReliefHealthKey = "stress_relief"

func (s *StressRelief) Start() error {
	s.Logger.Debug().Logf("Starting StressRelief system")
	defer func() { s.Logger.Debug().Logf("Finished starting StressRelief system") }()

	// register with health
	s.Health.Register(StressReliefHealthKey, 3*time.Second)

	// register stress level metrics
	s.RefineryMetrics.Register("cluster_stress_level", "gauge")
	s.RefineryMetrics.Register("individual_stress_level", "gauge")
	s.RefineryMetrics.Register("stress_level", "gauge")
	s.RefineryMetrics.Register("stress_relief_activated", "gauge")

	// We use an algorithms map so that we can name these algorithms, which makes it easier for several things:
	// - change our mind about which algorithm to use
	// - logging the algorithm actually used
	// - making it easier to make them configurable
	// At the moment, we are not permitting these to be configurable, but we might change our minds on this.
	// Thus, we're also including a couple of algorithms we don't currently use for convenience.
	s.algorithms = map[string]func(string, string) float64{
		"linear":  s.linear,  // just use the ratio
		"sqrt":    s.sqrt,    // small values are inflated
		"square":  s.square,  // big values are deflated
		"sigmoid": s.sigmoid, // don't worry about small stuff, but if we cross the midline, start worrying quickly
	}

	// All of the numerator metrics are gauges. The denominator metrics are constants.
	s.calcs = []StressReliefCalculation{
		{Numerator: "collector_peer_queue_length", Denominator: "PEER_CAP", Algorithm: "sqrt", Reason: "CacheCapacity (peer)"},
		{Numerator: "collector_incoming_queue_length", Denominator: "INCOMING_CAP", Algorithm: "sqrt", Reason: "CacheCapacity (incoming)"},
		{Numerator: "libhoney_peer_queue_length", Denominator: "PEER_BUFFER_SIZE", Algorithm: "sqrt", Reason: "PeerBufferSize"},
		{Numerator: "libhoney_upstream_queue_length", Denominator: "UPSTREAM_BUFFER_SIZE", Algorithm: "sqrt", Reason: "UpstreamBufferSize"},
		{Numerator: "memory_heap_allocation", Denominator: "MEMORY_MAX_ALLOC", Algorithm: "sigmoid", Reason: "MaxAlloc"},
	}

	var err error
	s.hostID, err = s.Peer.GetInstanceID()
	if err != nil {
		return fmt.Errorf("failed to get host ID: %w", err)
	}

	s.stressLevels = make(map[string]stressReport)

	// Subscribe to the stress relief topic so we can react to stress level
	// changes in the cluster.
	s.PubSub.Subscribe(context.Background(), stressReliefTopic, s.onStressLevelUpdate)

	// start our monitor goroutine that periodically calls recalc
	// and also reports that it's healthy

	go func(s *StressRelief) {
		// only publish stress level if it has changed or if it's been a while since the last publish
		if s.disableStressLevelReport {
			return
		}
		const maxTicksBetweenReports = 30
		var (
			lastLevel   uint = 0
			tickCounter      = 0
		)

		tick := s.Clock.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-tick.Chan():
				currentLevel := s.Recalc()

				if lastLevel != currentLevel || tickCounter == maxTicksBetweenReports {
					err := s.PubSub.Publish(context.Background(), stressReliefTopic, newStressReliefMessage(currentLevel, s.hostID).String())
					if err != nil {
						s.Logger.Error().Logf("failed to publish stress level: %s", err)
					}

					lastLevel = currentLevel
					tickCounter = 0
				}

				tickCounter++

				s.Health.Ready(StressReliefHealthKey, true)
			case <-s.Done:
				s.Health.Unregister(StressReliefHealthKey)
				s.Logger.Debug().Logf("Stopping StressRelief system")
				return
			}
		}
	}(s)
	return nil
}

type stressReliefMessage struct {
	peerID string
	level  uint
}

func newStressReliefMessage(level uint, peerID string) *stressReliefMessage {
	return &stressReliefMessage{level: level, peerID: peerID}
}

func (msg *stressReliefMessage) String() string {
	return msg.peerID + "|" + fmt.Sprint(msg.level)
}

func unmarshalStressReliefMessage(msg string) (*stressReliefMessage, error) {
	if len(msg) < 2 {
		return nil, fmt.Errorf("empty message")
	}

	parts := strings.SplitN(msg, "|", 2)
	level, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, err
	}

	return newStressReliefMessage(uint(level), parts[0]), nil
}

func (s *StressRelief) onStressLevelUpdate(ctx context.Context, msg string) {
	stressMsg, err := unmarshalStressReliefMessage(msg)
	if err != nil {
		s.Logger.Error().Logf("failed to unmarshal stress relief message: %s", err)
		return
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	s.stressLevels[stressMsg.peerID] = stressReport{
		key:       stressMsg.peerID,
		level:     stressMsg.level,
		timestamp: s.Clock.Now(),
	}
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

func clamp(f float64, min float64, max float64) float64 {
	if f < min {
		return min
	}
	if f > max {
		return max
	}
	return f
}

// ratio is a function that returns the ratio of two values looked up in the metrics,
// clamped between 0 and 1. Since we know this is the range, we know that
// sqrt has the effect of making small values larger, and square has the
// effect of making large values smaller. We can use these functions to bias the
// weighting of our calculations.
func (s *StressRelief) ratio(num, denom string) float64 {
	numerator, ok := s.RefineryMetrics.Get(num)
	if !ok {
		s.Logger.Debug().Logf("stress recalc: missing numerator %s", num)
		return 0
	}
	denominator, ok := s.RefineryMetrics.Get(denom)
	if !ok {
		s.Logger.Debug().Logf("stress recalc: missing denominator %s", denom)
		return 0
	}
	if denominator != 0 {
		stress := clamp(numerator/denominator, 0, 1)
		s.Logger.Debug().
			WithField("numerator_name", num).
			WithField("numerator_value", numerator).
			WithField("denominator_name", denom).
			WithField("denominator_value", denominator).
			WithField("unscaled_result", stress).
			Logf("stress recalc: detail")
		return stress
	}
	return 0
}

// linear simply returns the value it calculates
func (s *StressRelief) linear(num, denom string) float64 {
	stress := s.ratio(num, denom)
	s.Logger.Debug().
		WithField("algorithm", "linear").
		WithField("result", stress).
		Logf("stress recalc: result")
	return stress
}

// sqrt returns the square root of the value calculated, which (in the range [0-1])
// inflates them a bit without affecting the ends of the range.
func (s *StressRelief) sqrt(num, denom string) float64 {
	stress := math.Sqrt(s.ratio(num, denom))
	s.Logger.Debug().
		WithField("algorithm", "sqrt").
		WithField("result", stress).
		Logf("stress recalc: result")
	return stress
}

// square returns the square of the value calculated, which (in the range [0-1])
// deflates them a bit without affecting the ends of the range.
func (s *StressRelief) square(num, denom string) float64 {
	r := s.ratio(num, denom)
	stress := r * r
	s.Logger.Debug().
		WithField("algorithm", "square").
		WithField("result", stress).
		Logf("stress recalc: result")
	return stress
}

// sigmoid returns a value along a sigmoid (s-shaped) curve of the value
// calculated, which (in the range [0-1]) deflates low values and inflates high
// values without affecting the ends of the range. We use this one for memory pressure,
// under the presumption that if we're using less than half of RAM,
func (s *StressRelief) sigmoid(num, denom string) float64 {
	r := s.ratio(num, denom)
	// This is an S curve from 0 to 1, centered around 0.5 -- constants were
	// empirically determined by messing around with a graphing calculator. The
	// only reason you might change these is if you want to change the bendiness
	// of the S curve.
	stress := 0.400305589*math.Atan(6*(r-0.5)) + 0.5
	s.Logger.Debug().
		WithField("algorithm", "sigmoid").
		WithField("result", stress).
		Logf("stress recalc: result")
	return stress
}

type StressReliefCalculation struct {
	Numerator   string
	Denominator string
	Algorithm   string
	Reason      string
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
	s.Logger.Debug().WithField("individual_stress_level", maximumLevel).WithField("stress_formula", s.formula).WithField("reason", reason).Logf("calculated stress level")

	s.RefineryMetrics.Gauge("individual_stress_level", float64(maximumLevel))
	localLevel := uint(maximumLevel)

	clusterStressLevel := s.clusterStressLevel(localLevel)
	s.RefineryMetrics.Gauge("cluster_stress_level", clusterStressLevel)

	s.lock.Lock()
	defer s.lock.Unlock()

	// The overall stress level is the max of the individual and cluster stress levels
	// If a single node is under significant stress, it can activate stress relief mode
	s.overallStressLevel = uint(math.Max(float64(clusterStressLevel), float64(localLevel)))
	s.RefineryMetrics.Gauge("stress_level", s.overallStressLevel)

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
				"individual_stress_level": localLevel,
				"cluster_stress_level":    clusterStressLevel,
				"stress_level":            s.overallStressLevel,
				"stress_formula":          s.formula,
				"reason":                  s.reason,
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
				"individual_stress_level": localLevel,
				"cluster_stress_level":    clusterStressLevel,
				"stress_level":            s.overallStressLevel,
			}).Logf("StressRelief has been deactivated")
		}
	}

	if s.stressed {
		s.RefineryMetrics.Gauge("stress_relief_activated", 1)
	} else {
		s.RefineryMetrics.Gauge("stress_relief_activated", 0)
	}

	return localLevel
}

// clusterStressLevel calculates the overall stress level for the cluster
// by using the stress levels reported by each node.
// It uses the geometric mean of the stress levels reported by each node to
// calculate the overall stress level for the cluster.
func (s *StressRelief) clusterStressLevel(localLevel uint) uint {
	// we need to calculate the stress level from the levels we've been given
	// and then publish it to the cluster
	report := stressReport{
		key:       s.hostID,
		level:     localLevel,
		timestamp: s.Clock.Now(),
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	s.stressLevels[report.key] = report
	var total float64
	availablePeers := 0
	for _, report := range s.stressLevels {
		if s.Clock.Since(report.timestamp) > peer.PeerEntryTimeout {
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

// Stressed() indicates whether the system should act as if it's stressed.
// Note that the stress_level metric is independent of mode.
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

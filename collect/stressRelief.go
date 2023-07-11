package collect

import (
	"math"
	"sync"
	"time"

	"github.com/dgryski/go-wyhash"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
)

type StressReliever interface {
	Start() error
	UpdateFromConfig(cfg config.StressReliefConfig) error
	Recalc()
	StressLevel() uint
	Stressed() bool
	GetSampleRate(traceID string) (rate uint, keep bool, reason string)
}

type MockStressReliever struct{}

func (m *MockStressReliever) Start() error                                         { return nil }
func (m *MockStressReliever) UpdateFromConfig(cfg config.StressReliefConfig) error { return nil }
func (m *MockStressReliever) Recalc()                                              {}
func (m *MockStressReliever) StressLevel() uint                                    { return 0 }
func (m *MockStressReliever) Stressed() bool                                       { return false }
func (m *MockStressReliever) GetSampleRate(traceID string) (rate uint, keep bool, reason string) {
	return 1, false, ""
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

type StressRelief struct {
	mode            StressReliefMode
	activateLevel   uint
	deactivateLevel uint
	sampleRate      uint64
	upperBound      uint64
	stressLevel     uint
	reason          string
	stressed        bool
	stayOnUntil     time.Time
	minDuration     time.Duration
	RefineryMetrics metrics.Metrics `inject:"metrics"`
	Logger          logger.Logger   `inject:""`
	Done            chan struct{}

	algorithms map[string]func(string, string) float64
	calcs      []StressReliefCalculation
	lock       sync.RWMutex
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

	// start our monitor goroutine that periodically calls recalc
	go func(s *StressRelief) {
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				s.Recalc()
			case <-s.Done:
				s.Logger.Debug().Logf("Stopping StressRelief system")
				return
			}
		}
	}(s)
	return nil
}

func (s *StressRelief) UpdateFromConfig(cfg config.StressReliefConfig) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	switch cfg.Mode {
	case "never", "":
		s.mode = Never
	case "monitor":
		// If we're switching into monitor mode from some other state (which
		// happens on startup), we will start up in stressed mode for a
		// configurable time to try to make sure that we can handle the load
		// before we start processing it in earnest. This is to help address the
		// problem of trying to bring a new node into an already-overloaded
		// cluster. If the time is 0 we won't do this at all.
		if s.mode != Monitor && cfg.MinimumStartupDuration != 0 {
			s.stressed = true
			s.stayOnUntil = time.Now().Add(time.Duration(cfg.MinimumStartupDuration))
			s.Logger.Warn().WithField("stress_level", s.stressLevel).WithField("reason", "MinimumStartupDuration").Logf("StressRelief has been activated")
		}
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

	return nil
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
func (s *StressRelief) Recalc() {
	// we have multiple queues to watch, and for each we calculate a stress level for that queue, which is
	// 100 * the fraction of its capacity in use. Our overall stress level is the max of those values.
	// We track the config value that is under stress as "reason".

	var level float64
	var reason string
	for _, c := range s.calcs {
		stress := 100 * s.algorithms[c.Algorithm](c.Numerator, c.Denominator)
		if stress > level {
			level = stress
			reason = c.Reason
		}
	}
	s.Logger.Debug().WithField("stress_level", level).WithField("reason", reason).Logf("calculated stress level")

	s.lock.Lock()
	defer s.lock.Unlock()

	s.stressLevel = uint(level)
	s.reason = reason

	switch s.mode {
	case Never:
		s.stressed = false
	case Always:
		s.stressed = true
	case Monitor:
		// If it's off, should we activate it?
		if !s.stressed && s.stressLevel >= s.activateLevel {
			s.stressed = true
			s.Logger.Warn().WithField("stress_level", s.stressLevel).WithField("reason", s.reason).Logf("StressRelief has been activated")
		}
		// We want make sure that stress relief is below the deactivate level
		// for a minimum time after the last time we said it should be, so
		// whenever it's above that value we push the time out.
		if s.stressed && s.stressLevel >= s.deactivateLevel {
			s.stayOnUntil = time.Now().Add(s.minDuration)
		}
		// If it's on, should we deactivate it?
		if s.stressed && s.stressLevel < s.deactivateLevel && time.Now().After(s.stayOnUntil) {
			s.stressed = false
			s.Logger.Warn().WithField("stress_level", s.stressLevel).Logf("StressRelief has been deactivated")
		}
	}
}

func (s *StressRelief) StressLevel() uint {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.stressLevel
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

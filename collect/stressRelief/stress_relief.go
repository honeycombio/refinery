package stressRelief

import (
	"math"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
)

type StressReliever interface {
	Start() error
	UpdateFromConfig(cfg config.StressReliefConfig)
	Recalc() uint
	Stressed() bool
	GetSampleRate(traceID string) (rate uint, keep bool, reason string)
	ShouldSampleDeterministically(traceID string) bool
}

var _ StressReliever = &MockStressReliever{}

type MockStressReliever struct {
	IsStressed              bool
	SampleDeterministically bool
	SampleRate              uint
	ShouldKeep              bool
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

type stressReliefAlgorithm struct {
	data   metrics.Metrics
	logger logger.Logger
}

// ratio is a function that returns the ratio of two values looked up in the metrics,
// clamped between 0 and 1. Since we know this is the range, we know that
// sqrt has the effect of making small values larger, and square has the
// effect of making large values smaller. We can use these functions to bias the
// weighting of our calculations.
func (s *stressReliefAlgorithm) ratio(num, denom string) float64 {
	numerator, ok := s.data.Get(num)
	if !ok {
		s.logger.Debug().Logf("stress recalc: missing numerator %s", num)
		return 0
	}
	denominator, ok := s.data.Get(denom)
	if !ok {
		s.logger.Debug().Logf("stress recalc: missing denominator %s", denom)
		return 0
	}
	if denominator != 0 {
		stress := clamp(numerator/denominator, 0, 1)
		s.logger.Debug().
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
func (s *stressReliefAlgorithm) linear(num, denom string) float64 {
	stress := s.ratio(num, denom)
	s.logger.Debug().
		WithField("algorithm", "linear").
		WithField("result", stress).
		Logf("stress recalc: result")
	return stress
}

// sqrt returns the square root of the value calculated, which (in the range [0-1])
// inflates them a bit without affecting the ends of the range.
func (s *stressReliefAlgorithm) sqrt(num, denom string) float64 {
	stress := math.Sqrt(s.ratio(num, denom))
	s.logger.Debug().
		WithField("algorithm", "sqrt").
		WithField("result", stress).
		Logf("stress recalc: result")
	return stress
}

// square returns the square of the value calculated, which (in the range [0-1])
// deflates them a bit without affecting the ends of the range.
func (s *stressReliefAlgorithm) square(num, denom string) float64 {
	r := s.ratio(num, denom)
	stress := r * r
	s.logger.Debug().
		WithField("algorithm", "square").
		WithField("result", stress).
		Logf("stress recalc: result")
	return stress
}

// sigmoid returns a value along a sigmoid (s-shaped) curve of the value
// calculated, which (in the range [0-1]) deflates low values and inflates high
// values without affecting the ends of the range. We use this one for memory pressure,
// under the presumption that if we're using less than half of RAM,
func (s *stressReliefAlgorithm) sigmoid(num, denom string) float64 {
	r := s.ratio(num, denom)
	// This is an S curve from 0 to 1, centered around 0.5 -- constants were
	// empirically determined by messing around with a graphing calculator. The
	// only reason you might change these is if you want to change the bendiness
	// of the S curve.
	stress := 0.400305589*math.Atan(6*(r-0.5)) + 0.5
	s.logger.Debug().
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

func clamp(f float64, min float64, max float64) float64 {
	if f < min {
		return min
	}
	if f > max {
		return max
	}
	return f
}

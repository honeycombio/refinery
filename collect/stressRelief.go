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
	stressed        bool
	RefineryMetrics metrics.Metrics `inject:"metrics"`
	Logger          logger.Logger   `inject:""`
	Done            chan struct{}

	lock sync.RWMutex
}

func (d *StressRelief) Start() error {
	d.Logger.Debug().Logf("Starting StressRelief system")
	defer func() { d.Logger.Debug().Logf("Finished starting StressRelief system") }()

	// start our monitor goroutine that periodically calls recalc
	go func(d *StressRelief) {
		tick := time.NewTicker(1000 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				d.Recalc()
			case <-d.Done:
				d.Logger.Debug().Logf("Stopping StressRelief system")
				return
			}
		}
	}(d)
	return nil
}

func (d *StressRelief) UpdateFromConfig(cfg config.StressReliefConfig) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	switch cfg.Mode {
	case "never", "":
		d.mode = Never
		d.Logger.Debug().Logf("StressRelief mode is 'never'")
	case "monitor":
		d.mode = Monitor
		d.Logger.Debug().Logf("StressRelief mode is 'monitor'")
	case "always":
		d.mode = Always
		d.Logger.Debug().Logf("StressRelief mode is 'always'")
	default: // validation shouldn't let this happen but we'll be safe...
		d.mode = Never
		d.Logger.Debug().Logf("StressRelief mode is %s which shouldn't happen - using 'never'", cfg.Mode)
	}

	d.activateLevel = cfg.ActivationLevel
	d.deactivateLevel = cfg.DeactivationLevel
	d.sampleRate = cfg.StressSamplingRate
	if d.sampleRate == 0 {
		d.sampleRate = 1
	}
	d.Logger.Debug().Logf(
		"StressRelief ActivationLevel %d, DeactivationLevel %d, SamplingRate %d'",
		d.activateLevel, d.deactivateLevel, d.sampleRate)

	// Get the actual upper bound - the largest possible value divided by
	// the sample rate. In the case where the sample rate is 1, this should
	// sample every value.
	d.upperBound = math.MaxUint64 / d.sampleRate

	return nil
}

// getMetric retrieves a named value from one of the sources of metrics.
func (d *StressRelief) getMetric(name string) (float64, bool) {
	if v, ok := d.RefineryMetrics.Get(name); ok {
		return v, true
	}
	return 0, false
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

// We want to calculate the stress from various values around the system. Each key value
// can be reported as a key-value.
// This should be called periodically.
func (d *StressRelief) Recalc() {
	// we have multiple queues to watch, and for each we calculate a stress level for that queue, which is
	// 100 * the fraction of its capacity in use. Our overall stress level is the max of those values.
	// The numerators and denominators here need to be in the same order.

	queues := map[string]string{
		"collector_peer_queue_length":     "PEER_CAP",
		"collector_incoming_queue_length": "INCOMING_CAP",
		"libhoney_peer_queue_length":      "PEER_BUFFER_SIZE",
		"libhoney_upstream_queue_length":  "UPSTREAM_BUFFER_SIZE",
	}
	d.Logger.Debug().Logf("stress recalc: Ref: '%v'", d.RefineryMetrics.(*metrics.HoneycombMetrics).GetAllNames())

	var level float64
	for num, denom := range queues {
		numerator, ok := d.getMetric(num)
		if !ok {
			d.Logger.Debug().Logf("stress recalc: missing numerator %s", num)
			continue
		}
		denominator, ok := d.getMetric(denom)
		if !ok {
			d.Logger.Debug().Logf("stress recalc: missing denominator %s", num)
			continue
		}
		if denominator != 0 {
			stress := 100 * math.Sqrt(clamp(numerator/denominator, 0, 1))
			if stress > level {
				level = stress
			}
		}
		d.Logger.Debug().Logf("stress recalc: %s=%v/%s=%v", num, numerator, denom, denominator)
	}
	d.Logger.Debug().Logf("calculated stress_level = %v", level)

	d.lock.Lock()
	d.stressLevel = uint(level)
	d.lock.Unlock()
}

func (d *StressRelief) StressLevel() uint {
	return d.stressLevel
}

// Stressed() indicates whether the system should act as if it's stressed.
// Note that the stress_level metric is independent of mode.
func (d *StressRelief) Stressed() bool {
	switch d.mode {
	case Never:
		d.stressed = false
	case Always:
		d.stressed = true
	case Monitor:
		if d.stressLevel >= d.activateLevel {
			d.stressed = true
			d.Logger.Debug().Logf("StressRelief has been activated at stressLevel %d", d.stressLevel)
		}
		if d.stressed && d.stressLevel < d.deactivateLevel {
			d.stressed = false
			d.Logger.Debug().Logf("StressRelief has been deactivated at stressLevel %d", d.stressLevel)
		}
	}
	return d.stressed
}

func (d *StressRelief) GetSampleRate(traceID string) (rate uint, keep bool, reason string) {
	if d.sampleRate <= 1 {
		return 1, true, "stress_relief/always"
	}
	hash := wyhash.Hash([]byte(traceID), hashSeed)
	return uint(d.sampleRate), hash <= d.upperBound, "stress_relief/deterministic"
}

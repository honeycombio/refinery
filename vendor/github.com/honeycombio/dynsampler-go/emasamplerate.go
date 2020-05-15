package dynsampler

import (
	"encoding/json"
	"errors"
	"math"
	"sort"
	"sync"
	"time"
)

// EMASampleRate implements Sampler and attempts to average a given sample rate,
// weighting rare traffic and frequent traffic differently so as to end up with
// the correct average. This method breaks down when total traffic is low
// because it will be excessively sampled.
//
// Based on the AvgSampleRate implementation, EMASampleRate differs in that rather
// than compute rate based on a periodic sample of traffic, it maintains an Exponential
// Moving Average of counts seen per key, and adjusts this average at regular intervals.
// The weight applied to more recent intervals is defined by `weight`, a number between
// (0, 1) - larger values weight the average more toward recent observations. In other words,
// a larger weight will cause sample rates more quickly adapt to traffic patterns,
// while a smaller weight will result in sample rates that are less sensitive to bursts or drops
// in traffic and thus more consistent over time.
//
// Keys that are not found in the EMA will always have a sample
// rate of 1. Keys that occur more frequently will be sampled on a logarithmic
// curve. In other words, every key will be represented at least once in any
// given window and more frequent keys will have their sample rate
// increased proportionally to wind up with the goal sample rate.
type EMASampleRate struct {
	// AdjustmentInterval defines how often (in seconds) we adjust the moving average from
	// recent observations. Default 15s
	AdjustmentInterval int

	// Weight is a value between (0, 1) indicating the weighting factor used to adjust
	// the EMA. With larger values, newer data will influence the average more, and older
	// values will be factored out more quickly.  In mathematical literature concerning EMA,
	// this is referred to as the `alpha` constant.
	// Default is 0.5
	Weight float64

	// GoalSampleRate is the average sample rate we're aiming for, across all
	// events. Default 10
	GoalSampleRate int

	// MaxKeys, if greater than 0, limits the number of distinct keys tracked in EMA.
	// Once MaxKeys is reached, new keys will not be included in the sample rate map, but
	// existing keys will continue to be be counted.
	MaxKeys int

	// AgeOutValue indicates the threshold for removing keys from the EMA. The EMA of any key will approach 0
	// if it is not repeatedly observed, but will never truly reach it, so we have to decide what constitutes "zero".
	// Keys with averages below this threshold will be removed from the EMA. Default is the same as Weight, as this prevents
	// a key with the smallest integer value (1) from being aged out immediately. This value should generally be <= Weight,
	// unless you have very specific reasons to set it higher.
	AgeOutValue float64

	// BurstMultiple, if set, is multiplied by the sum of the running average of counts to define
	// the burst detection threshold. If total counts observed for a given interval exceed the threshold
	// EMA is updated immediately, rather than waiting on the AdjustmentInterval.
	// Defaults to 2; negative value disables. With a default of 2, if your traffic suddenly doubles,
	// burst detection will kick in.
	BurstMultiple float64

	// BurstDetectionDelay indicates the number of intervals to run after Start is called before burst detection kicks in.
	// Defaults to 3
	BurstDetectionDelay uint

	savedSampleRates map[string]int
	currentCounts    map[string]float64
	movingAverage    map[string]float64
	burstThreshold   float64
	currentBurstSum  float64
	intervalCount    uint
	burstSignal      chan struct{}

	// haveData indicates that we have gotten a sample of traffic. Before we've
	// gotten any samples of traffic, we should we should use the default goal
	// sample rate for all events instead of sampling everything at 1
	haveData bool
	updating bool

	lock sync.Mutex

	// used only in tests
	testSignalMapsDone chan struct{}
}

func (e *EMASampleRate) Start() error {
	// apply defaults
	if e.AdjustmentInterval == 0 {
		e.AdjustmentInterval = 15
	}
	if e.GoalSampleRate == 0 {
		e.GoalSampleRate = 10
	}
	if e.Weight == 0 {
		e.Weight = 0.5
	}
	if e.AgeOutValue == 0 {
		e.AgeOutValue = e.Weight
	}
	if e.BurstMultiple == 0 {
		e.BurstMultiple = 2
	}
	if e.BurstDetectionDelay == 0 {
		e.BurstDetectionDelay = 3
	}

	// Don't override these maps at startup in case they were loaded from a previous state
	e.currentCounts = make(map[string]float64)
	if e.savedSampleRates == nil {
		e.savedSampleRates = make(map[string]int)
	}
	if e.movingAverage == nil {
		e.movingAverage = make(map[string]float64)
	}
	e.burstSignal = make(chan struct{})

	go func() {
		ticker := time.NewTicker(time.Second * time.Duration(e.AdjustmentInterval))
		for {
			select {
			case <-e.burstSignal:
				// reset ticker when we get a burst
				ticker.Stop()
				ticker = time.NewTicker(time.Second * time.Duration(e.AdjustmentInterval))
				e.updateMaps()
			case <-ticker.C:
				e.updateMaps()
				e.intervalCount++
			}
		}
	}()
	return nil
}

// updateMaps calculates a new saved rate map based on the contents of the
// counter map
func (e *EMASampleRate) updateMaps() {
	e.lock.Lock()
	if e.testSignalMapsDone != nil {
		defer func() {
			e.testSignalMapsDone <- struct{}{}
		}()
	}
	// short circuit if no traffic
	if len(e.currentCounts) == 0 {
		// No traffic the last interval, don't update anything. This is deliberate to avoid
		// the average decaying when there's no traffic (comes in bursts, or there's some kind of outage).
		e.lock.Unlock()
		return
	}
	// If there is another updateMaps going, bail
	if e.updating {
		e.lock.Unlock()
		return
	}
	e.updating = true
	// make a local copy of the sample counters for calculation
	tmpCounts := e.currentCounts
	e.currentCounts = make(map[string]float64)
	e.currentBurstSum = 0
	e.lock.Unlock()

	e.updateEMA(tmpCounts)

	// Goal events to send this interval is the total count of events in the EMA
	// divided by the desired average sample rate
	var sumEvents float64
	for _, count := range e.movingAverage {
		sumEvents += math.Max(1, count)
	}

	// Store this for burst detection. This is checked in GetSampleRate
	// so we need to grab the lock when we update it.
	e.lock.Lock()
	e.burstThreshold = sumEvents * e.BurstMultiple
	e.lock.Unlock()

	goalCount := float64(sumEvents) / float64(e.GoalSampleRate)
	// goalRatio is the goalCount divided by the sum of all the log values - it
	// determines what percentage of the total event space belongs to each key
	var logSum float64
	for _, count := range e.movingAverage {
		// We take the max of (1, count) because count * weight is < 1 for
		// very small counts, which throws off the logSum and can cause
		// incorrect samples rates to be computed when throughput is low
		logSum += math.Log10(math.Max(1, count))
	}
	goalRatio := goalCount / logSum

	// must go through the keys in a fixed order to prevent rounding from changing
	// results
	keys := make([]string, len(e.movingAverage))
	var i int
	for k := range e.movingAverage {
		keys[i] = k
		i++
	}
	sort.Strings(keys)

	// goal number of events per key is goalRatio * key count, but never less than
	// one. If a key falls below its goal, it gets a sample rate of 1 and the
	// extra available events get passed on down the line.
	newSavedSampleRates := make(map[string]int)
	keysRemaining := len(e.movingAverage)
	var extra float64
	for _, key := range keys {
		count := math.Max(1, e.movingAverage[key])
		// take the max of 1 or my log10 share of the total
		goalForKey := math.Max(1, math.Log10(count)*goalRatio)
		// take this key's share of the extra and pass the rest along
		extraForKey := extra / float64(keysRemaining)
		goalForKey += extraForKey
		extra -= extraForKey
		keysRemaining--
		if count <= goalForKey {
			// there are fewer samples than the allotted number for this key. set
			// sample rate to 1 and redistribute the unused slots for future keys
			newSavedSampleRates[key] = 1
			extra += goalForKey - count
		} else {
			// there are more samples than the allotted number. Sample this key enough
			// to knock it under the limit (aka round up)
			rate := math.Ceil(count / goalForKey)
			// if counts are <= 1 we can get values for goalForKey that are +Inf
			// and subsequent division ends up with NaN. If that's the case,
			// fall back to 1
			if math.IsNaN(rate) {
				newSavedSampleRates[key] = 1
			} else {
				newSavedSampleRates[key] = int(rate)
			}
			extra += goalForKey - (count / float64(newSavedSampleRates[key]))
		}
	}
	e.lock.Lock()
	defer e.lock.Unlock()
	e.savedSampleRates = newSavedSampleRates
	e.haveData = true
	e.updating = false
}

// GetSampleRate takes a key and returns the appropriate sample rate for that
// key. Will never return zero.
func (e *EMASampleRate) GetSampleRate(key string) int {
	e.lock.Lock()
	defer e.lock.Unlock()

	// Enforce MaxKeys limit on the size of the map
	if e.MaxKeys > 0 {
		// If a key already exists, increment it. If not, but we're under the limit, store a new key
		if _, found := e.currentCounts[key]; found || len(e.currentCounts) < e.MaxKeys {
			e.currentCounts[key]++
			e.currentBurstSum++
		}
	} else {
		e.currentCounts[key]++
		e.currentBurstSum++
	}

	// Enforce the burst threshold
	if e.burstThreshold > 0 && e.currentBurstSum >= e.burstThreshold && e.intervalCount >= e.BurstDetectionDelay {
		// reset the burst sum to prevent additional burst updates from occurring while updateMaps is running
		e.currentBurstSum = 0
		// send but don't block - consuming is blocked on updateMaps, which takes the same lock we're holding
		select {
		case e.burstSignal <- struct{}{}:
		default:
		}
	}

	if !e.haveData {
		return e.GoalSampleRate
	}
	if rate, found := e.savedSampleRates[key]; found {
		return rate
	}
	return 1
}

func (e *EMASampleRate) updateEMA(newCounts map[string]float64) {
	keysToUpdate := make([]string, 0, len(e.movingAverage))
	for key := range e.movingAverage {
		keysToUpdate = append(keysToUpdate, key)
	}

	// Update any existing keys with new values
	for _, key := range keysToUpdate {
		var newAvg float64
		// Was this key seen in the last interval? Adjust by that amount
		if val, found := newCounts[key]; found {
			newAvg = adjustAverage(e.movingAverage[key], val, e.Weight)
		} else {
			// Otherwise adjust by zero
			newAvg = adjustAverage(e.movingAverage[key], 0, e.Weight)
		}

		// Age out this value if it's too small to care about for calculating sample rates
		// This is also necessary to keep our map from going forever.
		if newAvg < e.AgeOutValue {
			delete(e.movingAverage, key)
		} else {
			e.movingAverage[key] = newAvg
		}
		// We've processed this key - don't process it again when we look at new counts
		delete(newCounts, key)
	}

	for key := range newCounts {
		newAvg := adjustAverage(0, newCounts[key], e.Weight)
		if newAvg >= e.AgeOutValue {
			e.movingAverage[key] = newAvg
		}
	}
}

type emaSampleRateState struct {
	// These fields are exported for use by `JSON.Marshal` and `JSON.Unmarshal`
	SavedSampleRates map[string]int     `json:"saved_sample_rates"`
	MovingAverage    map[string]float64 `json:"moving_average"`
}

// SaveState returns a byte array with a JSON representation of the sampler state
func (e *EMASampleRate) SaveState() ([]byte, error) {
	e.lock.Lock()
	defer e.lock.Unlock()

	if e.savedSampleRates == nil {
		return nil, errors.New("saved sample rate map is nil")
	}
	if e.movingAverage == nil {
		return nil, errors.New("moving average map is nil")
	}
	s := &emaSampleRateState{SavedSampleRates: e.savedSampleRates, MovingAverage: e.movingAverage}
	return json.Marshal(s)
}

// LoadState accepts a byte array with a JSON representation of a previous instance's
// state
func (e *EMASampleRate) LoadState(state []byte) error {
	e.lock.Lock()
	defer e.lock.Unlock()

	s := emaSampleRateState{}
	err := json.Unmarshal(state, &s)
	if err != nil {
		return err
	}

	// Load the previously calculated sample rates
	e.savedSampleRates = s.SavedSampleRates
	e.movingAverage = s.MovingAverage
	// Allow GetSampleRate to return calculated sample rates from the loaded map
	e.haveData = true

	return nil
}

func adjustAverage(oldAvg, value float64, alpha float64) float64 {
	adjustedNewVal := value * alpha
	adjustedOldAvg := (1.0 - alpha) * oldAvg

	return adjustedNewVal + adjustedOldAvg
}

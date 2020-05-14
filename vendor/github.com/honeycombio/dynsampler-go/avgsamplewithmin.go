package dynsampler

import (
	"math"
	"sort"
	"sync"
	"time"
)

// AvgSampleWithMin implements Sampler and attempts to average a given sample
// rate, with a minimum number of events per second (i.e. it will reduce
// sampling if it would end up sending fewer than the mininum number of events).
// This method attempts to get the best of the normal average sample rate
// method, without the failings it shows on the low end of total traffic
// throughput
//
// Keys that occur only once within ClearFrequencySec will always have a sample
// rate of 1. Keys that occur more frequently will be sampled on a logarithmic
// curve. In other words, every key will be represented at least once per
// ClearFrequencySec and more frequent keys will have their sample rate
// increased proportionally to wind up with the goal sample rate.
type AvgSampleWithMin struct {
	// ClearFrequencySec is how often the counters reset in seconds; default 30
	ClearFrequencySec int

	// GoalSampleRate is the average sample rate we're aiming for, across all
	// events. Default 10
	GoalSampleRate int

	// MaxKeys, if greater than 0, limits the number of distinct keys used to build
	// the sample rate map within the interval defined by `ClearFrequencySec`. Once
	// MaxKeys is reached, new keys will not be included in the sample rate map, but
	// existing keys will continue to be be counted.
	MaxKeys int

	// MinEventsPerSec - when the total number of events drops below this
	// threshold, sampling will cease. default 50
	MinEventsPerSec int

	savedSampleRates map[string]int
	currentCounts    map[string]int

	// haveData indicates that we have gotten a sample of traffic. Before we've
	// gotten any samples of traffic, we should we should use the default goal
	// sample rate for all events instead of sampling everything at 1
	haveData bool

	lock sync.Mutex
}

func (a *AvgSampleWithMin) Start() error {
	// apply defaults
	if a.ClearFrequencySec == 0 {
		a.ClearFrequencySec = 30
	}
	if a.GoalSampleRate == 0 {
		a.GoalSampleRate = 10
	}
	if a.MinEventsPerSec == 0 {
		a.MinEventsPerSec = 50
	}

	// initialize internal variables
	a.savedSampleRates = make(map[string]int)
	a.currentCounts = make(map[string]int)

	// spin up calculator
	go func() {
		ticker := time.NewTicker(time.Second * time.Duration(a.ClearFrequencySec))
		for range ticker.C {
			a.updateMaps()
		}
	}()
	return nil
}

// updateMaps calculates a new saved rate map based on the contents of the
// counter map
func (a *AvgSampleWithMin) updateMaps() {
	// make a local copy of the sample counters for calculation
	a.lock.Lock()
	tmpCounts := a.currentCounts
	a.currentCounts = make(map[string]int)
	a.lock.Unlock()
	newSavedSampleRates := make(map[string]int)
	// short circuit if no traffic
	numKeys := len(tmpCounts)
	if numKeys == 0 {
		// no traffic the last 30s. clear the result map
		a.lock.Lock()
		defer a.lock.Unlock()
		a.savedSampleRates = newSavedSampleRates
		return
	}

	// Goal events to send this interval is the total count of received events
	// divided by the desired average sample rate
	var sumEvents int
	for _, count := range tmpCounts {
		sumEvents += count
	}
	goalCount := float64(sumEvents) / float64(a.GoalSampleRate)
	// check to see if we fall below the minimum
	if sumEvents < a.MinEventsPerSec*a.ClearFrequencySec {
		// we still need to go through each key to set sample rates individually
		for k := range tmpCounts {
			newSavedSampleRates[k] = 1
		}
		a.lock.Lock()
		defer a.lock.Unlock()
		a.savedSampleRates = newSavedSampleRates
		return
	}
	// goalRatio is the goalCount divided by the sum of all the log values - it
	// determines what percentage of the total event space belongs to each key
	var logSum float64
	for _, count := range tmpCounts {
		logSum += math.Log10(float64(count))
	}
	// Note that this can produce Inf if logSum is 0
	goalRatio := goalCount / logSum

	// must go through the keys in a fixed order to prevent rounding from changing
	// results
	keys := make([]string, len(tmpCounts))
	var i int
	for k := range tmpCounts {
		keys[i] = k
		i++
	}
	sort.Strings(keys)

	// goal number of events per key is goalRatio * key count, but never less than
	// one. If a key falls below its goal, it gets a sample rate of 1 and the
	// extra available events get passed on down the line.
	keysRemaining := len(tmpCounts)
	var extra float64
	for _, key := range keys {
		count := float64(tmpCounts[key])
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
	a.lock.Lock()
	defer a.lock.Unlock()
	a.savedSampleRates = newSavedSampleRates
	a.haveData = true
}

// GetSampleRate takes a key and returns the appropriate sample rate for that
// key
func (a *AvgSampleWithMin) GetSampleRate(key string) int {
	a.lock.Lock()
	defer a.lock.Unlock()

	// Enforce MaxKeys limit on the size of the map
	if a.MaxKeys > 0 {
		// If a key already exists, increment it. If not, but we're under the limit, store a new key
		if _, found := a.currentCounts[key]; found || len(a.currentCounts) < a.MaxKeys {
			a.currentCounts[key]++
		}
	} else {
		a.currentCounts[key]++
	}
	if !a.haveData {
		return a.GoalSampleRate
	}
	if rate, found := a.savedSampleRates[key]; found {
		return rate
	}
	return 1
}

// SaveState is not implemented
func (a *AvgSampleWithMin) SaveState() ([]byte, error) {
	return nil, nil
}

// LoadState is not implemented
func (a *AvgSampleWithMin) LoadState(state []byte) error {
	return nil
}

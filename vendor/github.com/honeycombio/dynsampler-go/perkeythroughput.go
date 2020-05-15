package dynsampler

import (
	"math"
	"sync"
	"time"
)

// PerKeyThroughput implements Sampler and attempts to meet a goal of a fixed
// number of events per key per second sent to Honeycomb.
//
// This method is to guarantee that at most a certain number of events per key
// get transmitted, no matter how many keys you have or how much traffic comes
// through. In other words, if capturing a minimum amount of traffic per key is
// important but beyond that doesn't matter much, this is the best method.
type PerKeyThroughput struct {
	// ClearFrequency is how often the counters reset in seconds; default 30
	ClearFrequencySec int

	// PerKeyThroughputPerSec is the target number of events to send per second
	// per key. Sample rates are generated on a per key basis to squash the
	// throughput down to match the goal throughput. default 10
	PerKeyThroughputPerSec int

	// MaxKeys, if greater than 0, limits the number of distinct keys used to build
	// the sample rate map within the interval defined by `ClearFrequencySec`. Once
	// MaxKeys is reached, new keys will not be included in the sample rate map, but
	// existing keys will continue to be be counted.
	MaxKeys int

	savedSampleRates map[string]int
	currentCounts    map[string]int

	lock sync.Mutex
}

func (p *PerKeyThroughput) Start() error {
	// apply defaults
	if p.ClearFrequencySec == 0 {
		p.ClearFrequencySec = 30
	}
	if p.PerKeyThroughputPerSec == 0 {
		p.PerKeyThroughputPerSec = 10
	}

	// initialize internal variables
	p.savedSampleRates = make(map[string]int)
	p.currentCounts = make(map[string]int)

	// spin up calculator
	go func() {
		ticker := time.NewTicker(time.Second * time.Duration(p.ClearFrequencySec))
		for range ticker.C {
			p.updateMaps()
		}
	}()
	return nil
}

// updateMaps calculates a new saved rate map based on the contents of the
// counter map
func (p *PerKeyThroughput) updateMaps() {
	// make a local copy of the sample counters for calculation
	p.lock.Lock()
	tmpCounts := p.currentCounts
	p.currentCounts = make(map[string]int)
	p.lock.Unlock()
	// short circuit if no traffic
	numKeys := len(tmpCounts)
	if numKeys == 0 {
		// no traffic the last 30s. clear the result map
		p.lock.Lock()
		defer p.lock.Unlock()
		p.savedSampleRates = make(map[string]int)
		return
	}
	actualPerKeyRate := p.PerKeyThroughputPerSec * p.ClearFrequencySec
	// for each key, calculate sample rate by dividing counted events by the
	// desired number of events
	newSavedSampleRates := make(map[string]int)
	for k, v := range tmpCounts {
		rate := int(math.Max(1, (float64(v) / float64(actualPerKeyRate))))
		newSavedSampleRates[k] = rate
	}
	// save newly calculated sample rates
	p.lock.Lock()
	defer p.lock.Unlock()
	p.savedSampleRates = newSavedSampleRates
}

// GetSampleRate takes a key and returns the appropriate sample rate for that
// key
func (p *PerKeyThroughput) GetSampleRate(key string) int {
	p.lock.Lock()
	defer p.lock.Unlock()
	// Enforce MaxKeys limit on the size of the map
	if p.MaxKeys > 0 {
		// If a key already exists, increment it. If not, but we're under the limit, store a new key
		if _, found := p.currentCounts[key]; found || len(p.currentCounts) < p.MaxKeys {
			p.currentCounts[key]++
		}
	} else {
		p.currentCounts[key]++
	}
	if rate, found := p.savedSampleRates[key]; found {
		return rate
	}
	return 1
}

// SaveState is not implemented
func (p *PerKeyThroughput) SaveState() ([]byte, error) {
	return nil, nil
}

// LoadState is not implemented
func (p *PerKeyThroughput) LoadState(state []byte) error {
	return nil
}

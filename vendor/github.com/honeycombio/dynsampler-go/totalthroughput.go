package dynsampler

import (
	"math"
	"sync"
	"time"
)

// TotalThroughput implements Sampler and attempts to meet a goal of a fixed
// number of events per second sent to Honeycomb.
//
// If your key space is sharded across different servers, this is a good method
// for making sure each server sends roughly the same volume of content to
// Honeycomb. It performs poorly when active the keyspace is very large.
//
// GoalThroughputSec * ClearFrequencySec defines the upper limit of the number
// of keys that can be reported and stay under the goal, but with that many
// keys, you'll only get one event per key per ClearFrequencySec, which is very
// coarse. You should aim for at least 1 event per key per sec to 1 event per
// key per 10sec to get reasonable data. In other words, the number of active
// keys should be less than 10*GoalThroughputSec.
type TotalThroughput struct {
	// ClearFrequency is how often the counters reset in seconds; default 30
	ClearFrequencySec int

	// GoalThroughputPerSec is the target number of events to send per second.
	// Sample rates are generated to squash the total throughput down to match the
	// goal throughput. Actual throughput may exceed goal throughput. default 100
	GoalThroughputPerSec int

	savedSampleRates map[string]int
	currentCounts    map[string]int

	lock sync.Mutex
}

func (t *TotalThroughput) Start() error {
	// apply defaults
	if t.ClearFrequencySec == 0 {
		t.ClearFrequencySec = 30
	}
	if t.GoalThroughputPerSec == 0 {
		t.GoalThroughputPerSec = 100
	}

	// initialize internal variables
	t.savedSampleRates = make(map[string]int)
	t.currentCounts = make(map[string]int)

	// spin up calculator
	go func() {
		ticker := time.NewTicker(time.Second * time.Duration(t.ClearFrequencySec))
		for range ticker.C {
			t.updateMaps()
		}
	}()
	return nil
}

// updateMaps calculates a new saved rate map based on the contents of the
// counter map
func (t *TotalThroughput) updateMaps() {
	// make a local copy of the sample counters for calculation
	t.lock.Lock()
	tmpCounts := t.currentCounts
	t.currentCounts = make(map[string]int)
	t.lock.Unlock()
	// short circuit if no traffic
	numKeys := len(tmpCounts)
	if numKeys == 0 {
		// no traffic the last 30s. clear the result map
		t.lock.Lock()
		defer t.lock.Unlock()
		t.savedSampleRates = make(map[string]int)
		return
	}
	// figure out our target throughput per key over ClearFrequencySec
	totalGoalThroughput := t.GoalThroughputPerSec * t.ClearFrequencySec
	// floor the throughput but min should be 1 event per bucket per time period
	throughputPerKey := int(math.Max(1, float64(totalGoalThroughput)/float64(numKeys)))
	// for each key, calculate sample rate by dividing counted events by the
	// desired number of events
	newSavedSampleRates := make(map[string]int)
	for k, v := range tmpCounts {
		rate := int(math.Max(1, (float64(v) / float64(throughputPerKey))))
		newSavedSampleRates[k] = rate
	}
	// save newly calculated sample rates
	t.lock.Lock()
	defer t.lock.Unlock()
	t.savedSampleRates = newSavedSampleRates
}

// GetSampleRate takes a key and returns the appropriate sample rate for that
// key
func (t *TotalThroughput) GetSampleRate(key string) int {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.currentCounts[key]++
	if rate, found := t.savedSampleRates[key]; found {
		return rate
	}
	return 1
}

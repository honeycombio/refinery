package dynsampler

import (
	"sync"
	"time"
)

// OnlyOnce implements Sampler and returns a sample rate of 1 the first time a
// key is seen and 1,000,000,000 every subsequent time.  Essentially, this means
// that every key will be reported the first time it's seen during each
// ClearFrequencySec and never again.  Set ClearFrequencySec to -1 to report
// each key only once for the life of the process.
//
// (Note that it's not guaranteed that each key will be reported exactly once,
// just that the first seen event will be reported and subsequent events are
// unlikely to be reported. It is probable that an additional event will be
// reported for every billion times the key appears.)
//
// This emulates what you might expect from something catching stack traces -
// the first one is important but every subsequent one just repeats the same
// information.
type OnlyOnce struct {
	// ClearFrequencySec is how often the counters reset in seconds; default 30
	ClearFrequencySec int

	seen map[string]bool
	lock sync.Mutex
}

// Start initializes the static dynsampler
func (o *OnlyOnce) Start() error {
	//
	if o.ClearFrequencySec == -1 {
		return nil
	}
	if o.ClearFrequencySec == 0 {
		o.ClearFrequencySec = 30
	}
	o.seen = make(map[string]bool)

	// spin up calculator
	go func() {
		ticker := time.NewTicker(time.Second * time.Duration(o.ClearFrequencySec))
		for range ticker.C {
			o.updateMaps()
		}
	}()
	return nil
}

func (o *OnlyOnce) updateMaps() {
	o.lock.Lock()
	defer o.lock.Unlock()
	o.seen = make(map[string]bool)
}

// GetSampleRate takes a key and returns the appropriate sample rate for that
// key
func (o *OnlyOnce) GetSampleRate(key string) int {
	o.lock.Lock()
	defer o.lock.Unlock()
	if _, found := o.seen[key]; found {
		return 1000000000
	}
	o.seen[key] = true
	return 1
}

// SaveState is not implemented
func (o *OnlyOnce) SaveState() ([]byte, error) {
	return nil, nil
}

// LoadState is not implemented
func (o *OnlyOnce) LoadState(state []byte) error {
	return nil
}

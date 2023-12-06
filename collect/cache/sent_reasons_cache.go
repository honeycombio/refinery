package cache

import (
	"sync"
)

// SentReasonsCache is a cache of reasons a trace was sent.
// It acts as a mapping between the string representation of send reason
// and a uint.
// This is used to reduce the memory footprint of the trace cache.
type SentReasonsCache struct {
	// TODO: maybe add a metric for the size of this cache?
	data    map[string]uint
	counter uint

	mu sync.Mutex
}

// NewSentReasonsCache returns a new SentReasonsCache.
func NewSentReasonsCache() *SentReasonsCache {
	return &SentReasonsCache{
		data:    map[string]uint{},
		counter: 1,
	}
}

// Set adds a new reason to the cache, returning the key.
// The key is generated by incrementing a counter.
func (c *SentReasonsCache) Set(key string) uint {
	c.mu.Lock()
	defer c.mu.Unlock()

	val, ok := c.data[key]
	if !ok {
		val = c.counter
		c.data[key] = val
		c.counter++
	}
	return val
}

// Get returns a reason from the cache, if it exists.
func (c *SentReasonsCache) Get(key uint) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for k, v := range c.data {
		if v == key {
			return k, true
		}
	}
	return "", false
}

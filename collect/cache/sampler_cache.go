package cache

import (
	"math/rand"
	"slices"
	"sync"

	"github.com/dgryski/go-wyhash"
	"github.com/honeycombio/refinery/sample"
)

type SamplerKeyer interface {
	GetSamplerKey(prefix string) string
}

type SamplerCache struct {
	mut                   sync.RWMutex
	hashSeed              uint64
	samplerIdx            []uint64
	samplersByDestination map[uint64]sample.Sampler
	samplerFactory        *sample.SamplerFactory
	datasetPrefix         string
}

func NewSamplerCache(samplerFactory *sample.SamplerFactory, datasetPrefix string) *SamplerCache {
	return &SamplerCache{
		hashSeed:              rand.Uint64(),
		samplersByDestination: make(map[uint64]sample.Sampler),
		samplerFactory:        samplerFactory,
		datasetPrefix:         datasetPrefix,
	}
}

func (c *SamplerCache) Get(trace SamplerKeyer) (sample.Sampler, int) {
	samplerKey := trace.GetSamplerKey(c.datasetPrefix)
	hash := wyhash.Hash([]byte(samplerKey), c.hashSeed)

	c.mut.RLock()
	defer c.mut.RUnlock()
	sampler, ok := c.samplersByDestination[hash]
	if !ok {
		c.mut.Lock()
		c.samplerIdx = append(c.samplerIdx, hash)
		slices.SortStableFunc(c.samplerIdx, func(i, j uint64) int {
			return int(i - j)
		})
		idx := slices.Index(c.samplerIdx, hash)
		sampler = c.samplerFactory.GetSamplerImplementationForKey(samplerKey)
		c.samplersByDestination[hash] = sampler
		c.mut.Unlock()
		return sampler, int(idx)
	}

	return sampler, -1
}

func (c *SamplerCache) GetByIndex(idx int) sample.Sampler {
	c.mut.RLock()
	defer c.mut.RUnlock()
	if idx < 0 || idx >= len(c.samplerIdx) {
		return nil
	}
	return c.samplersByDestination[c.samplerIdx[idx]]
}

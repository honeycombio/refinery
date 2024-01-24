package cache_test

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/honeycombio/refinery/collect/cache"
	"github.com/honeycombio/refinery/metrics"
	"github.com/stretchr/testify/assert"
)

func TestSentReasonCache(t *testing.T) {
	s := &metrics.MockMetrics{}
	s.Start()
	c := cache.NewSentReasonsCache(s)
	keys := make([]uint, 0)
	entries := []string{"foo", "bar", "baz"}
	for _, item := range entries {
		keys = append(keys, c.Set(item))
	}
	for i, key := range keys {
		item, ok := c.Get(key)
		assert.True(t, ok, "key %d should exist", key)
		assert.Equal(t, entries[i], item)
	}
}

func BenchmarkSentReasonCache_Set(b *testing.B) {
	s := &metrics.MockMetrics{}
	s.Start()
	for _, numItems := range []int{10, 100, 1000, 10000, 100000} {
		entries := make([]string, numItems)
		for i := 0; i < numItems; i++ {
			entries[i] = randomString(50)
		}
		b.Run(strconv.Itoa(numItems), func(b *testing.B) {
			cache := cache.NewSentReasonsCache(s)
			for i := 0; i < b.N; i++ {
				cache.Set(entries[seededRand.Intn(numItems)])
			}
		})
	}
}
func BenchmarkSentReasonCache_Get(b *testing.B) {
	s := &metrics.MockMetrics{}
	s.Start()
	for _, numItems := range []int{10, 100, 1000, 10000, 100000} {
		cache := cache.NewSentReasonsCache(s)
		for i := 0; i < numItems; i++ {
			cache.Set(randomString(50))
		}
		b.Run(strconv.Itoa(numItems), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = cache.Get(uint(seededRand.Intn(numItems)))
			}
		})
	}
}

func BenchmarkSentReasonsCache_Get_Parallel(b *testing.B) {
	for _, numGoroutines := range []int{1, 50, 300} {
		for _, numUniqueEntries := range []int{50, 500, 2000} {
			b.Run(fmt.Sprintf("entries%d-g%d", numUniqueEntries, numGoroutines), func(b *testing.B) {
				s := &metrics.MockMetrics{}
				s.Start()
				cache := cache.NewSentReasonsCache(s)

				entries := make([]string, numUniqueEntries)
				for i := 0; i < numUniqueEntries; i++ {
					entries[i] = randomString(50)
					cache.Set(entries[i])
				}

				wg := sync.WaitGroup{}
				count := b.N / numGoroutines
				if count == 0 {
					count = 1
				}
				b.ResetTimer()
				for g := 0; g < numGoroutines; g++ {
					wg.Add(1)
					go func() {
						for n := 0; n < count; n++ {
							_, _ = cache.Get(uint(count % numUniqueEntries))
						}
						wg.Done()
					}()
				}
				wg.Wait()
			})
		}
	}
}

func BenchmarkSentReasonsCache_Set_Parallel(b *testing.B) {
	for _, numGoroutines := range []int{1, 50, 300} {
		for _, numUniqueEntries := range []int{50, 500, 2000} {
			b.Run(fmt.Sprintf("entries%d-g%d", numUniqueEntries, numGoroutines), func(b *testing.B) {
				s := &metrics.MockMetrics{}
				s.Start()
				entries := make([]string, numUniqueEntries)
				for i := 0; i < numUniqueEntries; i++ {
					entries[i] = randomString(50)
				}
				cache := cache.NewSentReasonsCache(s)
				wg := sync.WaitGroup{}
				count := b.N / numGoroutines
				if count == 0 {
					count = 1
				}
				b.ResetTimer()
				for g := 0; g < numGoroutines; g++ {
					wg.Add(1)
					go func() {
						for n := 0; n < count; n++ {
							_ = cache.Set(entries[count%numUniqueEntries])
						}
						wg.Done()
					}()
				}
				wg.Wait()
			})
		}
	}
}

const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var seededRand *rand.Rand = rand.New(
	rand.NewSource(time.Now().UnixNano()))

func stringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func randomString(length int) string {
	return stringWithCharset(length, charset)
}

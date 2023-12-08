package cache_test

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/honeycombio/refinery/collect/cache"
)

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

func BenchmarkSentReasonCache_Set(b *testing.B) {
	for _, numItems := range []int{10, 100, 1000, 10000, 100000} {
		entries := make([]string, numItems)
		for i := 0; i < numItems; i++ {
			entries[i] = randomString(50)
		}
		b.Run(strconv.Itoa(numItems), func(b *testing.B) {
			cache := cache.NewSentReasonsCache()
			for i := 0; i < b.N; i++ {
				cache.Set(entries[seededRand.Intn(numItems)])
			}
		})
	}
}
func BenchmarkSentReasonCache_Get(b *testing.B) {
	for _, numItems := range []int{10, 100, 1000, 10000, 100000} {
		cache := cache.NewSentReasonsCache()
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
func BenchmarkSentReasonsCache_Set_Parallel(b *testing.B) {
	for _, numGoroutines := range []int{1, 50, 300} {
		for _, numUniqueEntries := range []int{50, 500, 2000} {
			b.Run(fmt.Sprintf("f%d-g%d", numUniqueEntries, numGoroutines), func(b *testing.B) {
				entries := make([]string, numUniqueEntries)
				for i := 0; i < numUniqueEntries; i++ {
					entries[i] = randomString(50)
				}
				cache := cache.NewSentReasonsCache()
				b.ResetTimer()
				wg := sync.WaitGroup{}
				count := b.N / numGoroutines
				if count == 0 {
					count = 1
				}
				for g := 0; g < numGoroutines; g++ {
					wg.Add(1)
					go func() {
						for n := 0; n < count; n++ {
							cache.Set(entries[seededRand.Intn(numUniqueEntries)])
						}
						wg.Done()
					}()
				}
				wg.Wait()
			})
		}
	}
}

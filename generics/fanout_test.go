package generics

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFanout(t *testing.T) {
	input := []int{1, 2, 3, 4, 5}
	parallelism := 3
	workerFactory := func(i int) (func(int) int, func(int)) {
		worker := func(i int) int {
			return i * 2
		}
		return worker, nil
	}

	result := Fanout(input, parallelism, workerFactory, nil)
	assert.ElementsMatch(t, []int{2, 4, 6, 8, 10}, result)
}

func TestFanoutWithPredicate(t *testing.T) {
	input := []int{1, 2, 3, 4, 5}
	parallelism := 3
	workerFactory := func(i int) (func(int) int, func(int)) {
		worker := func(i int) int {
			return i * 2
		}
		return worker, nil
	}
	predicate := func(i int) bool {
		return i%4 == 0
	}

	result := Fanout(input, parallelism, workerFactory, predicate)
	assert.ElementsMatch(t, []int{4, 8}, result)
}

func TestFanoutWithCleanup(t *testing.T) {
	input := []int{1, 2, 3, 4, 5}
	parallelism := 4
	cleanups := []int{}
	mut := sync.Mutex{}
	workerFactory := func(i int) (func(int) int, func(int)) {
		worker := func(i int) int {
			return i * 2
		}
		cleanup := func(i int) {
			mut.Lock()
			cleanups = append(cleanups, i)
			mut.Unlock()
		}
		return worker, cleanup
	}

	result := Fanout(input, parallelism, workerFactory, nil)
	assert.ElementsMatch(t, []int{2, 4, 6, 8, 10}, result)
	assert.ElementsMatch(t, []int{0, 1, 2, 3}, cleanups)
}

var expected = map[int]int{
	1: 2,
	2: 4,
	3: 6,
	4: 8,
	5: 10,
}

func TestFanoutMap(t *testing.T) {
	input := []int{1, 2, 3, 4, 5}
	parallelism := 3
	workerFactory := func(i int) (func(int) int, func(int)) {
		worker := func(i int) int {
			return i * 2
		}
		return worker, nil
	}

	result := FanoutToMap(input, parallelism, workerFactory, nil)
	assert.EqualValues(t, expected, result)
}

func TestFanoutMapWithPredicate(t *testing.T) {
	input := []int{1, 2, 3, 4, 5}
	parallelism := 3
	workerFactory := func(i int) (func(int) int, func(int)) {
		worker := func(i int) int {
			return i * 2
		}
		return worker, nil
	}
	predicate := func(i int) bool {
		return i%4 == 0
	}

	result := FanoutToMap(input, parallelism, workerFactory, predicate)
	assert.EqualValues(t, map[int]int{2: 4, 4: 8}, result)
}

func TestFanoutMapWithCleanup(t *testing.T) {
	input := []int{1, 2, 3, 4, 5}
	parallelism := 4
	cleanups := []int{}
	mut := sync.Mutex{}
	workerFactory := func(i int) (func(int) int, func(int)) {
		worker := func(i int) int {
			return i * 2
		}
		cleanup := func(i int) {
			mut.Lock()
			cleanups = append(cleanups, i)
			mut.Unlock()
		}
		return worker, cleanup
	}

	result := FanoutToMap(input, parallelism, workerFactory, nil)
	assert.EqualValues(t, expected, result)
	assert.ElementsMatch(t, []int{0, 1, 2, 3}, cleanups)
}

func TestEasyFanout(t *testing.T) {
	input := []int{1, 2, 3, 4, 5}
	worker := func(i int) int {
		return i * 2
	}

	result := EasyFanout(input, 3, worker)
	assert.ElementsMatch(t, []int{2, 4, 6, 8, 10}, result)
}

func TestEasyFanoutToMap(t *testing.T) {
	input := []int{1, 2, 3, 4, 5}
	worker := func(i int) int {
		return i * 2
	}

	result := EasyFanoutToMap(input, 3, worker)
	assert.EqualValues(t, expected, result)
}

func BenchmarkFanoutParallelism(b *testing.B) {
	parallelisms := []int{1, 3, 6, 10, 25, 100}
	for _, parallelism := range parallelisms {
		b.Run(fmt.Sprintf("parallelism%02d", parallelism), func(b *testing.B) {

			input := make([]int, b.N)
			for i := range input {
				input[i] = i
			}

			workerFactory := func(i int) (func(int) string, func(int)) {
				worker := func(i int) string {
					h := sha256.Sum256(([]byte(fmt.Sprintf("%d", i))))
					time.Sleep(1 * time.Millisecond)
					return hex.EncodeToString(h[:])
				}
				cleanup := func(i int) {}
				return worker, cleanup
			}
			b.ResetTimer()
			_ = Fanout(input, parallelism, workerFactory, nil)
		})
	}
}

func BenchmarkFanoutMapParallelism(b *testing.B) {
	parallelisms := []int{1, 3, 6, 10, 25, 100}
	for _, parallelism := range parallelisms {
		b.Run(fmt.Sprintf("parallelism%02d", parallelism), func(b *testing.B) {

			input := make([]int, b.N)
			for i := range input {
				input[i] = i
			}

			workerFactory := func(i int) (func(int) string, func(int)) {
				worker := func(i int) string {
					h := sha256.Sum256(([]byte(fmt.Sprintf("%d", i))))
					time.Sleep(1 * time.Millisecond)
					return hex.EncodeToString(h[:])
				}
				cleanup := func(i int) {}
				return worker, cleanup
			}
			b.ResetTimer()
			_ = FanoutToMap(input, parallelism, workerFactory, nil)
		})
	}
}

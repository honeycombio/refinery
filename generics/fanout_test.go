package generics

import (
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

func TestFanoutIsActuallyParallel(t *testing.T) {
	input := []int{1, 2, 3, 4, 5}
	workerFactory := func(i int) (func(int) int, func(int)) {
		worker := func(i int) int {
			time.Sleep(time.Duration(i) * time.Millisecond)
			return i * 2
		}
		cleanup := func(i int) {}
		return worker, cleanup
	}

	// with parallelism = 1, this should take at least 15ms
	start := time.Now()
	result := Fanout(input, 1, workerFactory, nil)
	dur := time.Since(start)
	assert.ElementsMatch(t, []int{2, 4, 6, 8, 10}, result)
	assert.Greater(t, dur.Milliseconds(), int64(14))

	// with parallelism = 5, this should take about 5ms, although
	// it might take longer if the machine is under heavy load, so we use an
	// eventually loop to make sure it passes
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		start = time.Now()
		result = Fanout(input, 5, workerFactory, nil)
		dur = time.Since(start)
		assert.ElementsMatch(t, []int{2, 4, 6, 8, 10}, result)
		assert.Less(t, dur.Milliseconds(), int64(10))
	}, 2*time.Second, 50*time.Millisecond)
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
	cleanupTotal := 0
	mut := sync.Mutex{}
	workerFactory := func(i int) (func(int) int, func(int)) {
		worker := func(i int) int {
			return i * 2
		}
		cleanup := func(i int) {
			mut.Lock()
			cleanupTotal += i
			mut.Unlock()
		}
		return worker, cleanup
	}

	result := Fanout(input, parallelism, workerFactory, nil)
	assert.ElementsMatch(t, []int{2, 4, 6, 8, 10}, result)
	assert.Equal(t, 6, cleanupTotal) // 0 + 1 + 2 + 3
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

func TestFanoutMapIsActuallyParallel(t *testing.T) {
	input := []int{1, 2, 3, 4, 5}
	workerFactory := func(i int) (func(int) int, func(int)) {
		worker := func(i int) int {
			time.Sleep(time.Duration(i) * time.Millisecond)
			return i * 2
		}
		cleanup := func(i int) {}
		return worker, cleanup
	}

	// with parallelism = 1, this should take at least 15ms
	start := time.Now()
	result := FanoutToMap(input, 1, workerFactory, nil)
	dur := time.Since(start)
	assert.EqualValues(t, expected, result)
	assert.Greater(t, dur.Milliseconds(), int64(14))

	// with parallelism = 5, this should take about 5ms
	// but we'll give it a little extra time to account for CI load
	// and try for a while to get it right
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		start = time.Now()
		result = FanoutToMap(input, 5, workerFactory, nil)
		dur = time.Since(start)
		assert.EqualValues(t, expected, result)
		assert.Less(t, dur.Milliseconds(), int64(10))
	}, 2*time.Second, 50*time.Millisecond)
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
	cleanupTotal := 0
	mut := sync.Mutex{}
	workerFactory := func(i int) (func(int) int, func(int)) {
		worker := func(i int) int {
			return i * 2
		}
		cleanup := func(i int) {
			mut.Lock()
			cleanupTotal += i
			mut.Unlock()
		}
		return worker, cleanup
	}

	result := FanoutToMap(input, parallelism, workerFactory, nil)
	assert.EqualValues(t, expected, result)
	assert.Equal(t, 6, cleanupTotal) // 0 + 1 + 2 + 3
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

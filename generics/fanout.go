package generics

import "sync"

// Fanout takes a slice of input, a parallelism factor, and a worker factory. It
// calls the generated worker on every element of the input, and returns a
// (possibly filtered) slice of the outputs in no particular order. Only the
// outputs that pass the predicate (if it is not nil) will be added to the
// slice.
//
// The factory takes an integer (the worker number) and constructs a function of
// type func(T) U that processes a single input and produces a single output. It
// also constructs a cleanup function, which may be nil. The cleanup function is
// called once for each worker, after the worker has completed processing all of
// its inputs. It is given the same index as the corresponding worker factory.
//
// If predicate is not nil, it will only add the output to the result slice if
// the predicate returns true. It will fan out the input to the worker function
// in parallel, and fan in the results to the output slice.
func Fanout[T, U any](input []T, parallelism int, workerFactory func(int) (worker func(T) U, cleanup func(int)), predicate func(U) bool) []U {
	result := make([]U, 0)

	fanoutChan := make(chan T, parallelism)
	faninChan := make(chan U, parallelism)

	// send all the trace IDs to the fanout channel
	wgFans := sync.WaitGroup{}
	wgFans.Add(1)
	go func() {
		defer wgFans.Done()
		defer close(fanoutChan)
		for i := range input {
			fanoutChan <- input[i]
		}
	}()

	wgFans.Add(1)
	go func() {
		defer wgFans.Done()
		for r := range faninChan {
			result = append(result, r)
		}
	}()

	wgWorkers := sync.WaitGroup{}
	for i := 0; i < parallelism; i++ {
		wgWorkers.Add(1)
		worker, cleanup := workerFactory(i)
		go func(i int) {
			defer wgWorkers.Done()
			if cleanup != nil {
				defer cleanup(i)
			}
			for u := range fanoutChan {
				product := worker(u)
				if predicate == nil || predicate(product) {
					faninChan <- product
				}
			}
		}(i)
	}

	// wait for the workers to finish
	wgWorkers.Wait()
	// now we can close the fanin channel and wait for the fanin goroutine to finish
	// fanout should already be done but this makes sure we don't lose track of it
	close(faninChan)
	wgFans.Wait()

	return result
}

// EasyFanout is a convenience function for when you don't need all the
// features. It takes a slice of input, a parallelism factor, and a worker
// function. It calls the worker on every element of the input with the
// specified parallelism, and returns a slice of the outputs in no particular
// order.
func EasyFanout[T, U any](input []T, parallelism int, worker func(T) U) []U {
	return Fanout(input, parallelism, func(int) (func(T) U, func(int)) {
		return worker, nil
	}, nil)
}

// FanoutToMap takes a slice of input, a parallelism factor, and a worker
// factory. It calls the generated worker on every element of the input, and
// returns a (possibly filtered) map of the inputs to the outputs. Only the
// outputs that pass the predicate (if it is not nil) will be added to the map.
//
// The factory takes an integer (the worker number) and constructs a function of
// type func(T) U that processes a single input and produces a single output. It
// also constructs a cleanup function, which may be nil. The cleanup function is
// called once for each worker, after the worker has completed processing all of
// its inputs. It is given the same index as the corresponding worker factory.
//
// If predicate is not nil, it will only add the output to the result slice if
// the predicate returns true. It will fan out the input to the worker function
// in parallel, and fan in the results to the output slice.
func FanoutToMap[T comparable, U any](input []T, parallelism int, workerFactory func(int) (worker func(T) U, cleanup func(int)), predicate func(U) bool) map[T]U {
	result := make(map[T]U)
	type resultPair struct {
		key T
		val U
	}

	fanoutChan := make(chan T, parallelism)
	faninChan := make(chan resultPair, parallelism)

	// send all the trace IDs to the fanout channel
	wgFans := sync.WaitGroup{}
	wgFans.Add(1)
	go func() {
		defer wgFans.Done()
		defer close(fanoutChan)
		for i := range input {
			fanoutChan <- input[i]
		}
	}()

	wgFans.Add(1)
	go func() {
		defer wgFans.Done()
		for r := range faninChan {
			result[r.key] = r.val
		}
	}()

	wgWorkers := sync.WaitGroup{}
	for i := 0; i < parallelism; i++ {
		wgWorkers.Add(1)
		worker, cleanup := workerFactory(i)
		go func(i int) {
			defer wgWorkers.Done()
			if cleanup != nil {
				defer cleanup(i)
			}
			for t := range fanoutChan {
				product := worker(t)
				if predicate == nil || predicate(product) {
					faninChan <- resultPair{t, product}
				}
			}
		}(i)
	}

	// wait for the workers to finish
	wgWorkers.Wait()
	// now we can close the fanin channel and wait for the fanin goroutine to finish
	// fanout should already be done but this makes sure we don't lose track of it
	close(faninChan)
	wgFans.Wait()

	return result
}

// EasyFanoutToMap is a convenience function for when you don't need all the
// features. It takes a slice of input, a parallelism factor, and a worker
// function. It calls the worker on every element of the input with the
// specified parallelism, and returns a map of the inputs to the outputs.
func EasyFanoutToMap[T comparable, U any](input []T, parallelism int, worker func(T) U) map[T]U {
	return FanoutToMap(input, parallelism, func(int) (func(T) U, func(int)) {
		return worker, nil
	}, nil)
}

// FanoutChunksToMap takes a slice of input, a chunk size, a maximum parallelism
// factor, and a worker factory. It calls the generated worker on every chunk of
// the input, and returns a (possibly filtered) map of the inputs to the
// outputs. Only the outputs that pass the predicate (if it is not nil) will be
// added to the map.
//
// The maximum parallelism factor is the maximum number of workers that will be
// run in parallel. The actual number of workers will be the minimum of the
// maximum parallelism factor and the number of chunks in the input.
func FanoutChunksToMap[T comparable, U any](input []T, chunkSize int, maxParallelism int, workerFactory func(int) (worker func([]T) map[T]U, cleanup func(int)), predicate func(U) bool) map[T]U {
	result := make(map[T]U, 0)

	if chunkSize <= 0 {
		chunkSize = 1
	}

	type resultPair struct {
		key T
		val U
	}
	parallelism := min(maxParallelism, max(len(input)/chunkSize, 1))
	fanoutChan := make(chan []T, parallelism)
	faninChan := make(chan resultPair, parallelism)

	// send all the trace IDs to the fanout channel
	wgFans := sync.WaitGroup{}
	wgFans.Add(1)
	go func() {
		defer wgFans.Done()
		defer close(fanoutChan)
		for i := 0; i < len(input); i += chunkSize {
			end := min(i+chunkSize, len(input))
			fanoutChan <- input[i:end]
		}
	}()

	wgFans.Add(1)
	go func() {
		defer wgFans.Done()
		for r := range faninChan {
			result[r.key] = r.val
		}
	}()

	wgWorkers := sync.WaitGroup{}
	for i := 0; i < parallelism; i++ {
		wgWorkers.Add(1)
		worker, cleanup := workerFactory(i)
		go func(i int) {
			defer wgWorkers.Done()
			if cleanup != nil {
				defer cleanup(i)
			}
			for u := range fanoutChan {
				products := worker(u)
				for key, product := range products {
					if predicate == nil || predicate(product) {
						faninChan <- resultPair{key: key, val: product}
					}
				}
			}
		}(i)
	}

	// wait for the workers to finish
	wgWorkers.Wait()
	// now we can close the fanin channel and wait for the fanin goroutine to finish
	// fanout should already be done but this makes sure we don't lose track of it
	close(faninChan)
	wgFans.Wait()

	return result
}

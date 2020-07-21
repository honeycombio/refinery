package collect

import (
	"sync"

	"github.com/honeycombio/samproxy/types"
)

type spanInput struct {
	ch          <-chan *types.Span
	concurrency int
	name        string
}

// mergeIncomingSpans accepts a variable number of spanInputs. Each spanInput says 
// which span channel to pull from and how many goroutines should be doing the pulling.
// By adjusting the concurrency field in the spanInput, you can give some channels a 
// higher throughput than others. The resulting channel merges inputs from all listed 
// incoming channels.
func mergeIncomingSpans(in ...spanInput) <-chan *types.Span {
	var wg sync.WaitGroup
	out := make(chan *types.Span)

	output := func(c spanInput) {
		for n := range c.ch {
			out <- n
		}
		wg.Done()
	}

	for _, c := range in {
		for i := 0; i < c.concurrency; i++ {
			wg.Add(1)
			go output(c)
		}
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

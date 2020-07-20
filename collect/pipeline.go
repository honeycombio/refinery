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

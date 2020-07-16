package collect

import (
	"sync"

	"github.com/honeycombio/samproxy/types"
)

func mergeIncomingSpans(in ...<-chan *types.Span) <-chan *types.Span {
	var wg sync.WaitGroup
	out := make(chan *types.Span)

	output := func(c <-chan *types.Span) {
		for n := range c {
			out <- n
		}
		wg.Done()
	}

	wg.Add(len(in))

	for _, c := range in {
		go output(c)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

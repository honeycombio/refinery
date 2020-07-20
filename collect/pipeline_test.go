package collect

import (
	"testing"

	"github.com/honeycombio/samproxy/types"
)

func TestMergeIncomingSpans(t *testing.T) {
	n := 20

	ch1 := make(chan *types.Span)
	ch2 := make(chan *types.Span)

	out := mergeIncomingSpans(spanInput{
		ch:          ch1,
		concurrency: 1,
		name:        "1",
	}, spanInput{
		ch:          ch2,
		concurrency: 2,
		name:        "2",
	})

	var count int

	go func() {
		defer close(ch1)
		defer close(ch2)

		for i := 0; i < n/2; i++ {
			ch1 <- &types.Span{}
			ch2 <- &types.Span{}
		}
	}()

	for range out {
		count++
	}

	if count != n {
		t.Fail()
	}
}

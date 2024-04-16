package collect

import (
	"sync"
)

// limiter is a simple goroutine limiter that allows you to limit the number of
// retries that a function can be called.
type limiter struct {
	name           string
	limit          int
	wg             *sync.WaitGroup
	done           chan struct{}
	retryExhausted chan struct{}
}

func newLimiter(name string, n int) *limiter {
	return &limiter{
		name:           name,
		limit:          n,
		wg:             &sync.WaitGroup{},
		done:           make(chan struct{}),
		retryExhausted: make(chan struct{}),
	}
}

func (l *limiter) Go(fn func() error) {
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		limit := l.limit
		var err error
		for limit > 0 {
			select {
			case <-l.done:
				return
			default:
				err = fn()
				limit = limit - 1
			}
		}

		// If we've reached the limit, we need to signal
		// a shutdown.
		if err != nil {
			close(l.retryExhausted)
		}

	}()
}

func (l *limiter) Close() {
	close(l.done)
	l.wg.Wait()

	select {
	case <-l.retryExhausted:
	default:
		close(l.retryExhausted)
	}
}

func (l *limiter) RetryExhausted() <-chan struct{} {
	return l.retryExhausted
}

package collect

import "sync"

type retryLimiter struct {
	limit int
	wg    *sync.WaitGroup
	done  chan struct{}
}

func newRetryLimiter(n int) *retryLimiter {
	return &retryLimiter{
		limit: n,
		done:  make(chan struct{}),
		wg:    &sync.WaitGroup{},
	}
}

func (l *retryLimiter) Go(fn func()) {
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		limit := l.limit
		for limit > 0 {
			select {
			case <-l.done:
				return
			default:
				fn()
				limit = limit - 1
			}
		}
	}()
}

func (l *retryLimiter) Close() {
	close(l.done)
	l.wg.Wait()
}

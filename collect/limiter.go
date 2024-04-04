package collect

import "sync"

type limiter struct {
	limit int
	wg    *sync.WaitGroup
	done  chan struct{}
}

func newLimiter(n int) *limiter {
	return &limiter{
		limit: n,
		done:  make(chan struct{}),
		wg:    &sync.WaitGroup{},
	}
}

func (l *limiter) Go(fn func()) {
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

func (l *limiter) Close() {
	close(l.done)
	l.wg.Wait()
}

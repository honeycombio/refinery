package collect

type concurrencyLimiter struct {
	limit chan struct{}
	done  chan struct{}
}

func newConcurrencyLimiter(n int) *concurrencyLimiter {
	return &concurrencyLimiter{
		limit: make(chan struct{}, n),
		done:  make(chan struct{}),
	}
}

func (l *concurrencyLimiter) Go(fn func()) {

	select {
	case l.limit <- struct{}{}:
	case <-l.done:
		return
	}

	go func() {
		defer func() { <-l.limit }()
		fn()
	}()
}

func (l *concurrencyLimiter) Close() {
	close(l.done)

	// make sure all the existing goroutines can finish
	for i := 0; i < cap(l.limit); i++ {
		l.limit <- struct{}{}
	}
}

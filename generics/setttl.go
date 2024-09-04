package generics

import (
	"cmp"
	"sort"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	"golang.org/x/exp/maps"
)

// SetWithTTL is a unique set of items with a TTL (time to live) for each item.
// After the TTL expires, the item is automatically removed from the set when either Members or Length is called.
// It is safe for concurrent use.
type SetWithTTL[T cmp.Ordered] struct {
	Items map[T]time.Time
	TTL   time.Duration
	Clock clockwork.Clock
	mut   sync.RWMutex
}

// NewSetWithTTL returns a new SetWithTTL with elements `es` and a TTL of `ttl`.
func NewSetWithTTL[T cmp.Ordered](ttl time.Duration, es ...T) *SetWithTTL[T] {
	s := &SetWithTTL[T]{
		Items: make(map[T]time.Time, len(es)),
		TTL:   ttl,
		Clock: clockwork.NewRealClock(),
	}
	s.Add(es...)
	return s
}

// Add adds elements `es` to the SetWithTTL.
func (s *SetWithTTL[T]) Add(es ...T) {
	s.mut.Lock()
	defer s.mut.Unlock()
	for _, e := range es {
		s.Items[e] = s.Clock.Now().Add(s.TTL)
	}
}

// Remove removes elements `es` from the SetWithTTL.
func (s *SetWithTTL[T]) Remove(es ...T) {
	s.mut.Lock()
	defer s.mut.Unlock()
	for _, e := range es {
		delete(s.Items, e)
	}
}

// Contains returns true if the SetWithTTL contains `e`.
// We don't have to clean up first because the test checks the TTL.
func (s *SetWithTTL[T]) Contains(e T) bool {
	s.mut.RLock()
	item, ok := s.Items[e]
	s.mut.RUnlock()
	if !ok {
		return false
	}
	return item.After(s.Clock.Now())
}

func (s *SetWithTTL[T]) cleanup() int {
	s.mut.Lock()
	defer s.mut.Unlock()
	maps.DeleteFunc(s.Items, func(k T, exp time.Time) bool {
		return exp.Before(s.Clock.Now())
	})
	return len(s.Items)
}

// Members returns the unique elements of the SetWithTTL in sorted order.
// It also removes any items that have expired.
func (s *SetWithTTL[T]) Members() []T {
	s.cleanup()
	s.mut.RLock()
	members := maps.Keys(s.Items)
	s.mut.RUnlock()
	sort.Slice(members, func(i, j int) bool {
		return cmp.Less(members[i], members[j])
	})
	return members
}

// Length returns the number of items in the SetWithTTL after removing any expired items.
func (s *SetWithTTL[T]) Length() int {
	return s.cleanup()
}

package generics

import (
	"cmp"
	"sort"
	"time"

	"golang.org/x/exp/maps"
)

// SetWithTTL is a unique set of items with a TTL (time to live) for each item.
// After the TTL expires, the item is automatically removed from the set.
type SetWithTTL[T cmp.Ordered] struct {
	Items map[T]time.Time
	TTL   time.Duration
}

// NewSetWithTTL returns a new SetWithTTL with elements `es` and a TTL of `ttl`.
func NewSetWithTTL[T cmp.Ordered](ttl time.Duration, es ...T) SetWithTTL[T] {
	s := SetWithTTL[T]{
		Items: make(map[T]time.Time, len(es)),
		TTL:   ttl,
	}
	s.Add(es...)
	return s
}

// Add adds elements `es` to the SetWithTTL.
func (s SetWithTTL[T]) Add(es ...T) {
	for _, e := range es {
		s.Items[e] = time.Now().Add(s.TTL)
	}
}

// Remove removes elements `es` from the SetWithTTL.
func (s SetWithTTL[T]) Remove(es ...T) {
	for _, e := range es {
		delete(s.Items, e)
	}
}

// Contains returns true if the SetWithTTL contains `e`.
func (s SetWithTTL[T]) Contains(e T) bool {
	item, ok := s.Items[e]
	if !ok {
		return false
	}
	return item.After(time.Now())
}

func (s SetWithTTL[T]) cleanup() {
	maps.DeleteFunc(s.Items, func(k T, exp time.Time) bool {
		return exp.Before(time.Now())
	})
}

// Members returns the unique elements of the SetWithTTL in sorted order.
// It also removes any items that have expired.
func (s SetWithTTL[T]) Members() []T {
	s.cleanup()
	members := make([]T, 0, len(s.Items))
	for member := range s.Items {
		members = append(members, member)
	}
	sort.Slice(members, func(i, j int) bool {
		return cmp.Less(members[i], members[j])
	})
	return members
}

// Length returns the number of items in the SetWithTTL after removing any expired items.
func (s SetWithTTL[T]) Length() int {
	s.cleanup()
	return len(s.Items)
}

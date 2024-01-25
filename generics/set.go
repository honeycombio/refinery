package generics

import "golang.org/x/exp/maps"

// Set is a map[T]struct{}-backed unique set of items.
type Set[T comparable] map[T]struct{}

// NewSet returns a new Set with elements `es`.
func NewSet[T comparable](es ...T) Set[T] {
	s := make(Set[T], len(es))
	s.Add(es...)
	return s
}

func NewSetWithCapacity[T comparable](c int) Set[T] {
	return make(Set[T], c)
}

// Add adds elements `es` to the Set.
func (s Set[T]) Add(es ...T) {
	for _, e := range es {
		s[e] = struct{}{}
	}

}
func (s Set[T]) Remove(es ...T) {
	for _, e := range es {
		delete(s, e)
	}
}

// Add adds members of `b` to the Set.
func (s Set[T]) AddMembers(b Set[T]) {
	for v := range b {
		s[v] = struct{}{}
	}
}

// Contains returns true if the Set contains `e`.
func (s Set[T]) Contains(e T) bool {
	_, ok := s[e]
	return ok
}

// Members returns the unique elements of the Set in indeterminate order.
func (s Set[T]) Members() []T {
	return maps.Keys(s)
}

// Intersect returns the common elements of the Set and `b`.
func (s Set[T]) Intersect(b Set[T]) Set[T] {
	c := NewSet[T]()
	for v := range s {
		if b.Contains(v) {
			c.Add(v)
		}
	}
	return c
}

// Difference returns the elements from the Set that do not exist in `b`.
func (s Set[T]) Difference(b Set[T]) Set[T] {
	c := NewSet[T]()
	for v := range s {
		if !b.Contains(v) {
			c.Add(v)
		}
	}
	return c
}

// Union returns the a new Set containing the combination of all unique elements
// from the Set and `b`.
func (s Set[T]) Union(b Set[T]) Set[T] {
	c := make(Set[T], len(s)+len(b))
	c.AddMembers(s)
	c.AddMembers(b)
	return c
}

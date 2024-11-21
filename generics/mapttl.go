package generics

import (
	"cmp"
	"slices"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
)

type itemWithTTL[V any] struct {
	Value      V
	Expiration time.Time
}

// MapWithTTL is a map with a TTL (time to live) for each item. After the TTL expires,
// the item is automatically removed from the map when any of Length, Keys, SortedKeys,
// Values, or SortedValues is called.
type MapWithTTL[K cmp.Ordered, V any] struct {
	Items map[K]itemWithTTL[V]
	TTL   time.Duration
	Clock clockwork.Clock
	mut   sync.RWMutex
}

func NewMapWithTTL[K cmp.Ordered, V any](ttl time.Duration, initialValues map[K]V) *MapWithTTL[K, V] {
	m := &MapWithTTL[K, V]{
		Items: make(map[K]itemWithTTL[V]),
		TTL:   ttl,
		Clock: clockwork.NewRealClock(),
	}
	for k, v := range initialValues {
		m.Set(k, v)
	}
	return m
}

func (m *MapWithTTL[K, V]) Set(k K, v V) {
	m.mut.Lock()
	defer m.mut.Unlock()
	item := itemWithTTL[V]{Value: v, Expiration: m.Clock.Now().Add(m.TTL)}
	m.Items[k] = item
}

func (m *MapWithTTL[K, V]) Get(k K) (V, bool) {
	var zero V

	m.mut.RLock()
	defer m.mut.RUnlock()
	item, ok := m.Items[k]
	if !ok {
		return zero, false
	}
	if item.Expiration.Before(m.Clock.Now()) {
		return zero, false
	}
	return item.Value, true
}

func (m *MapWithTTL[K, V]) Delete(k K) {
	m.mut.Lock()
	defer m.mut.Unlock()
	delete(m.Items, k)
}

func (m *MapWithTTL[K, V]) cleanup() int {
	m.mut.Lock()
	defer m.mut.Unlock()
	count := 0
	for k, v := range m.Items {
		if v.Expiration.Before(m.Clock.Now()) {
			delete(m.Items, k)
			continue
		}
		// only count items that are not expired
		count++
	}
	return count
}

// Keys returns the keys of the map in arbitrary order
func (m *MapWithTTL[K, V]) Keys() []K {
	m.cleanup()
	m.mut.RLock()
	defer m.mut.RUnlock()
	keys := make([]K, 0, len(m.Items))
	for k := range m.Items {
		keys = append(keys, k)
	}
	return keys
}

// Keys returns the keys of the map, sorted
func (m *MapWithTTL[K, V]) SortedKeys() []K {
	keys := m.Keys()
	slices.Sort(keys)
	return keys
}

// values returns all the values in the map in arbitrary order
func (m *MapWithTTL[K, V]) Values() []V {
	m.cleanup()
	m.mut.RLock()
	defer m.mut.RUnlock()
	values := make([]V, 0, len(m.Items))
	for _, v := range m.Items {
		values = append(values, v.Value)
	}
	return values
}

// SortedValues returns the values of the map, sorted by key
func (m *MapWithTTL[K, V]) SortedValues() []V {
	keys := m.SortedKeys()

	m.mut.RLock()
	defer m.mut.RUnlock()
	values := make([]V, 0, len(keys))
	for _, k := range keys {
		values = append(values, m.Items[k].Value)
	}
	return values
}

func (m *MapWithTTL[K, V]) Length() int {
	return m.cleanup()
}

package generics

import (
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
)

func TestMapWithTTLBasics(t *testing.T) {
	m := NewMapWithTTL[string, string](100*time.Millisecond, map[string]string{"a": "A", "b": "B"})
	fakeclock := clockwork.NewFakeClock()
	m.Clock = fakeclock
	assert.Equal(t, 2, m.Length())
	fakeclock.Advance(50 * time.Millisecond)
	m.Set("c", "C")
	assert.Equal(t, 3, m.Length())
	assert.Equal(t, []string{"a", "b", "c"}, m.SortedKeys())
	assert.Equal(t, []string{"A", "B", "C"}, m.SortedValues())
	fakeclock.Advance(60 * time.Millisecond)
	assert.Equal(t, 1, m.Length())
	assert.Equal(t, []string{"c"}, m.Keys())
	ch, ok := m.Get("c")
	assert.True(t, ok)
	assert.Equal(t, "C", ch)
	fakeclock.Advance(100 * time.Millisecond)
	assert.Equal(t, 0, m.Length())
	assert.Equal(t, m.Keys(), []string{})
}

func BenchmarkMapWithTTLContains(b *testing.B) {
	m := NewMapWithTTL[string, struct{}](10*time.Second, nil)
	fc := clockwork.NewFakeClock()
	m.Clock = fc

	n := 10000
	traceIDs := make([]string, n)
	for i := 0; i < n; i++ {
		traceIDs[i] = genID(32)
		if i%2 == 0 {
			m.Set(traceIDs[i], struct{}{})
		}
		fc.Advance(1 * time.Microsecond)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Get(traceIDs[i%n])
	}
}

func BenchmarkMapWithTTLExpire(b *testing.B) {
	m := NewMapWithTTL[string, struct{}](1*time.Second, nil)
	fc := clockwork.NewFakeClock()
	m.Clock = fc

	// 1K ids created at 1ms intervals
	// we'll check them over the course of 1 second as well, so they should all expire by the end
	n := 1000
	traceIDs := make([]string, n)
	for i := 0; i < n; i++ {
		traceIDs[i] = genID(32)
		m.Set(traceIDs[i], struct{}{})
		fc.Advance(1 * time.Millisecond)
	}
	// make sure we have 1000 ids now
	assert.Equal(b, n, m.Length())
	b.ResetTimer()
	advanceTime := 100 * time.Second / time.Duration(b.N)
	for i := 0; i < b.N; i++ {
		m.Get(traceIDs[i%n])
		if i%100 == 0 {
			fc.Advance(advanceTime)
		}
	}
	b.StopTimer()
	// make sure all ids have expired by now (there might be 1 or 2 that haven't)
	assert.GreaterOrEqual(b, 2, m.Length())
}

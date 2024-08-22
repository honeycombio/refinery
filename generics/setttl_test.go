package generics

import (
	"testing"
	"time"

	"github.com/dgryski/go-wyhash"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
)

var seed = 3565269841805
var rng = wyhash.Rng(seed)

const charset = "abcdef0123456789"

func genID(numChars int) string {

	id := make([]byte, numChars)
	for i := 0; i < numChars; i++ {
		id[i] = charset[int(rng.Next()%uint64(len(charset)))]
	}
	return string(id)
}

func TestSetTTLBasics(t *testing.T) {
	s := NewSetWithTTL(100*time.Millisecond, "a", "b", "b")
	fakeclock := clockwork.NewFakeClock()
	s.Clock = fakeclock
	assert.Equal(t, 2, s.Length())
	fakeclock.Advance(50 * time.Millisecond)
	s.Add("c")
	assert.Equal(t, 3, s.Length())
	assert.Equal(t, s.Members(), []string{"a", "b", "c"})
	fakeclock.Advance(60 * time.Millisecond)
	assert.Equal(t, 1, s.Length())
	assert.Equal(t, s.Members(), []string{"c"})
	fakeclock.Advance(100 * time.Millisecond)
	assert.Equal(t, 0, s.Length())
	assert.Equal(t, s.Members(), []string{})
}

func BenchmarkSetWithTTLContains(b *testing.B) {
	s := NewSetWithTTL[string](10 * time.Second)
	fc := clockwork.NewFakeClock()
	s.Clock = fc

	n := 10000
	traceIDs := make([]string, n)
	for i := 0; i < n; i++ {
		traceIDs[i] = genID(32)
		if i%2 == 0 {
			s.Add(traceIDs[i])
		}
		fc.Advance(1 * time.Microsecond)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Contains(traceIDs[i%n])
	}
}

func BenchmarkSetWithTTLExpire(b *testing.B) {
	s := NewSetWithTTL[string](1 * time.Second)
	fc := clockwork.NewFakeClock()
	s.Clock = fc

	// 1K ids created at 1ms intervals
	// we'll check them over the course of 1 second as well, so they should all expire by the end
	n := 1000
	traceIDs := make([]string, n)
	for i := 0; i < n; i++ {
		traceIDs[i] = genID(32)
		s.Add(traceIDs[i])
		fc.Advance(1 * time.Millisecond)
	}
	// make sure we have 1000 ids now
	assert.Equal(b, n, s.Length())
	b.ResetTimer()
	advanceTime := 100 * time.Second / time.Duration(b.N)
	for i := 0; i < b.N; i++ {
		s.Contains(traceIDs[i%n])
		if i%100 == 0 {
			fc.Advance(advanceTime)
		}
	}
	b.StopTimer()
	// make sure all ids have expired by now (there might be 1 or 2 that haven't)
	assert.GreaterOrEqual(b, 2, s.Length())
}

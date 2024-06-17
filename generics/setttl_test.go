package generics

import (
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
)

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

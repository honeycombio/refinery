package generics

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSetTTLBasics(t *testing.T) {
	s := NewSetWithTTL(100*time.Millisecond, "a", "b", "b")
	assert.Equal(t, 2, s.Length())
	time.Sleep(50 * time.Millisecond)
	s.Add("c")
	assert.Equal(t, 3, s.Length())
	assert.Equal(t, s.Members(), []string{"a", "b", "c"})
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.Equal(collect, 1, s.Length())
		assert.Equal(collect, s.Members(), []string{"c"})
	}, 100*time.Millisecond, 20*time.Millisecond)
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.Equal(collect, 0, s.Length())
		assert.Equal(collect, s.Members(), []string{})
	}, 100*time.Millisecond, 20*time.Millisecond)
}

package generics

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSet(t *testing.T) {
	s := NewSet(1, 2, 2)
	s.Add(3, 3)

	assert.True(t, s.Contains(1))
	assert.True(t, s.Contains(2))
	assert.True(t, s.Contains(3))
	assert.ElementsMatch(t, []int{1, 2, 3}, s.Members())

	s.AddMembers(NewSet(4))
	assert.True(t, s.Contains(4))
	assert.ElementsMatch(t, []int{1, 2, 3, 4}, s.Members())

}

func TestUnion(t *testing.T) {
	a := NewSet(1, 1, 2, 3, 3, 4)
	b := NewSet(3, 3, 4, 5, 6, 6)
	c := a.Union(b)
	assert.ElementsMatch(t, []int{1, 2, 3, 4, 5, 6}, c.Members())

	d := a.Union(NewSet[int]())
	assert.ElementsMatch(t, []int{1, 2, 3, 4}, d.Members())
}

func TestIntersect(t *testing.T) {
	a := NewSet(1, 1, 2, 3, 3, 4)
	b := NewSet(3, 3, 4, 5, 6, 6)
	c := a.Intersect(b)
	assert.ElementsMatch(t, []int{3, 4}, c.Members())

	assert.Empty(t, a.Intersect(NewSet(9)).Members())
}

func TestDifference(t *testing.T) {
	a := NewSet(1, 1, 2, 3, 3, 4)
	b := NewSet(3, 3, 4, 5, 6, 6)
	c := a.Difference(b)
	assert.ElementsMatch(t, []int{1, 2}, c.Members())

	d := b.Difference(a)
	assert.ElementsMatch(t, []int{5, 6}, d.Members())

}

var res Set[int]

func BenchmarkUnion(b *testing.B) {
	x := NewSet(0, 1, 2, 3, 4, 5)
	y := NewSet(1, 3, 5, 7, 9)
	for i := 0; i < 100; i++ {
		x.Add(i)
		y.Add(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res = x.Union(y)
	}
}

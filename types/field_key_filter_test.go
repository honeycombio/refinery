package types

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFieldKeyFilter(t *testing.T) {
	longName := strings.Repeat("x", 300)
	tests := []struct {
		name  string
		lists [][]string
		key   string
		want  bool
	}{
		{"configured name", [][]string{{"trace.trace_id"}, {"parent_id"}}, "parent_id", true},
		{"name in a later list", [][]string{nil, nil, {"http.method"}}, "http.method", true},
		{"unconfigured length", [][]string{{"trace.trace_id"}}, "name", false},
		{"unconfigured first byte", [][]string{{"trace.trace_id"}}, "xrace.trace_id", false},
		{"empty key", [][]string{{"a"}}, "", false},
		{"no names", nil, "anything", false},
		{"key longer than any name", [][]string{{"a"}}, longName, false},
		{"name too long to track passes every key", [][]string{{longName}}, "anything", true},
		{"empty name passes every key", [][]string{{""}}, "anything", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, newFieldKeyFilter(tt.lists...).mayMatch([]byte(tt.key)))
		})
	}

	t.Run("nil filter passes every key", func(t *testing.T) {
		var filter *fieldKeyFilter
		assert.True(t, filter.mayMatch([]byte("anything")))
	})
}

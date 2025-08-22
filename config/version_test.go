package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompareVersions(t *testing.T) {
	tests := []struct {
		current string
		target  string
		want    int
		wantErr bool
	}{
		{"v2.8.0", "v2.9.7", -1, false}, // current < target
		{"v2.9.7", "v2.9.7", 0, false},  // current == target
		{"v2.10.0", "v2.9.7", 1, false}, // current > target
		{"v3.0.0", "v2.9.7", 1, false},  // major version higher
		{"v1.9.7", "v2.9.7", -1, false}, // major version lower
		{"v2.8.9", "v2.9.0", -1, false}, // minor version lower
		{"v2.10.0", "v2.9.9", 1, false}, // minor version higher
		{"v2.9.6", "v2.9.7", -1, false}, // patch version lower
		{"v2.9.8", "v2.9.7", 1, false},  // patch version higher
		{"2.9.7", "v2.9.7", 0, false},   // with and without v prefix
		{"invalid", "v2.9.7", 0, true},  // invalid current version
		{"v2.9.7", "invalid", 0, true},  // invalid target version
	}

	for _, tt := range tests {
		t.Run(tt.current+"_vs_"+tt.target, func(t *testing.T) {
			got, err := compareVersions(tt.current, tt.target)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestIsVersionBefore(t *testing.T) {
	tests := []struct {
		current string
		target  string
		want    bool
	}{
		{"v2.8.0", "v2.9.7", true},   // should show warning
		{"v2.9.7", "v2.9.7", false},  // should not show warning (equal)
		{"v2.10.0", "v2.9.7", false}, // should not show warning (after)
		{"invalid", "v2.9.7", true},  // should show warning (parsing error)
		{"v2.9.7", "invalid", true},  // should show warning (parsing error)
	}

	for _, tt := range tests {
		t.Run(tt.current+"_before_"+tt.target, func(t *testing.T) {
			got := isVersionBefore(tt.current, tt.target)
			assert.Equal(t, tt.want, got)
		})
	}
}

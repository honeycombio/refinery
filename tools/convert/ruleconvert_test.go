package main

import (
	"reflect"
	"testing"
)

func Test_keysToLowercase(t *testing.T) {
	tests := []struct {
		name string
		m    map[string]any
		want map[string]any
	}{
		{"empty", map[string]any{}, map[string]any{}},
		{"one", map[string]any{"A": "b"}, map[string]any{"a": "b"}},
		{"two", map[string]any{"A": "b", "C": "d"}, map[string]any{"a": "b", "c": "d"}},
		{"recursive", map[string]any{"A": "b", "C": map[string]any{"D": "e"}}, map[string]any{"a": "b", "c": map[string]any{"d": "e"}}},
		{"slices", map[string]any{"A": "b", "C": []any{map[string]any{"D": "e"}, "F"}}, map[string]any{"a": "b", "c": []any{map[string]any{"d": "e"}, "F"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transformSamplerMap(tt.m); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("_keysToLowercase() = %v, want %v", got, tt.want)
			}
		})
	}
}

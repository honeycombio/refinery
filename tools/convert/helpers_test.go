package main

import (
	"reflect"
	"testing"
)

func Test_conditional(t *testing.T) {
	tests := []struct {
		name  string
		data  map[string]any
		key   string
		extra string
		want  string
	}{
		{"eq false", map[string]any{"a": "b"}, "Field", "eq a c", "# Field: false"},
		{"eq true", map[string]any{"a": "b"}, "Field", "eq a b", "Field: true"},
		{"eq missing", map[string]any{"a": "b"}, "Field", "eq b c", "# Field: false"},
		{"nostar", map[string]any{"a": []string{"abc"}}, "Field", "nostar a", "Field: true"},
		{"yesstar 1", map[string]any{"a": []string{"*"}}, "Field", "nostar a", "# Field: false"},
		{"yesstar 2", map[string]any{"a": []string{"abc", "*", "def"}}, "Field", "nostar a", "# Field: false"},
		{"eq Metrics honeycomb", map[string]any{"a": "b", "Metrics": "honeycomb"}, "Field", "eq Metrics honeycomb", "Field: true"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := conditional(tt.data, tt.key, tt.extra); got != tt.want {
				t.Errorf("conditional() = %v, want %v", got, tt.want)
			}
		})
	}
}

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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := _keysToLowercase(tt.m); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("_keysToLowercase() = %v, want %v", got, tt.want)
			}
		})
	}
}

package main

import (
	"reflect"
	"testing"
	"time"

	"github.com/honeycombio/refinery/config"
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

func Test_ConvertTypes(t *testing.T) {
	tests := []struct {
		name string
		m    map[string]any
		want map[string]any
	}{
		{"clearfreq_int", map[string]any{
			"clearfrequencysec": int(10),
		}, map[string]any{
			"clearfrequency": config.Duration(10) * config.Duration(time.Second),
		}},
		{"clearfreq_int64", map[string]any{
			"clearfrequencysec": int64(10),
		}, map[string]any{
			"clearfrequency": config.Duration(10) * config.Duration(time.Second),
		}},
		{"adjustmentinterval_int", map[string]any{
			"adjustmentinterval": int(10),
		}, map[string]any{
			"adjustmentinterval": config.Duration(10) * config.Duration(time.Second),
		}},
		{"adjustmentinterval_int64", map[string]any{
			"adjustmentinterval": int64(10),
		}, map[string]any{
			"adjustmentinterval": config.Duration(10) * config.Duration(time.Second),
		}},
		{"adjustmentinterval_duration", map[string]any{
			"adjustmentinterval": config.Duration(10) * config.Duration(time.Second),
		}, map[string]any{
			"adjustmentinterval": config.Duration(10) * config.Duration(time.Second),
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transformSamplerMap(tt.m); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("_keysToLowercase() = %v, want %v", got, tt.want)
			}
		})
	}
}

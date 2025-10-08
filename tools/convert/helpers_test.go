package main

import (
	"reflect"
	"testing"

	"github.com/honeycombio/refinery/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func Test_evaluateFormula(t *testing.T) {
	tests := []struct {
		name    string
		formula string
		value   any
		want    int64
		wantErr bool
	}{
		{"simple value", "value", int(10), 10, false},
		{"simple value int64", "value", int64(20), 20, false},
		{"multiply left", "3 * value", int(10), 30, false},
		{"multiply right", "value * 5", int(10), 50, false},
		{"multiply with spaces", " 3  *  value ", int(10), 30, false},
		{"unsupported type", "value", "string", 0, true},
		{"invalid formula", "value + 5", int(10), 0, true},
		{"invalid multiplier", "abc * value", int(10), 0, true},
		{"missing operand", "3 *", int(10), 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := evaluateFormula(tt.formula, tt.value)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_setFieldValue(t *testing.T) {
	tests := []struct {
		name      string
		data      map[string]any
		groupName string
		fieldName string
		value     any
		expected  map[string]any
	}{
		{
			name:      "create new group and field",
			data:      map[string]any{},
			groupName: "TestGroup",
			fieldName: "TestField",
			value:     42,
			expected: map[string]any{
				"TestGroup": map[string]any{
					"TestField": 42,
				},
			},
		},
		{
			name: "add field to existing group",
			data: map[string]any{
				"TestGroup": map[string]any{
					"ExistingField": "existing",
				},
			},
			groupName: "TestGroup",
			fieldName: "NewField",
			value:     "new",
			expected: map[string]any{
				"TestGroup": map[string]any{
					"ExistingField": "existing",
					"NewField":      "new",
				},
			},
		},
		{
			name: "overwrite existing field",
			data: map[string]any{
				"TestGroup": map[string]any{
					"TestField": "old",
				},
			},
			groupName: "TestGroup",
			fieldName: "TestField",
			value:     "new",
			expected: map[string]any{
				"TestGroup": map[string]any{
					"TestField": "new",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setFieldValue(tt.data, tt.groupName, tt.fieldName, tt.value)
			assert.True(t, reflect.DeepEqual(tt.data, tt.expected))
		})
	}
}

func Test_applyReplacement(t *testing.T) {
	tests := []struct {
		name            string
		replacement     config.Replacement
		groupName       string
		deprecatedValue any
		data            map[string]any
		wantMessage     string
		wantApplied     bool
		expectedData    map[string]any
	}{
		{
			name: "apply replacement when field doesn't exist",
			replacement: config.Replacement{
				Field:   "NewField",
				Formula: "3 * value",
			},
			groupName:       "TestGroup",
			deprecatedValue: int(10),
			data:            map[string]any{},
			wantMessage:     "NewField set to 30 (using formula: 3 * value)",
			wantApplied:     true,
			expectedData: map[string]any{
				"TestGroup": map[string]any{
					"NewField": int64(30),
				},
			},
		},
		{
			name: "skip when field already exists",
			replacement: config.Replacement{
				Field:   "ExistingField",
				Formula: "value",
			},
			groupName:       "TestGroup",
			deprecatedValue: int(10),
			data: map[string]any{
				"TestGroup": map[string]any{
					"ExistingField": "already here",
				},
			},
			wantMessage: "",
			wantApplied: false,
			expectedData: map[string]any{
				"TestGroup": map[string]any{
					"ExistingField": "already here",
				},
			},
		},
		{
			name: "apply when field doesn't exist in populated data",
			replacement: config.Replacement{
				Field:   "NewField",
				Formula: "2 * value",
			},
			groupName:       "TestGroup",
			deprecatedValue: int(5),
			data: map[string]any{
				"TestGroup": map[string]any{
					"OtherField": "other",
				},
			},
			wantMessage: "NewField set to 10 (using formula: 2 * value)",
			wantApplied: true,
			expectedData: map[string]any{
				"TestGroup": map[string]any{
					"OtherField": "other",
					"NewField":   int64(10),
				},
			},
		},
		{
			name: "skip replacement when target field exists (overwrite protection)",
			replacement: config.Replacement{
				Field:   "ExistingField",
				Formula: "5 * value",
			},
			groupName:       "TestGroup",
			deprecatedValue: int(20),
			data: map[string]any{
				"TestGroup": map[string]any{
					"ExistingField": "original_value",
				},
			},
			wantMessage: "",
			wantApplied: false,
			expectedData: map[string]any{
				"TestGroup": map[string]any{
					"ExistingField": "original_value", // Should remain unchanged
				},
			},
		},
		{
			name: "invalid formula returns not applied",
			replacement: config.Replacement{
				Field:   "NewField",
				Formula: "invalid formula",
			},
			groupName:       "TestGroup",
			deprecatedValue: int(10),
			data:            map[string]any{},
			wantMessage:     "",
			wantApplied:     false,
			expectedData:    map[string]any{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			message, applied := applyReplacement(tt.replacement, tt.groupName, tt.deprecatedValue, tt.data)
			assert.Equal(t, tt.wantMessage, message)
			assert.Equal(t, tt.wantApplied, applied)
			assert.True(t, reflect.DeepEqual(tt.data, tt.expectedData))
		})
	}
}

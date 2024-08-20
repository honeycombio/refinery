package config

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testStruct struct {
	F1 string
	F2 int
	F3 bool
	F4 []string
	F5 map[string]string
}

type parentStruct struct {
	F0   string
	Test testStruct
}

// constructs a parent struct based on the values in the fields string
// each char corresponds to a field in the testStruct
// depending on the type of the field
func build(fields string) *parentStruct {
	ts := parentStruct{
		Test: testStruct{
			F4: []string{},
			F5: map[string]string{},
		},
	}
	for i, c := range fields {
		// dots mean the struct has the zero value in that field
		if c == '.' {
			continue
		}
		switch i {
		case 0:
			ts.F0 = string(c)
		case 1:
			ts.Test.F1 = string(c)
		case 2:
			ts.Test.F2 = int(c - '0')
		case 3:
			ts.Test.F3 = c == 't'
		case 4:
			ts.Test.F4 = []string{string(c), string(c)}
		case 5:
			ts.Test.F5 = map[string]string{strings.ToLower(string(c)): string(c)}
		}
	}

	return &ts
}

func Test_overlay(t *testing.T) {
	doubleslice := build("")
	doubleslice.Test.F4 = []string{"a", "a", "b", "b"}
	doublemap := build("")
	doublemap.Test.F5 = map[string]string{"a": "a", "b": "b"}
	mapoverwrite := build("")
	mapoverwrite.Test.F5 = map[string]string{"a": "A"}

	cfg1 := &configContents{}
	cfg1.Specialized.AdditionalAttributes = map[string]string{}
	cfg2 := &configContents{}
	cfg2.Specialized.AdditionalAttributes = map[string]string{"a": "b"}
	cfg3 := &configContents{}
	cfg3.Specialized.AdditionalAttributes = map[string]string{"a": "b"}

	tests := []struct {
		name    string
		bottom  any
		top     any
		want    any
		wantErr bool
	}{
		{"empty top", build("ab1tde"), build(""), build("ab1tde"), false},
		{"empty bottom", build(""), build("ab1tde"), build("ab1tde"), false},
		{"simple", build("ab1"), build(".c"), build("ac1"), false},
		{"zero", build("ab5"), build("..0"), build("ab5"), false},
		{"int", build("ab5"), build("..7"), build("ab7"), false},
		{"boolf", build("ab5t"), build("...f"), build("ab5t"), false},
		{"boolt", build("ab5f"), build("...t"), build("ab5t"), false},
		{"string at parent empty", build("a"), build("."), build("a"), false},
		{"string at parent override", build("a"), build("b"), build("b"), false},
		{"string at child empty", build(".a"), build(".."), build(".a"), false},
		{"string at child override", build(".a"), build(".b"), build(".b"), false},
		{"slice extend", build("....a"), build("....b"), doubleslice, false},
		{"map extend", build(".....a"), build(".....b"), doublemap, false},
		{"map overwrite", build(".....a"), build(".....A"), mapoverwrite, false},
		{"bad bottom", 0, build("ab1"), nil, true},
		{"bad top", build("ab1"), "hello", nil, true},
		{"nil bottom", nil, build("ab1"), nil, true},
		{"nil top", build("ab1"), nil, nil, true},
		{"don't crash with real config", cfg1, cfg2, cfg3, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := overlay(reflect.ValueOf(tt.bottom), reflect.ValueOf(tt.top))
			if (err != nil) != tt.wantErr {
				t.Errorf("overlay() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				// don't test the result if we expect an error
				return
			}
			gotv := got.Interface()
			assert.Equal(t, tt.want, gotv)
		})
	}
}

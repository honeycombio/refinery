package config

import (
	"reflect"
	"testing"
)

type TestFielder struct {
	S  string
	S2 string
	I  int
	F  float64
}

// implement getFielder
func (t *TestFielder) GetField(name string) reflect.Value {
	return reflect.ValueOf(t).Elem().FieldByName(name)
}

type TestConfig struct {
	St string  `cmdenv:"S"`
	It int     `cmdenv:"I"`
	Fl float64 `cmdenv:"F"`
	No string
}

type FallbackConfig struct {
	St string `cmdenv:"S,S2"`
}

type BadTestConfig1 struct {
	It int `cmdenv:"Q"`
}
type BadTestConfig2 struct {
	It int `cmdenv:"S"`
}

func TestApplyCmdEnvTags(t *testing.T) {
	tests := []struct {
		name    string
		fielder getFielder
		cfg     any
		want    any
		wantErr bool
	}{
		{"normal", &TestFielder{"foo", "bar", 1, 2.3}, &TestConfig{}, &TestConfig{"foo", 1, 2.3, ""}, false},
		{"bad", &TestFielder{"foo", "bar", 1, 2.3}, &BadTestConfig1{}, &BadTestConfig1{}, true},
		{"type mismatch", &TestFielder{"foo", "bar", 1, 2.3}, &BadTestConfig2{17}, &BadTestConfig2{17}, true},
		{"fallback1", &TestFielder{"foo", "bar", 1, 2.3}, &FallbackConfig{}, &FallbackConfig{"foo"}, false},
		{"fallback2", &TestFielder{"", "bar", 1, 2.3}, &FallbackConfig{}, &FallbackConfig{"bar"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.cfg
			err := applyCmdEnvTags(reflect.ValueOf(cfg), tt.fielder)

			if (err != nil) != tt.wantErr {
				t.Errorf("ApplyCmdEnvTags() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !reflect.DeepEqual(cfg, tt.want) {
				t.Errorf("ApplyCmdEnvTags() = %v, want %v", cfg, tt.want)
			}
		})
	}
}

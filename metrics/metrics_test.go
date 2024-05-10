package metrics

import "testing"

func TestConvertNumeric(t *testing.T) {
	tests := []struct {
		name string
		val  any
		want float64
	}{
		{"int", int(17), 17},
		{"uint", uint(17), 17},
		{"int64", int64(17), 17},
		{"uint64", uint64(17), 17},
		{"int32", int32(17), 17},
		{"uint32", uint32(17), 17},
		{"int16", int16(17), 17},
		{"uint16", uint16(17), 17},
		{"int8", int8(17), 17},
		{"uint8", uint8(17), 17},
		{"float64", float64(17), 17},
		{"float32", float32(17), 17},
		{"bool", true, 1},
		{"bool", false, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ConvertNumeric(tt.val); got != tt.want {
				t.Errorf("ConvertNumeric() = %v, want %v", got, tt.want)
			}
		})
	}
}

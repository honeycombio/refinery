package config

import "testing"

func Test_setCompareOperators(t *testing.T) {
	tests := []struct {
		name       string
		datatype   string
		testvalue  any
		condition  string
		value      any
		wantResult bool
		wantErr    bool
	}{
		// we want to test all the different combinations of datatypes and conditions
		// datatypes: string, int, float, bool, for all 3 of datatype, value, testvalue
		// conditions: EQ, NE, GT, GE, LT, LE -- plus 4 different boolean states. That works
		// out to more than 1000 tests. We'll try some representative ones to limit the scope.
		// This table reads as written -- "foo" LT "bar" should be false.
		{"LT1", "string", "foo", LT, "bar", false, false},
		{"LT2", "string", "bar", LT, "foo", true, false},
		{"LT3", "string", "1", LT, 10, true, false},
		{"LT4", "string", "10", LT, 1, false, false},
		{"LT5", "int", "1", LT, 10, true, false},
		{"LT6", "int", "10", LT, 1, false, false},
		{"LT7", "float", "1", LT, 10, true, false},
		{"LT8", "float", "10", LT, 1, false, false},
		{"LTE1", "string", "foo", LTE, "bar", false, false},
		{"LTE2", "string", "foo", LTE, "foo", true, false},
		{"LTE3", "string", "1", LTE, 10, true, false},
		{"LTE4", "string", "1", LTE, 1, true, false},
		{"LTE5", "int", "1", LTE, 10, true, false},
		{"LTE6", "int", "1", LTE, 1, true, false},
		{"LTE7", "float", "1", LTE, 10, true, false},
		{"LTE8", "float", "1", LTE, 1, true, false},
		{"EQ1", "string", "bar", EQ, "bar", true, false},
		{"EQ2", "string", "bar", EQ, "foo", false, false},
		{"EQ3", "int", "1", EQ, 1, true, false},
		{"EQ4", "int", "1", EQ, 2, false, false},
		{"EQ5", "int", "1", EQ, 2.2, false, false},
		{"EQ6", "int", "1", EQ, 1.0, true, false},
		{"EQ7", "float", "1", EQ, 1, true, false},
		{"EQ8", "float", "1", EQ, 2, false, false},
		{"EQ9", "float", "1", EQ, 2.2, false, false},
		{"EQ10", "float", "1", EQ, 1.0, true, false},
		{"EQ11", "bool", "true", EQ, true, true, false},
		{"EQ12", "bool", "true", EQ, false, false, false},
		{"EQ13", "bool", "false", EQ, true, false, false},
		{"EQ14", "bool", "false", EQ, false, true, false},
		{"EQ15", "bool", "1", EQ, 1, true, false},
		{"EQ16", "bool", "", EQ, 0, true, false},
		{"GT1", "string", "foo", GT, "bar", true, false},
		{"GT2", "int", "1", GT, 10, false, false},
		{"GT3", "float", "1", GT, 10, false, false},
		{"GTE1", "string", "foo", GTE, "bar", true, false},
		{"GTE2", "string", "foo", GTE, "foo", true, false},
		{"GTE3", "string", "1", GTE, 10, false, false},
		{"GTE4", "string", "1", GTE, 1, true, false},
		{"GTE5", "int", "1", GTE, 10, false, false},
		{"GTE6", "int", "1", GTE, 1, true, false},
		{"GTE7", "float", "1", GTE, 10, false, false},
		{"GTE8", "float", "1", GTE, 1, true, false},
		{"NEQ1", "string", "bar", NEQ, "bar", false, false},
		{"NEQ2", "string", "bar", NEQ, "foo", true, false},
		{"NEQ3", "int", "1", NEQ, 1, false, false},
		{"NEQ4", "int", "1", NEQ, 2, true, false},
		{"NEQ5", "int", "1", NEQ, 2.2, true, false},
		{"NEQ6", "int", "1", NEQ, 1.0, false, false},
		{"NEQ7", "float", "1", NEQ, 1, false, false},
		{"NEQ8", "float", "1", NEQ, 2, true, false},
		{"NEQ9", "float", "1", NEQ, 2.2, true, false},
		{"NEQ10", "float", "1", NEQ, 1.0, false, false},
		{"NEQ11", "bool", "true", NEQ, true, false, false},
		{"NEQ12", "bool", "true", NEQ, false, true, false},
		{"NEQ13", "bool", "false", NEQ, true, true, false},
		{"NEQ14", "bool", "false", NEQ, false, false, false},
		{"NEQ15", "bool", "1", NEQ, 1, false, false},
		{"NEQ16", "bool", "", NEQ, 0, false, false},
		{"ERR1", "blah", "foo", EQ, "bar", false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rbsc := &RulesBasedSamplerCondition{
				Datatype: tt.datatype,
				Value:    tt.value,
			}
			err := setCompareOperators(rbsc, tt.condition)
			if (err != nil) != tt.wantErr {
				t.Errorf("setCompareOperators() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				result := rbsc.Matches(tt.testvalue, true)
				if result != tt.wantResult {
					t.Errorf("setCompareOperators() result = %v, wantResult %v", result, tt.wantResult)
				}
			}
		})
	}
}

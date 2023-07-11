package config

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_asFloat(t *testing.T) {
	tests := []struct {
		name string
		v    any
		f    float64
		msg  string
	}{
		{"int", 1, 1, ""},
		{"int64", int64(1), 1, ""},
		{"float64", float64(1), 1, ""},
		{"Duration1", Duration(1), 1, ""},
		{"Duration2", "1s", 1000, ""},
		{"Duration3", "1m", 60000, ""},
		{"Duration4", "1h", 3600000, ""},
		{"string1", "1", 0, `"1" (string) cannot be interpreted as a quantity`},
		{"nil", nil, 0, `<nil> (<nil>) cannot be interpreted as a quantity`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := asFloat(tt.v)
			if got != tt.f {
				t.Errorf("asFloat() got f = %v, want %v", got, tt.f)
			}
			if got1 != tt.msg {
				t.Errorf("asFloat() got msg = %v, want %v", got1, tt.msg)
			}
		})
	}
}

func Test_validateType(t *testing.T) {
	tests := []struct {
		name string
		k    string
		v    any
		typ  string
		want string
	}{
		{"string", "k", "v", "string", ""},
		{"int", "k", 1, "int", ""},
		{"bool", "k", true, "bool", ""},
		{"float", "k", 1.0, "float", ""},
		{"stringarray1", "k", []any{"v"}, "stringarray", ""},
		{"stringarray2", "k", []any{1}, "stringarray", "field k must be a string array but contains non-string 1"},
		{"stringarray3", "k", []any{true}, "stringarray", "field k must be a string array but contains non-string true"},
		{"stringarray4", "k", []any{1.0}, "stringarray", "field k must be a string array but contains non-string 1"},
		{"stringarray5", "k", []any{nil}, "stringarray", "field k must be a string array but contains non-string <nil>"},
		{"stringarray6", "k", []any{"v", 1}, "stringarray", "field k must be a string array but contains non-string 1"},
		{"stringarray7", "k", []any{"v", true}, "stringarray", "field k must be a string array but contains non-string true"},
		{"stringarray8", "k", []any{"v", 1.0}, "stringarray", "field k must be a string array but contains non-string 1"},
		{"stringarray9", "k", []any{"v", nil}, "stringarray", "field k must be a string array but contains non-string <nil>"},
		{"stringarray10", "k", []any{"v", "v"}, "stringarray", ""},
		{"map1", "k", map[string]any{"k": "v"}, "map", ""},
		{"map2", "k", map[string]any{"k": 1}, "map", ""},
		{"map3", "k", map[string]any{"k": true, "x": 1, "y": "hi"}, "map", ""},
		{"map4", "k", "v", "map", "field k must be a map"},
		{"duration1", "k", "1m", "duration", ""},
		{"duration2", "k", "1h", "duration", ""},
		{"duration3", "k", "1s", "duration", ""},
		{"duration4", "k", "1", "duration", `field k (1) must be a valid duration like '3m30s' or '100ms'`},
		{"duration5", "k", 1, "duration", `field k (1) must be a valid duration like '3m30s' or '100ms'`},
		{"hostport", "k", "host:port", "hostport", ""},
		{"hostport bad", "k", "host:port:port", "hostport", `field k (host:port:port) must be a hostport: address host:port:port: too many colons in address`},
		{"hostport blank", "k", "", "hostport", ""},
		{"url", "k", "http://example.com", "url", ""},
		{"url bad", "k", "not a url", "url", `field k (not a url) must be a valid URL with a host`},
		{"url blank", "k", "", "url", `field k may not be blank`},
		{"url noscheme", "k", "example.com", "url", `field k (example.com) must be a valid URL with a host`},
		{"url badscheme", "k", "ftp://example.com", "url", `field k (ftp://example.com) must use an http or https scheme`},
		{"invalid memorysize", "k", "test", "memorysize", `field k (test) must be a valid memory size like '1Gb' or '100_000_000'`},
		{"valid memorysize G", "k", "1G", "memorysize", ""},
		{"valid memorysize Gi", "k", "1Gi", "memorysize", ""},
		{"valid memorysize GiB", "k", "1GiB", "memorysize", ""},
		{"valid memorysize GB", "k", "1GB", "memorysize", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := validateDatatype(tt.k, tt.v, tt.typ)
			if tt.want == "" && len(got) != 0 {
				t.Errorf("validateType() = %v, want empty", got)
			}
			if got != tt.want {
				t.Errorf("validateType() = %v, want %v", got, tt.want)
			}
		})
	}
}

var configDataYaml = `
groups:
  - name: General
    fields:
      - name: Version
        type: string
        validations:
          - type: format
            arg: version
      - name: Prefix
        type: string
        validations:
          - type: format
            arg: alphanumeric
      - name: LoadInterval
        type: duration
        validations:
          - type: minimum
            arg: 1s

  - name: Network
    fields:
      - name: ListenAddr
        type: hostport
        validations:
            - type: notempty
      - name: API
        type: url
      - name: APIKey
        type: string
        validations:
          - type: requiredInGroup
          - type: format
            arg: apikey

  - name: Peer
    fields:
      - name: ListenAddr
        type: hostport
        validations:
            - type: notempty

  - name: RequireTest
    fields:
      - name: FieldA
        type: int
      - name: FieldB
        type: int
      - name: FieldC
        type: int
        validations:
          - type: requiredInGroup
      - name: FieldD
        type: int
        validations:
          - type: requiredWith
            arg: FieldB


  - name: Traces
    fields:
      - name: MaxBatchSize
        type: int
        validations:
          - type: minOrZero
            arg: 100
      - name: TestScore
        type: int
        validations:
          - type: minimum
            arg: 200
          - type: maximum
            arg: 800
      - name: ADuration
        type: duration
        validations:
          - type: minOrZero
            arg: 1s
      - name: ABool
        type: bool
      - name: AChoice
        type: string
        choices: [ A, B, C ]
        validations:
          - type: choice
      - name: AStringArray
        type: stringarray
        validations:
          - type: elementType
            arg: hostport
      - name: AStringMap
        type: map
        validations:
          - type: elementType
            arg: string

`

// helper function to build a nested map from a dotted name
// with only one dot.
func mm(values ...any) map[string]any {
	m := map[string]any{}
	for i := 0; i < len(values); i += 2 {
		name := values[i].(string)
		v := values[i+1]
		parts := strings.Split(name, ".")
		if _, ok := m[parts[0]]; !ok {
			m[parts[0]] = map[string]any{parts[1]: v}
			continue
		}
		m[parts[0]].(map[string]any)[parts[1]] = v
	}
	return m
}

func Test_validate(t *testing.T) {
	rdr := strings.NewReader(configDataYaml)
	metadata := Metadata{}
	err := metadata.LoadFrom(rdr)
	assert.NoError(t, err)
	// want is one string that can be:
	// - empty
	// - a single error
	// we search the returned errors for that string if not empty
	tests := []struct {
		name string
		data map[string]any
		want string
	}{
		{"empty", map[string]any{}, ""},
		{"bad group", mm("foo.bar", 1), `unknown group foo`},
		{"bad type", mm("foo.bar", 1), `unknown field foo.bar`},
		{"good field", mm("General.LoadInterval", "4m"), ""},
		{"bad field in good group", mm("General.LoadIntervalx", "4m"),
			`unknown field General.LoadIntervalx`},
		{"good field in bad group1", mm("Generalx.LoadInterval", "4m"),
			`unknown group Generalx`},
		{"good field in bad group2", mm("Generalx.LoadInterval", "4m"),
			`unknown field Generalx.LoadInterval`},
		{"int for duration", mm("General.LoadInterval", 5),
			`field General.LoadInterval (5) must be a valid duration like '3m30s' or '100ms'`},
		{"string for int", mm("Traces.MaxBatchSize", "500"),
			`field Traces.MaxBatchSize must be an int`},
		{"string for bool", mm("Traces.ABool", "true"),
			`field Traces.ABool must be a bool`},
		{"good choice", mm("Traces.AChoice", "B"), ""},
		{"bad choice", mm("Traces.AChoice", "Z"),
			`field Traces.AChoice (Z) must be one of [A B C]`},
		{"bad format apikey", mm("Network.APIKey", "abc"),
			`field Network.APIKey (****) must be a valid Honeycomb API key`},
		{"bad format apikey long", mm("Network.APIKey", "abc123abc123whee"),
			`field Network.APIKey (****whee) must be a valid Honeycomb API key`},
		{"good format apikey", mm("Network.APIKey", "abc123abc123abc123abc123abc123ab"), ""},
		{"good format apikey", mm("Network.APIKey", "NewStyleKeyWith22chars"), ""},
		{"good format version", mm("General.Version", "v2.0"), ""},
		{"bad format version1", mm("General.Version", "2.0"), "field General.Version (2.0) must be a valid major.minor version number, like v2.0"},
		{"bad format version2", mm("General.Version", "v2.0.0"), "field General.Version (v2.0.0) must be a valid major.minor version number, like v2.0"},
		{"good format alphanumeric", mm("General.Prefix", "Production123"), ""},
		{"bad format alphanumeric", mm("General.Prefix", "Production-123"),
			`field General.Prefix (Production-123) must be purely alphanumeric`},
		{"bad url", mm("Network.API", "example.com", "Network.APIKey", "NewStyleKeyWith22chars"),
			`field Network.API (example.com) must be a valid URL with a host`},
		{"good url", mm("Network.API", "https://example.com", "Network.APIKey", "NewStyleKeyWith22chars"), ""},
		{"bad url scheme", mm("Network.API", "ftp://example.com", "Network.APIKey", "NewStyleKeyWith22chars"),
			`field Network.API (ftp://example.com) must use an http or https scheme`},
		{"good minimum duration", mm("General.LoadInterval", "10s"), ""},
		{"bad minimum duration", mm("General.LoadInterval", "10ms"), "field General.LoadInterval (10ms) must be at least 1s"},
		{"good minimum int", mm("Traces.TestScore", 500), ""},
		{"bad minimum int", mm("Traces.TestScore", 50), "field Traces.TestScore (50) must be at least 200"},
		{"good maximum int", mm("Traces.TestScore", 500), ""},
		{"bad maximum int", mm("Traces.TestScore", 5000), "field Traces.TestScore (5000) must be at most 800"},
		{"good minOrZero duration", mm("Traces.ADuration", "10s"), ""},
		{"bad minOrZero duration", mm("Traces.ADuration", "10ms"), "field Traces.ADuration must be at least 1s, or zero"},
		{"good minOrZero int", mm("Traces.MaxBatchSize", 500), ""},
		{"bad minOrZero int", mm("Traces.MaxBatchSize", 50), "field Traces.MaxBatchSize must be at least 100, or zero"},
		{"good hostport", mm("Peer.ListenAddr", "0.0.0.0:8080"), ""},
		{"bad hostport", mm("Peer.ListenAddr", "0.0.0.0"), "field Peer.ListenAddr (0.0.0.0) must be a hostport: address 0.0.0.0: missing port in address"},
		{"bad hostport", mm("Peer.ListenAddr", "0.0.0.0:8080:8080"), "field Peer.ListenAddr (0.0.0.0:8080:8080) must be a hostport: address 0.0.0.0:8080:8080: too many colons in address"},
		{"good require A", mm("RequireTest.FieldA", 1, "RequireTest.FieldC", 3), ""},
		{"good require B", mm("RequireTest.FieldB", 2, "RequireTest.FieldC", 3, "RequireTest.FieldD", 4), ""},
		{"good require C", mm("RequireTest.FieldC", 3), ""},
		{"bad require", mm("RequireTest.FieldA", 1), "the group RequireTest is missing its required field FieldC"},
		{"bad require", mm("RequireTest.FieldA", 1, "RequireTest.FieldB", 2), "the group RequireTest includes FieldB, which also requires FieldD"},
		{"good slice elementType", mm("Traces.AStringArray", []any{"0.0.0.0:8080", "192.168.1.1:8080"}), ""},
		{"bad slice elementType", mm("Traces.AStringArray", []any{"0.0.0.0"}), "field Traces.AStringArray[0] (0.0.0.0) must be a hostport: address 0.0.0.0: missing port in address"},
		{"good map elementType", mm("Traces.AStringMap", map[string]any{"k": "v"}), ""},
		{"bad map elementType", mm("Traces.AStringMap", map[string]any{"k": 1}), "field Traces.AStringMap[k] must be a string"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := metadata.Validate(tt.data)
			if tt.want == "" && len(got) != 0 {
				t.Errorf("validate() = %v, want empty", got)
			}
			if tt.want != "" {
				found := false
				for _, e := range got {
					if strings.Contains(e, tt.want) {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("validate() got %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func Test_flatten(t *testing.T) {
	input := map[string]any{
		"A": 2,
		"B": map[string]any{
			"C": map[string]any{
				"D": map[string]any{
					"E": 1,
				},
			},
		},
	}
	output := flatten(input, true)
	expected := map[string]any{
		"A": 2,
		"B.C": map[string]any{
			"D": map[string]any{
				"E": 1,
			},
		},
	}
	assert.Equal(t, expected, output)
}

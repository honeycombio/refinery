package config

import (
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		{"MemorySize1", "1", 1, ""},
		{"MemorySize1K", "1K", 1000, ""},
		{"MemorySize1KiB", "1Kib", 1024, ""},
		{"MemorySize1G", "1G", 1_000_000_000, ""},
		{"MemorySize1717600000", "1717600000", 1_717_600_000, ""},
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
		{"valid memorysize", "k", "1717600000", "memorysize", ""},
		{"valid memorysize", "k", "1717600K", "memorysize", ""},
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

  - name: PeerManagement
    fields:
      - name: Peers
        type: stringarray
        validations:
          - type: elementType
            arg: url

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
      - name: FieldE
        type: int
        validations:
          - type: requiredWith
            arg: FieldA
        default: 100

  - name: ConflictTest
    fields:
      - name: FieldA
        type: int
      - name: FieldB
        type: int
        validations:
          - type: conflictsWith
            arg: FieldA

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
		{"bad format ingest key long", mm("Network.APIKey", "xxxxx-abcdefgh12345678abcdefgh12345678abcdefgh12345678aabbccddee"),
			`field Network.APIKey (****ddee) must be a valid Honeycomb API key`},
		{"good format apikey", mm("Network.APIKey", "abc123abc123abc123abc123abc123ab"), ""},
		{"good format apikey", mm("Network.APIKey", "NewStyleKeyWith22chars"), ""},
		{"good format ingest key", mm("Network.APIKey", "hcaik_01hshz0tyh2fqa9wznx5a1jf4exbmsd3jj4p89k8c02eb7tx4mwgs7tf99"), ""},
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
		{"good require E with default", mm("RequireTest.FieldA", 2, "RequireTest.FieldC", 3), ""},
		{"bad require", mm("RequireTest.FieldA", 1), "the group RequireTest is missing its required field FieldC"},
		{"bad conflicts with A", mm("ConflictTest.FieldA", 2, "ConflictTest.FieldB", 3), "the group ConflictTest includes FieldA, which conflicts with FieldB"},
		{"good conflicts with A", mm("ConflictTest.FieldA", 2), ""},
		{"good slice elementType", mm("Traces.AStringArray", []any{"0.0.0.0:8080", "192.168.1.1:8080"}), ""},
		{"bad slice elementType", mm("Traces.AStringArray", []any{"0.0.0.0"}), "field Traces.AStringArray[0] (0.0.0.0) must be a hostport: address 0.0.0.0: missing port in address"},
		{"good map elementType", mm("Traces.AStringMap", map[string]any{"k": "v"}), ""},
		{"bad map elementType", mm("Traces.AStringMap", map[string]any{"k": 1}), "field Traces.AStringMap[k] must be a string"},
		{"bad peer url", mm("PeerManagement.Peers", []any{"0.0.0.0:8082", "http://192.168.1.1:8088"}), "must be a valid UR"},
		{"good peer url", mm("PeerManagement.Peers", []any{"http://0.0.0.0:8082", "http://192.168.1.1:8088"}), ""},
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
					if strings.Contains(e.Message, tt.want) {
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

func TestValidateDeprecationWarnings(t *testing.T) {
	metadata := &Metadata{
		Groups: []Group{
			{
				Name: "Collection",
				Fields: []Field{
					{
						Name: "CacheCapacity",
						Type: "int",
						Deprecation: Deprecation{
							LastVersion:     "v2.9.7",
							DeprecationText: "CacheCapacity is deprecated since version v2.9.7. Set PeerQueueSize and IncomingQueueSize instead.",
						},
					},
					{
						Name: "PeerQueueSize",
						Type: "int",
						// No LastVersion, so not deprecated
					},
					{
						Name: "OtherDeprecatedField",
						Type: "string",
						Deprecation: Deprecation{
							LastVersion: "v2.5.0",
							// No DeprecationText, should use default message
						},
					},
				},
			},
		},
	}

	tests := []struct {
		name           string
		data           map[string]any
		currentVersion string
		expected       []string
	}{
		{
			name: "no deprecated fields used",
			data: map[string]any{
				"Collection": map[string]any{
					"PeerQueueSize": 30000,
				},
			},
			currentVersion: "v2.8.0",
			expected:       []string{},
		},
		{
			name: "deprecated field used, current version before lastversion",
			data: map[string]any{
				"Collection": map[string]any{
					"CacheCapacity": 10000,
				},
			},
			currentVersion: "v2.8.0",
			expected:       []string{"CacheCapacity is deprecated since version v2.9.7. Set PeerQueueSize and IncomingQueueSize instead."},
		},
		{
			name: "deprecated field used, current version equals lastversion",
			data: map[string]any{
				"Collection": map[string]any{
					"CacheCapacity": 10000,
				},
			},
			currentVersion: "v2.9.7",
			expected:       []string{"CacheCapacity is deprecated since version v2.9.7. Set PeerQueueSize and IncomingQueueSize instead."},
		},
		{
			name: "deprecated field used, current version after lastversion",
			data: map[string]any{
				"Collection": map[string]any{
					"CacheCapacity": 10000,
				},
			},
			currentVersion: "v2.10.0",
			expected:       []string{"CacheCapacity is deprecated since version v2.9.7. Set PeerQueueSize and IncomingQueueSize instead."},
		},
		{
			name: "multiple deprecated fields, mixed versions",
			data: map[string]any{
				"Collection": map[string]any{
					"CacheCapacity":        10000,
					"OtherDeprecatedField": "test",
					"PeerQueueSize":        30000, // not deprecated
				},
			},
			currentVersion: "v2.6.0", // After v2.5.0 but before v2.9.7
			expected: []string{
				"CacheCapacity is deprecated since version v2.9.7. Set PeerQueueSize and IncomingQueueSize instead.",
				"config Collection.OtherDeprecatedField is deprecated since version v2.5.0"},
		},
		{
			name: "old version shows all deprecated field warnings",
			data: map[string]any{
				"Collection": map[string]any{
					"CacheCapacity":        10000,
					"OtherDeprecatedField": "test",
				},
			},
			currentVersion: "v2.4.0", // Before both deprecation versions
			expected: []string{
				"CacheCapacity is deprecated since version v2.9.7. Set PeerQueueSize and IncomingQueueSize instead.",
				"config Collection.OtherDeprecatedField is going to be deprecated starting at version v2.5.0"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results := metadata.Validate(tt.data, tt.currentVersion)
			allErrors := make([]string, 0, len(results))
			for _, result := range results {
				allErrors = append(allErrors, result.Message)
			}

			// Sort both slices to ensure consistent comparison
			sort.Strings(tt.expected)
			sort.Strings(allErrors)

			require.Equal(t, tt.expected, allErrors)
		})
	}
}

func TestGroup_IsDeprecated(t *testing.T) {
	tests := []struct {
		name     string
		group    Group
		expected bool
	}{
		{
			name: "group with lastversion is deprecated",
			group: Group{
				Name:        "testgroup",
				LastVersion: "v2.0.0",
				Fields: []Field{
					{Name: "field1"},
					{Name: "field2"},
				},
			},
			expected: true,
		},
		{
			name: "group without lastversion but all fields deprecated",
			group: Group{
				Name: "testgroup",
				Fields: []Field{
					{
						Name:        "field1",
						Deprecation: Deprecation{LastVersion: "v1.5.0"},
					},
					{
						Name:        "field2",
						Deprecation: Deprecation{LastVersion: "v1.8.0"},
					},
				},
			},
			expected: true,
		},
		{
			name: "group without lastversion and some fields not deprecated",
			group: Group{
				Name: "testgroup",
				Fields: []Field{
					{
						Name:        "field1",
						Deprecation: Deprecation{LastVersion: "v1.5.0"},
					},
					{Name: "field2"}, // no LastVersion
				},
			},
			expected: false,
		},
		{
			name: "group lastversion takes precedence over non-deprecated fields",
			group: Group{
				Name:        "testgroup",
				LastVersion: "v2.0.0",
				Fields: []Field{
					{Name: "field1"}, // not deprecated
					{Name: "field2"}, // not deprecated
				},
			},
			expected: true, // group lastversion takes precedence
		},
		{
			name: "empty group without lastversion",
			group: Group{
				Name:   "testgroup",
				Fields: []Field{},
			},
			expected: true, // empty group with all fields deprecated returns true
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.group.IsDeprecated()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGroup_GetDeprecationVersion(t *testing.T) {
	tests := []struct {
		name     string
		group    Group
		expected string
	}{
		{
			name: "group lastversion takes precedence over later field versions",
			group: Group{
				Name:        "testgroup",
				LastVersion: "v1.0.0", // earlier version than fields
				Fields: []Field{
					{
						Name:        "field1",
						Deprecation: Deprecation{LastVersion: "v2.0.0"}, // later version
					},
					{
						Name:        "field2",
						Deprecation: Deprecation{LastVersion: "v3.0.0"}, // even later version
					},
				},
			},
			expected: "v1.0.0", // group version returned, not field versions
		},
		{
			name: "group lastversion takes precedence over earlier field versions",
			group: Group{
				Name:        "testgroup",
				LastVersion: "v2.0.0",
				Fields: []Field{
					{
						Name:        "field1",
						Deprecation: Deprecation{LastVersion: "v1.5.0"}, // earlier version
					},
					{
						Name:        "field2",
						Deprecation: Deprecation{LastVersion: "v1.8.0"}, // earlier version
					},
				},
			},
			expected: "v2.0.0", // group version returned, not field versions
		},
		{
			name: "no group lastversion, returns latest field version",
			group: Group{
				Name: "testgroup",
				Fields: []Field{
					{
						Name:        "field1",
						Deprecation: Deprecation{LastVersion: "v1.5.0"},
					},
					{
						Name:        "field2",
						Deprecation: Deprecation{LastVersion: "v2.1.0"}, // latest
					},
					{
						Name:        "field3",
						Deprecation: Deprecation{LastVersion: "v1.8.0"},
					},
				},
			},
			expected: "v2.1.0",
		},
		{
			name: "no deprecation versions anywhere",
			group: Group{
				Name: "testgroup",
				Fields: []Field{
					{Name: "field1"},
					{Name: "field2"},
				},
			},
			expected: "",
		},
		{
			name: "empty group with lastversion",
			group: Group{
				Name:        "testgroup",
				LastVersion: "v1.0.0",
				Fields:      []Field{},
			},
			expected: "v1.0.0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.group.GetDeprecationVersion()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGroup_DeprecationTextAndReplacements(t *testing.T) {
	tests := []struct {
		name    string
		group   Group
		hasText bool
		hasRepl bool
	}{
		{
			name: "group with deprecation text and replacements",
			group: Group{
				Name:            "testgroup",
				LastVersion:     "v2.0.0",
				DeprecationText: "TestGroup is deprecated since v2.0.0. Use NewGroup instead.",
				Replacements: []Replacement{
					{Field: "NewGroup"}, // For groups, only Field is used (Formula ignored)
				},
			},
			hasText: true,
			hasRepl: true,
		},
		{
			name: "group with deprecation text only",
			group: Group{
				Name:            "testgroup",
				LastVersion:     "v2.0.0",
				DeprecationText: "TestGroup is deprecated since v2.0.0.",
			},
			hasText: true,
			hasRepl: false,
		},
		{
			name: "group with replacements only",
			group: Group{
				Name:        "testgroup",
				LastVersion: "v2.0.0",
				Replacements: []Replacement{
					{Field: "NewGroup"}, // For groups, only Field is used (Formula ignored)
				},
			},
			hasText: false,
			hasRepl: true,
		},
		{
			name: "group with lastversion but no text or replacements",
			group: Group{
				Name:        "testgroup",
				LastVersion: "v2.0.0",
			},
			hasText: false,
			hasRepl: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test that DeprecationText is properly set
			if tt.hasText {
				assert.NotEmpty(t, tt.group.DeprecationText, "DeprecationText should not be empty")
				assert.Contains(t, tt.group.DeprecationText, "deprecated", "DeprecationText should contain 'deprecated'")
			} else {
				assert.Empty(t, tt.group.DeprecationText, "DeprecationText should be empty")
			}

			// Test that Replacements are properly set
			if tt.hasRepl {
				assert.NotEmpty(t, tt.group.Replacements, "Replacements should not be empty")
				for _, repl := range tt.group.Replacements {
					assert.NotEmpty(t, repl.Field, "Replacement Field should not be empty")
					// Note: Group replacements don't use Formula (only Field is used)
				}
			} else {
				assert.Empty(t, tt.group.Replacements, "Replacements should be empty")
			}

			// Group should be deprecated if it has LastVersion
			if tt.group.LastVersion != "" {
				assert.True(t, tt.group.IsDeprecated(), "Group with LastVersion should be deprecated")
				assert.Equal(t, tt.group.LastVersion, tt.group.GetDeprecationVersion(), "Should return group's LastVersion")
			}
		})
	}
}

func TestGroup_DeprecationConsistencyWithFields(t *testing.T) {
	// Test that Group deprecation features work consistently with Field deprecation features
	group := Group{
		Name:            "TestGroup",
		LastVersion:     "v2.0.0",
		DeprecationText: "TestGroup is deprecated since v2.0.0. Use NewTestGroup instead.",
		Replacements: []Replacement{
			{Field: "NewTestGroup"}, // Formula is ignored for Group replacements
		},
		Fields: []Field{
			{
				Name: "OldField",
				Deprecation: Deprecation{
					LastVersion:     "v2.5.0", // later than group version
					DeprecationText: "OldField is deprecated since v2.5.0.",
					Replacements: []Replacement{
						{Field: "NewField", Formula: "value"},
					},
				},
			},
			{
				Name: "CurrentField", // not deprecated
			},
		},
	}

	// Group deprecation should take precedence
	assert.True(t, group.IsDeprecated(), "Group should be deprecated")
	assert.Equal(t, "v2.0.0", group.GetDeprecationVersion(), "Should return group's version, not field version")

	// Group should have its own deprecation metadata
	assert.Equal(t, "TestGroup is deprecated since v2.0.0. Use NewTestGroup instead.", group.DeprecationText)
	assert.Len(t, group.Replacements, 1, "Group should have 1 replacement")

	// Fields should maintain their own deprecation metadata
	assert.Equal(t, "OldField is deprecated since v2.5.0.", group.Fields[0].GetDeprecationText())
	assert.Len(t, group.Fields[0].Replacements, 1, "Field should have 1 replacement")
	assert.Empty(t, group.Fields[1].GetDeprecationText(), "Non-deprecated field should have no deprecation text")
}

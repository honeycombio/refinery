package config

import (
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func Test_formatFromFilename(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		want     Format
	}{
		{"a", "a", FormatUnknown},
		{"a.yaml", "a.yaml", FormatYAML},
		{"a.yml", "a.yml", FormatYAML},
		{"a.YAML", "a.YAML", FormatYAML},
		{"a.YML", "a.YML", FormatYAML},
		{"a.toml", "a.toml", FormatTOML},
		{"a.TOML", "a.TOML", FormatTOML},
		{"a.json", "a.json", FormatJSON},
		{"a.JSON", "a.JSON", FormatJSON},
		{"a.txt", "a.txt", FormatUnknown},
		{"a.", "a.", FormatUnknown},
		{"a", "a", FormatUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatFromFilename(tt.filename); got != tt.want {
				t.Errorf("formatFromFilename() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_formatFromResponse(t *testing.T) {
	tests := []struct {
		name string
		resp *http.Response
		want Format
	}{
		{"application/json", &http.Response{Header: http.Header{"Content-Type": []string{"application/json"}}}, FormatJSON},
		{"text/json", &http.Response{Header: http.Header{"Content-Type": []string{"text/json"}}}, FormatJSON},
		{"application/x-toml", &http.Response{Header: http.Header{"Content-Type": []string{"application/x-toml"}}}, FormatTOML},
		{"application/toml", &http.Response{Header: http.Header{"Content-Type": []string{"application/toml"}}}, FormatTOML},
		{"text/x-toml", &http.Response{Header: http.Header{"Content-Type": []string{"text/x-toml"}}}, FormatTOML},
		{"text/toml", &http.Response{Header: http.Header{"Content-Type": []string{"text/toml"}}}, FormatTOML},
		{"application/x-yaml", &http.Response{Header: http.Header{"Content-Type": []string{"application/x-yaml"}}}, FormatYAML},
		{"application/yaml", &http.Response{Header: http.Header{"Content-Type": []string{"application/yaml"}}}, FormatYAML},
		{"text/x-yaml", &http.Response{Header: http.Header{"Content-Type": []string{"text/x-yaml"}}}, FormatYAML},
		{"text/yaml", &http.Response{Header: http.Header{"Content-Type": []string{"text/yaml"}}}, FormatYAML},
		{"text/plain", &http.Response{Header: http.Header{"Content-Type": []string{"text/plain"}}}, FormatUnknown},
		{"text/html", &http.Response{Header: http.Header{"Content-Type": []string{"text/html"}}}, FormatUnknown},
		{"text/xml", &http.Response{Header: http.Header{"Content-Type": []string{"text/xml"}}}, FormatUnknown},
		{"application/xml", &http.Response{Header: http.Header{"Content-Type": []string{"application/xml"}}}, FormatUnknown},
		{"application/octet-stream", &http.Response{Header: http.Header{"Content-Type": []string{"application/octet-stream"}}}, FormatUnknown},
		{"application/x-www-form-urlencoded", &http.Response{Header: http.Header{"Content-Type": []string{"application/x-www-form-urlencoded"}}}, FormatUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatFromResponse(tt.resp); got != tt.want {
				t.Errorf("formatFromResponse() = %v, want %v", got, tt.want)
			}
		})
	}
}

// Verifies that we can load a time.Duration from a string.
func Test_loadDuration(t *testing.T) {
	type dur struct {
		D Duration
	}

	tests := []struct {
		name    string
		format  Format
		text    string
		into    any
		want    any
		wantErr bool
	}{
		{"json", FormatJSON, `{"d": "15s"}`, &dur{}, &dur{Duration(15 * time.Second)}, false},
		{"yaml", FormatYAML, `d: 15s`, &dur{}, &dur{Duration(15 * time.Second)}, false},
		{"toml", FormatTOML, `d="15s"`, &dur{}, &dur{Duration(15 * time.Second)}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := load(strings.NewReader(tt.text), tt.format, tt.into); (err != nil) != tt.wantErr {
				t.Errorf("load() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.into, tt.want) {
				t.Errorf("load() = %#v, want %#v", tt.into, tt.want)
			}
		})
	}
}

// Verifies that we can load a memory size from a string.
func Test_loadMemsize(t *testing.T) {
	type mem struct {
		M MemorySize `yaml:"M" json:"M" toml:"M"`
	}

	tests := []struct {
		name    string
		format  Format
		text    string
		into    any
		want    any
		wantErr bool
	}{
		{"yaml", FormatYAML, `M: 1Gb`, &mem{}, &mem{MemorySize(G)}, false},
		{"json", FormatJSON, `{"M": "1Gb"}`, &mem{}, &mem{MemorySize(G)}, false},
		{"toml", FormatTOML, `M="1Gb"`, &mem{}, &mem{MemorySize(G)}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := load(strings.NewReader(tt.text), tt.format, tt.into); (err != nil) != tt.wantErr {
				t.Errorf("load() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.into, tt.want) {
				t.Errorf("load() = %#v, want %#v", tt.into, tt.want)
			}
		})
	}
}

func Test_ConfigHashMetrics(t *testing.T) {
	testcases := []struct {
		name     string
		hash     string
		expected int64
	}{
		{name: "valid hash", hash: "7f1237f7db723f4e874a7a8269081a77", expected: 6775},
		{name: "invalid length", hash: "1a8", expected: 0},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			result := ConfigHashMetrics(tc.hash)
			require.Equal(t, tc.expected, result)
		})
	}
}

// Creates temporary yaml files from the strings passed in and returns a slice of their filenames
// Because we use t.TempDir() the files will be cleaned up automatically.
func createTempConfigs(t *testing.T, cfgs ...string) []string {
	tmpDir := t.TempDir()

	var cfgFiles []string
	for _, cfg := range cfgs {

		configFile, err := os.CreateTemp(tmpDir, "cfg_*.yaml")
		assert.NoError(t, err)

		_, err = configFile.WriteString(cfg)
		assert.NoError(t, err)
		configFile.Close()
		cfgFiles = append(cfgFiles, configFile.Name())
	}
	return cfgFiles
}

func setMap(m map[string]any, key string, value any) {
	if strings.Contains(key, ".") {
		parts := strings.Split(key, ".")
		if _, ok := m[parts[0]]; !ok {
			m[parts[0]] = make(map[string]any)
		}
		setMap(m[parts[0]].(map[string]any), strings.Join(parts[1:], "."), value)
		return
	}
	m[key] = value
}

func makeYAML(args ...interface{}) string {
	m := make(map[string]any)
	for i := 0; i < len(args); i += 2 {
		setMap(m, args[i].(string), args[i+1])
	}
	b, err := yaml.Marshal(m)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func Test_loadConfigsInto(t *testing.T) {
	cm1 := makeYAML("General.ConfigurationVersion", 2, "General.ConfigReloadInterval", Duration(1*time.Second), "Network.ListenAddr", "0.0.0.0:8080")
	cm2 := makeYAML("General.ConfigReloadInterval", Duration(2*time.Second), "General.DatasetPrefix", "hello")
	cfgfiles := createTempConfigs(t, cm1, cm2)

	cfg := configContents{}
	hash, err := loadConfigsInto(&cfg, cfgfiles)
	require.NoError(t, err)
	require.Equal(t, "2381a6563085f50ac56663b67ca85299", hash)
	require.Equal(t, 2, cfg.General.ConfigurationVersion)
	require.Equal(t, Duration(2*time.Second), cfg.General.ConfigReloadInterval)
	require.Equal(t, "0.0.0.0:8080", cfg.Network.ListenAddr)
	require.Equal(t, "hello", cfg.General.DatasetPrefix)
}

func Test_loadConfigsIntoMap(t *testing.T) {
	cm1 := makeYAML("General.ConfigurationVersion", 2, "General.ConfigReloadInterval", Duration(1*time.Second), "Network.ListenAddr", "0.0.0.0:8080")
	cm2 := makeYAML("General.ConfigReloadInterval", Duration(2*time.Second), "General.DatasetPrefix", "hello")
	cfgfiles := createTempConfigs(t, cm1, cm2)

	cfg := map[string]any{}
	err := loadConfigsIntoMap(cfg, cfgfiles)
	require.NoError(t, err)
	gen := cfg["General"].(map[string]any)
	require.Equal(t, 2, gen["ConfigurationVersion"])
	require.Equal(t, "2s", gen["ConfigReloadInterval"])
	require.Equal(t, "hello", gen["DatasetPrefix"])
	net := cfg["Network"].(map[string]any)
	require.Equal(t, "0.0.0.0:8080", net["ListenAddr"])
}

func Test_validateConfigs(t *testing.T) {
	emptySlice := []string{}
	tests := []struct {
		name    string
		cfgs    []string
		want    []string
		wantErr bool
	}{
		{
			"test1", []string{
				makeYAML("General.ConfigurationVersion", 2, "General.ConfigReloadInterval", Duration(1*time.Second), "Network.ListenAddr", "0.1.2.3:8080"),
			},
			emptySlice,
			false,
		},
		{
			"test2", []string{
				makeYAML("General.ConfigurationVersion", 2, "General.ConfigReloadInterval", Duration(1*time.Second), "Network.ListenAddr", "0.1.2.3:8080"),
				makeYAML("General.ConfigReloadInterval", Duration(2*time.Second)),
			},
			emptySlice,
			false,
		},
		{
			"test3", []string{
				makeYAML("General.ConfigurationVersion", 2, "General.ConfigReloadInterval", Duration(1*time.Second), "Network.ListenAddr", "0.1.2.3:8080"),
				makeYAML("General.ConfigReloadInterval", Duration(2*time.Second), "General.DatasetPrefix", 7),
			},
			[]string{"field General.DatasetPrefix must be a string but 7 is int"},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfgfiles := createTempConfigs(t, tt.cfgs...)
			opts := &CmdEnv{ConfigLocations: cfgfiles}
			got, err := validateConfigs(opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateConfigs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("validateConfigs() = %v, want %v", got, tt.want)
			}
		})
	}
}

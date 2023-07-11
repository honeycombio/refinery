package config

import (
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"
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

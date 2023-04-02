package main

import (
	"net/http"
	"testing"
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

package main

import (
	"os"
	"testing"
	"text/template"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v2"
)

type Field struct {
	Name         string   `json:"name"`
	V1Group      string   `json:"v1group"`
	V1Name       string   `json:"v1name"`
	FirstVersion string   `json:"firstversion"`
	LastVersion  string   `json:"lastversion"`
	Type         string   `json:"type"`
	ValueType    string   `json:"valuetype"`
	Default      any      `json:"default,omitempty"`
	Choices      []string `json:"choices,omitempty"`
	Example      string   `json:"example,omitempty"`
	Validation   string   `json:"validation,omitempty"`
	Reload       bool     `json:"reload"`
	Summary      string   `json:"summary"`
	Description  string   `json:"description"`
}

type Group struct {
	Name        string  `json:"name"`
	Title       string  `json:"title"`
	Description string  `json:"description"`
	Fields      []Field `json:"fields,omitempty"`
}

type ConfigData struct {
	Groups []Group `json:"groups"`
}

// This is a weird little hack; we're using test code to generate the template
// used by the convert tool. Every time we run tests, the template will be
// regenerated.  This is a bit of a hack, but it's better than maintaining two
// different executables, one of which is only used to generate the data for the
// other. Plus we get to use the test framework to save on error handling, and
// we get to reuse the file loading functions for the template generator.
func TestGenerateTemplate(t *testing.T) {
	input := "configData.yaml"
	rdr, err := os.Open(input)
	assert.NoError(t, err)
	defer rdr.Close()

	var config ConfigData
	decoder := yaml.NewDecoder(rdr)
	err = decoder.Decode(&config)
	assert.NoError(t, err)

	tmpl := template.New("group")
	tmpl.Funcs(helpers())
	tmpl, err = tmpl.Parse(fileTemplate)
	assert.NoError(t, err)
	tmpl, err = tmpl.Parse(groupTemplate)
	assert.NoError(t, err)
	tmpl, err = tmpl.Parse(fieldTemplate)
	assert.NoError(t, err)

	f, err := os.Create("configV2.tmpl")
	assert.NoError(t, err)
	defer f.Close()

	// fmt.Printf("%#v\n", config)
	err = tmpl.ExecuteTemplate(f, "file", config)
	assert.NoError(t, err)
}

// This generates a nested list of the groups and names in configData.yaml.
func TestPrintNames(t *testing.T) {
	input := "configData.yaml"
	rdr, err := os.Open(input)
	assert.NoError(t, err)
	defer rdr.Close()

	var config ConfigData
	decoder := yaml.NewDecoder(rdr)
	err = decoder.Decode(&config)
	assert.NoError(t, err)

	templ := `
	{{- define "file" -}}
	{{- $file := . -}}
	{{ range $file.Groups -}}
		{{- $group := . }}
		{{ $group.Name }}:
		{{- range $group.Fields -}}
			{{- $field := . }}
			{{ print "  - " $field.Name -}}
			{{- if $field.V1Name -}}
				{{- print " (originally " -}}
				{{- if $field.V1Group -}}
					{{- print $field.V1Group "." -}}
				{{- end -}}
				{{- print $field.V1Name ")" -}}
			{{- end -}}
		{{ end }}
	{{- end -}}
	{{- end -}}
	`

	tmpl := template.New("group")
	// tmpl.Funcs(helpers())
	tmpl, err = tmpl.Parse(templ)
	assert.NoError(t, err)

	f, err := os.Create("configDataNames.txt")
	assert.NoError(t, err)
	defer f.Close()

	err = tmpl.ExecuteTemplate(f, "file", config)
	assert.NoError(t, err)
}

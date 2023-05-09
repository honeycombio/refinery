package main

// This file contains the templates used for the test that reads configData.yaml
// and generates the configV2.tmpl file (see main_test.go)

var fileTemplate = `
{{- define "file" -}}
{{- $file := . -}}
{{ "Honeycomb Refinery Configuration" | box }}
#
# created on {{ ".Now" | meta }} from {{ ".Input" | meta }}

# This file contains the configuration for the Honeycomb Refinery.
# More stuff will go here later.

{{ range $file.Groups -}}
{{ template "group" . }}
{{- end -}}
{{- end -}}
`

var groupTemplate = `
{{- define "group" -}}
{{- $group := . }}
{{ box $group.Title }}
{{ $group.Name }}:
{{ $group.Description | wordwrap | comment | indent 2 }}
  ####
{{- range $group.Fields -}}
{{- if .LastVersion -}}
  {{- continue -}}
{{- end -}}
{{ template "field" . }}
{{- end -}}

{{- end }}
`

var fieldTemplate = `
{{- define "field" -}}
{{- $field := . -}}
{{/* {{ print $field.Name " " $field.Summary | wordwrap | comment | indent 4 }} */}}
{{- if $field.Description }}
{{ $field.Description | wci 4 }}
{{ reload $field.Reload | indent 4 }}
{{- if eq $field.ValueType "nondefault" }}
{{ printf "nonDefaultOnly .Data \"%s\" %#v" $field.Name $field.Default | meta | indent 4 }}
{{- else if eq $field.ValueType "nonzero" }}
{{ printf "nonZero .Data \"%s\" %#v" $field.Name $field.Example | meta | indent 4 }}
{{- else if eq $field.ValueType "nonemptystring" }}
{{ printf "nonEmptyString .Data \"%s\" %#v" $field.Name $field.Example| meta | indent 4 }}
{{- else if eq $field.ValueType "choice" }}
{{ printf "Options: %s" (join $field.Choices " ") | comment | indent 4 }}
{{ printf "choice .Data \"%s\" %s \"%s\"" $field.Name (genSlice $field.Choices) $field.Default | meta | indent 4 }}
{{- else if eq $field.ValueType "new" }}
{{ print $field.Name ": " $field.Default | indent 4 }}
{{- else if eq $field.ValueType "map" }}
{{ printf "renderMap .Data \"%s\" \"%s\"" $field.Name $field.Example | meta | indent 4 }}
{{- else if eq $field.ValueType "stringarray" }}
{{ printf "renderStringarray .Data \"%s\" \"%s\"" $field.Name $field.Example | meta | indent 4 }}
{{- else }}
{{ printf ">%#v<" $field | comment | indent 4 }}
{{ end -}}
{{ end }}
{{ end -}}
`

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
{{- println -}}
{{ box "Config values removed by the config converter" }}
{{ println "The following configuration options are obsolete and are not included in the new configuration:" | wci 2 }}
{{ range $file.Groups -}}
{{ template "removed" . }}
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

var removedTemplate = `
{{- define "removed" -}}
{{- $group := . }}
{{- range $group.Fields -}}
{{- $field := . -}}
{{- $oldname := $field.Name -}}
{{- if $field.V1Name -}}
  {{- if $field.V1Group -}}
    {{- $oldname = (print $field.V1Group "." $field.V1Name) -}}
  {{- else -}}
    {{- $oldname = $field.V1Name -}}
  {{- end -}}
{{- end -}}
{{- if $field.LastVersion -}}
  {{- printf "- %s" $oldname | comment | indent 4  -}}
  {{- println -}}
{{- end -}}
{{- end -}}

{{- end }}
`

var fieldTemplate = `
{{- define "field" -}}
{{- $field := . -}}
{{- $oldname := $field.Name -}}
{{- if $field.V1Name -}}
  {{- if $field.V1Group -}}
    {{- $oldname = (print $field.V1Group "." $field.V1Name) -}}
  {{- else -}}
    {{- $oldname = $field.V1Name -}}
  {{- end -}}
{{- end -}}
{{/* {{ print $field.Name " " $field.Summary | wordwrap | comment | indent 4 }} */}}
{{- if $field.Description }}
{{ $field.Description | wci 4 }}
{{ formatExample $field.Type $field.Default $field.Example | comment | indent 4 }}
{{ reload $field.Reload | indent 4 }}
{{- if eq $field.ValueType "nondefault" }}
{{ printf "nonDefaultOnly .Data \"%s\" \"%s\" %#v" $field.Name $oldname $field.Default | meta | indent 4 }}
{{- else if eq $field.ValueType "nonzero" }}
{{ printf "nonZero .Data \"%s\" \"%s\" %#v" $field.Name $oldname $field.Example | meta | indent 4 }}
{{- else if eq $field.ValueType "nonemptystring" }}
{{ printf "nonEmptyString .Data \"%s\" \"%s\" %#v" $field.Name $oldname $field.Example| meta | indent 4 }}
{{- else if eq $field.ValueType "secondstoduration" }}
{{ printf "secondsToDuration .Data \"%s\" \"%s\" %#v" $field.Name $oldname $field.Example| meta | indent 4 }}
{{- else if eq $field.ValueType "choice" }}
{{ printf "Options: %s" (join $field.Choices " ") | comment | indent 4 }}
{{ printf "choice .Data \"%s\" \"%s\" %s \"%s\"" $field.Name $oldname (genSlice $field.Choices) $field.Default | meta | indent 4 }}
{{- else if eq $field.ValueType "new" }}
{{ print $field.Name ": " $field.Default | indent 4 }}
{{- else if eq $field.ValueType "map" }}
{{ printf "renderMap .Data \"%s\" \"%s\" \"%s\"" $field.Name $oldname $field.Example | meta | indent 4 }}
{{- else if eq $field.ValueType "stringarray" }}
{{ printf "renderStringarray .Data \"%s\" \"%s\" \"%s\"" $field.Name $oldname $field.Example | meta | indent 4 }}
{{- else if eq $field.ValueType "conditional" }}
{{ printf "conditional .Data \"%s\" \"%s\"" $field.Name $field.Extra | meta | indent 4 }}
{{- else }}
{{ printf "******** ERROR %#v has bad ValueType %v" $field.Name $field.ValueType }}
{{ end -}}
{{ end }}
{{ end -}}
`

package main

import (
	"fmt"
	"github.com/honeycombio/refinery/config"
	"html/template"
	"regexp"
	"strings"
	"time"
)

// This file contains template helper functions, which must be listed in this
// map if they're going to be available to the template.
// The map key is the name of the function as it will be used in the template,
// and the value is the function itself.
// The function must return a string, and may take any number of arguments.
// The functions are listed below in alphabetical order; please keep them that way.
func helpers() template.FuncMap {
	return map[string]any{
		"anchorize":         anchorize,
		"box":               box,
		"choice":            choice,
		"comment":           comment,
		"conditional":       conditional,
		"envvar":            envvar,
		"formatExample":     formatExample,
		"genSlice":          genSlice,
		"indent":            indent,
		"indentRest":        indentRest,
		"join":              join,
		"makeSlice":         makeSlice,
		"memorysize":        memorysize,
		"meta":              meta,
		"nonDefaultOnly":    nonDefaultOnly,
		"nonEmptyString":    nonEmptyString,
		"nonZero":           nonZero,
		"now":               now,
		"pattern":           pattern,
		"reload":            reload,
		"renderMap":         renderMap,
		"renderStringarray": renderStringarray,
		"schemaType":        schemaType,
		"secondsToDuration": secondsToDuration,
		"stringArray":       stringArray,
		"split":             split,
		"wci":               wci,
		"wordwrap":          wordwrap,
		"wrapForDocs":       wrapForDocs,
		"yamlf":             yamlf,
	}
}

func anchorize(s string) string {
	pat := regexp.MustCompile(`[^a-zA-Z0-9]+`)
	return strings.ToLower(pat.ReplaceAllLiteralString(s, "-"))
}

func box(s string) string {
	boxwidth := len(s) + 6
	result := strings.Repeat("#", boxwidth)
	result += fmt.Sprintf("\n## %s ##\n", s)
	result += strings.Repeat("#", boxwidth)
	return result
}

func choice(data map[string]any, key, oldkey string, choices []string, def string) string {
	if value, ok := _fetch(data, oldkey); ok {
		if _equivalent(value, def) {
			return fmt.Sprintf("# %s: %v", key, yamlf(def))
		}
		for _, c := range choices {
			if _equivalent(value, c) {
				return fmt.Sprintf("%s: %v", key, yamlf(value))
			}
		}
		return fmt.Sprintf("# %s: %v  ### Invalid option!", key, yamlf(value))
	}
	return fmt.Sprintf("# %s: %v", key, yamlf(def))
}

func comment(s string) string {
	return strings.TrimRight("## "+strings.Replace(s, "\n", "\n## ", -1), " ")
}

func conditional(data map[string]any, key string, extra string) string {
	extras := strings.Split(extra, " ")
	switch extras[0] {
	case "eq":
		k := extras[1]
		v := extras[2]
		if value, ok := _fetch(data, k); ok {
			if _equivalent(value, v) {
				return fmt.Sprintf("%s: true", key)
			}
		}
	case "nostar":
		// if the slice named exists, has no "*" values, and has at least one value, return true
		k := extras[1]
		if value, ok := _fetch(data, k); ok {
			list := _getStringsFrom(value)
			hasStar := false
			for _, v := range list {
				if v == "*" {
					hasStar = true
				}
			}

			if len(list) > 0 && !hasStar {
				return fmt.Sprintf("%s: true", key)
			}
		}
	case "nonempty":
		k := extras[1]
		if value, ok := _fetch(data, k); ok {
			v := fmt.Sprintf("%v", value)
			if v != "" {
				return fmt.Sprintf("%s: true", key)
			}
		}
	default:
		panic("Unknown conditional: " + extra)
	}
	return fmt.Sprintf("# %s: false", key)
}

// Describes an environment variable
func envvar(s string) string {
	return fmt.Sprintf("# May be specified in the environment as %s.", s)
}

// Returns standardized format text
func formatExample(typ string, def, example any) string {
	if def != nil && !_isZeroValue(def) {
		example = def
	}
	switch typ {
	case "hostport":
		return fmt.Sprintf(`Should be an ip:port like "%v".`, example)
	case "duration":
		return fmt.Sprintf(`Accepts a duration string with units, like "%s".`, example)
	default:
		return ""
	}
}

func genSlice(s []string) string {
	quoted := make([]string, len(s))
	for i, v := range s {
		quoted[i] = fmt.Sprintf(`"%s"`, v)
	}
	return fmt.Sprintf("(makeSlice %s)", strings.Join(quoted, " "))
}

func indent(count int, s string) string {
	return strings.Repeat(" ", count) + indentRest(count, s)
}

func indentRest(count int, s string) string {
	eolpat := regexp.MustCompile(`[ \t]*\n[ \t]*`)
	return eolpat.ReplaceAllString(s, "\n"+strings.Repeat(" ", count))
}

func join(a []string, sep string) string {
	return strings.Join(a, sep)
}

func makeSlice(a ...string) []string {
	return a
}

// memorysize takes a memory size (if the previous value had it) and returns a string representation
// of memory size in human-readable form.
func memorysize(data map[string]any, key, oldkey string, example string) string {
	i64 := int64(0)
	if value, ok := _fetch(data, oldkey); ok && value != "" {
		switch i := value.(type) {
		case int64:
			i64 = i
		case int:
			i64 = int64(i)
		}
		txt, _ := config.MemorySize(i64).MarshalText()
		return fmt.Sprintf(`%s: %s`, key, string(txt))
	}
	return fmt.Sprintf(`# %s: %v`, key, yamlf(example))
}

func meta(s string) string {
	return "{{ " + s + " }}"
}

// Takes a key that may or may not be in the incoming data, and a default value.
// If the key exists, AND the value is not equivalent to the default value,
// it returns "Key: value" for the value found.
// Otherwise, it returns "# Key: default" to show a default value.
func nonDefaultOnly(data map[string]any, key, oldkey string, def any) string {
	if value, ok := _fetch(data, oldkey); ok && !_equivalent(value, def) {
		return fmt.Sprintf("%s: %s", key, yamlf(value))
	}
	return fmt.Sprintf("# %s: %v", key, yamlf(def))
}

// Takes a key that may or may not be in the incoming data, and an example value.
// If the key exists, AND the value is not an empty string,
// it returns "Key: value" for the value found.
// Otherwise, it returns "# Key: example" to show the example value.
func nonEmptyString(data map[string]any, key, oldkey string, example string) string {
	if value, ok := _fetch(data, oldkey); ok && value != "" {
		return fmt.Sprintf("%s: %v", key, yamlf(value))
	}
	return fmt.Sprintf(`# %s: %v`, key, yamlf(example))
}

// Takes a key that may or may not be in the incoming data.
// If the key exists, AND the value at that key is not equivalent to the zero value,
// it returns "Key: value" for the value found.
// Otherwise, it returns "# Key: example" to show an example value.
func nonZero(data map[string]any, key, oldkey string, example string) string {
	if value, ok := _fetch(data, oldkey); ok {
		comment := ""
		if _isZeroValue(value) {
			comment = "# "
		}
		return fmt.Sprintf(`%s%s: %v`, comment, key, yamlf(value))
	}
	return fmt.Sprintf(`# %s: %v`, key, yamlf(example))
}

func now() string {
	t := time.Now().UTC()
	return fmt.Sprintf("on %s at %s UTC", t.Format("2006-01-02"), t.Format("15:04:05"))
}

func pattern(typ, pattyp string) string {
	s := typ
	if s == "string" && pattyp != "" {
		s = pattyp
	}

	switch s {
	case "hostport":
		return ""
	case "apikey":
		// classic keys are 32 hex digits
		// new keys are 20-23 base64 digits
		return `^(\*|[A-Fa-f0-9]{32}|[A-Za-z0-9]{20,23})$`
	case "duration":
		return `^([0-9]*(\.[0-9]*)?(ns|us|µs|ms|s|m|h))+$`
	default:
		return ""
	}
}

// Returns the reload eligibility string
func reload(b bool) string {
	if b {
		return "## Eligible for live reload."
	}
	return "## Not eligible for live reload."
}

func renderMap(data map[string]any, key, oldkey string, example string) string {
	var mapValues map[string]string
	comment := ""
	if value, ok := data[key]; ok {
		mapValues = value.(map[string]string)
	} else {
		values := strings.Split(example, ",")
		mapValues = make(map[string]string)
		for _, v := range values {
			kv := strings.Split(v, ":")
			mapValues[kv[0]] = kv[1]
			comment = "# "
		}
	}
	var output []string
	for k, v := range mapValues {
		output = append(output, fmt.Sprintf("%s %s: %s", comment, k, v))
	}
	return "# " + key + ":\n      " + strings.Join(output, "\n      ")
}

func renderStringarray(data map[string]any, key, oldkey string, example string) string {
	var sa []string
	comment := ""
	if v, ok := _fetch(data, oldkey); ok {
		switch value := v.(type) {
		case []interface{}:
			for _, s := range value {
				sa = append(sa, s.(string))
			}
		case []string:
			sa = value
		}
	}

	if len(sa) == 0 {
		sa = strings.Split(example, ",")
		comment = "# "
	}

	var output []string
	for _, s := range sa {
		output = append(output, fmt.Sprintf("%s- %s", comment, s))
	}
	return comment + key + ":\n      " + strings.Join(output, "\n      ")
}

func schemaType(typ string) string {
	switch typ {
	case "int", "percentage":
		return "integer"
	case "float":
		return "number"
	case "string":
		return "string"
	case "bool":
		return "boolean"
	case "duration":
		return "string"
	case "hostport", "url":
		return "string"
	case "stringarray":
		return "array"
	case "map":
		return "object"
	default:
		return "UNKNOWN"
	}
}

// secondsToDuration takes a number of seconds (if the previous value had it) and returns a string duration
func secondsToDuration(data map[string]any, key, oldkey string, example string) string {
	i64 := int64(0)
	if value, ok := _fetch(data, oldkey); ok && value != "" {
		switch i := value.(type) {
		case int64:
			i64 = i
		case int:
			i64 = int64(i)
		}
		dur := time.Duration(i64) * time.Second
		return fmt.Sprintf("%s: %v", key, yamlf(dur))
	}
	return fmt.Sprintf(`# %s: %v`, key, yamlf(example))
}

func split(s, sep string) []string {
	return strings.Split(s, sep)
}

// Prints a nicely-formatted string array; if the incoming string array doesn't exist, or
// exactly matches the default, then it's commented out.
func stringArray(data map[string]any, key, oldkey string, indent int, examples ...any) string {
	var keys []string
	for _, e := range examples {
		if s, ok := e.(string); ok {
			keys = append(keys, s)
		}
	}

	comment := "# "
	// if the user has keys we want them, unless it's bad or just ["*"]
	if value, ok := _fetch(data, oldkey); ok {
		userkeys := _getStringsFrom(value)

		if !_equivalent(keys, userkeys) {
			comment = ""
			keys = userkeys
		}
	}

	s := fmt.Sprintf("%s%s:\n", comment, key)
	for _, k := range keys {
		s += fmt.Sprintf("%s%s- %v\n", strings.Repeat(" ", indent), comment, yamlf(k))
	}
	return s
}

func wci(ind int, s string) string {
	return indent(ind, comment(wordwrap(s)))
}

func wordwrap(s string) string {
	const width = 70

	var lines []string = strings.Split(s, "\n")
	var output []string
	for _, l := range lines {
		var result string
		var line string
		var words []string = strings.Split(l, " ")
		for _, w := range words {
			if len(line)+len(w) > width {
				result += strings.TrimSpace(line) + "\n"
				line = ""
			}
			line += w + " "
		}
		result += strings.TrimSpace(line)
		output = append(output, result)
	}
	return strings.Join(output, "\n")
}

func wrapForDocs(s string) string {
	paragraphBreak := regexp.MustCompile(`\n\s*\n`)
	s = paragraphBreak.ReplaceAllString(s, "__PARAGRAPH_BREAK__")
	sentenceEnd := regexp.MustCompile(`([.?!])\s+|__PARAGRAPH_BREAK__`)
	s = sentenceEnd.ReplaceAllString(s, "$1\n")
	return s
}

// simplistic YAML formatting of a value
func yamlf(a any) string {
	switch v := a.(type) {
	case string:
		pat := regexp.MustCompile("^[a-zA-z0-9]+$")
		if pat.MatchString(v) {
			return v
		}
		hasSingleQuote := strings.Contains(v, "'")
		hasDoubleQuote := strings.Contains(v, `"`)
		switch {
		case hasDoubleQuote && !hasSingleQuote:
			return fmt.Sprintf(`'%s'`, v)
		default:
			return fmt.Sprintf("%#v", v)
		}
	case int:
		return _formatIntWithUnderscores(v)
	case float64:
		return fmt.Sprintf("%f", v)
	case time.Duration:
		return v.String()
	default:
		return fmt.Sprintf("%v", a)
	}
}

// The functions below are internal to this file hence the leading underscore.

// internal function to compare two "any" values for equivalence
func _equivalent(a, b any) bool {
	va := fmt.Sprintf("%v", a)
	vb := fmt.Sprintf("%v", b)
	return va == vb
}

// this formats an integer with underscores for readability.
// e.g. 1000000 becomes 1_000_000
// The code sucks but Copilot wrote it and performance doesn't matter.
func _formatIntWithUnderscores(i int) string {
	s := fmt.Sprintf("%d", i)
	var output []string
	for len(s) > 3 {
		output = append([]string{s[len(s)-3:]}, output...)
		s = s[:len(s)-3]
	}
	output = append([]string{s}, output...)
	return strings.Join(output, "_")
}

func _isZeroValue(value any) bool {
	switch v := value.(type) {
	case string:
		return v == ""
	case int:
		return v == 0
	case int64:
		return v == 0
	case float64:
		return v == 0.0
	case bool:
		return !v
	case []string:
		return len(v) == 0
	case map[string]string:
		return len(v) == 0
	case map[string]any:
		return len(v) == 0
	default:
		return false
	}
}

// Takes a key that may or may not be in the incoming data,
// and returns the value found, possibly doing a recursive call
// separated by dots in the key.
func _fetch(data map[string]any, key string) (any, bool) {
	if value, ok := data[key]; ok {
		return value, true
	}
	if strings.Contains(key, ".") {
		parts := strings.SplitN(key, ".", 2)
		groups := strings.Split(parts[0], "/")
		for _, g := range groups {
			if value, ok := data[g]; ok {
				if submap, ok := value.(map[string]any); ok {
					return _fetch(submap, parts[1])
				}
			}
		}
	}
	return nil, false
}

// Takes a value that is a slice of strings or any and returns a slice of
// strings.
func _getStringsFrom(value any) []string {
	result := make([]string, 0)

	if ary, ok := value.([]string); ok {
		return ary
	}

	if ary, ok := value.([]any); ok {
		for _, elt := range ary {
			if v, ok := elt.(string); ok {
				result = append(result, v)
			}
		}
	}
	return result
}

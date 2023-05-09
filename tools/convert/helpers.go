package main

import (
	"fmt"
	"html/template"
	"regexp"
	"strings"
)

// This file contains template helper functions, which must be listed in this
// map if they're going to be available to the template
func helpers() template.FuncMap {
	return map[string]any{
		"box":               box,
		"choice":            choice,
		"comment":           comment,
		"envvar":            envvar,
		"formatExample":     formatExample,
		"genSlice":          genSlice,
		"indent":            indent,
		"indentRest":        indentRest,
		"join":              join,
		"meta":              meta,
		"nonDefaultOnly":    nonDefaultOnly,
		"nonEmptyString":    nonEmptyString,
		"nonZero":           nonZero,
		"reload":            reload,
		"renderMap":         renderMap,
		"renderStringarray": renderStringarray,
		"slice":             slice,
		"stringArray":       stringArray,
		"wci":               wci,
		"wordwrap":          wordwrap,
	}
}

func wci(ind int, s string) string {
	return indent(ind, comment(wordwrap(s)))
}

func indent(spaces int, s string) string {
	return strings.Repeat(" ", spaces) + indentRest(spaces, s)
}

func indentRest(spaces int, s string) string {
	return strings.Replace(s, "\n", "\n"+strings.Repeat(" ", spaces), -1)
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
				result += line + "\n"
				line = ""
			}
			line += w + " "
		}
		result += line
		output = append(output, result)
	}
	return strings.Join(output, "\n")
}

func comment(s string) string {
	return "## " + strings.Replace(s, "\n", "\n## ", -1)
}

func meta(s string) string {
	return "{{ " + s + " }}"
}

// internal function to compare two "any" values for equivalence
func equivalent(a, b any) bool {
	va := fmt.Sprintf("%v", a)
	vb := fmt.Sprintf("%v", b)
	return va == vb
}

// this formats an integer with underscores for readability.
// e.g. 1000000 becomes 1_000_000
// The code sucks but Copilot wrote it and performance doesn't matter.
func formatIntWithUnderscores(i int) string {
	s := fmt.Sprintf("%d", i)
	var output []string
	for len(s) > 3 {
		output = append([]string{s[len(s)-3:]}, output...)
		s = s[:len(s)-3]
	}
	output = append([]string{s}, output...)
	return strings.Join(output, "_")
}

// simplistic YAML formatting of a value
func yamlFormat(a any) string {
	switch v := a.(type) {
	case string:
		pat := regexp.MustCompile("^[a-zA-z][a-zA-z0-9]*$")
		if pat.MatchString(v) {
			return v
		}
		return fmt.Sprintf(`"%s"`, v)
	case int:
		return formatIntWithUnderscores(v)
	default:
		return fmt.Sprintf("%v", a)
	}
}

func join(a []string, sep string) string {
	return strings.Join(a, sep)
}

func slice(a ...string) []string {
	return a
}

func genSlice(s []string) string {
	quoted := make([]string, len(s))
	for i, v := range s {
		quoted[i] = fmt.Sprintf(`"%s"`, v)
	}
	return fmt.Sprintf("(slice %s)", strings.Join(quoted, " "))
}

func renderMap(data map[string]any, key string, example string) string {
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

func renderStringarray(data map[string]any, key string, example string) string {
	var sa []string
	comment := ""
	if value, ok := data[key]; ok {
		sa = value.([]string)
	}

	if len(sa) == 0 {
		sa = strings.Split(example, ",")
		comment = "# "
	}

	var output []string
	for _, s := range sa {
		output = append(output, fmt.Sprintf("%s- %s", comment, s))
	}
	return "# " + key + ":\n      " + strings.Join(output, "\n      ")
}

// Takes a key that may or may not be in the incoming data, and a default value.
// If the key exists, AND the value is not equivalent to the default value,
// it returns "Key: value" for the value found.
// Otherwise, it returns "# Key: default" to show a default value.
func nonDefaultOnly(data map[string]any, key string, def any) string {
	if value, ok := data[key]; ok && !equivalent(value, def) {
		return fmt.Sprintf("%s: %s", key, yamlFormat(value))
	}
	return fmt.Sprintf("# %s: %v", key, yamlFormat(def))
}

// Takes a key that may or may not be in the incoming data, and an example value.
// If the key exists, AND the value is not an empty string,
// it returns "Key: value" for the value found.
// Otherwise, it returns "# Key: example" to show the example value.
func nonEmptyString(data map[string]any, key string, example string) string {
	if value, ok := data[key]; ok && value != "" {
		return fmt.Sprintf("%s: %v", key, yamlFormat(value))
	}
	return fmt.Sprintf(`# %s: %v`, key, yamlFormat(example))
}

// Takes a key that may or may not be in the incoming data.
// If the key exists, AND the value at that key is not equivalent to the zero value,
// it returns "Key: value" for the value found.
// Otherwise, it returns "# Key: example" to show an example value.
func nonZero(data map[string]any, key string, example string) string {
	if value, ok := data[key]; ok {
		output := ""
		switch v := value.(type) {
		case string:
			if v != "" {
				output = v
			}
		case int:
			if v != 0 {
				output = fmt.Sprintf("%d", v)
			}
		case bool:
			if v {
				output = "true"
			}
		default:
			output = fmt.Sprintf("%v", value)
		}
		if output != "" {
			return fmt.Sprintf("%s: %v", key, yamlFormat(value))
		}
	}
	return fmt.Sprintf(`# %s: %v`, key, yamlFormat(example))
}

func choice(data map[string]any, key string, choices []string, def string) string {
	if value, ok := data[key]; ok {
		if equivalent(value, def) {
			return fmt.Sprintf("# %s: %v", key, yamlFormat(def))
		}
		for _, c := range choices {
			if equivalent(value, c) {
				return fmt.Sprintf("%s: %v", key, yamlFormat(value))
			}
		}
		return fmt.Sprintf("# %s: %v  ### Invalid option!", key, yamlFormat(value))
	}
	return fmt.Sprintf("# %s: %v", key, yamlFormat(def))
}

// Prints a nicely-formatted string array; if the incoming string array doesn't exist, or
// exactly matches the default, then it's commented out.
func stringArray(data map[string]any, key string, indent int, examples ...string) string {
	var keys []string = examples
	var userdata []string

	comment := "# "

	// if the user has keys we want them, unless it's bad or just ["*"]
	if value, ok := data[key]; ok {
		if userkeys, ok := value.([]any); ok {
			for _, u := range userkeys {
				if uv, ok := u.(string); ok {
					userdata = append(userdata, uv)
				}
			}
		}

		if !equivalent(keys, userdata) {
			comment = ""
			keys = userdata
		}
	}

	s := fmt.Sprintf("%s%s:\n", comment, key)
	for _, k := range keys {
		s += fmt.Sprintf("%s%s- %v\n", strings.Repeat(" ", indent), comment, yamlFormat(k))
	}
	return s
}

// Returns the reload eligibility string
func reload(b bool) string {
	if b {
		return "## Eligible for live reload."
	}
	return "## Not eligible for live reload."
}

// Describes an environment variable
func envvar(s string) string {
	return fmt.Sprintf("# May be specified in the environment as %s.", s)
}

// Returns standardized format text
func formatExample(name string, example any) (string, error) {
	switch name {
	case "form":
		return fmt.Sprintf(`# Should be of the form "%v".`, example), nil
	case "ipport":
		return fmt.Sprintf(`# Should be an ip:port like "0.0.0.0:%v".`, example), nil
	case "duration":
		return fmt.Sprintf(`# Accepts a duration string with units, like "%s".`, example), nil
	default:
		return "", fmt.Errorf("requires a known example type")
	}
}

func box(s string) string {
	boxwidth := len(s) + 6
	result := strings.Repeat("#", boxwidth)
	result += fmt.Sprintf("\n## %s ##\n", s)
	result += strings.Repeat("#", boxwidth)
	return result
}

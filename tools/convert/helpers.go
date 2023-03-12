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
		"box":            box,
		"envvar":         envvar,
		"formatExample":  formatExample,
		"nonDefaultOnly": nonDefaultOnly,
		"nonEmptyString": nonEmptyString,
		"reload":         reload,
		"stringArray":    stringArray,
	}
}

// internal function to compare two "any" values for equivalence
func equivalent(a, b any) bool {
	va := fmt.Sprintf("%v", a)
	vb := fmt.Sprintf("%v", b)
	return va == vb
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
	default:
		return fmt.Sprintf("%v", a)
	}
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
		return "# Eligible for live reload."
	}
	return "# Not eligible for live reload."
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

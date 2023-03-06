package main

import (
	"fmt"
	"html/template"
	"strings"
)

// This file contains template helper functions, which must be listed in this
// map if they're going to be available to the template
func helpers() template.FuncMap {
	return map[string]any{
		"apikeys":        apikeys,
		"box":            box,
		"formatExample":  formatExample,
		"nonDefaultOnly": nonDefaultOnly,
		"reload":         reload,
	}
}

// internal function to compare two "any" values for equivalence
func equivalent(a, b any) bool {
	va := fmt.Sprintf("%v", a)
	vb := fmt.Sprintf("%v", b)
	return va == vb
}

// Takes a key that may or may not be in the incoming data, and a default value.
// If the key exists, AND the value is not equivalent to the default value,
// it returns "Key: value" for the value found.
// Otherwise, it returns "# Key: default" to show a default value.
func nonDefaultOnly(data map[string]any, key string, def any) string {
	if value, ok := data[key]; ok && !equivalent(value, def) {
		return fmt.Sprintf("%s: %v", key, value)
	}
	return fmt.Sprintf("# %s: %v", key, def)
}

// we want to show a good example for API keys if the data is just ["*"],
// but if the user has done something meaningful we want to keep it
func apikeys(data map[string]any, key string, indent int, examples ...string) string {
	var keys []string

	// if the user has keys we want them, unless it's bad or just ["*"]
	if value, ok := data[key]; ok {
		if userkeys, ok := value.([]any); ok {
			for _, u := range userkeys {
				keys = append(keys, fmt.Sprintf(`- "%s"`, u.(string)))
			}
		}
	}
	if len(keys) == 0 || (len(keys) == 1 && keys[0] == `- "*"`) {
		keys = examples
	}
	s := "APIKeys:\n"
	for _, k := range keys {
		s += fmt.Sprintf("%s%v\n", strings.Repeat(" ", indent), k)
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

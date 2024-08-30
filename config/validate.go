package config

import (
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strings"
	"time"

	"golang.org/x/exp/slices"
)

// Takes a map and flattens the top level of it into a new map with a dotted key.
func flatten(data map[string]any, recur bool) map[string]any {
	result := make(map[string]any)
	for k, v := range data {
		switch val := v.(type) {
		case map[string]any:
			if recur {
				for kk, vv := range flatten(val, false) {
					result[k+"."+kk] = vv
				}
			} else {
				result[k] = v
			}
		default:
			result[k] = v
		}
	}
	return result
}

func isString(v any) bool {
	switch v.(type) {
	case string:
		return true
	default:
		return false
	}
}

// returns a value as a float if possible so that we can do comparisons
func asFloat(v any) (float64, string) {
	switch val := v.(type) {
	case int:
		return float64(val), ""
	case int64:
		return float64(val), ""
	case float64:
		return float64(val), ""
	case Duration:
		return float64(val), ""
	case string:
		// can we interpret it as a duration?
		f, err := time.ParseDuration(v.(string))
		if err == nil {
			return float64(f.Milliseconds()), ""
		}
		// can we interpret it as a memory size?
		var m MemorySize
		err = m.UnmarshalText([]byte(v.(string)))
		if err == nil {
			return float64(m), ""
		}
	default:
	}
	return 0, fmt.Sprintf("%#v (%T) cannot be interpreted as a quantity", v, v)
}

func mustFloat(v any) float64 {
	f, msg := asFloat(v)
	if msg != "" {
		panic(msg)
	}
	return f
}

func validateDatatype(k string, v any, typ string) string {
	if v == nil {
		return fmt.Sprintf("field %s must not be nil", k)
	}
	switch typ {
	case "object":
		// special case that this means we should recurse -- but not here
		// just make sure that the value is a map
		if _, ok := v.(map[string]any); !ok {
			return fmt.Sprintf("field %s must be an object", k)
		}
	case "objectarray":
		// special case that this means we should recurse -- but not here
		// just make sure that the value is an array
		if _, ok := v.([]any); !ok {
			return fmt.Sprintf("field %s must be an array of objects", k)
		}
	case "anyscalar":
		switch v.(type) {
		case string, int, int64, float64, bool:
		default:
			return fmt.Sprintf("field %s must be a string, int, float, or bool", k)
		}
	case "sliceorscalar":
		switch vt := v.(type) {
		case string, int, int64, float64, bool:
			// we're good
		case []any:
			// we need to check that the slice is all the same type
			// if it's empty or 1 element, it's fine
			if len(v.([]any)) > 1 {
				firstType := fmt.Sprintf("%T", vt[0])
				for i, a := range vt {
					if fmt.Sprintf("%T", a) != firstType {
						return fmt.Sprintf("field %s must be a slice of all the same type, but element %d is %T", k, i, a)
					}
				}
			}
		default:
			return fmt.Sprintf("field %s must be a list of string, int, float, or bool", k)
		}
	case "string":
		if !isString(v) {
			return fmt.Sprintf("field %s must be a string but %v is %T", k, v, v)
		}
	case "int", "percentage":
		i, ok := v.(int)
		if !ok {
			return fmt.Sprintf("field %s must be an int", k)
		}
		if typ == "percentage" && (i < 0 || i > 100) {
			return fmt.Sprintf("field %s must be a percentage between 0 and 100", k)
		}
	case "bool":
		if _, ok := v.(bool); !ok {
			return fmt.Sprintf("field %s must be a bool", k)
		}
	case "float":
		if _, ok := v.(float64); !ok {
			if _, ok := v.(int); !ok {
				return fmt.Sprintf("field %s must be a float but %v is %T", k, v, v)
			}
		}
	case "stringarray":
		if arr, ok := v.([]any); ok {
			for _, a := range arr {
				if !isString(a) {
					return fmt.Sprintf("field %s must be a string array but contains non-string %#v", k, a)
				}
			}
		} else {
			return fmt.Sprintf("field %s must be a string array but %v is %T", k, v, v)
		}
	case "map":
		if _, ok := v.(map[string]any); !ok {
			return fmt.Sprintf("field %s must be a map", k)
		}
	case "duration":
		if !isString(v) {
			return fmt.Sprintf("field %s (%v) must be a valid duration like '3m30s' or '100ms'", k, v)
		}
		if _, err := time.ParseDuration(v.(string)); err != nil {
			return fmt.Sprintf("field %s (%v) must be a valid duration like '3m30s' or '100ms'", k, v)
		}
	case "memorysize":
		if !isString(v) {
			// we support pure numbers here, so if it's not already a string, then stringize it
			v = fmt.Sprintf("%v", v)
		}
		var m MemorySize
		// When validating we need to take advantage of the custom logic in MemorySize.UnmarshalText
		err := m.UnmarshalText([]byte(v.(string)))
		if err != nil {
			return fmt.Sprintf("field %s (%v) must be a valid memory size like '1Gb' or '100_000_000'", k, v)
		}
	case "hostport":
		if !isString(v) {
			return fmt.Sprintf("field %s must be a hostport", k)
		}
		if typ == "hostport" && v.(string) != "" {
			_, _, err := net.SplitHostPort(v.(string))
			if err != nil {
				return fmt.Sprintf("field %s (%v) must be a hostport: %v", k, v, err)
			}
		}
	case "url":
		if !isString(v) {
			return fmt.Sprintf("field %s must be a URL", k)
		}
		if typ == "url" && v.(string) == "" {
			return fmt.Sprintf("field %s may not be blank", k)
		}
		if typ == "urlOrBlank" && v.(string) == "" {
			return ""
		}

		u, err := url.Parse(v.(string))
		if err != nil {
			return fmt.Sprintf("field %s (%v) must be a valid URL: %v", k, v, err)
		}
		if u.Host == "" {
			return fmt.Sprintf("field %s (%v) must be a valid URL with a host", k, v)
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return fmt.Sprintf("field %s (%v) must use an http or https scheme", k, v)
		}
	case "defaulttrue":
		switch val := v.(type) {
		case bool:
			return ""
		case string:
			switch strings.ToLower(val) {
			case "t", "true", "f", "false":
			default:
				return fmt.Sprintf("field %s (%v) must be 'true', 'false', 't', or 'f'", k, v)
			}
		default:
			return fmt.Sprintf("field %s (%v) must be a bool or string with value true/false or 'true'/'false'/'t'/'f'", k, v)
		}
	default:
		panic("unknown data type " + typ)
	}
	return ""
}

func maskString(s string) string {
	if len(s) < 4 {
		return "****"
	}
	return "****" + s[len(s)-4:]
}

// Validate checks that the given data is valid according to the metadata.
// It returns a list of errors, or an empty list if there are no errors.
// The errors are strings that are suitable for showing to the user.
// The data is a map of group names to maps of field names to values.
// If a field name is of type "object" then this function is called
// recursively to validate the sub-object.
// Note that the flatten function returns only 2 levels.
func (m *Metadata) Validate(data map[string]any) []string {
	errors := make([]string, 0)
	// check for unknown groups in the userdata
	for k := range data {
		if m.GetGroup(k) == nil {
			possibilities := m.ClosestNamesTo(k)
			guesses := strings.Join(possibilities, " or ")
			errors = append(errors, fmt.Sprintf("unknown group %s; did you mean %s?", k, guesses))
		}
	}

	flatdata := flatten(data, true)
	// validate all the user fields and make sure there are no extras we don't understand
	for k, v := range flatdata {
		field := m.GetField(k)
		if field == nil {
			if m.GetGroup(k) != nil {
				continue // it's an empty group with no fields
			}
			possibilities := m.ClosestNamesTo(k)
			guesses := strings.Join(possibilities, " or ")
			errors = append(errors, fmt.Sprintf("unknown field %s; did you mean %s?", k, guesses))
			continue
		}
		if e := validateDatatype(k, v, field.Type); e != "" {
			errors = append(errors, e)
			continue // if type is wrong we can't validate further
		}
		switch field.Type {
		case "object":
			// if it's an object, we need to recurse
			if _, ok := v.(map[string]any); ok {
				suberrors := m.Validate(v.(map[string]any))
				for _, e := range suberrors {
					errors = append(errors, fmt.Sprintf("Within field %s: %s", k, e))
				}
			}
		case "objectarray":
			// if it's an object array, we need to iterate the array and recurse
			// and we need to specify the group as rules, so we make a new map
			if arr, ok := v.([]any); ok {
				for i, a := range arr {
					subname := strings.Split(k, ".")[1]
					rulesmap := map[string]any{subname: a}
					suberrors := m.Validate(rulesmap)
					for _, e := range suberrors {
						errors = append(errors, fmt.Sprintf("Within field %s[%d]: %s", k, i, e))
					}
				}
			}
		}
		for _, validation := range field.Validations {
		nextValidation:
			switch validation.Type {
			case "choice":
				if !(isString(v) && slices.Contains(field.Choices, v.(string))) {
					errors = append(errors, fmt.Sprintf("field %s (%v) must be one of %v", k, v, field.Choices))
				}
			case "format":
				var pat *regexp.Regexp
				var format string
				mask := false
				switch validation.Arg.(string) {
				case "apikeyOrBlank":
					// allow an empty string as well as a valid API key
					if v.(string) == "" {
						break nextValidation
					}
					fallthrough // fallthrough to the apikey case
				case "apikey":
					// valid API key formats are:
					// 1. 32 hex characters ("classic" Honeycomb API key)
					// 2. 20-23 alphanumeric characters (new-style Honeycomb API key)
					// 3. hc<1 letter region)><2 letter keytype>_<58 alphanumeric characters>} (ingest key)
					pat = regexp.MustCompile(`^([a-f0-9]{32}|[a-zA-Z0-9]{20,23}|hc[a-z][a-z]{2}_[a-z0-9]{58})$`)
					format = "field %s (%v) must be a valid Honeycomb API key"
					mask = true
				case "version":
					pat = regexp.MustCompile(`^v[0-9]+\.[0-9]+$`)
					format = "field %s (%v) must be a valid major.minor version number, like v2.0"
				case "alphanumeric":
					pat = regexp.MustCompile(`^[a-zA-Z0-9]*$`)
					format = "field %s (%v) must be purely alphanumeric"
				default:
					panic("unknown pattern type " + validation.Arg.(string))
				}
				if !(isString(v) && pat.MatchString(v.(string))) {
					if mask {
						v = maskString(v.(string))
					}
					errors = append(errors, fmt.Sprintf(format, k, v))
				}
			case "minimum":
				fv, msg := asFloat(v)
				if msg != "" {
					errors = append(errors, msg)
				} else {
					fm := mustFloat(validation.Arg)
					if fv < fm {
						errors = append(errors, fmt.Sprintf("field %s (%v) must be at least %v", k, v, validation.Arg))
					}
				}
			case "minOrZero":
				fv, msg := asFloat(v)
				if msg != "" {
					errors = append(errors, msg)
				} else {
					fm := mustFloat(validation.Arg)
					if fv != 0 && fv < fm {
						errors = append(errors, fmt.Sprintf("field %s must be at least %v, or zero", k, validation.Arg))
					}
				}
			case "maximum":
				fv, msg := asFloat(v)
				if msg != "" {
					errors = append(errors, msg)
				} else {
					fm := mustFloat(validation.Arg)
					if fv > fm {
						errors = append(errors, fmt.Sprintf("field %s (%v) must be at most %v", k, v, validation.Arg))
					}
				}
			case "notempty":
				if isString(v) && v.(string) == "" {
					errors = append(errors, fmt.Sprintf("field %s must not be empty", k))
				}
			case "elementType":
				switch val := v.(type) {
				case []any:
					for i, vv := range val {
						e := validateDatatype(fmt.Sprintf("%s[%d]", k, i), vv, validation.Arg.(string))
						if e != "" {
							errors = append(errors, e)
						}
					}
				case map[string]any:
					for kk, vv := range val {
						e := validateDatatype(fmt.Sprintf("%s[%s]", k, kk), vv, validation.Arg.(string))
						if e != "" {
							errors = append(errors, e)
						}
					}
				default:
					errors = append(errors, fmt.Sprintf("field %s must be a map or array", k))
				}
			case "validChildren":
				if _, ok := v.(map[string]any); ok {
					for kk := range v.(map[string]any) {
						if !slices.Contains(validation.GetArgAsStringSlice(), kk) {
							errors = append(errors, fmt.Sprintf("field %s contains unknown key %s", k, kk))
						}
					}
				}
			case "required", "requiredInGroup", "requiredWith", "conflictsWith":
				// these are handled below
			default:
				panic("unknown validation type: " + validation.Type)
			}
		}
	}

	// validate all the required fields and make sure they're present
	for _, group := range m.Groups {
		for _, field := range group.Fields {
			for _, validation := range field.Validations {
				switch validation.Type {
				case "required":
					if _, ok := flatdata[group.Name+"."+field.Name]; !ok {
						errors = append(errors, fmt.Sprintf("missing required field %s.%s", group.Name, field.Name))
					}
				case "requiredInGroup":
					// if we have the group but don't have the field, that's an error
					if _, ok := data[group.Name]; ok {
						if _, ok := flatdata[group.Name+"."+field.Name]; !ok {
							errors = append(errors, fmt.Sprintf("the group %s is missing its required field %s", group.Name, field.Name))
						}
					}
				case "requiredWith":
					// if the named key is specified then this one must be also
					otherName := validation.Arg.(string)
					if _, ok := flatdata[group.Name+"."+otherName]; ok {
						if _, ok := flatdata[group.Name+"."+field.Name]; !ok && m.GetField(group.Name+"."+field.Name).Default == nil {
							errors = append(errors, fmt.Sprintf("the group %s includes %s, which also requires %s", group.Name, otherName, field.Name))
						}
					}
				case "conflictsWith":
					// if both values are defined then report an error. Default values can also lead to conflicts and should not be
					// used with this validation.
					otherName := validation.Arg.(string)
					if _, ok := flatdata[group.Name+"."+otherName]; ok {
						if _, ok := flatdata[group.Name+"."+field.Name]; ok {
							errors = append(errors, fmt.Sprintf("the group %s includes %s, which conflicts with %s", group.Name, otherName, field.Name))
						}
					}
				}
			}
		}
	}

	return errors
}

// ValidateRules checks that the given data (which is expected to be a
// rules file) is valid according to the metadata.
// It returns a list of errors, or an empty list if there are no errors.
// The errors are strings that are suitable for showing to the user.
// This function isn't the same as a normal Validate because the rules
// file is a map of user-supplied names as keys, and we can't validate
// those as groups because they're not groups.
func (m *Metadata) ValidateRules(data map[string]any) []string {
	errors := make([]string, 0)

	hasVersion := false
	hasSamplers := false
	// validate the top-level keys in the data
	for k, v := range data {
		switch k {
		case "RulesVersion":
			if i, ok := v.(int); !ok {
				errors = append(errors, fmt.Sprintf("RulesVersion must be an int, but %v is %T", v, v))
			} else if i != 2 {
				errors = append(errors, fmt.Sprintf("RulesVersion must be 2, but it is %d", i))
			}
			hasVersion = true
		case "Samplers":
			if samplers, ok := v.(map[string]any); !ok {
				errors = append(errors, fmt.Sprintf("Samplers must be a collection of samplers, but %v is %T", v, v))
			} else {
				foundDefault := false
				for k, v := range samplers {
					if _, ok := v.(map[string]any); !ok {
						errors = append(errors, fmt.Sprintf("Sampler %s must be a map, but %v is %T", k, v, v))
					}
					if k == "__default__" {
						foundDefault = true
					}
				}
				if !foundDefault {
					errors = append(errors, "Samplers must include a __default__ sampler")
				}
			}
			hasSamplers = true
		default:
			errors = append(errors, fmt.Sprintf("unknown top-level key %s", k))
		}
	}
	if !hasVersion {
		errors = append(errors, "RulesVersion is required")
	}
	if !hasSamplers {
		errors = append(errors, "Samplers is required")
	}

	// bail if there are any errors at this level -- we don't know what we're working with
	if len(errors) > 0 {
		return errors
	}

	// now validate the individual samplers
	samplers := data["Samplers"].(map[string]any)
	for k, v := range samplers {
		suberrors := m.Validate(v.(map[string]any))
		for _, e := range suberrors {
			errors = append(errors, fmt.Sprintf("Within sampler %s: %s", k, e))
		}
	}

	return errors
}

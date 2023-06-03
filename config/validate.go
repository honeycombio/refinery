package config

import (
	"fmt"
	"net"
	"net/url"
	"regexp"
	"time"

	"golang.org/x/exp/slices"
)

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
	default:
		panic("unknown data type " + typ)
	}
	return ""
}

func Validate(data map[string]any, config Metadata) []string {
	errors := make([]string, 0)
	// validate that there are no unknown groups in the userdata
	for k := range data {
		if config.GetGroup(k) == nil {
			errors = append(errors, fmt.Sprintf("unknown group %s", k))
		}
	}

	flatdata := flatten(data, true)
	// validate all the user fields and make sure there are no extras we don't understand
	for k, v := range flatdata {
		field := config.GetField(k)
		if field == nil {
			errors = append(errors, fmt.Sprintf("unknown field %s", k))
			continue
		}
		if e := validateDatatype(k, v, field.Type); e != "" {
			errors = append(errors, e)
			continue // if type is wrong we can't validate further
		}

		for _, validation := range field.Validations {
			switch validation.Type {
			case "choice":
				if !(isString(v) && slices.Contains(field.Choices, v.(string))) {
					errors = append(errors, fmt.Sprintf("field %s (%v) must be one of %v", k, v, field.Choices))
				}
			case "format":
				var pat *regexp.Regexp
				var format string
				switch validation.Arg.(string) {
				case "apikey":
					pat = regexp.MustCompile(`^[a-f0-9]{32}|[a-zA-Z0-9]{20,23}$`)
					format = "field %s (%v) must be a Honeycomb API key"
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
					if fv == 0 || fv < fm {
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
			case "required", "requiredInGroup", "requiredWith":
				// these are handled below
			default:
				panic("unknown validation type: " + validation.Type)
			}
		}
	}

	// validate all the required fields and make sure they're present
	for _, group := range config.Groups {
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
						if _, ok := flatdata[group.Name+"."+field.Name]; !ok {
							errors = append(errors, fmt.Sprintf("the group %s includes %s, which also requires %s", group.Name, otherName, field.Name))
						}
					}
				}
			}
		}
	}

	return errors
}

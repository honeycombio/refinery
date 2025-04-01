package config

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/creasty/defaults"
	"github.com/pelletier/go-toml/v2"
	"gopkg.in/yaml.v3"
)

type Format string

const (
	FormatUnknown Format = "unknown"
	FormatYAML    Format = "yaml"
	FormatJSON    Format = "json"
	FormatTOML    Format = "toml"
)

// formatFromFilename returns the format of the file based on the filename extension.
func formatFromFilename(filename string) Format {
	switch filepath.Ext(filename) {
	case ".yaml", ".yml", ".YAML", ".YML":
		return FormatYAML
	case ".toml", ".TOML":
		return FormatTOML
	case ".json", ".JSON":
		return FormatJSON
	default:
		return FormatUnknown
	}
}

// formatFromResponse returns the format of the file based on the Content-Type header.
func formatFromResponse(resp *http.Response) Format {
	switch resp.Header.Get("Content-Type") {
	case "application/json", "text/json":
		return FormatJSON
	case "application/x-toml", "application/toml", "text/x-toml", "text/toml":
		return FormatTOML
	case "application/x-yaml", "application/yaml", "text/x-yaml", "text/yaml":
		return FormatYAML
	default:
		return FormatUnknown
	}
}

// getBytesFor returns an []byte for the given URL or filename.
func getBytesFor(u string) ([]byte, Format, error) {
	if u == "" {
		return nil, FormatUnknown, fmt.Errorf("empty url")
	}
	uu, err := url.Parse(u)
	if err != nil {
		return nil, FormatUnknown, err
	}
	switch uu.Scheme {
	case "file", "": // we treat an empty scheme as a filename
		r, err := os.ReadFile(uu.Path)
		if err != nil {
			return nil, FormatUnknown, err
		}
		return r, formatFromFilename(uu.Path), nil
	case "http", "https":
		// We need to make an HTTP request but we might need to add the
		// x-honeycomb-team header which we get from the environment; we use the
		// same header for all requests. This isn't particularly flexible, but
		// we think that it's good enough for the experimental stage of this
		// feature.
		req, err := http.NewRequest("GET", u, nil)
		if err != nil {
			return nil, FormatUnknown, err
		}

		// We use a different envvar for the team key because it's not the same
		// key used for ingestion. This is not currently documented because it's
		// experimental and we might change it later.
		key := os.Getenv("HONEYCOMB_CONFIG_KEY")
		if key != "" {
			req.Header.Set("X-Honeycomb-Team", key)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, FormatUnknown, err
		}
		format := formatFromResponse(resp)
		// if we don't get the format from the Content-Type header, try the path we were given
		// to see if it offers a hint
		if format == FormatUnknown {
			format = formatFromFilename(uu.Path)
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, FormatUnknown, err
		}
		return body, format, nil
	default:
		return nil, FormatUnknown, fmt.Errorf("unknown scheme %q", uu.Scheme)
	}
}

func load(data []byte, format Format, into any) error {
	switch format {
	case FormatYAML:
		err := yaml.Unmarshal(data, into)
		return err
	case FormatTOML:
		err := toml.Unmarshal(data, into)
		return err
	case FormatJSON:
		err := json.Unmarshal(data, into)
		return err
	default:
		return fmt.Errorf("unable to determine data format")
	}
}

type configData struct {
	data     []byte
	format   Format
	location string
}

// getConfigDataForLocations returns a slice of configData grabbed from each location.
func getConfigDataForLocations(locations []string) ([]configData, error) {
	results := make([]configData, len(locations))
	for i, location := range locations {
		// trim leading and trailing whitespace just in case
		location := strings.TrimSpace(location)
		data, format, err := getBytesFor(location)
		if err != nil {
			return nil, err
		}
		results[i] = configData{
			data:     data,
			format:   format,
			location: location,
		}
	}
	return results, nil
}

// This loads all the named configs into destination in the order they are listed.
// It returns the MD5 hash of the collected configs as a string (if there's only one
// config, this is the hash of that config; if there are multiple, it's the hash of
// all of them concatenated together).
func loadConfigsInto(dest any, configs []configData) (string, error) {
	// start a hash of the configs we process
	h := md5.New()
	for _, c := range configs {
		// write the data to the hash
		h.Write(c.data)

		// when working on a struct, load only overwrites destination values that are
		// explicitly named. So we can just keep loading successive sources into
		// the same object without losing data we've already specified.
		if err := load(c.data, c.format, dest); err != nil {
			return "", fmt.Errorf("loadConfigsInto unable to load config %s: %w", c.location, err)
		}
	}
	hash := hex.EncodeToString(h.Sum(nil))
	return hash, nil
}

type envGetter func(name string) string

// envGetterFunc is a helper function to get environment variables.
// This allows us to mock the os.Getenv function for testing purposes.
func envGetterFunc(name string) string {
	// Use the standard os.Getenv to get the environment variable
	return os.Getenv(name)
}

// expandEnvVarsInValues expands environment variables in string values of a map.
// Environment variables are in the form ${VAR_NAME} and will be replaced with
// their values. If an environment variable doesn't exist, the original string remains unchanged.
func expandEnvVarsInValues(m map[string]any, gf envGetter) map[string]any {
	result := make(map[string]any, len(m))

	for k, v := range m {
		switch val := v.(type) {
		case string:
			// Process string values to expand environment variables
			result[k] = expandEnvVarsInString(val, gf)
		case map[string]any:
			// Recursively process nested maps
			result[k] = expandEnvVarsInValues(val, gf)
		case []any:
			// Process array values
			newArray := make([]any, len(val))
			for i, item := range val {
				switch itemVal := item.(type) {
				case string:
					newArray[i] = expandEnvVarsInString(itemVal, gf)
				case map[string]any:
					newArray[i] = expandEnvVarsInValues(itemVal, gf)
				default:
					newArray[i] = item
				}
			}
			result[k] = newArray
		default:
			// Keep non-string values as they are
			result[k] = v
		}
	}

	return result
}

// expandEnvVarsInConfig expands environment variables in a pointer to a
// configuration struct using the yaml tags and a getter function for
// environment variables. This function will traverse the struct fields and
// expand any string values that contain environment variables in the form of
// ${VAR_NAME}. It uses go's reflection package to traverse the struct fields
// and expand the environment variables in place.
func expandEnvVarsInConfig(cfg any, gf envGetter) error {
	// Use reflection to traverse the struct fields
	val := reflect.ValueOf(cfg)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return fmt.Errorf("expected a struct or pointer to struct, got %T", cfg)
	}

	// Traverse the struct fields
	t := val.Type()
	for i := range t.NumField() {
		field := t.Field(i)
		fieldValue := val.Field(i)

		if fieldValue.IsValid() && fieldValue.CanInterface() {
			switch v := fieldValue.Interface().(type) {
			case string:
				if !fieldValue.CanSet() {
					fmt.Printf("Field %s is not settable\n", field.Name)
					continue
				}
				// Expand environment variables in string
				v2 := expandEnvVarsInString(v, gf)
				if v2 != v {
					val.Field(i).SetString(v2)
				}
			case map[string]any:
				newMap := expandEnvVarsInValues(v, gf)
				val.Field(i).Set(reflect.ValueOf(newMap))
			case map[string]string:
				for k, item := range v {
					expandedItem := expandEnvVarsInString(item, gf)
					if expandedItem != item {
						v[k] = expandedItem
					}
				}
			case []any:
				newArray := make([]any, len(v))
				for j, item := range v {
					switch itemVal := item.(type) {
					case string:
						newArray[j] = expandEnvVarsInString(itemVal, gf)
					case map[string]any:
						newArray[j] = expandEnvVarsInValues(itemVal, gf)
					default:
						newArray[j] = item
					}
				}
				val.Field(i).Set(reflect.ValueOf(newArray))
			case []string:
				// Create a new slice value using reflection
				for j, item := range v {
					expandedItem := expandEnvVarsInString(item, gf)
					if expandedItem != item {
						// If the expanded item is different, update it in the slice
						// Note: this won't change the original slice; it will only change the value in the new slice
						v[j] = expandedItem
					}
				}
			case *DefaultTrue, Duration, MemorySize, Level:
				// do nothing
			case bool, int, uint, uint64:
				// also do nothing
			default:
				// the field type may well be a subtype, so we can just recurse into them
				// for example, if you have a struct inside a struct, this will still work
				// for other types, we simply log the type to help with debugging
				// this includes slices, which are handled above
				valueKind := fieldValue.Kind()
				if valueKind == reflect.Struct {
					// construct a pointer to the struct
					// we need to use reflect.New to create a new pointer to the struct
					// and then set the field to the new value
					// this is a bit of a hack, but it works
					// create a new pointer to the struct
					newValue := reflect.New(fieldValue.Type()).Elem()
					// set the new value to the field value
					newValue.Set(fieldValue)
					// now recursively call this function on the new value
					if err := expandEnvVarsInConfig(newValue.Addr().Interface(), gf); err != nil {
						// if we fail to expand, return the error
						return fmt.Errorf("failed to expand environment variables in struct field %s: %w", field.Name, err)
					}
					// set the field back to the new value
					val.Field(i).Set(newValue)
					// continue to the next field
					continue
				}
				// log the type of the field if we couldn't work on it so we can make sure we're
				// hitting all the values we need
				fmt.Printf("Field %s has unsupported type %T, skipping env var expansion\n", field.Name, v)
			}
		}
	}
	return nil
}

// expandEnvVarsInString expands environment variables in a string.
// Variables in the form ${VAR_NAME} will be replaced with their values.
func expandEnvVarsInString(s string, gf envGetter) string {
	// Regular expression to find ${VAR_NAME} patterns
	re := regexp.MustCompile(`\${([^}]+)}`)

	// Replace all occurrences of ${VAR_NAME} with their values
	return re.ReplaceAllStringFunc(s, func(match string) string {
		// Extract variable name (remove ${ and })
		varName := match[2 : len(match)-1]

		// Get environment variable value
		value := gf(varName)
		if value != "" {
			return value
		}

		// If environment variable doesn't exist, return the original match
		return match
	})
}

func loadConfigsIntoMap(dest map[string]any, configs []configData) error {
	for _, c := range configs {
		// when working on a map, when loading a nested object, load will overwrite the entire destination
		// value, so we can't just keep loading successive files into the same object. Instead, we
		// need to load into a new object and then merge it into the map.
		temp := make(map[string]any)
		if err := load(c.data, c.format, &temp); err != nil {
			return fmt.Errorf("loadConfigsInto unable to load config %s: %w", c.location, err)
		}
		for k, v := range temp {
			switch vm := v.(type) {
			case map[string]any:
				// if the value is a map, we need to merge its value into the existing map value, if any.
				if dest[k] == nil {
					// no existing value, just copy it over
					dest[k] = vm
				} else {
					// this works without needing recursion because we know that
					// configurations can never be more than two levels deep.
					for kk, vv := range vm {
						dest[k].(map[string]any)[kk] = vv
					}
				}
			default:
				// everything else just gets copied over, including slices
				dest[k] = v
			}
		}
	}
	return nil
}

// validateConfigs gets the configs from the given location and validates them.
// It returns a list of failures; if the list is empty, the config is valid.
// err is non-nil only for significant errors like a missing file.
func validateConfigs(configs []configData, opts *CmdEnv) ([]string, error) {
	// first process the configs into a map so we can validate them
	userData := make(map[string]any)
	err := loadConfigsIntoMap(userData, configs)
	if err != nil {
		return nil, err
	}
	// now expand environment variables in the userData map
	userData = expandEnvVarsInValues(userData, envGetterFunc)

	metadata, err := LoadConfigMetadata()
	if err != nil {
		return nil, err
	}

	failures := metadata.Validate(userData)
	if len(failures) > 0 {
		return failures, nil
	}

	// Basic validation worked. Now we need to reload everything into our struct so that
	// we can apply defaults and options, and then validate a second time.
	var config configContents
	_, err = loadConfigsInto(&config, configs)
	if err != nil {
		return nil, err
	}

	// apply defaults and options
	if err := defaults.Set(&config); err != nil {
		return nil, fmt.Errorf("loadConfigsInto unable to apply defaults: %w", err)
	}

	// apply command line options
	if err := opts.ApplyTags(reflect.ValueOf(&config)); err != nil {
		return nil, fmt.Errorf("loadConfigsInto unable to apply command line options: %w", err)
	}

	// possibly inject some keys to keep the validator happy
	if config.HoneycombLogger.APIKey == "" {
		config.HoneycombLogger.APIKey = "InvalidHoneycombAPIKey"
	}
	if config.LegacyMetrics.APIKey == "" {
		config.LegacyMetrics.APIKey = "InvalidHoneycombAPIKey"
	}
	if config.OTelMetrics.APIKey == "" {
		config.OTelMetrics.APIKey = "InvalidHoneycombAPIKey"
	}
	if config.OTelTracing.APIKey == "" {
		config.OTelTracing.APIKey = "InvalidHoneycombAPIKey"
	}

	// The validator needs a map[string]any to work with, so we marshal to
	// yaml bytes for an easy conversion to map[string]any.
	data, err := yaml.Marshal(config)
	if err != nil {
		return nil, fmt.Errorf("loadConfigsInto unable to remarshal config: %w", err)
	}

	var rewrittenUserData map[string]any
	if err := load(data, FormatYAML, &rewrittenUserData); err != nil {
		return nil, fmt.Errorf("validateConfig unable to reload hydrated config from buffer: %w", err)
	}

	// now expand environment variables in the rewrittenUserData map
	rewrittenUserData = expandEnvVarsInValues(rewrittenUserData, envGetterFunc)

	// and finally validate the rewritten config
	failures = metadata.Validate(rewrittenUserData)
	return failures, nil
}

func validateRules(configs []configData) ([]string, error) {
	// first process the configs into a map so we can validate them
	userData := make(map[string]any)
	err := loadConfigsIntoMap(userData, configs)
	if err != nil {
		return nil, err
	}

	metadata, err := LoadRulesMetadata()
	if err != nil {
		return nil, err
	}

	failures := metadata.ValidateRules(userData)
	return failures, nil
}

// applyConfigInto applies the given configs to the given struct.
func applyConfigInto(dest any, configs []configData, opts *CmdEnv) (string, error) {
	hash, err := loadConfigsInto(dest, configs)
	if err != nil {
		return hash, err
	}

	// don't apply options and defaults if we're not given any
	if opts == nil {
		return hash, nil
	}

	// now we've got the config, apply defaults to zero values
	if err := defaults.Set(dest); err != nil {
		return hash, fmt.Errorf("applyConfigInto unable to apply defaults: %w", err)
	}

	// apply command line options
	if err := opts.ApplyTags(reflect.ValueOf(dest)); err != nil {
		return hash, fmt.Errorf("applyConfigInto unable to apply command line options: %w", err)
	}

	// and finally load envvars if necessary
	// this will expand environment variables in the struct itself
	if err := expandEnvVarsInConfig(dest, envGetterFunc); err != nil {
		return hash, fmt.Errorf("applyConfigInto unable to expand environment variables in config: %w", err)
	}

	return hash, nil
}

// ConfigHashMetrics takes a config hash and returns a integer value for use in metrics.
// The value is the last 4 characters of the config hash, converted to an integer.
// If the config hash is too short, or if there is an error converting the hash to an integer,
// it returns 0.
func ConfigHashMetrics(hash string) int64 {
	// get last 4 characters of config hash
	if len(hash) < 4 {
		return 0
	}
	suffix := hash[len(hash)-4:]
	CfgDecimal, err := strconv.ParseInt(suffix, 16, 64)
	if err != nil {
		return 0
	}

	return CfgDecimal
}

package config

import (
	"bytes"
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

// getReaderFor returns an io.ReadCloser for the given URL or filename.
func getReaderFor(u string) (io.ReadCloser, Format, error) {
	if u == "" {
		return nil, FormatUnknown, fmt.Errorf("empty url")
	}
	uu, err := url.Parse(u)
	if err != nil {
		return nil, FormatUnknown, err
	}
	switch uu.Scheme {
	case "file", "": // we treat an empty scheme as a filename
		r, err := os.Open(uu.Path)
		if err != nil {
			return nil, FormatUnknown, err
		}
		return r, formatFromFilename(uu.Path), nil
	case "http", "https":
		resp, err := http.Get(u)
		if err != nil {
			return nil, FormatUnknown, err
		}
		format := formatFromResponse(resp)
		// if we don't get the format from the Content-Type header, try the path we were given
		// to see if it offers a hint
		if format == FormatUnknown {
			format = formatFromFilename(uu.Path)
		}
		return resp.Body, format, nil
	default:
		return nil, FormatUnknown, fmt.Errorf("unknown scheme %q", uu.Scheme)
	}
}

func load(r io.Reader, format Format, into any) error {
	switch format {
	case FormatYAML:
		decoder := yaml.NewDecoder(r)
		err := decoder.Decode(into)
		return err
	case FormatTOML:
		decoder := toml.NewDecoder(r)
		err := decoder.Decode(into)
		return err
	case FormatJSON:
		decoder := json.NewDecoder(r)
		err := decoder.Decode(into)
		return err
	default:
		return fmt.Errorf("unable to determine data format")
	}
}

// This loads all the named configs into destination in the order they are listed.
// It returns the MD5 hash of the collected configs as a string (if there's only one
// config, this is the hash of that config; if there are multiple, it's the hash of
// all of them concatenated together).
func loadConfigsInto(dest any, locations []string) (string, error) {
	// start a hash of the configs we read
	h := md5.New()
	for _, location := range locations {
		// trim leading and trailing whitespace just in case
		location := strings.TrimSpace(location)
		r, format, err := getReaderFor(location)
		if err != nil {
			return "", err
		}
		defer r.Close()
		// write the data to the hash as we read it
		rdr := io.TeeReader(r, h)

		// when working on a struct, load only overwrites destination values that are
		// explicitly named. So we can just keep loading successive files into
		// the same object without losing data we've already specified.
		if err := load(rdr, format, dest); err != nil {
			return "", fmt.Errorf("loadConfigsInto unable to load config %s: %w", location, err)
		}
	}
	hash := hex.EncodeToString(h.Sum(nil))
	return hash, nil
}

func loadConfigsIntoMap(dest map[string]any, locations []string) error {
	for _, location := range locations {
		// trim leading and trailing whitespace just in case
		location := strings.TrimSpace(location)
		r, format, err := getReaderFor(location)
		if err != nil {
			return err
		}
		defer r.Close()

		// when working on a map, when loading a nested object, load will overwrite the entire destination
		// value, so we can't just keep loading successive files into the same object. Instead, we
		// need to load into a new object and then merge it into the map.
		temp := make(map[string]any)
		if err := load(r, format, &temp); err != nil {
			return fmt.Errorf("loadConfigsInto unable to load config %s: %w", location, err)
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

// validateConfigs reads the configs from the given location and validates them.
// It returns a list of failures; if the list is empty, the config is valid.
// err is non-nil only for significant errors like a missing file.
func validateConfigs(opts *CmdEnv) ([]string, error) {
	// first read the configs into a map so we can validate them
	userData := make(map[string]any)
	err := loadConfigsIntoMap(userData, opts.ConfigLocations)
	if err != nil {
		return nil, err
	}

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
	_, err = loadConfigsInto(&config, opts.ConfigLocations)
	if err != nil {
		return nil, err
	}

	// apply defaults and options
	if err := defaults.Set(&config); err != nil {
		return nil, fmt.Errorf("readConfigInto unable to apply defaults: %w", err)
	}

	// apply command line options
	if err := opts.ApplyTags(reflect.ValueOf(&config)); err != nil {
		return nil, fmt.Errorf("readConfigInto unable to apply command line options: %w", err)
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

	// The validator needs a map[string]any to work with, so we need to
	// write it out to a buffer (we always use YAML) and then reload it.
	buf := new(bytes.Buffer)
	encoder := yaml.NewEncoder(buf)
	encoder.SetIndent(2)
	if err := encoder.Encode(config); err != nil {
		return nil, fmt.Errorf("readConfigInto unable to reencode config: %w", err)
	}

	var rewrittenUserData map[string]any
	if err := load(buf, FormatYAML, &rewrittenUserData); err != nil {
		return nil, fmt.Errorf("validateConfig unable to reload hydrated config from buffer: %w", err)
	}

	// and finally validate the rewritten config
	failures = metadata.Validate(rewrittenUserData)
	return failures, nil
}

func validateRules(locations []string) ([]string, error) {
	// first read the configs into a map so we can validate them
	userData := make(map[string]any)
	err := loadConfigsIntoMap(userData, locations)
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

// readConfigInto reads the config from the given location and applies it to the given struct.
func readConfigInto(dest any, locations []string, opts *CmdEnv) (string, error) {
	hash, err := loadConfigsInto(dest, locations)
	if err != nil {
		return hash, err
	}

	// don't apply options and defaults if we're not given any
	if opts == nil {
		return hash, nil
	}

	// now we've got the config, apply defaults to zero values
	if err := defaults.Set(dest); err != nil {
		return hash, fmt.Errorf("readConfigInto unable to apply defaults: %w", err)
	}

	// apply command line options
	if err := opts.ApplyTags(reflect.ValueOf(dest)); err != nil {
		return hash, fmt.Errorf("readConfigInto unable to apply command line options: %w", err)
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

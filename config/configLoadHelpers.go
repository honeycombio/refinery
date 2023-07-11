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

func validateConfig(opts *CmdEnv) ([]string, error) {
	location := opts.ConfigLocation
	r, format, err := getReaderFor(location)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	var userData map[string]any
	if err := load(r, format, &userData); err != nil {
		return nil, fmt.Errorf("validateConfig unable to load config %s: %w", location, err)
	}

	metadata, err := LoadConfigMetadata()
	if err != nil {
		return nil, err
	}

	failures := metadata.Validate(userData)
	if len(failures) > 0 {
		return failures, nil
	}

	// Basic validation worked. Now we need to reload it into the struct so that
	// we can apply defaults and options, and then validate a second time.

	// we need a new reader for the source data
	r2, _, err := getReaderFor(location)
	if err != nil {
		return nil, err
	}
	defer r2.Close()

	var config configContents
	if err := load(r2, format, &config); err != nil {
		// this should never happen, since we already validated the config
		return nil, fmt.Errorf("validateConfig unable to RELOAD config %s: %w", location, err)
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

	// write it out to a YAML buffer
	buf := new(bytes.Buffer)
	encoder := yaml.NewEncoder(buf)
	encoder.SetIndent(2)
	if err := encoder.Encode(config); err != nil {
		return nil, fmt.Errorf("readConfigInto unable to reencode config: %w", err)
	}

	var rewrittenUserData map[string]any
	if err := load(buf, format, &rewrittenUserData); err != nil {
		return nil, fmt.Errorf("validateConfig unable to reload hydrated config from buffer: %w", err)
	}

	// and finally validate the rewritten config
	failures = metadata.Validate(rewrittenUserData)
	return failures, nil
}

func validateRules(location string) ([]string, error) {
	r, format, err := getReaderFor(location)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	var userData map[string]any
	if err := load(r, format, &userData); err != nil {
		return nil, fmt.Errorf("validateRules unable to load config %s: %w", location, err)
	}

	metadata, err := LoadRulesMetadata()
	if err != nil {
		return nil, err
	}

	failures := metadata.ValidateRules(userData)
	return failures, nil
}

// readConfigInto reads the config from the given location and applies it to the given struct.
func readConfigInto(dest any, location string, opts *CmdEnv) (string, error) {
	r, format, err := getReaderFor(location)
	if err != nil {
		return "", err
	}
	defer r.Close()

	// we're going to use a TeeReader to calculate the hash while also reading the data
	h := md5.New()
	rdr := io.TeeReader(r, h)

	if err := load(rdr, format, dest); err != nil {
		return "", fmt.Errorf("readConfigInto unable to load config %s: %w", location, err)
	}
	// the hash is now the MD5 of the config file
	hash := hex.EncodeToString(h.Sum(nil))

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

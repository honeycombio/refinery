package main

import (
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"text/template"
	"time"

	"github.com/creasty/defaults"
	"github.com/honeycombio/refinery/config"
	"github.com/jessevdk/go-flags"
	"github.com/pelletier/go-toml/v2"
	"gopkg.in/yaml.v3"
)

// Embed the entire filesystem directory into the binary so that it stands alone,
// as well as the configData.yaml file.
//
//go:embed templates/*.tmpl configData.yaml
var filesystem embed.FS

type Options struct {
	Input  string `short:"i" long:"input" description:"the Refinery v1 config file to read" default:"config.toml"`
	Output string `short:"o" long:"output" description:"the Refinery v2 config file to write (goes to stdout by default)"`
	Type   string `short:"t" long:"type" description:"loads input file as YAML, TOML, or JSON (in case file extension doesn't work)" choice:"Y" choice:"T" choice:"J"`
}

func load(r io.Reader, typ string) (map[string]any, error) {
	var result map[string]any
	switch typ {
	case "Y":
		decoder := yaml.NewDecoder(r)
		err := decoder.Decode(&result)
		return result, err
	case "T":
		decoder := toml.NewDecoder(r)
		err := decoder.Decode(&result)
		return result, err
	case "J":
		decoder := json.NewDecoder(r)
		err := decoder.Decode(&result)
		return result, err
	default:
		panic("shouldn't happen: bad filetype to load")
	}
}

func getType(filename string) string {
	switch filepath.Ext(filename) {
	case ".yaml", ".yml", ".YAML", ".YML":
		return "Y"
	case ".toml", ".TOML":
		return "T"
	case ".json", ".JSON":
		return "J"
	default:
		return ""
	}
}

func main() {
	opts := Options{}

	parser := flags.NewParser(&opts, flags.Default)
	parser.Usage = `[OPTIONS]

	This tool converts a Refinery v1 config file (usually in TOML, but JSON and YAML are also
	supported) to a Refinery v2 config file in YAML. It reads the v1 config file, and then writes
	the v2 config file, copying non-default values from their v1 location to their v2 location
	(if they still apply). The new v2 config file is commented in detail to help explain what
	each value does in the new configuration.

	For example, if the v1 file specified "MetricsAPIKey" in the "HoneycombMetrics" section, the v2
	file will list that key under the "LegacyMetrics" section under the "APIKey" name.

	By default, it reads config.toml and writes to stdout. It will try to determine the
	filetype of the input file based on the extension, but you can override that with
	the --type flag.
`
	args, err := parser.Parse()
	if err != nil {
		switch flagsErr := err.(type) {
		case *flags.Error:
			if flagsErr.Type == flags.ErrHelp {
				os.Exit(0)
			}
			os.Exit(1)
		default:
			os.Exit(1)
		}
	}

	output := os.Stdout
	if opts.Output != "" {
		output, err = os.Create(opts.Output)
		if err != nil {
			fmt.Fprintf(os.Stderr, "'%v' opening %s for writing\n", err, opts.Output)
			os.Exit(1)
		}
		defer output.Close()
	}

	if len(args) > 0 {
		switch args[0] {
		case "template":
			GenerateTemplate(output)
			os.Exit(0)
		case "names":
			PrintNames(output)
			os.Exit(0)
		case "sample":
			GenerateMinimalSample(output)
			os.Exit(0)
		case "doc":
			GenerateMarkdown(output)
			os.Exit(0)
		case "config", "rules":
			// do nothing yet because we need to parse the input file
		default:
			fmt.Fprintf(os.Stderr, "unknown subcommand %s; valid commands are template, names, sample, doc, config, rules\n", args[0])
			os.Exit(1)
		}
	}

	rdr, err := os.Open(opts.Input)
	if err != nil {
		fmt.Fprintf(os.Stderr, "'%v' opening %s\n", err, opts.Input)
		os.Exit(1)
	}
	defer rdr.Close()

	typ := opts.Type
	if typ == "" {
		typ = getType(opts.Input)
		if typ == "" {
			fmt.Fprintf(os.Stderr, "'%v' determining filetype for %s, use --type\n", err, opts.Input)
			os.Exit(1)
		}
	}

	data, err := load(rdr, typ)
	if err != nil {
		fmt.Fprintf(os.Stderr, "'%v' loading config from %s with filetype %s\n", err, opts.Input, opts.Type)
		os.Exit(1)
	}

	tmplData := struct {
		Now   string
		Input string
		Data  map[string]any
	}{
		Now:   time.Now().Format(time.RFC3339),
		Input: opts.Input,
		Data:  data,
	}

	switch args[0] {
	case "config":
		tmpl := template.New("configV2.tmpl")
		tmpl.Funcs(helpers())
		tmpl, err = tmpl.ParseFS(filesystem, "templates/configV2.tmpl")
		if err != nil {
			fmt.Fprintf(os.Stderr, "template error %v\n", err)
			os.Exit(1)
		}

		err = tmpl.Execute(output, tmplData)
		if err != nil {
			fmt.Fprintf(os.Stderr, "template error %v\n", err)
			os.Exit(1)
		}
	case "rules":
		ConvertRules(data, output)
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %s; valid commands are template, names, and sample\n", args[0])
	}

}

// All of the code below is used when building and debugging this tool.
// There are three commands that can be run:
//   - `go run . template` will generate a template file from the current schema
//   - `go run . names` will print out all of the names of the fields in the schema
//   - `go run . sample` will generate a minimal sample config file
// These commands are not listed in the documentation.

// These are the data structures used by these commands and their templates
type Field struct {
	Name         string   `json:"name"`
	V1Group      string   `json:"v1group"`
	V1Name       string   `json:"v1name"`
	FirstVersion string   `json:"firstversion"`
	LastVersion  string   `json:"lastversion"`
	Type         string   `json:"type"`
	ValueType    string   `json:"valuetype"`
	Extra        string   `json:"extra"`
	Default      any      `json:"default,omitempty"`
	Choices      []string `json:"choices,omitempty"`
	Example      string   `json:"example,omitempty"`
	Validation   string   `json:"validation,omitempty"`
	Reload       bool     `json:"reload"`
	Summary      string   `json:"summary"`
	Description  string   `json:"description"`
}

type Group struct {
	Name        string  `json:"name"`
	Title       string  `json:"title"`
	Description string  `json:"description"`
	Fields      []Field `json:"fields,omitempty"`
}

type ConfigData struct {
	Groups []Group `json:"groups"`
}

func readConfigData() ConfigData {
	input := "configData.yaml"
	rdr, err := filesystem.Open(input)
	if err != nil {
		panic(err)
	}
	defer rdr.Close()

	var config ConfigData
	decoder := yaml.NewDecoder(rdr)
	err = decoder.Decode(&config)
	if err != nil {
		panic(err)
	}
	return config
}

// This generates the template used by the convert tool.
func GenerateTemplate(w io.Writer) {
	config := readConfigData()
	var err error
	tmpl := template.New("template generator")
	tmpl.Funcs(helpers())
	tmpl, err = tmpl.ParseFS(filesystem, "templates/genfile.tmpl", "templates/gengroup.tmpl", "templates/genremoved.tmpl", "templates/genfield.tmpl")
	if err != nil {
		panic(err)
	}

	err = tmpl.ExecuteTemplate(w, "genfile.tmpl", config)
	if err != nil {
		panic(err)
	}
}

// This generates a nested list of the groups and names.
func PrintNames(w io.Writer) {
	config := readConfigData()
	var err error
	tmpl := template.New("group")
	tmpl.Funcs(helpers())
	tmpl, err = tmpl.ParseFS(filesystem, "templates/names.tmpl")
	if err != nil {
		panic(err)
	}

	err = tmpl.ExecuteTemplate(w, "names.tmpl", config)
	if err != nil {
		panic(err)
	}
}

// This generates a minimal sample config file of all of the groups and names
// with default or example values into minimal_config.yaml. The file it
// produces is valid YAML for config, and could be the basis of a test file.
func GenerateMinimalSample(w io.Writer) {
	config := readConfigData()
	var err error
	tmpl := template.New("sample")
	tmpl.Funcs(helpers())
	tmpl, err = tmpl.ParseFS(filesystem, "templates/sample.tmpl")
	if err != nil {
		panic(err)
	}

	err = tmpl.ExecuteTemplate(w, "sample.tmpl", config)
	if err != nil {
		panic(err)
	}
}

func GenerateMarkdown(w io.Writer) {
	config := readConfigData()
	var err error
	tmpl := template.New("markdown generator")
	tmpl.Funcs(helpers())
	tmpl, err = tmpl.ParseFS(filesystem, "templates/docfile.tmpl", "templates/docgroup.tmpl", "templates/docfield.tmpl")
	if err != nil {
		panic(err)
	}

	err = tmpl.ExecuteTemplate(w, "docfile.tmpl", config)
	if err != nil {
		panic(err)
	}
}

func readV1RulesIntoV2Sampler(samplerType string, rulesmap map[string]any) (*config.V2SamplerChoice, string, error) {
	// construct a sampler of the appropriate type that we can treat as "any" for unmarshalling
	var sampler any
	switch samplerType {
	case "DeterministicSampler":
		sampler = &config.DeterministicSamplerConfig{}
	case "DynamicSampler":
		sampler = &config.DynamicSamplerConfig{}
	case "EMADynamicSampler":
		sampler = &config.EMADynamicSamplerConfig{}
	case "RulesBasedSampler":
		sampler = &config.RulesBasedSamplerConfig{}
	case "TotalThroughputSampler":
		sampler = &config.TotalThroughputSamplerConfig{}
	default:
		return nil, "not found", errors.New("no sampler found")
	}

	// We use a little trick here -- we have read the rules into a generic map.
	// First we convert the generic map into all lowercase keys, then marshal
	// them into a bytestream using JSON, then finally unmarshal them into their
	// final form. This lets us use the JSON tags to do the mapping of old field
	// names onto new names, while we use the YAML tags to render the new names
	// in the final output. So it's real important to have both tags on any
	// field that gets renamed!

	// convert all the keys to lowercase
	lowermap := _keysToLowercase(rulesmap)

	// marshal the rules into a bytestream
	b, err := json.Marshal(lowermap)
	if err != nil {
		return nil, "", fmt.Errorf("getV1RulesForSampler unable to marshal config: %w", err)
	}

	// and unmarshal them back into the sampler
	err = json.Unmarshal(b, sampler)
	if err != nil {
		return nil, "", fmt.Errorf("getV1RulesForSampler unable to unmarshal config: %w", err)
	}
	// now we've got the config, apply defaults to zero values
	if err := defaults.Set(sampler); err != nil {
		return nil, "", fmt.Errorf("getV1RulesForSampler unable to apply defaults: %w", err)
	}

	// and now put it into the V2 sampler config
	newSampler := &config.V2SamplerChoice{}
	switch samplerType {
	case "DeterministicSampler":
		newSampler.DeterministicSampler = sampler.(*config.DeterministicSamplerConfig)
	case "DynamicSampler":
		newSampler.DynamicSampler = sampler.(*config.DynamicSamplerConfig)
	case "EMADynamicSampler":
		newSampler.EMADynamicSampler = sampler.(*config.EMADynamicSamplerConfig)
	case "RulesBasedSampler":
		newSampler.RulesBasedSampler = sampler.(*config.RulesBasedSamplerConfig)
	case "TotalThroughputSampler":
		newSampler.TotalThroughputSampler = sampler.(*config.TotalThroughputSamplerConfig)
	}

	return newSampler, samplerType, nil
}

func ConvertRules(rules map[string]any, w io.Writer) {
	// this writes the rules to w as a YAML file for debugging
	// yaml.NewEncoder(w).Encode(rules)

	// get the sampler type for the default rule
	defaultSamplerType, _ := config.GetValueForCaseInsensitiveKey(rules, "sampler", "DeterministicSampler")

	newConfig := &config.V2SamplerConfig{
		ConfigVersion: 2,
		Samplers:      make(map[string]*config.V2SamplerChoice),
	}
	sampler, _, err := readV1RulesIntoV2Sampler(defaultSamplerType, rules)
	if err != nil {
		panic(err)
	}

	newConfig.Samplers["__Default__"] = sampler

	for k, v := range rules {
		// if it's not a map, skip it
		if _, ok := v.(map[string]any); !ok {
			continue
		}
		sub := v.(map[string]any)

		// make sure it's a sampler destination key by checking for the presence
		// of a "sampler" key or a "samplerate" key; having either one is good
		// enough
		if _, ok := config.GetValueForCaseInsensitiveKey(sub, "sampler", ""); !ok {
			if _, ok := config.GetValueForCaseInsensitiveKey(sub, "samplerate", ""); !ok {
				continue
			}
		}

		// get the sampler type for the rule
		samplerType, _ := config.GetValueForCaseInsensitiveKey(sub, "sampler", "DeterministicSampler")
		sampler, _, err := readV1RulesIntoV2Sampler(samplerType, sub)
		if err != nil {
			panic(err)
		}

		newConfig.Samplers[k] = sampler
	}

	yaml.NewEncoder(w).Encode(newConfig)
}

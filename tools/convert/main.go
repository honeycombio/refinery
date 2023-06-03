package main

import (
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"text/template"
	"time"

	"github.com/honeycombio/refinery/validation"
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
		case "config", "rules", "validate":
			// do nothing yet because we need to parse the input file
		default:
			fmt.Fprintf(os.Stderr, "unknown subcommand %s; valid commands are template, names, sample, doc, config, rules, schema, validate\n", args[0])
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

	userConfig, err := load(rdr, typ)
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
		Data:  userConfig,
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
		ConvertRules(userConfig, output)
	case "validate":
		ValidateFromConfig(userConfig, output)
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %s; valid commands are config, rules, and validate\n", args[0])
	}

}

func readConfigData() validation.ConfigData {
	input := "configData.yaml"
	rdr, err := filesystem.Open(input)
	if err != nil {
		panic(err)
	}
	defer rdr.Close()

	var config validation.ConfigData
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

func ValidateFromConfig(userData map[string]any, w io.Writer) bool {
	config := readConfigData()

	errors := validation.Validate(userData, config)
	if len(errors) > 0 {
		for _, e := range errors {
			fmt.Fprintf(w, "validation errors: %s\n", e)
		}
	}
	return len(errors) == 0
}

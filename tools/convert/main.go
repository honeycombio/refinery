package main

import (
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"text/template"

	"github.com/honeycombio/refinery/config"
	"github.com/jessevdk/go-flags"
	"github.com/pelletier/go-toml/v2"
	"gopkg.in/yaml.v3"
)

// Embed the entire filesystem directory into the binary so that it stands alone,
// as well as the configData.yaml file.
//
//go:embed templates/*.tmpl
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

type configTemplateData struct {
	Input string
	Data  map[string]any
}

func main() {
	opts := Options{}

	parser := flags.NewParser(&opts, flags.Default)
	parser.Usage = `[OPTIONS] COMMAND

	The main usage of this tool is to converts a Refinery v1 config file (usually in TOML, but
	JSON and YAML are also supported) to a Refinery v2 config file in YAML. It reads the v1
	config file, and then writes the v2 config file, copying non-default values from their v1
	location to their v2 location (if they still apply).

	For config files, the new v2 config file is commented in detail to help explain what each
	value does in the new configuration.

	For example, if the v1 file specified "MetricsAPIKey" in the "HoneycombMetrics" section, the v2
	file will list that key under the "LegacyMetrics" section under the "APIKey" name.

	The tool can also convert rules files to the new rules file format.

	By default, it reads config.toml and writes to stdout. It will try to determine the
	filetype of the input file based on the extension, but you can override that with
	the --type flag.

	Because many organizations use helm charts to manage their refinery deployments, there
	is a subcommand that can read a helm chart, extract both the rules and config from it,
	and write them back out to a helm chart, while preserving the non-refinery portions.

	It has other commands to help with the conversion process. Valid commands are:
		convert config:          convert a config file
		convert rules:           convert a rules file
		convert helm:            convert a helm values file
		convert validate config: validate a config file against the 2.0 format
		convert validate rules:  validate a rules file against the 2.0 format
		convert doc config:      generate markdown documentation for the config file
		convert doc rules:       generate markdown documentation for the rules file

	Examples:
		convert config --input config.toml --output config.yaml
		convert rules --input refinery_rules.yaml --output v2rules.yaml
		convert validate config --input config.yaml
		convert validate rules --input v2rules.yaml
`
	// Note that there are other commands not listed here, but they are not intended for use
	// by end users.

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

	if len(args) == 0 {
		fmt.Println("Usage: convert [OPTIONS] COMMAND")
		fmt.Println("Try 'convert --help' for more information.")
		os.Exit(1)
	}
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
		if len(args) > 1 && args[1] == "rules" {
			GenerateRulesMarkdown(output, "rules_docrepo.tmpl")
		} else if len(args) > 1 && args[1] == "config" {
			GenerateConfigMarkdown(output, "cfg_docrepo.tmpl")
		} else {
			fmt.Fprintf(os.Stderr, `doc subcommand requires "rules" or "config" as an argument\n`)
			os.Exit(1)
		}
		os.Exit(0)
	case "website":
		if len(args) > 1 && args[1] == "rules" {
			GenerateRulesMarkdown(output, "rules_docsite.tmpl")
		} else if len(args) > 1 && args[1] == "config" {
			GenerateConfigMarkdown(output, "cfg_docsite.tmpl")
		} else {
			fmt.Fprintf(os.Stderr, `doc subcommand requires "rules" or "config" as an argument\n`)
			os.Exit(1)
		}
		os.Exit(0)
	case "config", "rules", "validate", "helm":
		// do nothing yet because we need to parse the input file
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %s; valid commands are config, doc config, validate config, rules, doc rules, validate rules\n", args[0])
		os.Exit(1)
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

	tmplData := &configTemplateData{
		Input: opts.Input,
		Data:  userConfig,
	}

	switch args[0] {
	case "config":
		ConvertConfig(tmplData, output)
	case "rules":
		ConvertRules(userConfig, output)
	case "helm":
		ConvertHelm(tmplData, output)
	case "validate":
		if args[1] == "config" {
			if !ValidateFromMetadata(userConfig, output) {
				os.Exit(1)
			}
		} else if args[1] == "rules" {
			if !ValidateRules(userConfig, output) {
				os.Exit(1)
			}
		} else {
			fmt.Fprintf(os.Stderr, "unknown subcommand %s; valid commands are config and rules\n", args[0])
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %s; valid commands are config, rules, and validate\n", args[0])
		os.Exit(1)
	}

}

func loadConfigMetadata() *config.Metadata {
	m, err := config.LoadConfigMetadata()
	if err != nil {
		panic(err)
	}
	return m
}

func loadRulesMetadata() *config.Metadata {
	m, err := config.LoadRulesMetadata()
	if err != nil {
		panic(err)
	}
	// Sort the groups by sort order.
	sort.Slice(m.Groups, func(i, j int) bool {
		return m.Groups[i].SortOrder < m.Groups[j].SortOrder
	})

	return m
}

func ConvertConfig(tmplData *configTemplateData, w io.Writer) {
	tmpl := template.New("configV2.tmpl")
	tmpl.Funcs(helpers())
	tmpl, err := tmpl.ParseFS(filesystem, "templates/configV2.tmpl")
	if err != nil {
		fmt.Fprintf(os.Stderr, "template error %v\n", err)
		os.Exit(1)
	}

	err = tmpl.Execute(w, tmplData)
	if err != nil {
		fmt.Fprintf(os.Stderr, "template error %v\n", err)
		os.Exit(1)
	}
}

func removeEmpty(m map[string]any) map[string]any {
	result := make(map[string]any)
	for k, v := range m {
		switch val := v.(type) {
		case map[string]any:
			result[k] = removeEmpty(val)
		case nil:
		default:
			result[k] = v
		}
	}
	return result
}

func ConvertHelm(tmplData *configTemplateData, w io.Writer) {
	const rulesConfigMapName = "RulesConfigMapName"
	const liveReload = "LiveReload"
	// convert config if we have it
	helmConfigAny, ok := tmplData.Data["config"]
	if ok {
		helmConfig, ok := helmConfigAny.(map[string]any)
		if !ok {
			panic("config in helm chart is the wrong format!")
		}
		// we need to promote this special key for Honeycomb configs
		if mapname, ok := helmConfig[rulesConfigMapName]; ok {
			tmplData.Data[rulesConfigMapName] = mapname
			delete(helmConfig, rulesConfigMapName)
		}

		convertedConfig := &bytes.Buffer{}
		// make a copy of this tmplData and overwrite the Data part
		config := *tmplData
		config.Data = helmConfig
		// convert the config into the buffer
		ConvertConfig(&config, convertedConfig)
		// read the buffer as YAML
		decoder := yaml.NewDecoder(convertedConfig)
		var decodedConfig map[string]any
		saved := convertedConfig.String()
		err := decoder.Decode(&decodedConfig)
		if err != nil {
			s := err.Error()
			pat := regexp.MustCompile("yaml: line ([0-9]+):")
			m := pat.FindStringSubmatch(s)
			if len(m) > 1 {
				linenum, _ := strconv.Atoi(m[1])
				lines := strings.Split(saved, "\n")
				for i := linenum - 15; i < linenum+5; i++ {
					if len(lines) > i {
						fmt.Printf("%d: %s\n", i, lines[i])
					}
				}
			}
			panic(err)
		}
		tmplData.Data["config"] = removeEmpty(decodedConfig)
	}

	// now try the rules
	helmRulesAny, ok := tmplData.Data["rules"]
	if ok {
		helmRules, ok := helmRulesAny.(map[string]any)
		if !ok {
			panic("config in helm chart is the wrong format!")
		}
		// we need to promote this special key for Honeycomb configs
		if mapname, ok := helmRules[liveReload]; ok {
			tmplData.Data[liveReload] = mapname
			delete(helmRules, liveReload)
		}

		rules := convertRulesToNewConfig(helmRules)
		tmplData.Data["rules"] = rules
	}

	// now we have our tmplData.Data with converted contents
	// so we can just write it all back as YAML now
	encoder := yaml.NewEncoder(w)
	err := encoder.Encode(tmplData.Data)
	if err != nil {
		panic(err)
	}
}

// This generates the template used by the convert tool.
func GenerateTemplate(w io.Writer) {
	metadata := loadConfigMetadata()
	var err error
	tmpl := template.New("template generator")
	tmpl.Funcs(helpers())
	tmpl, err = tmpl.ParseFS(filesystem, "templates/genfile.tmpl", "templates/gengroup.tmpl", "templates/genremoved.tmpl", "templates/genfield.tmpl")
	if err != nil {
		panic(err)
	}

	err = tmpl.ExecuteTemplate(w, "genfile.tmpl", metadata)
	if err != nil {
		panic(err)
	}
}

// This generates a nested list of the groups and names.
func PrintNames(w io.Writer) {
	metadata := loadConfigMetadata()
	var err error
	tmpl := template.New("group")
	tmpl.Funcs(helpers())
	tmpl, err = tmpl.ParseFS(filesystem, "templates/names.tmpl")
	if err != nil {
		panic(err)
	}

	err = tmpl.ExecuteTemplate(w, "names.tmpl", metadata)
	if err != nil {
		panic(err)
	}
}

// This generates a minimal sample config file of all of the groups and names
// with default or example values into minimal_config.yaml. The file it
// produces is valid YAML for config, and could be the basis of a test file.
func GenerateMinimalSample(w io.Writer) {
	metadata := loadConfigMetadata()
	var err error
	tmpl := template.New("sample")
	tmpl.Funcs(helpers())
	tmpl, err = tmpl.ParseFS(filesystem, "templates/sample.tmpl")
	if err != nil {
		panic(err)
	}

	err = tmpl.ExecuteTemplate(w, "sample.tmpl", metadata)
	if err != nil {
		panic(err)
	}
}

func GenerateConfigMarkdown(w io.Writer, templateName string) {
	metadata := loadConfigMetadata()
	var err error
	tmpl := template.New("markdown generator")
	tmpl.Funcs(helpers())
	tmpl, err = tmpl.ParseFS(filesystem, "templates/"+templateName)
	if err != nil {
		panic(err)
	}

	buffer := &bytes.Buffer{}
	err = tmpl.ExecuteTemplate(buffer, templateName, metadata)
	if err != nil {
		panic(err)
	}
	pat := regexp.MustCompile("\n[\n\t ]*\n")
	w.Write(pat.ReplaceAll(buffer.Bytes(), []byte("\n\n")))
}

func GenerateRulesMarkdown(w io.Writer, templateName string) {
	metadata := loadRulesMetadata()
	var err error
	tmpl := template.New("markdown generator")
	tmpl.Funcs(helpers())
	tmpl, err = tmpl.ParseFS(filesystem, "templates/"+templateName)
	if err != nil {
		panic(err)
	}

	buffer := &bytes.Buffer{}
	err = tmpl.ExecuteTemplate(buffer, templateName, metadata)
	if err != nil {
		panic(err)
	}
	pat := regexp.MustCompile("\n[\n\t ]*\n")
	w.Write(pat.ReplaceAll(buffer.Bytes(), []byte("\n\n")))
}

func ValidateFromMetadata(userData map[string]any, w io.Writer) bool {
	metadata := loadConfigMetadata()

	errors := metadata.Validate(userData)
	if len(errors) > 0 {
		fmt.Fprintln(w, "Validation Errors in config file:")
		for _, e := range errors {
			fmt.Fprintf(w, "  %s\n", e)
		}
	}
	return len(errors) == 0
}

func ValidateRules(userData map[string]any, w io.Writer) bool {
	metadata := loadRulesMetadata()
	errors := metadata.ValidateRules(userData)
	if len(errors) > 0 {
		fmt.Fprintln(w, "Validation Errors in rules file:")
		for _, e := range errors {
			fmt.Fprintf(w, "  %s\n", e)
		}
	}
	return len(errors) == 0
}

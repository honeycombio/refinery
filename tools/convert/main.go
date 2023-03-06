package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"text/template"
	"time"

	"github.com/jessevdk/go-flags"
	"github.com/pelletier/go-toml/v2"
	"gopkg.in/yaml.v2"
)

type Options struct {
	Input    string `short:"i" long:"input" description:"the Refinery v1 config file to read" default:"config.toml"`
	Output   string `short:"o" long:"output" description:"the Refinery v2 config file to write" default:"-"`
	Type     string `short:"t" long:"type" description:"loads input file as YAML, TOML, or JSON" choice:"Y" choice:"T" choice:"J"`
	Print    bool   `short:"p" description:"prints what it loaded in Go format and quits"`
	Template string `long:"template" description:"template for output file" default:"configV2.tmpl"`
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

func parseTemplate(filename string) (*template.Template, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	return template.New(filename).Funcs(helpers()).Parse(string(data))
}

func main() {
	args := Options{}

	parser := flags.NewParser(&args, flags.Default)

	if _, err := parser.Parse(); err != nil {
		switch flagsErr := err.(type) {
		case flags.ErrorType:
			if flagsErr == flags.ErrHelp {
				os.Exit(0)
			}
			os.Exit(1)
		default:
			os.Exit(1)
		}
	}

	rdr, err := os.Open(args.Input)
	if err != nil {
		fmt.Fprintf(os.Stderr, "'%v' opening %s\n", err, args.Input)
		os.Exit(1)
	}
	defer rdr.Close()

	typ := args.Type
	if typ == "" {
		typ = getType(args.Input)
		if typ == "" {
			fmt.Fprintf(os.Stderr, "'%v' determining filetype for %s, use --type\n", err, args.Input)
			os.Exit(1)
		}
	}

	data, err := load(rdr, typ)
	if err != nil {
		fmt.Fprintf(os.Stderr, "'%v' loading config from %s with filetype %s\n", err, args.Input, args.Type)
		os.Exit(1)
	}

	if args.Print {
		fmt.Printf("%#v\n", data)
		os.Exit(0)
	}

	output := os.Stdout
	if args.Output != "-" {
		output, err = os.Create(args.Output)
		if err != nil {
			fmt.Fprintf(os.Stderr, "'%v' opening %s for writing\n", err, args.Output)
			os.Exit(1)
		}
		defer output.Close()
	}

	tmplData := struct {
		Now   string
		Input string
		Data  map[string]any
	}{
		Now:   time.Now().Format(time.RFC3339),
		Input: args.Input,
		Data:  data,
	}

	tmpl, err := parseTemplate(args.Template)
	if err != nil {
		fmt.Fprintf(os.Stderr, "'%v' reading template from %s\n", err, args.Template)
		os.Exit(1)
	}

	err = tmpl.Execute(output, tmplData)
	if err != nil {
		fmt.Fprintf(os.Stderr, "template error %v\n", err)
		os.Exit(1)
	}
}

# Configuration Conversion Tool

## Summary

Refinery has been around long enough that its configuration files have accumulated some cruft. For backwards-compatibility reasons, we've had to leave defaults unchanged as we adjusted refinery's operations. It now works better than it did, but by default can't take advantage of new things.

The `convert` tool is here to make it possible to upgrade Refinery by converting the old configuration to a new form, while preserving the important settings.

It also allows all of the comments in the configuration to be rewritten for clarity and accuracy.

## Usage

```
Usage:
  convert [OPTIONS] COMMAND

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


Application Options:
  -i, --input=       the Refinery v1 config file to read (default: config.toml)
  -o, --output=      the Refinery v2 config file to write (goes to stdout by default)
  -t, --type=[Y|T|J] loads input file as YAML, TOML, or JSON (in case file extension doesn't work)

Help Options:
  -h, --help         Show this help message

```

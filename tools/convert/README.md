# Configuration Conversion Tool

## Summary

Refinery has been around long enough that its configuration files have accumulated some cruft. For backwards-compatibility reasons, we've had to leave defaults unchanged as we adjusted refinery's operations. It now works better than it did, but by default can't take advantage of new things.

The `convert` tool is here to make it possible to upgrade Refinery by converting the old configuration to a new form, while preserving the important settings.

It also allows all of the comments in the configuration to be rewritten for clarity and accuracy.

## Usage

```
Usage:
  convert [OPTIONS]

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


Application Options:
  -i, --input=       the Refinery v1 config file to read (default: config.toml)
  -o, --output=      the Refinery v2 config file to write (default: -)
  -t, --type=[Y|T|J] loads input file as YAML, TOML, or JSON (in case file extension doesn't work)
  -p                 prints what it loaded in Go format and quits
      --template=    template for output file (default: configV2.tmpl)

Help Options:
  -h, --help         Show this help message
```

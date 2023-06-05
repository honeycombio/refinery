package config

import "embed"

// Embed all the yaml files in the metadata subdirectory into the binary.
//
//go:embed metadata/*.yaml
var metadataFS embed.FS

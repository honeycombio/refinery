# Contributing Guide

# Developer Tasks

## Setup Environment

First, You will need to install and configure [Go](https://golang.org/doc/install) on your computer.

## Running Refinery Locally

Once you have your local environment setup, clone the Refinery repository.
`git clone https://github.com/honeycombio/refinery.git`

Now that you have the source code, you can build refinery and ensure everything is working by running the command below
at the root of the Refinery directory.

`go run ./cmd/refinery/main.go -c config_complete.yaml -r rules_complete.yaml`

# Making changes to configuration code

With the new configuration format redesign in v2.0.0, the workflow for making a configuration requires the following steps:

- Make changes to the YAML tags for a configuration struct.
- Update `config/metadata/configMeta.yaml` or `config/metadata/rulesMeta.yaml` to reflect such change.
- Run the command below from inside the `./refinery/tools/` directory to generate the updated `config_complete.yaml` and `rules_complete.yaml`. These two files are used to generate the official Refinery configuration documentation.

`make all`

# References

Please see our [general guide for OSS lifecycle and practices.](https://github.com/honeycombio/home/blob/main/honeycomb-oss-lifecycle-and-practices.md)
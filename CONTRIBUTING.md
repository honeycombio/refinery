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

# Running Refinery tests locally

Tests require a local installation of redis. See [here](https://redis.io/docs/install/install-stack) for how to get it running.

You can run Refinery tests by running the command below at the root of the Refinery directory.

`make test`

# Generated licenses and documentation

Refinery uses generated data for licenses and documentation.

The documentation resides in the `config/config.meta` and `config/rules.meta` files.
These two files should be edited when information in them needs to change. They provide the base data for generated documentation.

It is not necessary to run the regeneration code licenses or documentation as part of a standard PR for Refinery. They cause a lot of noise and should generally only be run prior to a release.

# Making changes to configuration code

With the new configuration format redesign in v2.0.0, the workflow for making a configuration requires the following steps:

- Make changes to the YAML tags for a configuration struct.
- Update `config/metadata/configMeta.yaml` or `config/metadata/rulesMeta.yaml` to reflect such change.
- Run the command below from inside the `./refinery/tools/convert/` directory to generate the updated `config.md`, `config_complete.md`, and `rules.md`. These two files are used to generate the official Refinery configuration documentation.
`make all`
- The `rules_complete.yaml` file is an example file constructed manually. If there are substantive changes to rules, you should manually update `rules_complete.yaml`.
- If you add new configurations that expect the environment, they must also be available from the command line. This will only work if you add them within the struct for cmdenv.go.

# References

Please see our [general guide for OSS lifecycle and practices.](https://github.com/honeycombio/home/blob/main/honeycomb-oss-lifecycle-and-practices.md)
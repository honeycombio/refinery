# Contributing Guide

# Developer Tasks

## Setup Environment

Clone the Refinery repository: `git clone https://github.com/honeycombio/refinery.git`

The project has required versions of tools declared in a `.tool-versions` file in the repo, used by utilities like [mise](https://mise.jdx.dev/) (recommended) and [asdf](https://asdf-vm.com/) to ensure you have the expected local development tooling.
With mise, install necessary tools by running the following command at the root of the repo:

`mise install`

If you opt not to use either mise or asdf, you will need to install and configure the version of [Go](https://golang.org/doc/install) declared in that file.

## Running Refinery locally

Once your local environment is set up, you can build Refinery and ensure everything is working by running the command below at the root of the Refinery directory.

`go run ./cmd/refinery/main.go -c config_complete.yaml -r rules_complete.yaml`

## Running tests locally

Tests require a local instance of [redis](https://redis.io/docs/install/install-stack) or [valkey](https://hub.docker.com/r/valkey/valkey/#how-to-use-this-image) to be available.

You can run Refinery tests by running the following command at the root of the repo.

`make test`

# Making changes to configuration code

The workflow for making a configuration change requires the following steps:

- Make changes to the YAML tags for a configuration struct.
- Update `config/metadata/configMeta.yaml` or `config/metadata/rulesMeta.yaml` to reflect that change.
- The `rules_complete.yaml` file is an example file constructed manually.
  If there are substantive changes to rules, you should manually update `rules_complete.yaml`.
- If you add new configurations that expect the environment, they must also be available from the command line.
  This will only work if you add them within the struct for `cmdenv.go`.

# Generated licenses and documentation

Refinery generates the `LICENSES` directory and its reference documentation.

## Licenses

The `LICENSES` directory records the licenses of Refinery's dependencies.
CI runs `make verify-licenses` and fails if `LICENSES` is out of date.

If your PR changes dependencies, run `make update-licenses`, review the changes for license compatibility, and commit them.
We recommend putting the `LICENSES` update in its own commit on your branch so it is easy to review separate from code changes.

## Documentation

The `config/metadata/configMeta.yaml` and `config/metadata/rulesMeta.yaml` files provide the base content for generated documentation.
These are the files that should be edited when documentation needs updating.

Do not include the generated documentation output in your PR; it causes a lot of unnecessary noise for the review.
Regeneration happens as part of release preparation.
Running `make all` from `tools/convert` locally is good practice for checking that your metadata changes generate valid output.
It rewrites tracked files, so do not stage them, and discard the changes afterwards.

# References

Please see our [general guide for OSS lifecycle and practices.](https://github.com/honeycombio/home/blob/main/honeycomb-oss-lifecycle-and-practices.md)

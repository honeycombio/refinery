.PHONY: all
#: build all the things
all: template names sample docrules docconfig validateConfig validateRules build

.PHONY: build
#: build the binary
build: template
	@echo
	@echo "+++ building binary"
	@echo
	go install

.PHONY: template
#: generate the config template
template:
	@echo
	@echo "+++ generating template"
	@echo
	go run . template --output=templates/configV2.tmpl

.PHONY: names
#: generate the names
names:
	@echo
	@echo "+++ generating names"
	@echo
	go run . names --output=configDataNames.txt

.PHONY: sample
#: generate the sample config
sample:
	@echo
	@echo "+++ generating sample config"
	@echo
	go run . sample --output=minimal_config.yaml

.PHONY: docconfig
#: generate the documentation
docconfig:
	@echo
	@echo "+++ generating documentation"
	@echo
	go run . doc config --output=../../config.md

.PHONY: docrules
#: generate the documentation
docrules:
	@echo
	@echo "+++ generating documentation"
	@echo
	go run . doc rules --output=../../rules.md

.PHONY: validate
#: validate the sample config
validateConfig:
	@echo
	@echo "+++ validating sample config"
	@echo
	go run . validate config --input=minimal_config.yaml

validateRules:
	@echo
	@echo "+++ validating sample rules"
	@echo
	go run . validate rules --input=../../rules_complete.yaml

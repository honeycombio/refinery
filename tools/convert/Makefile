.PHONY: all
#: build all the things
all: template names sample docs validateConfig validateRules build

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

.PHONY: docs
docs: docconfig docrules websiteconfig websiterules

.PHONY: docconfig
#: generate the config documentation for the repo
docconfig:
	@echo
	@echo "+++ generating documentation"
	@echo
	go run . doc config --output=../../config.md

.PHONY: docrules
#: generate the rules documentation for the repo
docrules:
	@echo
	@echo "+++ generating documentation"
	@echo
	go run . doc rules --output=../../rules.md

.PHONY: websiteconfig
#: generate the website config documentation
websiteconfig:
	@echo
	@echo "+++ generating documentation"
	@echo
	go run . website config --output=../../refinery_config.md

.PHONY: websiterules
#: generate the website rules documentation
websiterules:
	@echo
	@echo "+++ generating documentation"
	@echo
	go run . website rules --output=../../refinery_rules.md

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

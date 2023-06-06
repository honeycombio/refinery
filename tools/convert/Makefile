.PHONY: all
#: build all the things
all: template names sample doc validateConfig validateRules

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

.PHONY: doc
#: generate the documentation
doc:
	@echo
	@echo "+++ generating documentation"
	@echo
	go run . doc --output=config.md

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
	go run . validate rules --input=rules_complete.yaml

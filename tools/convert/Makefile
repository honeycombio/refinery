.PHONY: all
#: build all the things
all: template names sample doc schema

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

.PHONY: schema
#: generate the schema
schema:
	@echo
	@echo "+++ generating schema"
	@echo
	go run . schema --output=config.yamlschema

.PHONY: validate
#: validate the sample config
validate: schema
	@echo
	@echo "+++ validating sample config"
	@echo
	go run . validate --input=minimal_config.yaml
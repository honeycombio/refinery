package validation

import "strings"

type Validation struct {
	Type string `json:"type"`
	Arg  any    `json:"arg"`
}

// These are the data structures used by these commands and their templates
type Field struct {
	Name         string       `json:"name"`
	V1Group      string       `json:"v1group"`
	V1Name       string       `json:"v1name"`
	FirstVersion string       `json:"firstversion"`
	LastVersion  string       `json:"lastversion"`
	Type         string       `json:"type"`
	ValueType    string       `json:"valuetype"`
	Extra        string       `json:"extra"`
	Default      any          `json:"default,omitempty"`
	Choices      []string     `json:"choices,omitempty"`
	Example      string       `json:"example,omitempty"`
	Validations  []Validation `json:"validations,omitempty"`
	Reload       bool         `json:"reload"`
	Summary      string       `json:"summary"`
	Description  string       `json:"description"`
	Pattern      string       `json:"pattern,omitempty"`
}

type Group struct {
	Name        string  `json:"name"`
	Title       string  `json:"title"`
	Description string  `json:"description"`
	Fields      []Field `json:"fields,omitempty"`
}

type ConfigData struct {
	Groups []Group `json:"groups"`
}

func (c *ConfigData) GetField(name string) *Field {
	parts := strings.Split(name, ".")
	if len(parts) != 2 {
		return nil
	}
	for _, g := range c.Groups {
		if g.Name == parts[0] {
			for _, f := range g.Fields {
				if f.Name == parts[1] {
					return &f
				}
			}
		}
	}
	return nil
}

func (c *ConfigData) GetGroup(name string) *Group {
	for _, g := range c.Groups {
		if g.Name == name {
			return &g
		}
	}
	return nil
}

package config

import (
	"io"
	"sort"
	"strings"

	"github.com/agnivade/levenshtein"
	"gopkg.in/yaml.v3"
)

type Validation struct {
	Type string `json:"type"`
	Arg  any    `json:"arg"`
}

func (v *Validation) GetArgAsStringSlice() []string {
	if aa, ok := v.Arg.([]any); ok {
		result := make([]string, len(aa))
		for i, a := range aa {
			result[i] = a.(string)
		}
		return result
	}
	panic("shouldn't happen: validation arg is not a slice of strings")
}

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

type Metadata struct {
	Groups []Group `json:"groups"`
}

func (c *Metadata) GetField(name string) *Field {
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

func (c *Metadata) GetGroup(name string) *Group {
	for _, g := range c.Groups {
		if g.Name == name {
			return &g
		}
	}
	return nil
}

// ClosestNamesTo finds the closest names to the given word in the metadata.
// If the word contains a dot, uses only the last part of the word.
// It returns a slice of names, sorted by closeness.
// It uses the Levenshtein distance to determine closeness.
func (c *Metadata) ClosestNamesTo(word string) []string {
	type lev struct {
		name     string
		distance int
	}
	words := []lev{}

	// if the word contains a dot, use only the last part of the word
	parts := strings.Split(word, ".")
	word = parts[len(parts)-1]
	for _, g := range c.Groups {
		distance := levenshtein.ComputeDistance(g.Name, word)
		words = append(words, lev{name: g.Name, distance: distance})
		for _, f := range g.Fields {
			distance = levenshtein.ComputeDistance(f.Name, word)
			words = append(words, lev{name: f.Name, distance: distance})
		}
	}
	sort.Slice(words, func(i, j int) bool {
		if words[i].distance == words[j].distance {
			return words[i].name < words[j].name
		}
		return words[i].distance < words[j].distance
	})
	closestNames := []string{words[0].name}
	for i := 1; i < len(words); i++ {
		if words[i].distance != words[0].distance {
			break
		}
		if words[i].name == words[0].name {
			continue
		}
		closestNames = append(closestNames, words[i].name)
	}
	return closestNames
}

func (c *Metadata) LoadFrom(rdr io.Reader) error {
	decoder := yaml.NewDecoder(rdr)
	err := decoder.Decode(c)
	if err != nil {
		return err
	}
	return nil
}

func LoadConfigMetadata() (*Metadata, error) {
	return loadNamedMetadata("metadata/configMeta.yaml")
}

func LoadRulesMetadata() (*Metadata, error) {
	return loadNamedMetadata("metadata/rulesMeta.yaml")
}

func loadNamedMetadata(input string) (*Metadata, error) {
	metadata := Metadata{}
	rdr, err := metadataFS.Open(input)
	if err != nil {
		return nil, err
	}
	defer rdr.Close()
	err = metadata.LoadFrom(rdr)
	return &metadata, err
}

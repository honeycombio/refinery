package config

import (
	"fmt"
	"regexp"
	"strings"
)

// metaFieldPrefix marks fields that Refinery and Honeycomb own. They are never
// copied from the root span.
const metaFieldPrefix = "meta."

// CopyFromRoot describes which root span fields are copied onto the other spans
// of a trace when the trace is sent. The zero value copies nothing.
type CopyFromRoot struct {
	fields   []string
	useRegex bool
	names    []string
	patterns []*regexp.Regexp
	prefix   string
}

// NewCopyFromRoot builds a CopyFromRoot. When useRegex is true each entry in
// fields is compiled as a regular expression and matched against root span
// field names, otherwise entries are exact field names. Every copied field is
// stored under prefix plus its original name.
func NewCopyFromRoot(fields []string, useRegex bool, prefix string) (CopyFromRoot, error) {
	c := CopyFromRoot{fields: fields, useRegex: useRegex, prefix: prefix}
	for _, f := range fields {
		if f == "" {
			continue
		}
		if !useRegex {
			c.names = append(c.names, f)
			continue
		}
		re, err := regexp.Compile(f)
		if err != nil {
			return CopyFromRoot{}, fmt.Errorf("invalid regular expression %q in FieldsToCopyFromRoot: %w", f, err)
		}
		c.patterns = append(c.patterns, re)
	}
	return c, nil
}

// Enabled reports whether any fields are configured to be copied.
func (c CopyFromRoot) Enabled() bool {
	return len(c.names) > 0 || len(c.patterns) > 0
}

// Fields returns the configured entries exactly as they were written.
func (c CopyFromRoot) Fields() []string {
	return c.fields
}

// UsesRegex reports whether the configured fields are regular expressions.
func (c CopyFromRoot) UsesRegex() bool {
	return c.useRegex
}

// Prefix returns the string prepended to the name of every copied field.
func (c CopyFromRoot) Prefix() string {
	return c.prefix
}

// Names returns the exact field names to copy. It is empty in regex mode.
func (c CopyFromRoot) Names() []string {
	return c.names
}

// Matches reports whether a root span field name is selected by one of the
// configured regular expressions.
func (c CopyFromRoot) Matches(field string) bool {
	for _, re := range c.patterns {
		if re.MatchString(field) {
			return true
		}
	}
	return false
}

// Selectable reports whether a field may be copied at all.
func (c CopyFromRoot) Selectable(field string) bool {
	return !strings.HasPrefix(field, metaFieldPrefix)
}

// Destination returns the field name a root span field is copied to.
func (c CopyFromRoot) Destination(field string) string {
	return c.prefix + field
}

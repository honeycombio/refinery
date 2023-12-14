# Refinery Conditions

## Overview

Refinery rules are described as a series of conditions.
Each condition is composed from a combination of these named elements:

- `Field` (or `Fields)
- `Operator`
- `Value`
- `Datatype`

The `Operator` is never optional, and controls which of the elements are required and which are optional.

## `Field` or `Fields`

text goes here

## `Operator`

Operators in Refinery rules configuration files may be one of the following:

### `=`

### `!=`

### `<`

### `<=`

### `>`

### `>=`

### starts-with

### contains

### does-not-contain

### exists

### not-exists

### matches

For clarity, regular expressions in YAML should usually be quoted with single
quotes (`'`). This is because this form is unambiguous and does not process
escape sequences, and thus regular expression character classes like `\d` for
digits can be used directly. For example, an expression to match arbitrary
strings of digits would be `'\d+'`.

Sometimes double-quoted (`"`) strings are required in order to express patterns
containing less common characters. These use escape sequences beginning with a
backslash (`\`). This implies that backslashes intended for the regular
expression will have to be doubled. The same expression as above using double
quotes looks like this: `"\\d+"`.

The Go language Regular expression syntax is documented [here](https://pkg.go.dev/regexp/syntax).

## `Value`

text goes here

## `Datatype`

text goes here


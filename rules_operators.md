# Refinery Rules Operators

Operators in Refinery rules configuration files may be one of the following:

## =, eq

## !=, ne

## <, lt



## starts-with

## contains

## does-not-contain, not-contains

## exists

## not-exists

## matches

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

## in

## not-in

Requires Datatype to be specified
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

A `Field` points to a specific attribute in the trace data. It can refer to various aspects of the trace,
such as HTTP-related information, duration, service names, and more.

When a Field value is absent in any spans within a trace, the associated rule does not apply to that trace.

```yaml
Condition:
    Field: http.route
    Operator: =
    Value: /health-check
```
### Leveraging Special Refinery Telemetry in Root Spans

Refinery enriches the configuration possibilities by introducing special attributes for root spans. For example,
when `AddCountsToRoot` is enabled, `meta.span_count`` is appended to all root spans, allowing for the creation
of conditions based on span counts.

```yaml
Condition:
    Field: "meta.span_count"
    Operator: ">"
    Value: 300
    Datatype: int
```
In this scenario, the rule applies to traces with more than 300 spans.
For details about all supported special fields, check out [documentation here](https://docs.honeycomb.io/manage-data-volume/refinery/configuration/#refinery-telemetry)

### Virtual Fields

To handle scenarios where rules are required before the arrival of root spans, Refinery introduces the concept of virtual fields. These fields provide metadata about traces that have timed out while waiting for their root span.

```yaml
Rules:
    - Name: Drop any big traces
      Drop: true
      Field: "?.NUM_DESCENDANTS"
      Operator: ">"
      Value: 1000
      Datatype: int

```
This example showcases a rule that drops traces containing more than 1000 spans, utilizing the virtual field "?.NUM_DESCENDANTS".

#### Supported Virtual Fields

All virtual fields has been prefixed with `?.`.

- "?.NUM_DESCENDANTS": the number of child elements within a trace.

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


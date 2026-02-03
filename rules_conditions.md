# Refinery Conditions

## Overview

Refinery rules are described as a series of conditions.
Each condition is composed from a combination of these named parameters:

- `Field` (or `Fields`)
- `Operator`
- `Value`
- `Datatype`

The `Operator` is a required parameter, and controls which of the condition's other elements are required and which are optional.

## `Field`

The `Field` parameter points to a specific named element in the trace data.
If a `Field` is named within a span, then it `exists`.
A specific `Field` may or may not exist on any specific span in a trace.
It might not even exist within a trace at all.

When a `Field` is absent in all spans within a trace, the associated rule does not apply to that trace.

A `Field` is always a single string.
A `Field` is always matched by exact comparison.
No transformations for case or punctuation are performed.

### Example use of `Field`

```yaml
Conditions:
    Field: http.route
    Operator: =
    Value: /health-check
```
### Special Refinery Telemetry in Root Spans (`meta` fields)

Some Refinery configuration options introduce special fields that are added to telemetry to help with certain queries.

For example, when `AddCountsToRoot` is enabled, `meta.span_count` is added to all root spans.
These `meta` fields are added only after a trace decision is made, and cannot be used in Refinery rules.
To make decisions based on the number of spans in a trace, consider using the `?.NUM_DESCENDANTS` [virtual field](#virtual-fields).

For details about all supported special fields, check out our [Refinery Telemetry documentation](https://docs.honeycomb.io/manage-data-volume/refinery/configuration/#refinery-telemetry).

### Virtual Fields

To handle some specific trace-aware scenarios, Refinery introduces the concept of virtual fields.
All virtual fields are prefixed with `?.` to distinguish them from normal fields.

Currently only one virtual field is supported.

- `?.NUM_DESCENDANTS`: the current number of child elements contained within a trace.

#### `?.NUM_DESCENDANTS`

##### Example: drop single-span traces

This example shows a rule to drop traces consisting of only a root span:

```yaml
    - Name: drop single-span traces
      Drop: true
      Conditions:
        - Operator: has-root-span
          Value: true
        - Field: "?.NUM_DESCENDANTS"
          Operator: =
          Value: 1
          Datatype: int
```

##### Example: drop large traces

This example shows a rule that drops traces containing more than 1000 spans.

```yaml
Rules:
    - Name: Drop any big traces
      Drop: true
      Conditions:
        Field: "?.NUM_DESCENDANTS"
        Operator: ">="
        Value: 1000
        Datatype: int
```

Combine this rule with setting the Traces -> SpanLimit config option to the same number.

```yaml
Traces:
  SpanLimit: 1000
```

The config change results in Refinery making a sampling decision for a trace once the 1,000th span for that trace arrives.
For the sampling decision, the rule will match, the trace marked for drop, and the trace's current spans will be removed from Refinery's in-memory cache.
Future arriving spans for the trace will be dropped and memory load on the cluster will be reduced.



## `Fields`

The `Fields` parameter allows a single rule to apply to the first match among multiple field names.
It is typically used when telemetry field names are being changed.
It is exactly equivalent to `Field`, except that it must be expressed as an array of strings instead of a single value.
The array defines a sequences of `Field` names that are checked in order for each span being considered.
The first field that `exists` on any given span is used for the condition.

### Example use of `Fields`

This example shows how one might write a rule designed to cope with an expected name change of a key field.

```yaml
Conditions:
    Fields:
        - http.status
        - http.request.status
    Operator: =
    Value: 200
    Datatype: int
```

## Using a Prefix to Identify a Field in a Related Span

Fields can contain a span selection prefix. Today, the only prefix supported is `root`.
This prefix causes the root span to be searched for the specified field, rather than the span being evaluated.

```yaml
Rules:
    - Name: limit by root span context
      Conditions:
        Field: "root.http.status"
        Operator: =
        Value: "500"
        Datatype: string
```


## `Operator`

The `Operator` parameter controls how rules are evaluated.
Because YAML treats certain characters like less-than (`<`) specially, it is good practice to always enclose the basic comparison operators in single quotes (like `'<'`).

The `Operator` may be one of the following:

### `'='`

Basic comparison -- `equals`.
The result is true if the value of the named `Field` is equal to the `Value` specified.
See [`Datatype`](#datatype) for how different datatypes are handled.

### `'!='`

Basic comparison -- `not equals`.
The result is true if the value of the named `Field` is not equal to the `Value` specified.
See [`Datatype`](#datatype) for how different datatypes are handled.

For most cases, use `'!='` in a rule with a scope of "span".
WARNING: Rules can have `Scope: trace` or `Scope: span`; `'!='` used with `Scope: trace` will be true if **any** single span in the entire trace matches the negative condition.
This is almost never desired behavior.

### `'<'`

Basic comparison -- `less than`.
The result is true if the value of the named `Field` is less than the `Value` specified.
See [`Datatype`](#datatype) for how different datatypes are handled.

### `'<='`

Basic comparison -- `less than or equal to`.
The result is true if the value of the named `Field` is less than or equal to the `Value` specified.
See [`Datatype`](#datatype) for how different datatypes are handled.

### `'>'`

Basic comparison -- `greater than`.
The result is true if the value of the named `Field` is greater than the `Value` specified.
See [`Datatype`](#datatype) for how different datatypes are handled.

### `'>='`

Basic comparison -- `greater than or equal to`.
The result is true if the value of the named `Field` is greater than or equal to the `Value` specified.
See [`Datatype`](#datatype) for how different datatypes are handled.

### `starts-with`

Tests if the span value named by the `Field` begins with the text specified in the `Value` parameter.
Comparisons are case-sensitive and exact.

Values are always coerced to strings -- the `Datatype` parameter is ignored.

### `contains`

Tests if the span value named by the `Field` contains the text specified in the `Value` parameter.
Comparisons are case-sensitive and exact.

Values are always coerced to strings -- the `Datatype` parameter is ignored.

### `does-not-contain`

Tests if the span value named by the `Field` does not contain the text specified in the `Value` parameter.
Comparisons are case-sensitive and exact.

Values are always coerced to strings -- the `Datatype` parameter is ignored.

### `in`

The `Value` parameter should be a list of items.

Tests if the span value named by the `Field` occurs exactly within the list specified in the `Value` parameter.
Comparisons are exact. For strings, comparisons are also case-sensitive.

### `not-in`

The `Value` parameter should be a list of items.

Tests if the span value named by the `Field` does not occur exactly within the list specified in the `Value` parameter.
Comparisons are exact. For strings, comparisons are also case-sensitive.



For most cases, use negative operators (`!=`, `does-not-contain`, `not-exists`,
and `not-in`) in a rule with a scope of "span".
WARNING: Rules can have `Scope: trace` or `Scope: span`.
Using a negative operator with `Scope: trace` will cause the condition be true if **any** single span in the entire trace matches.
Use `Scope: span` with negative operators.

### `exists`

Tests if the specified span contains the field named by the `Field` parameter, without considering its value.

Both the `Value` and the `Datatype` parameters are ignored.

### `not-exists`

Tests if the specified span does not contain the field named by the `Field` parameter, without considering its value.

Both the `Value` and the `Datatype` parameters are ignored.

For most cases, use `not-exists` in a rule with a scope of "span".
WARNING: Rules can have `Scope: trace` or `Scope: span`; `not-exists` used with `Scope: trace` will be true if **any** single span in the entire trace matches the negative condition.
This is almost never desired behavior.

### has-root-span

Tests if the trace as a whole has a root span.
The `Value` parameter can either be `true` or `false`.

WARNING: `has-root-span` is a trace-level operator that checks whether the trace **has** a root span, not whether a given span **is** a root span.
It **cannot be used with `Scope: span`**.
When a rule has `Scope: span`, all conditions must match on a single span, but `has-root-span` evaluates the entire trace.
Combining them will cause the rule to fail evaluation and be skipped, meaning traces that should match will not be sampled as expected.
Always use `has-root-span` with `Scope: trace` (the default) or omit the `Scope` parameter entirely.

Example:
```yaml
- Name: Small Fast Traces
  Scope: trace  # Use trace scope with has-root-span
  Conditions:
    - Operator: has-root-span
      Value: true
    - Field: root.duration_ms
      Operator: '<'
      Value: 1000
      Datatype: int
```

### `matches`

Tests if the span value specified by the `Field` parameter matches the regular expression specified by the `Value` parameter.
The regular expression grammar used is the syntax used by the Go programming language.
It is documented [here](https://pkg.go.dev/regexp/syntax).

For clarity, regular expressions in YAML should usually be quoted with single quotes (`'`).
This is because this form is unambiguous and does not process escape sequences, and thus regular expression character classes like `\d` for digits can be used directly.
For example, an expression to match arbitrary strings of digits would be `'\d+'`.

Sometimes double-quoted (`"`) strings are required in order to express patterns containing less common characters.
These use escape sequences beginning with a backslash (`\`). This implies that backslashes intended for the regular expression will have to be doubled.
The same expression as above using double quotes looks like this: `"\\d+"`.

Example:
```yaml
      RulesBasedSampler:
            Rules:
                - Name: Drop traces for any path element starting with /health or /status
                  Conditions:
                    - Field: http.target
                      Operator: matches
                      Value: '/(health/status)\w+'
 ```

Values are always coerced to strings -- the `Datatype` parameter is ignored.

## `Value`

The `Value` parameter can be any value of a supported type.
Its meaning and interpretation depends on the `Operator` in use.

See [`Datatype`](#datatype) for how different datatypes are handled.

## `Datatype`

The `Datatype` parameter controls the type of the values used when evaluating rules for the basic comparison operators.
When `Datatype` is present, before evaluating the `Operator`, both the span value and the `Value` parameter are coerced (converted if necessary) to the specified format, and the comparison takes place as appropriate for that datatype.

There are 4 possibilities:

- `string` -- The comparison uses string operations. (For example, "2" is considered to be greater than "10" because '2' > '1'.)
- `int` -- The comparison uses integers. (1.5 == 1 because `1.5` gets converted to `1`)
- `float` -- The comparison uses floating point values.
- `bool` -- The comparison tries to convert values to boolean. Note that because of the semantics of YAML, the `Value` parameter will interpret not only `true/false` but also all of `yes/no`, `y/n`, and `on/off` as boolean values. Span values, for historical reasons, interpret `true/false` and `1/0` as boolean, and all other values are considered to be `false`.

If the `Datatype` parameter is not specified, then Refinery determines the type of the incoming span value. If the value is numeric or boolean, it attempts to convert the `Value` parameter to the same type. If the span value is a string, the `Value` parameter must also be a string or the comparison will fail.



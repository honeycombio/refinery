# Honeycomb Refinery Rules Documentation

This is the documentation for the rules configuration for Honeycomb's Refinery.
It was automatically generated on 2024-09-03 at 19:48:56 UTC.

## The Rules file

The rules file is a YAML file.
Below is a simple example of a `rules` file.
View a [complete example](https://github.com/honeycombio/refinery/blob/main/rules_complete.yaml).

```yaml
RulesVersion: 2
Samplers:
    __default__:
        DeterministicSampler:
            SampleRate: 1
    production:
        DynamicSampler:
            SampleRate: 2
            ClearFrequency: 30s
            FieldList:
                - request.method
                - http.target
                - response.status_code
```

Name: `RulesVersion`

This is a required parameter used to verify the version of the rules file.
It must be set to 2.

Name: `Samplers`

Samplers is a mapping of targets to samplers.
Each target is a Honeycomb environment (or, for classic keys, a dataset).
The value is the sampler to use for that target.
The target called `__default__` will be used for any target that is not explicitly listed.
A `__default__` target is required.
The targets are determined by examining the API key used to send the trace.
If the API key is a 'classic' key (which is a 32-character hexadecimal value), the specified dataset name is used as the target.
If the API key is a new-style key (20-23 alphanumeric characters), the key's environment name is used as the target.

The remainder of this document describes the samplers that can be used within the `Samplers` section and the fields that control their behavior.

## Table of Contents
- [Deterministic Sampler](#deterministic-sampler)
- [Dynamic Sampler](#dynamic-sampler)
- [EMA Dynamic Sampler](#ema-dynamic-sampler)
- [EMA Throughput Sampler](#ema-throughput-sampler)
- [Windowed Throughput Sampler](#windowed-throughput-sampler)
- [Rules-based Sampler](#rules-based-sampler)
- [Rules for Rules-based Samplers](#rules-for-rules-based-samplers)
- [Conditions for the Rules in Rules-based Samplers](#conditions-for-the-rules-in-rules-based-samplers)
- [Total Throughput Sampler](#total-throughput-sampler)

---
## Deterministic Sampler

### Name: `DeterministicSampler`

The Deterministic Sampler (`DeterministicSampler`) uses a fixed sample rate to sample traces based on their trace ID.
This is the simplest sampling algorithm - it is a static sample rate, choosing traces randomly to either keep or send (at the appropriate rate).
It is not influenced by the contents of the trace other than the trace ID.

### `SampleRate`

The sample rate to use.
It indicates a ratio, where one sample trace is kept for every N traces seen.
For example, a `SampleRate` of `30` will keep 1 out of every 30 traces.
The choice on whether to keep any specific trace is random, so the rate is approximate.
The sample rate is calculated from the trace ID, so all spans with the same trace ID will be sampled or not sampled together.
A `SampleRate` of `1` or less will keep all traces.
Specifying this value is required.

Type: `int`

---
## Dynamic Sampler

### Name: `DynamicSampler`

The Dynamic Sampler (`DynamicSampler`) is the basic Dynamic Sampler implementation.
Most installations will find the EMA Dynamic Sampler to be a better choice.
This sampler collects the values of a number of fields from a trace and uses them to form a key.
This key is handed to the standard dynamic sampler algorithm, which generates a sample rate based on the frequency with which that key has appeared during the previous `ClearFrequency`.
See https://github.com/honeycombio/dynsampler-go for more detail on the mechanics of the Dynamic Sampler.
This sampler uses the `AvgSampleRate` algorithm from that package.

### `SampleRate`

The sample rate to use.
It indicates a ratio, where one sample trace is kept for every N traces seen.
For example, a `SampleRate` of `30` will keep 1 out of every 30 traces.
The choice on whether to keep any specific trace is random, so the rate is approximate.
The sample rate is calculated from the trace ID, so all spans with the same trace ID will be sampled or not sampled together.
A `SampleRate` of `1` or less will keep all traces.
Specifying this value is required.

Type: `int`

### `ClearFrequency`

The duration after which the Dynamic Sampler should reset its internal counters.
It should be specified as a duration string.
For example, "30s" or "1m".
Defaults to "30s".

Type: `duration`

### `FieldList`

A list of all the field names to use to form the key that will be handed to the Dynamic Sampler.
The combination of values from all of these fields should reflect how interesting the trace is compared to another.
When choosing field names for `FieldList`, a good field selection has consistent values for high-frequency, boring traffic, and unique values for outliers and interesting traffic.
Including an error field, or something like `HTTP status code`, is an excellent choice.
Using fields with very high cardinality, like `k8s.pod.id`, is a bad choice.
If the combination of fields essentially makes each trace unique, then the Dynamic Sampler will sample everything.
If the combination of fields is not unique enough, then you will not be guaranteed samples of the most interesting traces.
As an example, consider as a good set of fields: the combination of `HTTP endpoint` (high-frequency and boring), `HTTP method`, and `status code` (normally boring but can become interesting when indicating an error) since it will allowing proper sampling of all endpoints under normal traffic and call out when there is failing traffic to any endpoint.
In contrast, for example, consider as a bad set of fields: a combination of `HTTP endpoint`, `status code`, and `pod id`, since it would result in keys that are all unique, and therefore result in sampling 100% of traces.
For example, rather than a set of fields, using only the `HTTP endpoint` field is a **bad** choice, as it is not unique enough, and therefore interesting traces, like traces that experienced a `500`, might not be sampled.
Field names may come from any span in the trace; if they occur on multiple spans, then all unique values will be included in the key.

Type: `stringarray`

### `MaxKeys`

Limits the number of distinct keys tracked by the sampler.
Once `MaxKeys` is reached, new keys will not be included in the sample rate map, but existing keys will continue to be be counted.
Use this field to keep the sample rate map size under control.
Defaults to `500`; Dynamic Samplers will rarely achieve their sampling goals with more keys than this.

Type: `int`

### `UseTraceLength`

Indicates whether to include the trace length (number of spans in the trace) as part of the key.
The number of spans is exact, so if there are normally small variations in trace length, we recommend setting this field to `false` (the default).
If your traces are consistent lengths and changes in trace length is a useful indicator to view in Honeycomb, then set this field to `true`.

Type: `bool`

---
## EMA Dynamic Sampler

### Name: `EMADynamicSampler`

The Exponential Moving Average (EMA) Dynamic Sampler (`EMADynamicSampler`) attempts to average a given sample rate, weighting rare traffic and frequent traffic differently so as to end up with the correct average.
`EMADynamicSampler` is an improvement upon the simple `DynamicSampler` and is recommended for many use cases.
Based on the `DynamicSampler`, `EMADynamicSampler` differs in that rather than compute rate based on a periodic sample of traffic, it maintains an Exponential Moving Average of counts seen per key, and adjusts this average at regular intervals.
The weight applied to more recent intervals is defined by `weight`, a number between (0, 1).
Larger values weight the average more toward recent observations.
In other words, a larger weight will cause sample rates more quickly adapt to traffic patterns, while a smaller weight will result in sample rates that are less sensitive to bursts or drops in traffic and thus more consistent over time.
Keys that are not already present in the EMA will always have a sample rate of `1`.
Keys that occur more frequently will be sampled on a logarithmic curve.
Every key will be represented at least once in any given window and more frequent keys will have their sample rate increased proportionally to trend towards the goal sample rate.

### `GoalSampleRate`

The sample rate to use.
It indicates a ratio, where one sample trace is kept for every N traces seen.
For example, a `SampleRate` of `30` will keep 1 out of every 30 traces.
The choice on whether to keep any specific trace is random, so the rate is approximate.
The sample rate is calculated from the trace ID, so all spans with the same trace ID will be sampled or not sampled together.
A `SampleRate` of `1` or less will keep all traces.
Specifying this value is required.

Type: `int`

### `AdjustmentInterval`

The duration after which the EMA Dynamic Sampler should recalculate its internal counters.
It should be specified as a duration string.
For example, `30s` or `1m`.
Defaults to `15s`.

Type: `duration`

### `Weight`

The weight to use when calculating the EMA.
It should be a number between `0` and `1`.
Larger values weight the average more toward recent observations.
In other words, a larger weight will cause sample rates more quickly adapt to traffic patterns, while a smaller weight will result in sample rates that are less sensitive to bursts or drops in traffic and thus more consistent over time.
The default value is `0.5`.

Type: `float`

### `AgeOutValue`

Indicates the threshold for removing keys from the EMA.
The EMA of any key will approach `0` if it is not repeatedly observed, but will never truly reach it, so this field determines what constitutes "zero".
Keys with averages below this threshold will be removed from the EMA.
Default is the value of `Weight`, as this prevents a key with the smallest integer value (1) from being aged out immediately.
This value should generally be less than (<=) `Weight`, unless you have very specific reasons to set it higher.

Type: `float`

### `BurstMultiple`

If set, then this value is multiplied by the sum of the running average of counts to dynamically define the burst detection threshold.
If total counts observed for a given interval exceed this threshold, then EMA is updated immediately, rather than waiting on the `AdjustmentInterval`.
Defaults to `2`; a negative value disables.
With the default of `2`, if your traffic suddenly doubles, then burst detection will kick in.

Type: `float`

### `BurstDetectionDelay`

Indicates the number of intervals to run before burst detection kicks in.
Defaults to `3`.

Type: `int`

### `FieldList`

A list of all the field names to use to form the key that will be handed to the Dynamic Sampler.
The combination of values from all of these fields should reflect how interesting the trace is compared to another.
When choosing field names for `FieldList`, a good field selection has consistent values for high-frequency, boring traffic, and unique values for outliers and interesting traffic.
Including an error field, or something like `HTTP status code`, is an excellent choice.
Using fields with very high cardinality, like `k8s.pod.id`, is a bad choice.
If the combination of fields essentially makes each trace unique, then the Dynamic Sampler will sample everything.
If the combination of fields is not unique enough, then you will not be guaranteed samples of the most interesting traces.
As an example, consider as a good set of fields: the combination of `HTTP endpoint` (high-frequency and boring), `HTTP method`, and `status code` (normally boring but can become interesting when indicating an error) since it will allowing proper sampling of all endpoints under normal traffic and call out when there is failing traffic to any endpoint.
In contrast, for example, consider as a bad set of fields: a combination of `HTTP endpoint`, `status code`, and `pod id`, since it would result in keys that are all unique, and therefore result in sampling 100% of traces.
For example, rather than a set of fields, using only the `HTTP endpoint` field is a **bad** choice, as it is not unique enough, and therefore interesting traces, like traces that experienced a `500`, might not be sampled.
Field names may come from any span in the trace; if they occur on multiple spans, then all unique values will be included in the key.

Type: `stringarray`

### `MaxKeys`

Limits the number of distinct keys tracked by the sampler.
Once `MaxKeys` is reached, new keys will not be included in the sample rate map, but existing keys will continue to be be counted.
Use this field to keep the sample rate map size under control.
Defaults to `500`; Dynamic Samplers will rarely achieve their sampling goals with more keys than this.

Type: `int`

### `UseTraceLength`

Indicates whether to include the trace length (number of spans in the trace) as part of the key.
The number of spans is exact, so if there are normally small variations in trace length, we recommend setting this field to `false` (the default).
If your traces are consistent lengths and changes in trace length is a useful indicator to view in Honeycomb, then set this field to `true`.

Type: `bool`

---
## EMA Throughput Sampler

### Name: `EMAThroughputSampler`

The Exponential Moving Average (EMA) Throughput Sampler (`EMAThroughputSampler`) attempts to achieve a given throughput -- number of spans per second -- weighting rare traffic and frequent traffic differently so as to end up with the correct rate.
The `EMAThroughputSampler` is an improvement upon the Total Throughput Sampler and is recommended for most throughput-based use cases.
Because it like the `EMADynamicSampler`, `EMAThroughputSampler` maintains an Exponential Moving Average of counts seen per key, and adjusts this average at regular intervals.
The weight applied to more recent intervals is defined by `weight`, a number between (0, 1) - larger values weight the average more toward recent observations.
In other words, a larger weight will cause sample rates more quickly adapt to traffic patterns, while a smaller weight will result in sample rates that are less sensitive to bursts or drops in traffic and thus more consistent over time.
New keys that are not already present in the EMA will always have a sample rate of `1`.
Keys that occur more frequently will be sampled on a logarithmic curve.
Every key will be represented at least once in any given window and more frequent keys will have their sample rate increased proportionally to trend towards the goal throughput.

### `GoalThroughputPerSec`

The desired throughput **per second**.
This is the number of events per second you want to send to Honeycomb.
The sampler will adjust sample rates to try to achieve this desired throughput.
This value is calculated for the individual instance, not for the cluster; if your cluster has multiple instances, then you will need to divide your total desired sample rate by the number of instances to get this value.

Type: `int`

### `UseClusterSize`

Indicates whether to use the cluster size to calculate the goal throughput.
If `true`, then the goal throughput will be divided by the number of instances in the cluster.
If `false` (the default), then the goal throughput will be the value specified in `GoalThroughputPerSec`.

Type: `bool`

### `InitialSampleRate`

`InitialSampleRate` is the sample rate to use during startup, before the sampler has accumulated enough data to calculate a reasonable throughput.
This is mainly useful in situations where unsampled throughput is high enough to cause problems.
Default value is `10`.

Type: `int`

### `AdjustmentInterval`

The duration after which the EMA Dynamic Sampler should recalculate its internal counters.
It should be specified as a duration string.
For example, `30s` or `1m`.
Defaults to `15s`.

Type: `duration`

### `Weight`

The weight to use when calculating the EMA.
It should be a number between `0` and `1`.
Larger values weight the average more toward recent observations.
In other words, a larger weight will cause sample rates more quickly adapt to traffic patterns, while a smaller weight will result in sample rates that are less sensitive to bursts or drops in traffic and thus more consistent over time.
The default value is `0.5`.

Type: `float`

### `AgeOutValue`

Indicates the threshold for removing keys from the EMA.
The EMA of any key will approach `0` if it is not repeatedly observed, but will never truly reach it, so this field determines what constitutes "zero".
Keys with averages below this threshold will be removed from the EMA.
Default is the value of `Weight`, as this prevents a key with the smallest integer value (1) from being aged out immediately.
This value should generally be less than (<=) `Weight`, unless you have very specific reasons to set it higher.

Type: `float`

### `BurstMultiple`

If set, then this value is multiplied by the sum of the running average of counts to dynamically define the burst detection threshold.
If total counts observed for a given interval exceed this threshold, then EMA is updated immediately, rather than waiting on the `AdjustmentInterval`.
Defaults to `2`; a negative value disables.
With the default of `2`, if your traffic suddenly doubles, then burst detection will kick in.

Type: `float`

### `BurstDetectionDelay`

Indicates the number of intervals to run before burst detection kicks in.
Defaults to `3`.

Type: `int`

### `FieldList`

A list of all the field names to use to form the key that will be handed to the Dynamic Sampler.
The combination of values from all of these fields should reflect how interesting the trace is compared to another.
When choosing field names for `FieldList`, a good field selection has consistent values for high-frequency, boring traffic, and unique values for outliers and interesting traffic.
Including an error field, or something like `HTTP status code`, is an excellent choice.
Using fields with very high cardinality, like `k8s.pod.id`, is a bad choice.
If the combination of fields essentially makes each trace unique, then the Dynamic Sampler will sample everything.
If the combination of fields is not unique enough, then you will not be guaranteed samples of the most interesting traces.
As an example, consider as a good set of fields: the combination of `HTTP endpoint` (high-frequency and boring), `HTTP method`, and `status code` (normally boring but can become interesting when indicating an error) since it will allowing proper sampling of all endpoints under normal traffic and call out when there is failing traffic to any endpoint.
In contrast, for example, consider as a bad set of fields: a combination of `HTTP endpoint`, `status code`, and `pod id`, since it would result in keys that are all unique, and therefore result in sampling 100% of traces.
For example, rather than a set of fields, using only the `HTTP endpoint` field is a **bad** choice, as it is not unique enough, and therefore interesting traces, like traces that experienced a `500`, might not be sampled.
Field names may come from any span in the trace; if they occur on multiple spans, then all unique values will be included in the key.

Type: `stringarray`

### `MaxKeys`

Limits the number of distinct keys tracked by the sampler.
Once `MaxKeys` is reached, new keys will not be included in the sample rate map, but existing keys will continue to be be counted.
Use this field to keep the sample rate map size under control.
Defaults to `500`; Dynamic Samplers will rarely achieve their sampling goals with more keys than this.

Type: `int`

### `UseTraceLength`

Indicates whether to include the trace length (number of spans in the trace) as part of the key.
The number of spans is exact, so if there are normally small variations in trace length, we recommend setting this field to `false` (the default).
If your traces are consistent lengths and changes in trace length is a useful indicator to view in Honeycomb, then set this field to `true`.

Type: `bool`

---
## Windowed Throughput Sampler

### Name: `WindowedThroughputSampler`

Windowed Throughput Sampler (`WindowedThroughputSampler`) is an enhanced version of total throughput sampling.
Just like the `TotalThroughput` Sampler, `WindowedThroughputSampler` attempts to meet the goal of fixed number of events per second sent to Honeycomb.
The original throughput sampler updates the sampling rate every "ClearFrequency" seconds.
While this parameter is configurable, it suffers from the following tradeoff:
  - Decreasing it is more responsive to load spikes, but with the
  cost of making the sampling decision on less data.
- Increasing it is less responsive to load spikes, but sample rates
  will be more stable because they are made with more data.
The Windowed Throughput Sampler resolves this by introducing two different, tunable parameters:
  - `UpdateFrequency`: how often the sampling rate is recomputed
  - `LookbackFrequency`: how much total time is considered when
  recomputing sampling rate.
A standard configuration would be to set `UpdateFrequency` to `1s` and `LookbackFrequency` to `30s`.
In this configuration, for every second, we lookback at the last 30 seconds of data in order to compute the new sampling rate.
The actual sampling rate computation is nearly identical to the original Throughput Sampler, but this variant has better support for floating point numbers and does a better job with less-common keys.

### `GoalThroughputPerSec`

The desired throughput **per second**.
This is the number of events per second you want to send to Honeycomb.
The sampler will adjust sample rates to try to achieve this desired throughput.
This value is calculated for the individual instance, not for the cluster; if your cluster has multiple instances, then you will need to divide your total desired sample rate by the number of instances to get this value.

Type: `int`

### `UseClusterSize`

Indicates whether to use the cluster size to calculate the goal throughput.
If `true`, then the goal throughput will be divided by the number of instances in the cluster.
If `false` (the default), then the goal throughput will be the value specified in `GoalThroughputPerSec`.

Type: `bool`

### `UpdateFrequency`

The duration between sampling rate computations.
It should be specified as a duration string.
For example, `30s` or `1m`.
Defaults to `1s`.

Type: `duration`

### `LookbackFrequency`

This controls how far back in time to lookback to dynamically adjust the sampling rate.
Default is `30 * UpdateFrequencyDuration`.
This field is forced to be an **integer multiple** of `UpdateFrequencyDuration`.

Type: `duration`

### `FieldList`

A list of all the field names to use to form the key that will be handed to the Dynamic Sampler.
The combination of values from all of these fields should reflect how interesting the trace is compared to another.
When choosing field names for `FieldList`, a good field selection has consistent values for high-frequency, boring traffic, and unique values for outliers and interesting traffic.
Including an error field, or something like `HTTP status code`, is an excellent choice.
Using fields with very high cardinality, like `k8s.pod.id`, is a bad choice.
If the combination of fields essentially makes each trace unique, then the Dynamic Sampler will sample everything.
If the combination of fields is not unique enough, then you will not be guaranteed samples of the most interesting traces.
As an example, consider as a good set of fields: the combination of `HTTP endpoint` (high-frequency and boring), `HTTP method`, and `status code` (normally boring but can become interesting when indicating an error) since it will allowing proper sampling of all endpoints under normal traffic and call out when there is failing traffic to any endpoint.
In contrast, for example, consider as a bad set of fields: a combination of `HTTP endpoint`, `status code`, and `pod id`, since it would result in keys that are all unique, and therefore result in sampling 100% of traces.
For example, rather than a set of fields, using only the `HTTP endpoint` field is a **bad** choice, as it is not unique enough, and therefore interesting traces, like traces that experienced a `500`, might not be sampled.
Field names may come from any span in the trace; if they occur on multiple spans, then all unique values will be included in the key.

Type: `stringarray`

### `MaxKeys`

Limits the number of distinct keys tracked by the sampler.
Once `MaxKeys` is reached, new keys will not be included in the sample rate map, but existing keys will continue to be be counted.
Use this field to keep the sample rate map size under control.
Defaults to `500`; Dynamic Samplers will rarely achieve their sampling goals with more keys than this.

Type: `int`

### `UseTraceLength`

Indicates whether to include the trace length (number of spans in the trace) as part of the key.
The number of spans is exact, so if there are normally small variations in trace length, we recommend setting this field to `false` (the default).
If your traces are consistent lengths and changes in trace length is a useful indicator to view in Honeycomb, then set this field to `true`.

Type: `bool`

---
## Rules-based Sampler

### Name: `RulesBasedSampler`

The Rules-based sampler allows you to specify a set of rules that will determine whether a trace should be sampled or not.
Rules are evaluated in order, and the first rule that matches will be used to determine the sample rate.
If no rules match, then the `SampleRate` defaults to `1` and all traces will be kept.
Rules-based samplers will usually be configured to have the last rule be a default rule with no conditions that uses a downstream Dynamic Sampler to keep overall sample rate under control.

### `Rules`

`Rules` is a list of rules to use to determine the sample rate.

Type: `objectarray`

### `CheckNestedFields`

Indicates whether to expand nested JSON when evaluating rules.
If false (the default), nested JSON will be treated as a string.
If `true`, nested JSON will be expanded into a `map[string]interface{}` and the value of the field will be the value of the nested field.
For example, if you have a field called `http.request.headers` and you want to check the value of the `User-Agent` header, then you would set this to `true` and use `http.request.headers.User-Agent` as the field name in your rule.
This is a computationally expensive option and may cause performance problems if you have a large number of spans with nested JSON.

Type: `bool`

---
## Rules for Rules-based Samplers

### Name: `Rules`

Rules are evaluated in order, and the first rule that matches will be used to determine the sample rate.
If no rules match, then the `SampleRate` will be `1` and all traces will be kept.
If a rule matches, one of three things happens, and they are evaluated in this order: a) if the rule specifies a downstream Sampler, that sampler is used to determine the sample rate; b) if the rule has the `Drop` flag set to `true`, the trace is dropped; c) the rule's sample rate is used.

### `Name`

The name of the rule.
This field is used for debugging and will appear in the trace metadata if `AddRuleReasonToTrace` is set to `true`.

Type: `string`

### `Sampler`

The sampler to use if the rule matches.
If this is set, the sample rate will be determined by this downstream sampler.
If this is not set, the sample rate will be determined by the `Drop` flag or the `SampleRate` field.

Type: `object`

### `Drop`

Indicates whether to drop the trace if it matches this rule.
If `true`, then the trace will be dropped.
If `false`, then the trace will be kept.

Type: `bool`

### `SampleRate`

If the rule is matched, there is no Sampler specified, and the `Drop` flag is `false`, then this is the sample rate to use.

Type: `int`

### `Conditions`

Conditions is a list of conditions to use to determine whether the rule matches.
All conditions must be met for the rule to match.
If there are no conditions, then the rule will always match.
A no-condition rule is typically used for the last rule to provide a default behavior.

Type: `objectarray`

### `Scope`

Controls the scope of the rule evaluation.
If set to `trace` (the default), then each condition can apply to any span in the trace independently.
If set to `span`, then all of the conditions in the rule will be evaluated against each span in the trace and the rule only succeeds if all of the conditions match on a single span together.

Type: `string`

---
## Conditions for the Rules in Rules-based Samplers

### Name: `Conditions`

Conditions are evaluated in order, and the first condition that does not match will cause the rule to not match.
If all conditions match, then the rule will match.
If there are no conditions, then the rule will always match.

### `Field`

The field to check.
This can name any field in the trace.
If the field is not present, then the condition will not match.
The comparison is case-sensitive.
The field can also include a prefix that changes the span used for evaluation of the field.
The only prefix currently supported is `root`, as in `root.http.status`.
Specifying `root.` causes the condition to be evaluated against the root span.
For example, if the `Field` is `root.url`, then the condition will be processed using the url field from the root span.
The setting `Scope: span` for a rule does not change the meaning of this prefix -- the condition is still evaluated on the root span and is treated as if it were part of the span being processed.
When using the `root.` prefix on a field with a `not-exists` operator, include the `has-root-span: true` condition in the rule.
The `not-exists` condition on a `root.`-prefixed field will evaluate to false if the existence of the root span is not checked and the root span does not exist.
The primary reason a root span is not present on a trace when a sampling decision is being made is when the root span takes longer to complete than the configured TraceTimeout.

Type: `string`

### `Fields`

An array of field names to check.
These can name any field in the trace.
The fields are checked in the order defined here, and the first named field that contains a value will be used for the condition.
Only the first populated field will be used, even if the condition fails.
If a `root.` prefix is present on a field, but the root span is not on the trace, that field will be skipped.
If none of the fields are present, then the condition will not match.
The comparison is case-sensitive.
All fields are checked as individual fields before any of them are checked as nested fields (see `CheckNestedFields`).

Type: `stringarray`

### `Operator`

The comparison operator to use.
String comparisons are case-sensitive.
For most cases, use negative operators (`!=`, `does-not-contain`, `not-exists`, and `not-in`) in a rule with a scope of "span".
WARNING: Rules can have `Scope: trace` or `Scope: span`.
Using a negative operator with `Scope: trace` will cause the condition be true if **any** single span in the entire trace matches.
Use `Scope: span` with negative operators.

Type: `string`

- Options: `=`, `!=`, `>`, `<`, `>=`, `<=`, `starts-with`, `contains`, `does-not-contain`, `exists`, `not-exists`, `has-root-span`, `matches`, `in`, `not-in`

### `Value`

The value to compare against.
If `Datatype` is not specified, then the value and the field will be compared based on the type of the field.
The `in` and `not-in` operators can accept a list of values, which should all be of the same datatype.

Type: `sliceorscalar`

### `Datatype`

The datatype to use when comparing the value and the field.
If `Datatype` is specified, then both values will be converted (best-effort) to that type and then compared.
Errors in conversion will result in the comparison evaluating to `false`.
This is especially useful when a field like `http status code` may be rendered as strings by some environments and as numbers or booleans by others.
The best practice is to always specify `Datatype`; this avoids ambiguity, allows for more accurate comparisons, and offers a minor performance improvement.

Type: `string`

---
## Total Throughput Sampler

### Name: `TotalThroughputSampler`

Total Throughput Sampler (`TotalThroughputSampler`) attempts to meet a goal of a fixed number of events per second sent to Honeycomb.
This sampler is **deprecated** and present mainly for compatibility.
Consider using either `EMAThroughputSampler` or `WindowedThroughputSampler` instead.
If your key space is sharded across different servers, then this is a good method for making sure each server sends roughly the same volume of content to Honeycomb.
It performs poorly when the active keyspace is very large.
`GoalThroughputPerSec` * `ClearFrequency` defines the upper limit of the number of keys that can be reported and stay under the goal, but with that many keys, you'll only get one event per key per `ClearFrequencySec`, which is very coarse.
Aim for at least 1 event per key per sec to 1 event per key per 10sec to get reasonable data.
In other words, the number of active keys should be less than 10 * `GoalThroughputPerSec`.

### `GoalThroughputPerSec`

The desired throughput per second of events sent to Honeycomb.
This is the number of events per second you want to send.
This is not the same as the Sample Rate.

Type: `int`

### `UseClusterSize`

Indicates whether to use the cluster size to calculate the goal throughput.
If `true`, then the goal throughput will be divided by the number of instances in the cluster.
If `false` (the default), then the goal throughput will be the value specified in `GoalThroughputPerSec`.

Type: `bool`

### `ClearFrequency`

The duration after which the Dynamic Sampler should reset its internal counters.
It should be specified as a duration string.
For example, "30s" or "1m".
Defaults to "30s".

Type: `duration`

### `FieldList`

A list of all the field names to use to form the key that will be handed to the Dynamic Sampler.
The combination of values from all of these fields should reflect how interesting the trace is compared to another.
When choosing field names for `FieldList`, a good field selection has consistent values for high-frequency, boring traffic, and unique values for outliers and interesting traffic.
Including an error field, or something like `HTTP status code`, is an excellent choice.
Using fields with very high cardinality, like `k8s.pod.id`, is a bad choice.
If the combination of fields essentially makes each trace unique, then the Dynamic Sampler will sample everything.
If the combination of fields is not unique enough, then you will not be guaranteed samples of the most interesting traces.
As an example, consider as a good set of fields: the combination of `HTTP endpoint` (high-frequency and boring), `HTTP method`, and `status code` (normally boring but can become interesting when indicating an error) since it will allowing proper sampling of all endpoints under normal traffic and call out when there is failing traffic to any endpoint.
In contrast, for example, consider as a bad set of fields: a combination of `HTTP endpoint`, `status code`, and `pod id`, since it would result in keys that are all unique, and therefore result in sampling 100% of traces.
For example, rather than a set of fields, using only the `HTTP endpoint` field is a **bad** choice, as it is not unique enough, and therefore interesting traces, like traces that experienced a `500`, might not be sampled.
Field names may come from any span in the trace; if they occur on multiple spans, then all unique values will be included in the key.

Type: `stringarray`

### `MaxKeys`

Limits the number of distinct keys tracked by the sampler.
Once `MaxKeys` is reached, new keys will not be included in the sample rate map, but existing keys will continue to be be counted.
Use this field to keep the sample rate map size under control.
Defaults to `500`; Dynamic Samplers will rarely achieve their sampling goals with more keys than this.

Type: `int`

### `UseTraceLength`

Indicates whether to include the trace length (number of spans in the trace) as part of the key.
The number of spans is exact, so if there are normally small variations in trace length, we recommend setting this field to `false` (the default).
If your traces are consistent lengths and changes in trace length is a useful indicator to view in Honeycomb, then set this field to `true`.

Type: `bool`


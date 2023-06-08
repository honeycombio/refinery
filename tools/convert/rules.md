# Honeycomb Refinery Configuration Documentation

This is the documentation for the configuration file for Honeycomb's Refinery.

---
## DeterministicSampler: Deterministic Sampler

The deterministic sampler uses a fixed sample rate to sample traces
based on their trace ID. This is the simplest sampling algorithm - it
is a static sample rate, choosing traces randomly to either keep or
send (at the appropriate rate). It is not influenced by the contents
of the trace other than the trace ID.


### SampleRate

SampleRate is the sample rate to use.

The sample rate to use. It indicates a ratio, where one sample trace
is kept for every N traces seen. For example, a SampleRate of 30 will
keep 1 out of every 30 traces. The choice on whether to keep any
specific trace is random, so the rate is approximate.
The sample rate is calculated from the trace ID, so all spans with the
same trace ID will be sampled or not sampled together.


Not eligible for live reload.

Type: `int`




---
## DynamicSampler: Dynamic Sampler

DynamicSampler is the most basic Dynamic Sampler implementation. This
sampler collects the values of a number of fields from a trace and
uses them to form a key. This key is handed to the standard dynamic
sampler algorithm which generates a sample rate based on the frequency
with which that key has appeared during the previous ClearFrequency.
See https://github.com/honeycombio/dynsampler-go for more detail on
the mechanics of the dynamic sampler.  This sampler uses the
AvgSampleRate algorithm from that package.


### SampleRate

SampleRate is the sample rate to use.

The sample rate to use. It indicates a ratio, where one sample trace
is kept for every N traces seen. For example, a SampleRate of 30 will
keep 1 out of every 30 traces. The choice on whether to keep any
specific trace is random, so the rate is approximate.
The sample rate is calculated from the trace ID, so all spans with the
same trace ID will be sampled or not sampled together.


Not eligible for live reload.

Type: `int`




### ClearFrequency

ClearFrequency is the duration over which the sampler will calculate
the sample rate.

The duration after which the dynamic sampler should reset its internal
counters. It should be specified as a duration string, e.g. "30s" or
"1m".


Not eligible for live reload.

Type: `duration`




### FieldList

FieldList is the list of fields to use to create the key for the
dynamic sampler.

FieldList is a list of all the field names to use to form the key that
will be handed to the dynamic sampler. The combination of values from
all of these fields should reflect how interesting the trace is
compared to another. A good field selection has consistent values for
high-frequency, boring traffic, and unique values for outliers and
interesting traffic. Including an error field (or something like HTTP
status code) is an excellent choice. Using fields with very high
cardinality (like `k8s.pod.id`), is a bad choice. If the combination
of fields essentially makes them unique, the dynamic sampler will
sample everything. If the combination of fields is not unique enough,
you will not be guaranteed samples of the most interesting traces. As
an example, consider a combination of HTTP endpoint (high-frequency
and boring), HTTP method, and status code (normally boring but can
become interesting when indicating an error) as a good set of fields
since it will allowing proper sampling of all endpoints under normal
traffic and call out when there is failing traffic to any endpoint.
For example, in contrast, consider a combination of HTTP endpoint,
status code, and pod id as a bad set of fields, since it would result
in keys that are all unique, and therefore results in sampling 100% of
traces. Using only the HTTP endpoint field would be a **bad** choice,
as it is not unique enough and therefore interesting traces, like
traces that experienced a `500`, might not be sampled. Field names may
come from any span in the trace; if they occur on multiple spans, all
unique values will be included in the key.


Not eligible for live reload.

Type: `stringarray`




### MaxKeys

MaxKeys is the maximum number of keys to track.

MaxKeys limits the number of distinct keys tracked by the sampler.
Once MaxKeys is reached, new keys will not be included in the sample
rate map, but existing keys will continue to be be counted. You can
use this to keep the sample rate map size under control. Defaults to
500; dynamic samplers will rarely achieve their goals with more keys
than this.


Not eligible for live reload.

Type: `int`




### UseTraceLength

UseTraceLength indicates whether to include the trace length as part
of the key.

Indicates whether to include the trace length (number of spans in the
trace) as part of the key. The number of spans is exact, so if there
are normally small variations in trace length you may want to leave
this off. If traces are consistent lengths and changes in trace length
is a useful indicator of traces you'd like to see in Honeycomb, set
this to true.


Not eligible for live reload.

Type: `bool`




---
## EMADynamicSampler: EMA Dynamic Sampler

The Exponential Moving Average (EMA) Dynamic Sampler attempts to
average a given sample rate, weighting rare traffic and frequent
traffic differently so as to end up with the correct average.
EMADynamicSampler is an improvement upon the simple DynamicSampler and
is recommended for many use cases. Based on the DynamicSampler,
EMADynamicSampler differs in that rather than compute rate based on a
periodic sample of traffic, it maintains an Exponential Moving Average
of counts seen per key, and adjusts this average at regular intervals.
The weight applied to more recent intervals is defined by `weight`, a
number between (0, 1) - larger values weight the average more toward
recent observations. In other words, a larger weight will cause sample
rates more quickly adapt to traffic patterns, while a smaller weight
will result in sample rates that are less sensitive to bursts or drops
in traffic and thus more consistent over time.
Keys that are not already present in the EMA will always have a sample
rate of 1. Keys that occur more frequently will be sampled on a
logarithmic curve. Every key will be represented at least once in any
given window and more frequent keys will have their sample rate
increased proportionally to trend towards the goal sample rate.


### GoalSampleRate

GoalSampleRate is the sample rate to use.

The sample rate to use. It indicates a ratio, where one sample trace
is kept for every N traces seen. For example, a SampleRate of 30 will
keep 1 out of every 30 traces. The choice on whether to keep any
specific trace is random, so the rate is approximate.
The sample rate is calculated from the trace ID, so all spans with the
same trace ID will be sampled or not sampled together.


Not eligible for live reload.

Type: `int`




### AdjustmentInterval

AdjustmentInterval is how often the sampler will recalculate the
sample rate.

The duration after which the EMA dynamic sampler should recalculate
its internal counters. It should be specified as a duration string,
e.g. "30s" or "1m".


Not eligible for live reload.

Type: `duration`




### Weight

Weight is the weight to use when calculating the EMA.

The weight to use when calculating the EMA. It should be a number
between 0 and 1. Larger values weight the average more toward recent
observations. In other words, a larger weight will cause sample rates
more quickly adapt to traffic patterns, while a smaller weight will
result in sample rates that are less sensitive to bursts or drops in
traffic and thus more consistent over time.


Not eligible for live reload.

Type: `float`




### AgeOutValue

AgeOutValue is the value to use when aging out keys.

AgeOutValue indicates the threshold for removing keys from the EMA.
The EMA of any key will approach 0 if it is not repeatedly observed,
but will never truly reach it, so we have to decide what constitutes
"zero". Keys with averages below this threshold will be removed from
the EMA. Default is the same as Weight, as this prevents a key with
the smallest integer value (1) from being aged out immediately. This
value should generally be <= Weight, unless you have very specific
reasons to set it higher.


Not eligible for live reload.

Type: `float`




### BurstMultiple

BurstMultiple is the multiple of the sample rate to use when detecting
bursts.

BurstMultiple, if set, is multiplied by the sum of the running average
of counts to define the burst detection threshold. If total counts
observed for a given interval exceed this threshold, EMA is updated
immediately, rather than waiting on the AdjustmentInterval. Defaults
to 2; a negative value disables. With the default of 2, if your
traffic suddenly doubles, burst detection will kick in.


Not eligible for live reload.

Type: `float`




### BurstDetectionDelay

BurstDetectionDelay the number of AdjustmentIntervals to wait before
detecting bursts.

Indicates the number of intervals to run before burst detection kicks
in. Defaults to 3.


Not eligible for live reload.

Type: `int`




### FieldList

FieldList is the list of fields to use to create the key for the
dynamic sampler.

FieldList is a list of all the field names to use to form the key that
will be handed to the dynamic sampler. The combination of values from
all of these fields should reflect how interesting the trace is
compared to another. A good field selection has consistent values for
high-frequency, boring traffic, and unique values for outliers and
interesting traffic. Including an error field (or something like HTTP
status code) is an excellent choice. Using fields with very high
cardinality (like `k8s.pod.id`), is a bad choice. If the combination
of fields essentially makes them unique, the dynamic sampler will
sample everything. If the combination of fields is not unique enough,
you will not be guaranteed samples of the most interesting traces. As
an example, consider a combination of HTTP endpoint (high-frequency
and boring), HTTP method, and status code (normally boring but can
become interesting when indicating an error) as a good set of fields
since it will allowing proper sampling of all endpoints under normal
traffic and call out when there is failing traffic to any endpoint.
For example, in contrast, consider a combination of HTTP endpoint,
status code, and pod id as a bad set of fields, since it would result
in keys that are all unique, and therefore results in sampling 100% of
traces. Using only the HTTP endpoint field would be a **bad** choice,
as it is not unique enough and therefore interesting traces, like
traces that experienced a `500`, might not be sampled. Field names may
come from any span in the trace; if they occur on multiple spans, all
unique values will be included in the key.


Not eligible for live reload.

Type: `stringarray`




### MaxKeys

MaxKeys is the maximum number of keys to track.

MaxKeys limits the number of distinct keys tracked by the sampler.
Once MaxKeys is reached, new keys will not be included in the sample
rate map, but existing keys will continue to be be counted. You can
use this to keep the sample rate map size under control. Defaults to
500; dynamic samplers will rarely achieve their goals with more keys
than this.


Not eligible for live reload.

Type: `int`




### UseTraceLength

UseTraceLength indicates whether to include the trace length as part
of the key.

Indicates whether to include the trace length (number of spans in the
trace) as part of the key. The number of spans is exact, so if there
are normally small variations in trace length you may want to leave
this off. If traces are consistent lengths and changes in trace length
is a useful indicator of traces you'd like to see in Honeycomb, set
this to true.


Not eligible for live reload.

Type: `bool`




---
## EMAThroughputSampler: EMA Throughput Sampler

The Exponential Moving Average (EMA) Throughput Sampler attempts to
achieve a given throughput -- number of spans per second -- weighting
rare traffic and frequent traffic differently so as to end up with the
correct rate.
The EMAThroughputSampler is an improvement upon the TotalThroughput
Sampler and is recommended for most throughput-based use cases,
because it like the EMADynamicSampler, it maintains an Exponential
Moving Average of counts seen per key, and adjusts this average at
regular intervals. The weight applied to more recent intervals is
defined by `weight`, a number between (0, 1) - larger values weight
the average more toward recent observations. In other words, a larger
weight will cause sample rates more quickly adapt to traffic patterns,
while a smaller weight will result in sample rates that are less
sensitive to bursts or drops in traffic and thus more consistent over
time.
New keys that are not already present in the EMA will always have a
sample rate of 1. Keys that occur more frequently will be sampled on a
logarithmic curve. Every key will be represented at least once in any
given window and more frequent keys will have their sample rate
increased proportionally to trend towards the goal throughput.


### GoalThroughputPerSec

GoalThroughputPerSec is the desired throughput per second of events
sent to Honeycomb.

The desired throughput **per second**. This is the number of events
per second you want to send to Honeycomb. The sampler will adjust
sample rates to try to achieve this desired throughput. This value is
calculated for the individual instance, not for the cluster; if your
cluster has multiple instances, you will need to divide your total
desired sample rate by the number of instances to get this value.


Not eligible for live reload.

Type: `int`




### InitialSampleRate

InitialSampleRate is the sample rate to use.

InitialSampleRate is the sample rate to use during startup, before the
sampler has accumulated enough data to calculate a reasonable
throughput. This is mainly useful in situations where unsampled
throughput is high enough to cause problems.


Not eligible for live reload.

Type: `int`




### AdjustmentInterval

AdjustmentInterval is how often the sampler will recalculate the
sample rate.

The duration after which the EMAThroughput sampler should recalculate
its internal counters. It should be specified as a duration string,
e.g. "30s" or "1m".


Not eligible for live reload.

Type: `duration`




### Weight

Weight is the weight to use when calculating the EMA.

The weight to use when calculating the EMA. It should be a number
between 0 and 1. Larger values weight the average more toward recent
observations. A larger weight will cause sample rates more quickly
adapt to traffic patterns, while a smaller weight will result in
sample rates that are less sensitive to bursts or drops in traffic and
thus more consistent over time.


Not eligible for live reload.

Type: `float`




### AgeOutValue

AgeOutValue is the value to use when aging out keys.

AgeOutValue indicates the threshold for removing keys from the EMA.
The EMA of any key will approach 0 if it is not repeatedly observed,
but will never truly reach it, so we have to decide what constitutes
"zero". Keys with averages below this threshold will be removed from
the EMA. Default is the same as Weight, as this prevents a key with
the smallest integer value (1) from being aged out immediately. This
value should generally be <= Weight, unless you have very specific
reasons to set it higher.


Not eligible for live reload.

Type: `float`




### BurstMultiple

BurstMultiple is the multiple of the sample rate to use when detecting
bursts.

BurstMultiple, if set, is multiplied by the sum of the running average
of counts to define the burst detection threshold. If total counts
observed for a given interval exceed this threshold, EMA is updated
immediately, rather than waiting on the AdjustmentInterval. Defaults
to 2; a negative value disables. With the default of 2, if your
traffic suddenly doubles, burst detection will kick in.


Not eligible for live reload.

Type: `float`




### BurstDetectionDelay

BurstDetectionDelay the number of AdjustmentIntervals to wait before
detecting bursts.

Indicates the number of intervals to run before burst detection kicks
in. Defaults to 3.


Not eligible for live reload.

Type: `int`




### FieldList

FieldList is the list of fields to use to create the key for the
dynamic sampler.

FieldList is a list of all the field names to use to form the key that
will be handed to the dynamic sampler. The combination of values from
all of these fields should reflect how interesting the trace is
compared to another. A good field selection has consistent values for
high-frequency, boring traffic, and unique values for outliers and
interesting traffic. Including an error field (or something like HTTP
status code) is an excellent choice. Using fields with very high
cardinality (like `k8s.pod.id`), is a bad choice. If the combination
of fields essentially makes them unique, the dynamic sampler will
sample everything. If the combination of fields is not unique enough,
you will not be guaranteed samples of the most interesting traces. As
an example, consider a combination of HTTP endpoint (high-frequency
and boring), HTTP method, and status code (normally boring but can
become interesting when indicating an error) as a good set of fields
since it will allowing proper sampling of all endpoints under normal
traffic and call out when there is failing traffic to any endpoint.
For example, in contrast, consider a combination of HTTP endpoint,
status code, and pod id as a bad set of fields, since it would result
in keys that are all unique, and therefore results in sampling 100% of
traces. Using only the HTTP endpoint field would be a **bad** choice,
as it is not unique enough and therefore interesting traces, like
traces that experienced a `500`, might not be sampled. Field names may
come from any span in the trace; if they occur on multiple spans, all
unique values will be included in the key.


Not eligible for live reload.

Type: `stringarray`




### MaxKeys

MaxKeys is the maximum number of keys to track.

MaxKeys limits the number of distinct keys tracked by the sampler.
Once MaxKeys is reached, new keys will not be included in the sample
rate map, but existing keys will continue to be be counted. You can
use this to keep the sample rate map size under control. Defaults to
500; dynamic samplers will rarely achieve their goals with more keys
than this.


Not eligible for live reload.

Type: `int`




### UseTraceLength

UseTraceLength indicates whether to include the trace length as part
of the key.

Indicates whether to include the trace length (number of spans in the
trace) as part of the key. The number of spans is exact, so if there
are normally small variations in trace length you may want to leave
this off. If traces are consistent lengths and changes in trace length
is a useful indicator of traces you'd like to see in Honeycomb, set
this to true.


Not eligible for live reload.

Type: `bool`




---
## TotalThroughputSampler: Total Throughput Sampler

TotalThroughput attempts to meet a goal of a fixed number of events
per second sent to Honeycomb.
If your key space is sharded across different servers, this is a good
method for making sure each server sends roughly the same volume of
content to Honeycomb. It performs poorly when the active keyspace is
very large.
GoalThroughputPerSec * ClearFrequencyDuration (in seconds) defines the
upper limit of the number of keys that can be reported and stay under
the goal, but with that many keys, you'll only get one event per key
per ClearFrequencySec, which is very coarse. You should aim for at
least 1 event per key per sec to 1 event per key per 10sec to get
reasonable data. In other words, the number of active keys should be
less than 10*GoalThroughputPerSec.


### GoalThroughputPerSec

GoalThroughputPerSec is the desired throughput per second of events
sent to Honeycomb.

The desired throughput per second of events sent to Honeycomb. This is
the number of events per second you want to send. This is not the same
as the sample rate.


Not eligible for live reload.

Type: `int`




### ClearFrequency

ClearFrequency is the duration over which the sampler will calculate
the throughput.

The duration after which the dynamic sampler should reset its internal
counters. It should be specified as a duration string, e.g. "30s" or
"1m".


Not eligible for live reload.

Type: `duration`




### FieldList

FieldList is the list of fields to use to create the key for the
dynamic sampler.

FieldList is a list of all the field names to use to form the key that
will be handed to the dynamic sampler. The combination of values from
all of these fields should reflect how interesting the trace is
compared to another. A good field selection has consistent values for
high-frequency, boring traffic, and unique values for outliers and
interesting traffic. Including an error field (or something like HTTP
status code) is an excellent choice. Using fields with very high
cardinality (like `k8s.pod.id`), is a bad choice. If the combination
of fields essentially makes them unique, the dynamic sampler will
sample everything. If the combination of fields is not unique enough,
you will not be guaranteed samples of the most interesting traces. As
an example, consider a combination of HTTP endpoint (high-frequency
and boring), HTTP method, and status code (normally boring but can
become interesting when indicating an error) as a good set of fields
since it will allowing proper sampling of all endpoints under normal
traffic and call out when there is failing traffic to any endpoint.
For example, in contrast, consider a combination of HTTP endpoint,
status code, and pod id as a bad set of fields, since it would result
in keys that are all unique, and therefore results in sampling 100% of
traces. Using only the HTTP endpoint field would be a **bad** choice,
as it is not unique enough and therefore interesting traces, like
traces that experienced a `500`, might not be sampled. Field names may
come from any span in the trace; if they occur on multiple spans, all
unique values will be included in the key.


Not eligible for live reload.

Type: `stringarray`




### MaxKeys

MaxKeys is the maximum number of keys to track.

MaxKeys limits the number of distinct keys tracked by the sampler.
Once MaxKeys is reached, new keys will not be included in the sample
rate map, but existing keys will continue to be be counted. You can
use this to keep the sample rate map size under control. Defaults to
500; dynamic samplers will rarely achieve their goals with more keys
than this.


Not eligible for live reload.

Type: `int`




### UseTraceLength

UseTraceLength indicates whether to include the trace length as part
of the key.

Indicates whether to include the trace length (number of spans in the
trace) as part of the key. The number of spans is exact, so if there
are normally small variations in trace length you may want to leave
this off. If traces are consistent lengths and changes in trace length
is a useful indicator of traces you'd like to see in Honeycomb, set
this to true.


Not eligible for live reload.

Type: `bool`




---
## RulesBasedSampler: Rules-based Sampler

The Rules-based sampler allows you to specify a set of rules that will
determine whether a trace should be sampled or not. Rules are
evaluated in order, and the first rule that matches will be used to
determine the sample rate. If no rules match, the SampleRate will be 1
(i.e. all traces will be kept).
Rules-based samplers will usually be configured to have the last rule
be a default rule with no conditions that uses a downstream dynamic
sampler to keep overall sample rate under control.


### Rules

Rules is the list of rules to use.

Rules is a list of rules to use to determine the sample rate.


Not eligible for live reload.

Type: `objectarray`




### CheckNestedFields

CheckNestedFields indicates whether to expand nested JSON when
evaluating rules.

Indicates whether to expand nested JSON when evaluating rules. If
false, nested JSON will be treated as a string. If true, nested JSON
will be expanded into a map[string]interface{} and the value of the
field will be the value of the nested field. For example, if you have
a field called `http.request.headers` and you want to check the value
of the `User-Agent` header, you would set this to true and use
`http.request.headers.User-Agent` as the field name in your rule. This
is a computationally expensive option and may cause performance
problems if you have a large number of spans with nested JSON.


Not eligible for live reload.

Type: `bool`




---
## Rules: Rules for Rules-based Samplers

Rules are evaluated in order, and the first rule that matches will be
used to determine the sample rate. If no rules match, the SampleRate
will be 1 (i.e. all traces will be kept).
If a rule matches, one of three things happens, and they are evaluated
in this order: a) if the rule specifies a downstream Sampler, that
sampler is used to determine the sample rate; b) if the rule has the
Drop flag set to true, the trace is dropped; c) the rule's sample rate
is used.


### Name

Name is the name of the rule.

The name of the rule. This is used for debugging and will appear in
the trace metadata if AddRuleReasonToTrace is set to true.


Not eligible for live reload.

Type: `string`




### Sampler

Sampler is the dynamic sampler to use if the rule matches.

The sampler to use if the rule matches. If this is set, the sample
rate will be determined by the downstream sampler. If this is not set,
the sample rate will be determined by the Drop flag or the SampleRate
field.


Not eligible for live reload.

Type: `object`




### Drop

Drop indicates whether to drop the trace.

Indicates whether to drop the trace if it matches this rule. If true,
the trace will be dropped. If false, the trace will be kept.


Not eligible for live reload.

Type: `bool`




### SampleRate

SampleRate is the sample rate to use.

If the rule is matched, there is no Sampler specified, and the Drop
flag is false, then this is the sample rate to use.


Not eligible for live reload.

Type: `int`




### Conditions

Conditions is the list of conditions to use to determine whether the
rule matches.

Conditions is a list of conditions to use to determine whether the
rule matches. All conditions must be met for the rule to match. If
there are no conditions, the rule will always match (this is typically
done for the last rule to provide a default behavior).


Not eligible for live reload.

Type: `objectarray`




### Scope

Scope controls the scope of the rule.

Controls the scope of the rule evaluation. If set to "trace" (the
default), each condition can apply to any span in the trace
independently. If set to "span", all of the conditions in the rule
will be evaluated against each span in the trace and the rule only
succeeds if all of the conditions match on a single span together.


Not eligible for live reload.

Type: `string`




---
## Conditions: Conditions for the Rules in Rules-based Samplers

Conditions are evaluated in order, and the first condition that does
not match will cause the rule to not match. If all conditions match,
the rule will match. If there are no conditions, the rule will always
match.


### Field

Field is the field to check.

The field to check. This can be any field in the trace. If the field
is not present, the condition will not match. The comparison is
case-sensitive.


Not eligible for live reload.

Type: `string`




### Operator

Operator is the comparison operator to use.

The comparison operator to use. String comparisons are case-sensitive.


Not eligible for live reload.

Type: `string`




### Value

Value is the value to compare against.

The value to compare against. If Datatype is not specified, then the
value and the field will be compared based on the type of the field.


Not eligible for live reload.

Type: `anyscalar`




### Datatype

Datatype is the datatype to use when comparing the value and the
field.

The datatype to use when comparing the value and the field. If
Datatype is specified, then both values will be converted
(best-effort) to that type and compared. Errors in conversion will
result in the comparison evaluating to false. This is especially
useful when a field like http status code may be rendered as strings
by some environments and as numbers or booleans by others.


Not eligible for live reload.

Type: `string`




---
## Samplers: 

Samplers is a mapping of targets to samplers. Each target is a
Honeycomb environment (or, for classic keys, a dataset). The value is
the sampler to use for that target. The target called __default__ will
be used for any target that is not explicitly listed. A __default__
target is required.



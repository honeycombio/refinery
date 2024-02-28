# Release Notes

While [CHANGELOG.md](./CHANGELOG.md) contains detailed documentation and links to all the source code changes in a given release, this document is intended to be aimed at a more comprehensible version of the contents of the release from the point of view of users of Refinery.

## Version 2.4.2

This is a bug fix release for returning a improperly formatted OTLP error responses.
OTLP clients receiving the improper response would show errors about parsing the response, masking the error message within the response which complicated solving data send issues.
This release is a recommended upgrade for anyone sending OTLP data to Refinery.

## Version 2.4.1

This is a bug fix release for matching fields in the root span context.

The implementation in v2.4.0 can crash if the trace's root span is not present at the time a sampling decision is being made.
Root spans are often not present when the root span is taking longer to complete than the time configured for Refinery to wait for a trace's spans to arrive (`TraceTimeout`).
This release contains a fix for this crash and is a recommended upgrade for anyone using this new feature.

## Version 2.4.0

This release includes an update to allow users to specify root span context in their rules. It also includes some bug
fixes, improvements, and dependency updates.

### Root Span Context

Users can now specify rules that match only the root span of a trace (i.e. `root.http.status`).

### Notable Fixes
* Previously, rules with a default of boolean `true` that we set to `false` by configuration would be overridden back to `true` when defaults were applied to the config. We have fixed this by using the `*bool` type for these values as well as adding helper functions to avoid strange behavior related to how booleans work in Go.

## Version 2.3.0

This release is mainly focused on some improvements to rules and bug fixes. It is recommended for all Refinery users.

### Rules Improvements

Users of Rules-based samplers have several new features with this release:

* A new `matches` operator can match the contents of fields using a regular expression. The regular expression language supported is the one used by the Go programming language. This will enable certain advanced rules that were previously impossible to achieve. It should be noted that complex regular expressions can be significantly slower than normal comparisons, so use this feature with the appropriate level of caution.
* A new `Fields` parameter may be used in conditions instead of `Field`. This parameter takes a list of field names, and evaluates the rule based on the first name that matches an existing field. This is intended to be used mainly when telemetry field names are changing, to avoid having to create duplicated rules.
* Refinery now supports a "virtual" `Field` called `?.NUM_DESCENDANTS`. This field is evaluated as the current number of descendants in a trace, even if the root span has not arrived. This permits a rule that correctly evaluates the number of spans in a trace even if the trace is exceptionally long-lived. This sort of rule can be used to drop exceptionally large traces to avoid sending them to Honeycomb.
* There is a [new documentation page](rules_conditions.md) in this repository containing detailed information on constructing rule conditions.

### Other Notable Changes

* Previously, spans that arrived after the trace decision had been made were simply marked with `meta.refinery.reason: late`. Now, refinery will remember and attache the reason used when the span decision was made.
* MemorySize parameters in config can now accept a floating point value like `2.5Gb`, which is more compatible with values used in Kubernetes. This should help eliminate bugs in Helm charts.
* OTLP requests to `/v1/traces/` will now be accepted along with `/v1/traces`, which eliminates a minor annoyance when configuring upstream senders.
* There were many improvements to testing and documentation that should improve quality of life for contributors.

## Version 2.2.0

This is a minor release with several new features and bug fixes mostly around config values. This release is recommended for all Refinery users.

### Configuration
- `HTTPIdleTimeout` allows users to configure Refinery's http server's idle timeout.
- New GRPC config values that control the max size of send and receive blocks.
- New config values that provide separate control of the peer and incoming span queue.
- `AddCountsToRoot`, used instead of `AddSpanCountToRoot`, reports 4 separate values on a trace: child spans, span events, span links, and total child elements.
- Redis now supports Auth string for connection.

### Notable fixes
- The default stdout logger now supports sampling, so that in error loop situations, the number of log messages is constrained.
- Config values that need a memory size input can now use floating point values, which should mean that Refinery's parsing of memory size is compatible with that of Kubernetes, making helm charts easier to write.
- Documentation was improved.

See [the Changelog](./CHANGELOG.md) for the full list of changes.
## Version 2.1.0

This is a minor release with several new features and bug fixes, and is recommended for all Refinery users.

### Notable features
- The Throughput samplers now include an [optional configuration parameter](./refinery_rules.md#useclustersize-2) called `UseClusterSize` -- when set to `true`, this causes the sampler to treat `GoalThroughputPerSec` as a target for the cluster as a whole, meaning that if the number of instances changes, the configuration doesn't have to change.
- When using `AddRuleReasonToTrace`, Refinery now also [records an additional](./refinery_config.md#addrulereasontotrace) field, `meta.refinery.send_reason`, that indicates the event which caused refinery to evaluate the trace.
- There is a new rules operator, `has-root-span`, which returns a boolean value indicating if the trace being evaluated has a root span. The most likely use case is for dropping potentially large long-lived traces.
- Refinery builds now include binaries for OS/X using arm64 (Apple's M series CPUs)

### Notable fixes
- The default value for `ConfigReloadInterval` has been lowered to 15s and [its documentation](./refinery_config.md#configreloadinterval) was fixed; it had been incorrectly documented with a feature that was not implemented.
- OtelMetrics now includes some metrics that were inadvertently omitted from previous releases, like `num_goroutines` and `host.name`.
- LiveReload now works without deadlocking Refinery.
- Documentation was improved.

See [the Changelog](./CHANGELOG.md) for the full list of changes.

## Version 2.0.2

This is a patch release to address additional issues with Refinery 2.0:

- Fixes a performance issue where cluster membership was being queried from Redis with an unreasonably small limit, requiring many round trips and causing occasional timeouts in very large clusters.
- Fixes a situation where OTel metrics was inappropriately initialized when not configured, causing many errors to be logged as it tried to communicate with Honeycomb.

## Version 2.0.1

This is a patch release of Refinery that fixes several small, but annoying, bugs from Refinery 2.0.0.

### Configuration
- `AvailableMemory` and `MaxAlloc` now also support Kubernetes units.
- Configuration validation will now pass if an expected value is being set by an environment variable.
- `GRPCServerParameters.Enabled` is not properly used by Refinery.
- `debug` log level now works properly.

### Refinery Logs
- StressRelief logs will now log on log level `warn`.

### Conversion Tool
- Now properly treats the default value for `PeerManagement.Type` as `file` instead of `redis`.
- Now properly converts nested samplers within any RulesBasesSampler configurations.
- Now properly converts `APIKeys`.
- Now properly converts `InMemCollector.MaxAlloc`.

## Version 2.0.0

This is a major release of Refinery with a new configuration file format, new samplers, and updated runtime behavior. It has several breaking changes, especially for configuration and sampling, so it also comes with [a conversion tool](https://github.com/honeycombio/refinery/tree/main/tools/convert).

### Configuration File Changes

The configuration and sampler file formats have been completely redesigned. Key changes:

- The preferred file format is now YAML instead of TOML. All examples and documentation now use YAML format (the old *_complete.toml files now have 1.x in their names).
- A conversion tool has been provided to convert a v1.x configuration file to the new format.
- The config file is now organized into sections for clarity.
- Sampler rules now require default target named `__default__` to specify the default sampling behavior.
- Many default values have changed to be more useful.
- Configurations are now fully validated - misspellings, type errors, faulty indentions, and extra values are now detected. Refinery will no longer run if your configurations are invalid.
- Documentation for configuration is now automatically generated so that it will stay in sync with the source. This means that the [Honeycomb docs](https://docs.honeycomb.io/manage-data-volume/refinery/) stay in sync and you can now generate local copies using the [conversion tool](https://github.com/honeycombio/refinery/tree/main/tools/convert).


Specific configuration changes worth noting:

- There is now a required configuration version field; this is to permit future configuration format changes without breaking existing configurations again.
- All duration values like timeouts, tickers, and delays are now specified as durations like `5s`, `1m30s`, or `100ms`.
- Memory sizes can now have a standard suffix like `MiB` or `GB`.
- Instead of calculating a maximum memory usage value, it is now possible to specify `AvailableMemory` as the total memory available and `MaxMemoryPercentage` as the max percentage of `AvailableMemory` that Refinery can use.
- Some legacy operational controls relating to caching and memory management have been removed.
- Refinery can be run with a validation command-line switch (`-V`), which can be used to validate configuration (typically used for CI/CD).
- StressRelief activation level defaults have been changed to higher values.
- Log levels have been cleaned up so that they're more useful. The `info` level is less noisy and `warn` is now a valid option.
- Config file location can also be a URL instead of a file path. The contents are retrieved with a simple GET request.
- Config file contents are periodically refetched at the rate specified by `ConfigReloadInterval`. An immediate reload can be forced by sending the `SIGUSR1` signal.

### Sampler Changes

- All dynamic samplers now correctly count spans, not traces. Although they were documented as counting the number of spans in previous versions, they were in fact only counting traces, which often made it difficult to achieve appropriate target rates. **After running the conversion tool, existing configurations should be adjusted!**
- New Samplers: The `WindowedThroughputSampler` and `EMAThroughputSampler` are two new samplers. We highly recommend replacing any use of `TotalThroughputSampler` in favor of one of these. Both use dynamic sampling techniques to adjust sample rates to achieve a desired throughput. The `WindowedThroughputSampler` does so with a moving window of samples, while the `EMAThroughputSampler` maintains a moving average.
- Individual samplers now report metrics relating to key size and the number of spans and traces processed.
- Samplers now report metrics using their own prefix identifier instead of all using `dynamic_`. If you depended on this metric but are not using the Dynamic Sampler you will need to update your queries.
  - Deterministic Sampler uses `deterministic_`
  - Dynamic Sampler uses `dynamic_`
  - Dynamic EMA Sampler uses `emadynamic_`
  - EMA Throughput Sampler uses `emathroughput_`
  - Rules Sampler uses `rulesbased_`
  - Total Throughput Sampler uses `totalthroughput_`
  - Windowed Throughput Sampler uses `windowedthroughput_`
- Samplers now always have a bounded `MaxKeys` value, which defaults to `500`. Systems relying on a larger keyspace should set this value explicitly for a sampler.

### Refinery Metrics Updates

- Sending metrics with OpenTelemetry is now supported, and preferred over now legacy Honeycomb metrics.
- Refinery's metrics can now be sent to more than one destination (for example, both Prometheus and OpenTelemetry).

### Notable Bug Fixes

- Dynamic samplers now count spans, not traces (see above).
- The APIKeys list now applies to OTLP traffic as well as Honeycomb events.
- Stress Relief Mode is now more stable and effective.
- Cache overruns should occur much less often and are now a reliable indication that the cache is undersized.

### Conversion Tool

- There is now a `convert` tool included with Refinery -- binary builds are available as part of the release.
- `convert config` can read v1 config files and convert them to v2 YAML files, adding detailed comments.
- `convert rules` can read v1 rules files and convert them to v2 YAML files.
- `convert helm` can read a helm chart with `config` and `rules` sections and write out a revised, updated helm chart.
- `convert validate config` can validate a v2 config file.
- `convert validate rules` can validate a v2 rules file.

## Version 1.21.0

This is a small release with mostly bug fixes and minor changes related to Stress Relief Mode.

### Fixes for Stress Relief

This update includes many small changes geared at making Stress Relief Mode work better.
- Hostname is now annotated when Stress Relief is active.
- Stress Relief Mode can now only be activated when CacheOverrunStrategy is set to "impact" since it is not compatible with "legacy".
- The `Stop()` function was removed from Stress Relief Mode; it wasn't needed and was causing confusing errors.
- A fix for a very serious performance issue with the drop cache that could cause Refinery to get stuck in Stress Relief indefinitely.

### General Bug Fixes

There were other small changes to other parts of refinery.
- Systemd `Alias=` directive was replaced with `WantedBy=` directive which is more in line with best practices.
- Late spans are now only decorated when `AddRuleReasonToTrace` is set.
- Some potential fixes for flaky tests were added.

## Version 1.20.0

This is a significant new release of Refinery, with several features designed to help when operating Refinery at scale:

### Stress Relief

It has been hard to operate Refinery efficiently at scale. Because of the way it works, it can quickly transition into instability during a spike in traffic, and it has been hard to decide what to change to keep it stable.

In v1.20, a "Stress Relief" system has been added. When properly configured, it tracks refinery's load, and if it gets in danger of instability, switches into a high-performance load-shedding mode designed to relieve stress on the system.

When Stress Relief is Activated, Refinery stops collecting and distributing traces for evaluation after the trace is complete. Instead, it samples spans deterministically based on the TraceID and immediately forwards (or drops) them without further evaluation. It will continue doing so until the load subsides.

It also indicates in the logs which of its configuration values is most under stress, which should help tune it.

Stress Relief is controlled by the [StressRelief](https://github.com/honeycombio/refinery/blob/main/config_complete.toml#L512) section of the configuration.

Stress Relief generally operates by comparing specific metrics for memory and queue sizes to their configured maximum values. Each metric is treated differently, but in general the heuristic is an attempt to detect problems as they are about to happen rather than waiting for them to be in crisis. The new `stress_level` metric will show the results of this calculation on a scale from 0 to 100, and Stress Relief determines its activation by this metric.

The Stress Relief `Mode` can be set to:
- `never` -- It will never activate -- this is the default.
- `always` -- It is always active -- useful for testing or in an emergency
- `monitor` -- Refinery monitors its own status and adjusts its activity according to the `ActivationLevel` and `DeactivationLevel`.

The `ActivationLevel` and `DeactivationLevel` values control when the stress relief system turns on or off.

When Stress Relief activates, then its logs will indicate the activation along with the name of the particular configuration value that could be adjusted to reduce future stress.

Stress Relief currently monitors these metrics:
- `collector_peer_queue_length`
- `collector_incoming_queue_length`
- `libhoney_peer_queue_length`
- `libhoney_upstream_queue_length`
- `memory_heap_allocation`

### New Metrics

Some new metrics have been added and the internal metrics systems have been unified and made easier to update. This is to support the stress relief system (see above) and future plans.

New metrics include:
- `stress_level` -- a gauge from 0 to 100
- `stress_relief_activated` -- a gauge at 0 or 1

### New "Datatype" parameter in rule conditions

- An additional field may be specified in refinery's rule conditions -- if `Datatype` is specified (must be one of "bool", "int", "float", or "string") both the field and the comparison value are converted to that datatype before the comparison. This allows a single rule to handle multiple datatypes. Probably the best example is `http.status` which is sometimes a string and sometimes an integer, depending on the programming environment.

Example:
```toml
		[[myworld.rule.condition]]
			field = "status_code"
			operator = ">"
			value = "400"
			datatype = "int"
```

### Configurable Trace and Parent IDs

The names that Refinery uses for traceID and parentID are now configurable.

Se `TraceIdFieldNames` and `ParentIdFieldNames` in the [configuration](https://github.com/honeycombio/refinery/blob/main/config_complete.toml#L160) to the list of field names you prefer. The default values are those that Refinery has used to date: `trace.trace_id`, `trace.parent_id`, `traceId`, and `parentId`.

### Inject specific constant values to telemetry

`AdditionalAttributes` in config is a map that can be used to inject specific user-defined attributes, such as a cluster ID. These attributes will be added to all spans that are sent to Honeycomb. Both keys and values must be strings.

Example:
```toml
[[AdditionalAttributes]]
	ClusterName="MyCluster"
```

### Trace Decision Caching lasts longer

Refinery keeps a record of its trace decisions -- whether it kept or dropped a given trace. Past versions of refinery had a trace decision cache that was fixed to 5x the size of the trace cache. In v1.20, Refinery has a new cache strategy (called "cuckoo") that separates drop decisions (where all it needs to remember is that the trace was dropped) from kept decisions (where it also tracks some metadata about the trace, such as the number of spans in the trace). It can now cache millions of drop decisions, and many thousands of kept decisions, which should help ensure trace integrity for users with long-lived traces.

It is controlled by the [SampleCacheConfig](https://github.com/honeycombio/refinery/blob/main/config_complete.toml#L466) section of the config file.

To turn it on, set `Type = "cuckoo"`. For compatibility, it is disabled by default.

The defaults are reasonable for most configurations, but it has several options. To control it, set `KeptSize`, `DroppedSize`, and `SizeCheckInterval`. See the config for details.

### Late Span Metadata

If `AddRuleReasonToTrace` is specified, Refinery already adds metadata to spans indicating which Refinery rule caused the keep decision. In v1.20, spans arriving after the trace's sampling decision has already been made will have their `meta.refinery.reason` set to `late` before sending to Honeycomb. This should help in diagnosing trace timeout issues.

### Improved cluster operations

- When refinery shuts down, it will try to remove itself from the peers list, which should shorten the time of instability in the cluster.
- The algorithm controlling how traces are distributed to peers in the cluster has been revamped so that traces are much more likely to stay on the same peer during reconfiguration. In previous releases, a change in the peer count would affect roughly half of the traces in flight. With this new algorithm, only 1/N (where N is the number of peers) will be affected.Set the [Peer Management](https://github.com/honeycombio/refinery/blob/main/config_complete.toml#L183) `Strategy` to `hash` to enable it.
- More Redis configuration is available to make it possible for multiple deployments to share a single Redis instance. Adjust the `RedisPrefix` and `RedisDatabase` parameters in the `PeerManagement` section of the config.

### Dry Run works better

- Dry Run mode no longer sets the Sample Rate, which means that Honeycomb queries will still be accurate in this mode. Instead, it sets `meta.dryrun.sample_rate` to the calculated sample rate.



# Release Notes

While [CHANGELOG.md](./CHANGELOG.md) contains detailed documentation and links to all the source code changes in a given release, this document is intended to be aimed at a more comprehensible version of the contents of the release from the point of view of users of Refinery.

## Version 2.8.0

This release has a several significant changes that makes Refinery easier to operate at scale.

### Draining during Shutdown

When one Refinery in a cluster shuts down, it now:

* Immediately marks itself as "not ready" so that load balancers will stop sending it data
* Immediately removes itself from the peer list so that other Refineries will stop sending data
* Recalculates the appropriate destination for all the traces it has been tracking, and forwards those spans to the correct instance.

When other Refineries in a cluster see that the number of peers has changed, they:

* Check all the traces they have been tracking; for any that belong to a different Refinery, all the spans are forwarded to the new destination. (On average in an N-node cluster, 1/N of the existing traces will need to move.)

### Redis Cluster Support

Refinery now supports Redis instances deployed in Cluster Mode. There is a new configuration parameter, `ClusterHosts`, which can be used to enable this support.

This should make it easier to configure AWS ElastiCache for use with Refinery, since ElastiCache now uses Redis Cluster Mode by default.

In addition, Refinery now supports the use of TLS for communications with Redis.

### SpanLimit

Until this release, Refinery has marked a trace for trace decision when either:

* The root span arrives
* The TraceTimeout expires

Release 2.8 introduces a new feature, `SpanLimit`, which provides a third way to cause Refinery to make a trace decision. It sets the maximum number of descendants that a trace can have before it gets marked for a trace decision. This is to help with the memory consumption due to very large traces.

Suppose, for example, that a service generates a single trace with 10,000 spans. If SpanLimit is set to 1000, once the first 1000 spans have arrived, Refinery will immediately make a decision to keep or drop the trace. Every additional span is dispatched (using the same decision) without storing it. This means that Refinery never had to keep all 10,000 spans in its memory at one time.

For installations that sometimes see very large traces, this feature can have a significant impact on memory usage within a cluster, and can effectively prevent one Refinery in a cluster from running out of memory due to a big trace.

### `in` and `not-in` Operators in Rules

This release introduces `in` and `not-in` operators for rules. These operators allow the Value field to contain a list of values, and efficiently test for the presence or absence of a particular span field within that list.
A potential use for these operators would be to keep or drop traces originating from within a specific list of services.

### More flexible API key management with `SendKey` and `SendKeyMode`

This release allows an API key to be deployed alongside Refinery rather than with the sources of telemetry using the `SendKey` configuration.
The `SendKeyMode` value allows `SendKey` to be used (along with the existing `ReceiveKeys` value) in a variety of modes depending on the security requirements.

### Other Improvements

* Refinery rules now allow specifying `root.` prefixes for fields in dynamic samplers.
* The performance of the drop cache has been improved, which should help with stability for systems with a very high drop rate.
* The default maximum message sizes for OTLP have been increased from 5MB to 15MB.
* It is now possible to specify multiple config files, which can allow a layered approach to configuration (separating keys from other configuration, for example).


## Version 2.7.0

This release is a minor release focused on better cluster stability and data quality with a new system for communicating peer information across nodes.
As a result, clusters should generally behave more consistently.

Refinery 2.7 lays the groundwork for substantial future changes to Refinery.

### Publish/Subscribe on Redis
In this release, Redis is no longer a database for storing a list of peers.
Instead, it is used as a more general publish/subscribe framework for rapidly sharing information between nodes in the cluster.
Things that are shared with this connection are:

- Peer membership
- Stress levels
- News of configuration changes

Because of this mechanism, Refinery will now react more quickly to changes in any of these factors.
When one node detects a configuration change, all of its peers will be told about it immediately.

In addition, Refinery now publishes individual stress levels between peers.
Nodes calculate a cluster stress level as a weighted average (with nodes that are more stressed getting more weight).
If an individual node is stressed, it can enter stress relief individually.
This may happen, for example, when a single giant trace is concentrated on one node.
If the cluster as a whole is being stressed by a general burst in traffic, the entire cluster should now enter or leave stress relief at approximately the same time.

If your existing Redis instance is particularly small, you may find it necessary to increase its CPU or network allocations.

### Health checks now include both liveness and readiness

Refinery has always had only a liveness check on `/alive`, which always simply returned ok.

Starting with this release, Refinery now supports both `/alive` and `/ready`, which are based on internal status reporting.

The liveness check is alive whenever Refinery is awake and internal systems are functional.
It will return a failure if any of the monitored systems fail to report in time.

The readiness check returns ready whenever the monitored systems indicate readiness.
It will return a failure if any internal system returns not ready.
This is usually used to indicate to a load balancer that no new traffic should go to this node.
In this release, this will only happen when a Refinery node is shutting down.

### Metrics changes
There have also been some minor changes to metrics in this release:

We have two new metrics called `individual_stress_level` (the stress level as seen by a single node) and `cluster_stress_level` (the aggregated cluster level).
The `stress_level` metric indicates the maximum of the two values; it is this value which is used to determine whether an individual node activates stress relief.

There is also a new pair of metrics, `config_hash` and `rule_config_hash`.
These are numeric Gauge metrics that are set to the numeric value of the last 4 hex digits of the hash of the current config files.
These can be used to track that all refineries are using the same configuration file.

### Disabling Redis and using a static list of peers
Specifying `PeerManagement.Type=file` will cause Refinery to use the fixed list of peers found in the configuration.
This means that Refinery will operate without sharing changes to peers, stress, or configuration, as it has in previous releases.

### Config Change notifications
When deploying a cluster in Kubernetes, it is often the case that configurations are managed as a ConfigMap.
In the default setup, ConfigMaps are eventually consistent.
This may mean that one Refinery node will detect a configuration change and broadcast news of it, but a different node that receives the news will attempt to read the data and get the previous configuration.
In this situation, the change will still be detected by all Refineries within the `ConfigReloadInterval`.

## Version 2.6.1

This is a bug fix release.
In the log handling logic newly introduced in v2.6.0, Refinery would incorrectly consider log events to be root spans in a trace.
After this fix, log events can never be root spans.
This is recommended for everyone who wants to use the new log handling capabilities.

## Version 2.6.0

With this release, Refinery begins the process of integrating multiple telemetry signal types by handling logs as well as traces.
Refinery now handles the OpenTelemetry `/logs` endpoints over both gRPC and HTTP:
- Log records that are associated with a trace by including a TraceID are sampled alongside the trace's spans.
- Log records that do not have a TraceID are treated like regular events and forwarded directly to Honeycomb.

It also includes support for URL encoded dataset names in the non-OpenTelemetry URL paths.

## Version 2.5.2

This release fixes a race condition in OTel Metrics that caused Refinery to crash.
This update is recommended for everyone who has OTelMetrics enabled.

## Version 2.5.1

This is a bug fix release for a concurrent map read panic when loading items from the internal cache.
It also includes improvements for validation of ingest keys and resolves a lock issue during startup.

## Version 2.5

This release's main new feature adds support of Honeycomb Classic ingest keys.
There is also a performance improvement for the new `root.` rule feature, and a new metric to track traces dropped by rules.
This release is a recommended upgrade for anyone wishing to use ingest keys within a Honeycomb Classic environment.

## Version 2.4.3

A bug fix release for a regression introduced in the 2.4.2 bug fix release.
It was possible to trigger 500 errors in Refinery's OTLP error responses when sending traces in an unsupported content-type.
This release is a recommended upgrade for anyone sending OTLP data to Refinery.

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



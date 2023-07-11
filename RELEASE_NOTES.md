# Release Notes

While [CHANGELOG.md](./CHANGELOG.md) contains detailed documentation and links to all of the source code changes in a given release, this document is intended to be aimed at a more comprehensible version of the contents of the release from the point of view of users of Refinery.

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



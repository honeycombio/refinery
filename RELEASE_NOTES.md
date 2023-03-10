# Release Notes

While [CHANGELOG.md](./CHANGELOG.md) contains detailed documentation and links to all of the source code changes in a given release, this document is intended to be aimed at a more comprehensible version of the contents of the release from the point of view of users of Refinery.

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



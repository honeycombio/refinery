## Refinery Config File

The Refinery `config` file is a YAML file.
The file is split into sections; each section is a group of related configuration options.
Each section has a name, and the name is used to refer to the section in other parts of the config file.

## Example

This is an example `config` file:

```yaml
General:
  ConfigurationVersion: 2
Network:
  ListenAddr: "0.0.0.0:8080"
  PeerListenAddr: "0.0.0.0:8081"
OTelMetrics:
  Enabled: true
  APIKey: SetThisToAHoneycombKey
```

The remainder of this page describes the sections within the file and the fields in each.

## General Configuration

`General` contains general configuration options that apply to the entire Refinery process.

### `ConfigurationVersion`

`ConfigurationVersion` is the file format of this particular configuration file.

This file is version 2.
This field is required.
It exists to allow the configuration system to adapt to future changes in the configuration file format.

- Not eligible for live reload.
- Type: `int`
- Default: `2`

### `MinRefineryVersion`

`MinRefineryVersion` is the minimum version of Refinery that can load this configuration file.

This setting specifies the lowest Refinery version capable of loading all of the features used in this file.
If this value is present, then Refinery will refuse to start if its version is less than this setting.

- Not eligible for live reload.
- Type: `string`
- Default: `v2.0`

### `DatasetPrefix`

`DatasetPrefix` is a prefix that can be used to distinguish a dataset from an environment in the rules.

If telemetry is being sent to both a classic dataset and a new environment called the same thing, such as `production`, then this parameter can be used to distinguish these cases.
When Refinery receives telemetry using an API key associated with a Honeycomb Classic dataset, it will then use the prefix in the form `{prefix}.
{dataset}` when trying to resolve the rules definition.

- Not eligible for live reload.
- Type: `string`

### `ConfigReloadInterval`

`ConfigReloadInterval` is the average interval between attempts at reloading the configuration file.

Refinery will attempt to read its configuration and check for changes at approximately this interval.
This time is varied by a random amount up to 10% to avoid all instances refreshing together.
In installations where configuration changes are handled by restarting Refinery, which is often the case when using Kubernetes, disable this feature with a value of `0s`.
If the config file is being loaded from a URL, it may be wise to increase this value to avoid overloading the file server.

- Not eligible for live reload.
- Type: `duration`
- Default: `15s`

## Network Configuration

`Network` contains network configuration options.

### `ListenAddr`

`ListenAddr` is the address where Refinery listens for incoming requests.

This setting is the IP and port on which Refinery listens for incoming HTTP requests.
These requests include traffic formatted as Honeycomb events, proxied requests to the Honeycomb API, and OpenTelemetry data using the `http` protocol.
Incoming traffic is expected to be HTTP, so if SSL is a requirement, put something like `nginx` in front to do the decryption.

- Not eligible for live reload.
- Type: `hostport`
- Default: `0.0.0.0:8080`
- Environment variable: `REFINERY_HTTP_LISTEN_ADDRESS`
- Command line switch: `--http-listen-address`

### `PeerListenAddr`

`PeerListenAddr` is the IP and port on which to listen for traffic being rerouted from a peer.

Incoming traffic is expected to be HTTP, so if using SSL use something like nginx or a load balancer to do the decryption.

- Not eligible for live reload.
- Type: `hostport`
- Default: `0.0.0.0:8081`
- Environment variable: `REFINERY_PEER_LISTEN_ADDRESS`
- Command line switch: `--peer-listen-address`

### `HoneycombAPI`

`HoneycombAPI` is the URL of the upstream Honeycomb API where the data will be sent.

This setting is the destination to which Refinery sends all events that it decides to keep.

- Eligible for live reload.
- Type: `url`
- Default: `https://api.honeycomb.io`
- Environment variable: `REFINERY_HONEYCOMB_API`
- Command line switch: `--honeycomb-api`

## Access Key Configuration

`AccessKeys` contains access keys -- API keys that the proxy will treat specially, and other flags that control how the proxy handles API keys.

### `ReceiveKeys`

`ReceiveKeys` is a set of Honeycomb API keys that the proxy will treat specially.

This list only applies to span traffic - other Honeycomb API actions will be proxied through to the upstream API directly without modifying keys.

- Not eligible for live reload.
- Type: `stringarray`
- Example: `your-key-goes-here`

### `AcceptOnlyListedKeys`

`AcceptOnlyListedKeys` is a boolean flag that causes events arriving with API keys not in the `ReceiveKeys` list to be rejected.

If `true`, then only traffic using the keys listed in `ReceiveKeys` is accepted.
Events arriving with API keys not in the `ReceiveKeys` list will be rejected with an HTTP `401` error.
If `false`, then all traffic is accepted and `ReceiveKeys` is ignored.

- Eligible for live reload.
- Type: `bool`

## Refinery Telemetry

`RefineryTelemetry` contains configuration information for the telemetry that Refinery uses to record its own operation.

### `AddRuleReasonToTrace`

`AddRuleReasonToTrace` controls whether to decorate traces with Refinery rule evaluation results.

When enabled, this setting causes traces that are sent to Honeycomb to include the field `meta.refinery.reason`.
This field contains text indicating which rule was evaluated that caused the trace to be included.
This setting also includes the field `meta.refinery.send_reason`, which contains the reason that the trace was sent.
Possible values of this field are `trace_send_got_root`, which means that the root span arrived; `trace_send_expired`, which means that TraceTimeout was reached; `trace_send_ejected_full`, which means that the trace cache was full; and `trace_send_ejected_memsize`, which means that refinery was out of memory.
These names are also the names of metrics that refinery tracks.
We recommend enabling this setting whenever a rules-based sampler is in use, as it is useful for debugging and understanding the behavior of your Refinery installation.

- Eligible for live reload.
- Type: `bool`
- Example: `true`

### `AddSpanCountToRoot`

`AddSpanCountToRoot` controls whether to add a metadata field to root spans that indicates the number of child spans.

The added metadata field, `meta.span_count`, indicates the number of child spans on the trace at the time the sampling decision was made.
This value is available to the rules-based sampler, making it possible to write rules that are dependent upon the number of spans in the trace.
If `true`, then Refinery will add `meta.
span_count` to the root span.

- Eligible for live reload.
- Type: `bool`
- Default: `true`

### `AddHostMetadataToTrace`

`AddHostMetadataToTrace` specifies whether to add host metadata to traces.

If `true`, then Refinery will add the following tag to all traces: - `meta.refinery.local_hostname`: the hostname of the Refinery node

- Eligible for live reload.
- Type: `bool`
- Default: `true`

## Traces

`Traces` contains configuration for how traces are managed.

### `SendDelay`

`SendDelay` is the duration to wait before sending a trace.

This setting is a short timer that is triggered when a trace is complete.
Refinery waits for this duration before sending the trace.
The reason for this setting is to allow for small network delays or clock jitters to elapse and any final spans to arrive before sending the trace.
Set to "0" for immediate sending.

- Eligible for live reload.
- Type: `duration`
- Default: `2s`

### `BatchTimeout`

`BatchTimeout` is how frequently Refinery sends unfulfilled batches.

By default, this setting uses the `DefaultBatchTimeout` in `libhoney` as its value, which is `100ms`.

- Eligible for live reload.
- Type: `duration`
- Example: `500ms`

### `TraceTimeout`

`TraceTimeout` is the duration to wait before making the trace decision on an incomplete trace.

A long timer; it represents the outside boundary of how long to wait before making the trace decision about an incomplete trace.
Normally trace decisions (send or drop) are made when the root span arrives.
Sometimes the root span never arrives (for example, due to crashes) and this timer ensures sending a trace even without having received the root span.
If particularly long-lived traces are present in your data, then you should increase this timer.
Note that this increase will also increase the memory requirements for Refinery.

- Eligible for live reload.
- Type: `duration`
- Default: `60s`

### `MaxBatchSize`

`MaxBatchSize` is the maximum number of events to be included in each batch for sending.

This value is used to set the `BatchSize` field in the `libhoney` library used to send data to Honeycomb.
If you have particularly large traces, then you should increase this value.
Note that this will also increase the memory requirements for Refinery.

- Eligible for live reload.
- Type: `int`
- Default: `500`

### `SendTicker`

`SendTicker` is the interval between checks for traces to send.

A short timer that determines the duration between trace cache review runs to send.
Increasing this will spend more time processing incoming events to reduce `incoming_` or `peer_router_dropped` spikes.
Decreasing this will check the trace cache for timeouts more frequently.

- Eligible for live reload.
- Type: `duration`
- Default: `100ms`

## Debugging

`Debugging` contains configuration values used when setting up and debugging Refinery.

### `DebugServiceAddr`

`DebugServiceAddr` is the IP and port where the debug service runs.

The debug service is generally only used when debugging Refinery itself, and will only run if the command line option `-d` is specified.
If this value is not specified, then the debug service runs on the first open port between `localhost:6060` and `localhost:6069`.

- Not eligible for live reload.
- Type: `hostport`
- Example: `localhost:6060`

### `QueryAuthToken`

`QueryAuthToken` is the token that must be specified to access the `/query` endpoint.

This token must be specified with the header "X-Honeycomb-Refinery-Query" in order for a `/query` request to succeed.
These `/query` requests are intended for debugging Refinery during setup and are not typically needed in normal operation.
If not specified, then the `/query` endpoints are inaccessible.

- Not eligible for live reload.
- Type: `string`
- Example: `some-private-value`
- Environment variable: `REFINERY_QUERY_AUTH_TOKEN`

### `AdditionalErrorFields`

`AdditionalErrorFields` is a list of span fields to include when logging errors happen during the ingestion of events.

For example, the span too large error.
This is primarily useful in trying to track down misbehaving senders in a large installation.
The fields `dataset`, `apihost`, and `environment` are always included.
If a field is not present in the span, then it will not be present in the error log.

- Eligible for live reload.
- Type: `stringarray`
- Example: `trace.span_id`

### `DryRun`

`DryRun` controls whether sampling is applied to incoming traces.

If enabled, then Refinery marks the traces that would be dropped given the current sampling rules, and sends all traces regardless of the sampling decision.
This is useful for evaluating sampling rules.
When DryRun is enabled, traces is decorated with `meta.refinery.
dryrun.kept` that is set to `true` or `false`, based on whether the trace would be kept or dropped.
In addition, `SampleRate` will be set to the incoming rate for all traces, and the field `meta.refinery.dryrun.sample_rate` will be set to the sample rate that would have been used.

- Eligible for live reload.
- Type: `bool`
- Example: `true`

## Refinery Logger

`Logger` contains configuration for logging.

### `Type`

`Type` is the type of logger to use.

The setting specifies where (and if) Refinery sends logs.
`none` means that logs are discarded.
`honeycomb` means that logs will be forwarded to Honeycomb as events according to the set Logging settings.
`stdout` means that logs will be written to `stdout`.

- Not eligible for live reload.
- Type: `string`
- Default: `stdout`
- Options: `stdout`, `honeycomb`, `none`

### `Level`

`Level` is the logging level above which Refinery should send a log to the logger.

`warn` is the recommended level for production.
`debug` is very verbose, and should not be used in production environments.

- Not eligible for live reload.
- Type: `string`
- Default: `warn`
- Options: `debug`, `info`, `warn`, `error`, `panic`

## Honeycomb Logger

`HoneycombLogger` contains configuration for logging to Honeycomb.
Only used if `Logger.Type` is "honeycomb".

### `APIHost`

`APIHost` is the URL of the Honeycomb API where Refinery sends its logs.

Refinery's internal logs will be sent to this host using the standard Honeycomb Events API.

- Not eligible for live reload.
- Type: `url`
- Default: `https://api.honeycomb.io`

### `APIKey`

`APIKey` is the API key used to send Refinery's logs to Honeycomb.

It is recommended that you create a separate team and key for Refinery logs.

- Not eligible for live reload.
- Type: `string`
- Example: `SetThisToAHoneycombKey`
- Environment variable: `REFINERY_HONEYCOMB_LOGGER_API_KEY, REFINERY_HONEYCOMB_API_KEY`

### `Dataset`

`Dataset` is the dataset to which logs will be sent.

Only used if `APIKey` is specified.

- Not eligible for live reload.
- Type: `string`
- Default: `Refinery Logs`

### `SamplerEnabled`

`SamplerEnabled` controls whether logs are sampled before sending to Honeycomb.

The sample rate is controlled by the `SamplerThroughput` setting.

- Not eligible for live reload.
- Type: `bool`
- Default: `true`

### `SamplerThroughput`

`SamplerThroughput` is the sampling throughput for logs in events per second.

The sampling algorithm attempts to make sure that the average throughput approximates this value, while also ensuring that all unique logs arrive at Honeycomb at least once per sampling period.

- Not eligible for live reload.
- Type: `float`
- Default: `10`
- Example: `10`

## Stdout Logger

`StdoutLogger` contains configuration for logging to `stdout`.
Only used if `Logger.Type` is "stdout".

### `Structured`

`Structured` controls whether to use structured logging.

`true` generates structured logs (JSON).
`false` generates plain text logs.

- Not eligible for live reload.
- Type: `bool`

## Prometheus Metrics

`PrometheusMetrics` contains configuration for Refinery's internally-generated metrics as made available through Prometheus.

### `Enabled`

`Enabled` controls whether to expose Refinery metrics over the `PrometheusListenAddr` port.

Each of the metrics providers can be enabled or disabled independently.
Metrics can be sent to multiple destinations.

- Not eligible for live reload.
- Type: `bool`

### `ListenAddr`

`ListenAddr` is the IP and port the Prometheus Metrics server will run on.

Determines the interface and port on which Prometheus will listen for requests for `/metrics`.
Must be different from the main Refinery listener.
Only used if `Enabled` is `true` in `PrometheusMetrics`.

- Not eligible for live reload.
- Type: `hostport`
- Default: `localhost:2112`

## Legacy Metrics

`LegacyMetrics` contains configuration for Refinery's legacy metrics.
Version 1.x of Refinery used this format for sending Metrics to Honeycomb.
The metrics generated that way are nonstandard and will be deprecated in a future release.
New installations should prefer `OTelMetrics`.

### `Enabled`

`Enabled` controls whether to send legacy-formatted metrics to Honeycomb.

Each of the metrics providers can be enabled or disabled independently.
Metrics can be sent to multiple destinations.

- Not eligible for live reload.
- Type: `bool`

### `APIHost`

`APIHost` is the URL of the Honeycomb API where legacy metrics are sent.

Refinery's internal metrics will be sent to this host using the standard Honeycomb Events API.

- Not eligible for live reload.
- Type: `url`
- Default: `https://api.honeycomb.io`

### `APIKey`

`APIKey` is the API key used by Refinery to send its metrics to Honeycomb.

It is recommended that you create a separate team and key for Refinery metrics.

- Not eligible for live reload.
- Type: `string`
- Example: `SetThisToAHoneycombKey`
- Environment variable: `REFINERY_HONEYCOMB_METRICS_API_KEY, HONEYCOMB_API_KEY`

### `Dataset`

`Dataset` is the Honeycomb dataset where Refinery sends its metrics.

Only used if `APIKey` is specified.

- Not eligible for live reload.
- Type: `string`
- Default: `Refinery Metrics`

### `ReportingInterval`

`ReportingInterval` is the interval between sending legacy metrics to Honeycomb.

Between 1 and 60 seconds is typical.

- Not eligible for live reload.
- Type: `duration`
- Default: `30s`

## OpenTelemetry Metrics

`OTelMetrics` contains configuration for Refinery's OpenTelemetry (OTel) metrics.
This is the preferred way to send metrics to Honeycomb.
New installations should prefer `OTelMetrics`.

### `Enabled`

`Enabled` controls whether to send metrics via OpenTelemetry.

Each of the metrics providers can be enabled or disabled independently.
Metrics can be sent to multiple destinations.

- Not eligible for live reload.
- Type: `bool`

### `APIHost`

`APIHost` is the URL of the OpenTelemetry API to which metrics will be sent.

Refinery's internal metrics will be sent to the `/v1/metrics` endpoint on this host.

- Not eligible for live reload.
- Type: `url`
- Default: `https://api.honeycomb.io`

### `APIKey`

`APIKey` is the API key used to send Honeycomb metrics via OpenTelemetry.

It is recommended that you create a separate team and key for Refinery metrics.
If this is blank, then Refinery will not set the Honeycomb-specific headers for OpenTelemetry, and your `APIHost` must be set to a valid OpenTelemetry endpoint.

- Not eligible for live reload.
- Type: `string`
- Example: `SetThisToAHoneycombKey`
- Environment variable: `REFINERY_OTEL_METRICS_API_KEY, HONEYCOMB_API_KEY`

### `Dataset`

`Dataset` is the Honeycomb dataset that Refinery sends its OpenTelemetry metrics.

Only used if `APIKey` is specified.

- Not eligible for live reload.
- Type: `string`
- Default: `Refinery Metrics`

### `ReportingInterval`

`ReportingInterval` is the interval between sending OpenTelemetry metrics to Honeycomb.

Between `1` and `60` seconds is typical.

- Not eligible for live reload.
- Type: `duration`
- Default: `30s`

### `Compression`

`Compression` is the compression algorithm to use when sending OpenTelemetry metrics to Honeycomb.

`gzip` is the default and recommended value.
In rare circumstances, compression costs may outweigh the benefits, in which case `none` may be used.

- Not eligible for live reload.
- Type: `string`
- Default: `gzip`
- Options: `none`, `gzip`

## Peer Management

`PeerManagement` controls how the Refinery cluster communicates between peers.

### `Type`

`Type` is the type of peer management to use.

Peer management is the mechanism by which Refinery locates its peers.
`file` means that Refinery gets its peer list from the Peers list in this config file.
`redis` means that Refinery self-registers with a Redis instance and gets its peer list from there.

- Not eligible for live reload.
- Type: `string`
- Default: `file`
- Options: `redis`, `file`

### `Identifier`

`Identifier` specifies the identifier to use when registering itself with peers.

By default, when using a peer registry, Refinery will use the local hostname to identify itself to other peers.
If your environment requires something else, (for example, if peers cannot resolve each other by name), then you can specify the exact identifier, such as an IP address, to use here.
Overrides `IdentifierInterfaceName`, if both are set.

- Not eligible for live reload.
- Type: `string`
- Example: `192.168.1.1`

### `IdentifierInterfaceName`

`IdentifierInterfaceName` specifies a network interface to use when finding a local hostname.

By default, when using a peer registry, Refinery will use the local hostname to identify itself to other peers.
If your environment requires that you use IPs as identifiers (for example, if peers cannot resolve each other by name), then you can specify the network interface that Refinery is listening on here.
Refinery will use the first unicast address that it finds on the specified network interface as its identifier.

- Not eligible for live reload.
- Type: `string`
- Example: `eth0`

### `UseIPV6Identifier`

`UseIPV6Identifier` specifies that Refinery should use an IPV6 address as its identifier.

If using `IdentifierInterfaceName`, Refinery will default to the first IPv4 unicast address it finds for the specified interface.
If this value is specified, then Refinery will use the first IPV6 unicast address found.

- Not eligible for live reload.
- Type: `bool`

### `Peers`

`Peers` is the list of peers to use when Type is "file", excluding self.

This list is ignored when Type is "redis".
The format is a list of strings of the form "host:port".

- Not eligible for live reload.
- Type: `stringarray`
- Example: `192.168.1.11:8081,192.168.1.12:8081`

## Redis Peer Management

`RedisPeerManagement` controls how the Refinery cluster communicates between peers when using Redis.
Only applies when `PeerManagement.Type` is "redis".

### `Host`

`Host` is the host and port of the Redis instance to use for peer cluster membership management.

Must be in the form `host:port`.

- Not eligible for live reload.
- Type: `hostport`
- Example: `localhost:6379`
- Environment variable: `REFINERY_REDIS_HOST`

### `Username`

`Username` is the username used to connect to Redis for peer cluster membership management.

Many Redis installations do not use this field.

- Not eligible for live reload.
- Type: `string`
- Environment variable: `REFINERY_REDIS_USERNAME`

### `Password`

`Password` is the password used to connect to Redis for peer cluster membership management.

Many Redis installations do not use this field.

- Not eligible for live reload.
- Type: `string`
- Environment variable: `REFINERY_REDIS_PASSWORD`

### `AuthCode`

`AuthCode` is the string used to connect to Redis for peer cluster membership management using an explicit AUTH command.

Many Redis installations do not use this field.

- Not eligible for live reload.
- Type: `string`
- Environment variable: `REFINERY_REDIS_AUTH_CODE`

### `Prefix`

`Prefix` is a string used as a prefix for the keys in Redis while storing the peer membership.

It might be useful to override this in any situation where multiple Refinery clusters or multiple applications want to share a single Redis instance.
It may not be blank.

- Not eligible for live reload.
- Type: `string`
- Default: `refinery`
- Example: `customPrefix`

### `Database`

`Database` is the database number to use for the Redis instance storing the peer membership.

An integer from 0-15 indicating the database number to use for the Redis instance storing the peer membership.
It might be useful to set this in any situation where multiple Refinery clusters or multiple applications want to share a single Redis instance.

- Not eligible for live reload.
- Type: `int`
- Example: `1`

### `UseTLS`

`UseTLS` enables TLS when connecting to Redis for peer cluster membership management.

When enabled, this setting sets the `MinVersion` in the TLS configuration to `1.2`.

- Not eligible for live reload.
- Type: `bool`

### `UseTLSInsecure`

`UseTLSInsecure` disables certificate checks when connecting to Redis for peer cluster membership management.

This setting is intended for use with self-signed certificates and sets the `InsecureSkipVerify` flag within Redis.

- Not eligible for live reload.
- Type: `bool`

### `Timeout`

`Timeout` is the timeout to use when communicating with Redis.

It is rarely necessary to adjust this value.

- Not eligible for live reload.
- Type: `duration`
- Default: `5s`

## Collection Settings

`Collection` contains the settings that are relevant to collecting spans together to make traces.
If none of the memory settings are used, then Refinery will not attempt to limit its memory usage.
This is not recommended for production use since a burst of traffic could cause Refinery to run out of memory and crash.

### `CacheCapacity`

`CacheCapacity` is the number of traces to keep in the cache's circular buffer.

The collection cache is used to collect all spans into a trace as well as remember the sampling decision for any spans that might come in after the trace has been marked "complete" (either by timing out or seeing the root span).
The number of traces in the cache should be many multiples (100x to 1000x) of the total number of concurrently active traces (trace throughput * trace duration).

- Eligible for live reload.
- Type: `int`
- Default: `10000`

### `AvailableMemory`

`AvailableMemory` is the amount of system memory available to the Refinery process.

This value will typically be set through an environment variable controlled by the container or deploy script.
If this value is zero or not set, then `MaxMemory` cannot be used to calculate the maximum allocation and `MaxAlloc` will be used instead.
If set, then this must be a memory size.
64-bit values are supported.
Sizes with standard unit suffixes (`MB`, `GiB`, etc.) and Kubernetes units (`M`, `Gi`, etc.) are also supported.
If set, `Collections.MaxAlloc` must not be defined.

- Eligible for live reload.
- Type: `memorysize`
- Example: `4Gb`
- Environment variable: `REFINERY_AVAILABLE_MEMORY`
- Command line switch: `--available-memory`

### `MaxMemoryPercentage`

`MaxMemoryPercentage` is the maximum percentage of memory that should be allocated by the span collector.

If nonzero, then it must be an integer value between 1 and 100, representing the target maximum percentage of memory that should be allocated by the span collector.
If set to a non-zero value, then once per tick (see `SendTicker`) the collector will compare total allocated bytes to this calculated value.
If allocation is too high, then traces will be ejected from the cache early to reduce memory.
Useful values for this setting are generally in the range of 70-90.

- Eligible for live reload.
- Type: `percentage`
- Default: `75`
- Example: `75`

### `MaxAlloc`

`MaxAlloc` is the maximum number of bytes that should be allocated by the Collector.

If set, then this must be a memory size.
64-bit values are supported.
Sizes with standard unit suffixes (`MB`, `GiB`, etc) and Kubernetes units (`M`, `Gi`, etc) are also supported.
See `MaxMemory` for more details.
If set, `Collections.AvailableMemory` must not be defined.

- Eligible for live reload.
- Type: `memorysize`

## Buffer Sizes

`BufferSizes` contains the settings that are relevant to the sizes of communications buffers.

### `UpstreamBufferSize`

`UpstreamBufferSize` is the size of the queue used to buffer spans to send to the upstream Collector.

The size of the buffer is measured in spans.
If the buffer fills up, then performance will degrade because Refinery will block while waiting for space to become available.
If this happens, then you should increase the buffer size.

- Eligible for live reload.
- Type: `int`
- Default: `10000`

### `PeerBufferSize`

`PeerBufferSize` is the size of the queue used to buffer spans to send to peer nodes.

The size of the buffer is measured in spans.
If the buffer fills up, then performance will degrade because Refinery will block while waiting for space to become available.
If this happens, then you should increase this buffer size.

- Eligible for live reload.
- Type: `int`
- Default: `100000`

## Specialized Configuration

`Specialized` contains special-purpose configuration options that are not typically needed.

### `EnvironmentCacheTTL`

`EnvironmentCacheTTL` is the duration for which environment information is cached.

This is the amount of time for which Refinery caches environment information, which it looks up from Honeycomb for each different `APIKey`.
This information is used when making sampling decisions.
If you have a very large number of environments, then you may want to increase this value.

- Eligible for live reload.
- Type: `duration`
- Default: `1h`

### `CompressPeerCommunication`

`CompressPeerCommunication` determines whether Refinery will compress span data it forwards to peers.

If it costs money to transmit data between Refinery instances (for example, when spread across AWS availability zones), then you almost certainly want compression enabled to reduce your bill.
The option to disable it is provided as an escape hatch for deployments that value lower CPU utilization over data transfer costs.

- Not eligible for live reload.
- Type: `bool`
- Default: `true`

### `AdditionalAttributes`

`AdditionalAttributes` is a map that can be used for injecting user-defined attributes into every span.

For example, it could be used for naming a Refinery cluster.
Both keys and values must be strings.

- Eligible for live reload.
- Type: `map`
- Example: `ClusterName:MyCluster,environment:production`

## ID Fields

`IDFields` controls the field names to use for the event ID fields.
These fields are used to identify events that are part of the same trace.

### `TraceNames`

`TraceNames` is the list of field names to use for the trace ID.

The first field in the list that is present in an incoming span will be used as the trace ID.
If none of the fields are present, then Refinery treats the span as not being part of a trace and forwards it immediately to Honeycomb.

- Eligible for live reload.
- Type: `stringarray`
- Example: `trace.trace_id,traceId`

### `ParentNames`

`ParentNames` is the list of field names to use for the parent ID.

The first field in the list that is present in an event will be used as the parent ID.
A trace without a `parent_id` is assumed to be a root span.

- Eligible for live reload.
- Type: `stringarray`
- Example: `trace.parent_id,parentId`

## gRPC Server Parameters

`GRPCServerParameters` controls the parameters of the gRPC server used to receive OpenTelemetry data in gRPC format.

### `Enabled`

`Enabled` specifies whether the gRPC server is enabled.

If `false`, then the gRPC server is not started and no gRPC traffic is accepted.

- Not eligible for live reload.
- Type: `bool`

### `ListenAddr`

`ListenAddr` is the address Refinery listens to for incoming GRPC OpenTelemetry events.

Incoming traffic is expected to be unencrypted, so if using SSL, then put something like `nginx` in front to do the decryption.

- Not eligible for live reload.
- Type: `hostport`
- Environment variable: `REFINERY_GRPC_LISTEN_ADDRESS`
- Command line switch: `--grpc-listen-address`

### `MaxConnectionIdle`

`MaxConnectionIdle` is the amount of time to permit an idle connection.

A duration for the amount of time after which an idle connection will be closed by sending a GoAway.
"Idle" means that there are no active RPCs.
"0s" sets duration to infinity, but this is not recommended for Refinery deployments behind a load balancer, because it will prevent the load balancer from distributing load evenly among peers.

- Not eligible for live reload.
- Type: `duration`
- Default: `0s`
- Example: `1m`

### `MaxConnectionAge`

`MaxConnectionAge` is the maximum amount of time a gRPC connection may exist.

After this duration, the gRPC connection is closed by sending a `GoAway`.
A random jitter of +/-10% will be added to `MaxConnectionAge` to spread out connection storms.
`0s` sets duration to infinity; a value measured in low minutes will help load balancers to distribute load among peers more evenly.

- Not eligible for live reload.
- Type: `duration`
- Default: `3m`

### `MaxConnectionAgeGrace`

`MaxConnectionAgeGrace` is the duration beyond `MaxConnectionAge` after which the connection will be forcibly closed.

This setting is in case the upstream node ignores the `GoAway` request.
"0s" sets duration to infinity.

- Not eligible for live reload.
- Type: `duration`
- Default: `60s`

### `KeepAlive`

`KeepAlive` is the duration between keep-alive pings.

After this amount of time, if the client does not see any activity, then it pings the server to see if the transport is still alive.
"0s" sets duration to 2 hours.

- Not eligible for live reload.
- Type: `duration`
- Default: `1m`

### `KeepAliveTimeout`

`KeepAliveTimeout` is the duration the server waits for activity on the connection.

This is the amount of time after which if the server does not see any activity, then it pings the client to see if the transport is still alive.
"0s" sets duration to 20 seconds.

- Not eligible for live reload.
- Type: `duration`
- Default: `20s`

## Sample Cache

`SampleCache` controls the sample cache used to retain information about trace status after the sampling decision has been made.

### `KeptSize`

`KeptSize` is the number of traces preserved in the cuckoo kept traces cache.

Refinery keeps a record of each trace that was kept and sent to Honeycomb, along with some statistical information.
This is most useful in cases where the trace was sent before sending the root span, so that the root span can be decorated with accurate metadata.
Default is `10_000` traces.
Each trace in this cache consumes roughly 200 bytes.

- Eligible for live reload.
- Type: `int`
- Default: `10000`

### `DroppedSize`

`DroppedSize` is the size of the cuckoo dropped traces cache.

This cache consumes 4-6 bytes per trace at a scale of millions of traces.
Changing its size with live reload sets a future limit, but does not have an immediate effect.

- Eligible for live reload.
- Type: `int`
- Default: `1000000`

### `SizeCheckInterval`

`SizeCheckInterval` controls how often the cuckoo cache re-evaluates its remaining capacity.

This cache is quite resilient so it does not need to happen very often, but the operation is also inexpensive.
Default is 10 seconds.

- Eligible for live reload.
- Type: `duration`
- Default: `10s`

## Stress Relief

`StressRelief` controls the Stress Relief mechanism, which is used to prevent Refinery from being overwhelmed by a large number of traces.
There is a metric called `stress_level` that is emitted as part of Refinery metrics.
It is a measure of Refinery's throughput rate relative to its processing rate, combined with the amount of room in its internal queues, and ranges from `0` to `100`.
`stress_level` is generally expected to be `0` except under heavy load.
When stress levels reach `100`, there is an increased chance that Refinery will become unstable.
To avoid this problem, the Stress Relief system can do deterministic sampling on new trace traffic based solely on `TraceID`, without having to store traces in the cache or take the time processing sampling rules.
Existing traces in flight will be processed normally, but when Stress Relief is active, trace decisions are made deterministically on a per-span basis; all spans will be sampled according to the `SamplingRate` specified here.
Once Stress Relief activates (by exceeding the `ActivationLevel`), it will not deactivate until `stress_level` falls below the `DeactivationLevel`.
When it deactivates, normal trace decisions are made -- and any additional spans that arrive for traces that were active during Stress Relief will respect the decisions made during that time.
The measurement of stress is a lagging indicator and is highly dependent on Refinery configuration and scaling.
Other configuration values should be well tuned first, before adjusting the Stress Relief Activation parameters.
Stress Relief is not a substitute for proper configuration and scaling, but it can be used as a safety valve to prevent Refinery from becoming unstable under heavy load.

### `Mode`

`Mode` is a string indicating how to use Stress Relief.

This setting sets the Stress Relief mode.
"never" means that Stress Relief will never activate.
"monitor" is the recommended setting, and means that Stress Relief will monitor the status of Refinery and activate according to the levels set by fields such as `ActivationLevel`.
"always" means that Stress Relief is always on, which may be useful in an emergency situation.

- Eligible for live reload.
- Type: `string`
- Default: `never`

### `ActivationLevel`

`ActivationLevel` is the `stress_level` (from 0-100) at which Stress Relief is triggered.

This value must be greater than `DeactivationLevel` and should be high enough that it is not reached in normal operation.

- Eligible for live reload.
- Type: `percentage`
- Default: `90`

### `DeactivationLevel`

`DeactivationLevel` is the `stress_level` (from 0-100) at which Stress Relief is turned off.

This setting is subject to `MinimumActivationDuration`.
The value must be less than `ActivationLevel`.

- Eligible for live reload.
- Type: `percentage`
- Default: `70`

### `SamplingRate`

`SamplingRate` is the sampling rate to use when Stress Relief is activated.

All new traces will be deterministically sampled at this rate based only on the `traceID`.
It should be chosen to be a rate that sends fewer samples than the average sampling rate Refinery is expected to generate.
For example, if Refinery is configured to normally sample at a rate of 1 in 10, then Stress Relief should be configured to sample at a rate of at least 1 in 30.

- Eligible for live reload.
- Type: `int`
- Default: `100`

### `MinimumActivationDuration`

`MinimumActivationDuration` is the minimum time that Stress Relief will stay enabled once activated.

This setting helps to prevent oscillations.

- Eligible for live reload.
- Type: `duration`
- Default: `10s`

### `MinimumStartupDuration`

`MinimumStartupDuration` is the minimum time that Stress Relief will stay enabled.

This setting is used when switching into Monitor mode.
When Stress Relief is enabled, it will start up in stressed mode for at least this set duration of time to try to make sure that Refinery can handle the load before it begins processing it in earnest.
This is to help address the problem of trying to bring a new node into an already-overloaded cluster.
If this duration is `0`, then Refinery will not start in stressed mode, which will provide faster startup at the possible cost of startup instability.

- Eligible for live reload.
- Type: `duration`
- Default: `3s`


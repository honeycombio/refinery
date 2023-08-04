# Refinery - the Honeycomb Sampling Proxy

![refinery](https://user-images.githubusercontent.com/6510988/94976958-8cadba80-04cb-11eb-9883-6e8ea554a081.png)

[![OSS Lifecycle](https://img.shields.io/osslifecycle/honeycombio/refinery?color=success)](https://github.com/honeycombio/home/blob/main/honeycomb-oss-lifecycle-and-practices.md)
[![Build Status](https://circleci.com/gh/honeycombio/refinery.svg?style=shield)](https://circleci.com/gh/honeycombio/refinery)

## Release Information

For a detailed list of linked pull requests merged in each release, see [CHANGELOG.md](./CHANGELOG.md).
For more readable information about recent changes, please see [RELEASE_NOTES.md](./RELEASE_NOTES.md).

## Purpose

Refinery is a tail-based sampling proxy that operates at the the level of an entire trace. It examines whole traces and intelligently applies sampling decisions (whether to keep or discard) to each trace.

A tail-based sampling model allows you to inspect an entire trace at one time and make a decision to sample based on its contents. For example, perhaps the root span has the HTTP status code to serve for a request, whereas another span might have information on whether the data was served from a cache. Using Refinery, you can choose to keep only traces that had a `500` status code and were also served from a cache.

### Refinery's tail sampling capabilities

Refinery support several kinds of tail sampling:

* **Dynamic sampling** - This sampling type configures a key based on a trace's set of fields and automatically increases or decreases the sampling rate based on how frequently each unique value of that key occurs. For example, using a key based on `http.status_code`, requests that return `200` could be included less often in the forwarded sample data, since `200` indicates success and appears in higher frequency, compared to requests that return `404` errors and require investigation.
* **Rules-based sampling** - This sampling type enables you to define sampling rates for well-known conditions. For example, you can sample 100% of traces with an error and then fall back to dynamic sampling for all other traffic.
* **Throughput-based sampling** - This sampling type enables you to sample traces based on a fixed upper-bound for the number of spans per second. The sampler will dynamically sample traces with a goal to keep the throughput below the specified limit.
* **Deterministic probability sampling** - This sampling type consistently applies sampling decisions without considering the contents of the trace other than its trace ID. For example, you can include 1 out of every 12 traces in the sampled data sent to Honeycomb. This kind of sampling can also be done using [head sampling](https://docs.honeycomb.io/manage-data-volume/sampling/#head-sampling), and if you use both, Refinery takes that into account.

Refinery lets you combine all of the above techniques to achieve your desired sampling behavior.

## Setting up Refinery

Refinery is designed to sit within your infrastructure where all traces can reach it. Refinery can run standalone or be deployed in a cluster of two or more Refinery processes accessible via a separate load balancer.

Refinery processes must be able to communicate with each other to concentrate traces on single servers.

Within your application (or other Honeycomb event sources) you would configure the `API Host` to be http(s)://load-balancer/. Everything else remains the same (api key, dataset name, etc. - all that lives with the originating client).

### Minimum configuration

Every Refinery instance should have a minimum of 2GB RAM and at least 2 cores, preferably more. Refinery clusters usually run best with fewer, larger servers; for large installations, 16 cores and 16GB of RAM per server is not unusual (with appropriate tuning).

Additional RAM and CPU can be used by increasing configuration values; in particular, `CacheCapacity` is an important configuration value. Refinery's `Stress Relief` system provides a good indication of how hard Refinery is working, and when invoked, logs (as `reason`) the name of the Refinery configuration value that should be increased to reduce stress.

### Setting up Refinery in Kubernetes

Refinery is available as a Helm chart in the Honeycomb Helm repository.

You can install Refinery with the following command:

```bash
helm repo add honeycomb https://honeycombio.github.io/helm-charts
helm install refinery honeycomb/refinery
```

This will use the default values file. You can also supply your own:

```bash
helm install refinery honeycomb/refinery --values /path/to/refinery-values.yaml
```

## Peer Management

When operating in a cluster, Refinery expects to gather all of the spans in a trace onto a single instance so that it can make a trace decision. Since each span arrives independently, each Refinery instance needs to be able to communicate with all of its peers in order to distribute traces to the correct instance.

This communication can be managed in two ways: via an explicit list of peers in the configuration file, or by using self-registration via a shared Redis cache. Installations should generally prefer to use Redis. Even in large installations, the load on the Redis server is quite light, with each instance only making a few requests per minute. A single Redis instance with fractional CPU is usually sufficient.


## Configuration

Configuration is controlled by Refinery's two configuration files. For information on all of the configuration parameters that control Refinery's operation, please see [the configuration documentation](https://docs.honeycomb.io/manage-data-volume/refinery/configuration/).

For information on sampler configuration, please see [the sampling methods documentation](https://docs.honeycomb.io/manage-data-volume/refinery/sampling-methods/).

## Running Refinery

Refinery is a typical linux-style command line application, and supports several command line switches.

`refinery -h` will print an extended help text listing all command line options and supported environment variables.

### Environment Variables

Refinery supports the following key environment variables; please see the command line help or the online documentation for the full list. Command line switches take precedence over file configuration, and environment variables take precendence over both.

| Environment Variable                                              | Config Field                     |
|-------------------------------------------------------------------|----------------------------------|
| `REFINERY_GRPC_LISTEN_ADDRESS`                                    | `GRPCListenAddr`                 |
| `REFINERY_REDIS_HOST`                                             | `PeerManagement.RedisHost`       |
| `REFINERY_REDIS_USERNAME`                                         | `PeerManagement.RedisUsername`   |
| `REFINERY_REDIS_PASSWORD`                                         | `PeerManagement.RedisPassword`   |
| `REFINERY_HONEYCOMB_API_KEY`                                      | `HoneycombLogger.LoggerAPIKey`   |
| `REFINERY_HONEYCOMB_METRICS_API_KEY` `REFINERY_HONEYCOMB_API_KEY`    | `LegacyMetrics.APIKey`           |
| `REFINERY_QUERY_AUTH_TOKEN`                                       | `QueryAuthToken`                 |

Note, `REFINERY_HONEYCOMB_METRICS_API_KEY` takes precedence over `REFINERY_HONEYCOMB_API_KEY` for the `LegacyMetrics.APIKey` configuration.


## Dry Run Mode

When getting started with Refinery or when updating sampling rules, it may be helpful to verify that the rules are working as expected before you start dropping traffic. By enabling dry run mode, all spans in each trace will be marked with the sampling decision in a field called `refinery_kept`. All traces will be sent to Honeycomb regardless of the sampling decision. The SampleRate will not be changed, but the calculated SampleRate will be stored in a field called `meta.dryrun.sample_rate`. You can then run queries in Honeycomb to check your results and verify that the rules are working as intended. See the configuration documentation for information on enabling Dry Run mode.

When dry run mode is enabled, the metric `trace_send_kept` will increment for each trace, and the metric for `trace_send_dropped` will remain 0, reflecting that we are sending all traces to Honeycomb.

## Scaling Up

Refinery uses bounded queues and circular buffers to manage allocating traces, so even under high volume memory use shouldn't expand dramatically. However, given that traces are stored in a circular buffer, when the throughput of traces exceeds the size of the buffer, things will start to go wrong. If you have statistics configured, a counter named `collect_cache_buffer_overrun` will be incremented each time this happens. The symptoms of this will be that traces will stop getting accumulated together, and instead spans that should be part of the same trace will be treated as two separate traces. All traces will continue to be sent (and sampled), but some sampling decisions will be made on incomplete data. The size of the circular buffer is a configuration option named `CacheCapacity`. To choose a good value, you should consider the throughput of traces (e.g. traces / second started) and multiply that by the maximum duration of a trace (say, 3 seconds), then multiply that by some large buffer (maybe 10x). This will give you good headroom.

Determining the number of machines necessary in the cluster is not an exact science, and is best influenced by watching for buffer overruns. But for a rough heuristic, count on a single machine using about 2G of memory to handle 5000 incoming events and tracking 500 sub-second traces per second (for each full trace lasting less than a second and an average size of 10 spans per trace).

## Understanding Regular Operation

Refinery emits a number of metrics to give some indication about the health of the process. These metrics should be sent to Honeycomb, typically with Open Telemetry, and can also be exposed to Prometheus. The interesting ones to watch are:

- Sample rates: how many traces are kept / dropped, and what does the sample rate distribution look like?
- [incoming|peer]_router_\*: how many events (no trace info) vs. spans (have trace info) have been accepted, and how many sent on to peers?
- collect_cache_buffer_overrun: this should remain zero; a positive value indicates the need to grow the size of the collector's circular buffer (via configuration `CacheCapacity`).
- process_uptime_seconds: records the uptime of each process; look for unexpected restarts as a key towards memory constraints.

## Troubleshooting

### Logging

The default logging level of `warn` is fairly quiet. The `debug` level emits too much data to be used in production, but contains excellent information in a pre-production environment,including trace decision information. `info` is somewhere between. Setting the logging level to `debug` during initial configuration will help understand what's working and what's not, but when traffic volumes increase it should be set to `warn` or even `error`. Logs may be sent to stdout or to Honeycomb.

### Configuration Validation

Refinery validates its configuration on startup or when a configuration is reloaded, and it emits diagnostics for any problems. On startup, it will refuse to start; on reload, it will not change the existing configuration.

### Configuration Query

Because the configuration file formats (YAML) can sometimes be confusing to read and write, it may be valuable to check the loaded configuration by using one of the `/query` endpoints from the command line on a server that can access a refinery host.

The `/query` endpoints are protected and can be enabled by specifying `QueryAuthToken` in the configuration file or specifying `REFINERY_QUERY_AUTH_TOKEN` in the environment. All requests to any `/query` endpoint must include the header `X-Honeycomb-Refinery-Query` set to the value of the specified token.

`curl --include --get $REFINERY_HOST/query/allrules/$FORMAT --header "x-honeycomb-refinery-query: my-local-token"` will retrieve the entire rules configuration.

`curl --include --get $REFINERY_HOST/query/rules/$FORMAT/$DATASET --header "x-honeycomb-refinery-query: my-local-token"` will retrieve the rule set that refinery will use for the specified dataset. It comes back as a map of the sampler type to its rule set.

`curl --include --get $REFINERY_HOST/query/configmetadata --header "x-honeycomb-refinery-query: my-local-token"` will retrieve information about the configurations currently in use, including the timestamp when the configuration was last loaded.

For file-based configurations (the only type currently supported), the `hash` value is identical to the value generated by the `md5sum` command for the given config file.

For all of these commands:
- `$REFINERY_HOST` should be the url of your refinery.
- `$FORMAT` can be one of `json`, `yaml`, or `toml`.
- `$DATASET` is the name of the dataset you want to check.

### Sampling

Refinery can send telemetry that includes information that can help debug the sampling decisions that are made. To enable it, in the config file, set `AddRuleReasonToTrace` to `true`. This will cause traces that are sent to Honeycomb to include a field `meta.refinery.reason`, which will contain text indicating which rule was evaluated that caused the trace to be included.

## Restarts

Refinery does not yet buffer traces or sampling decisions to disk. When you restart the process all in-flight traces will be flushed (sent upstream to Honeycomb), but you will lose the record of past trace decisions. When started back up, it will start with a clean slate.

## Architecture of Refinery itself (for contributors)

Within each directory, the interface the dependency exports is in the file with the same name as the directory and then (for the most part) each of the other files are alternative implementations of that interface. For example, in `logger`, `/logger/logger.go` contains the interface definition and `logger/honeycomb.go` contains the implementation of the `logger` interface that will send logs to Honeycomb.

`main.go` sets up the app and makes choices about which versions of dependency implementations to use (eg which logger, which sampler, etc.) It starts up everything and then launches `App`

`app/app.go` is the main control point. When its `Start` function ends, the program shuts down. It launches two `Router`s which listen for incoming events.

`route/route.go` listens on the network for incoming traffic. There are two routers running and they handle different types of incoming traffic: events coming from the outside world (the `incoming` router) and events coming from another member of the Refinery cluster (`peer` traffic). Once it gets an event, it decides where it should go next: is this incoming request an event (or batch of events), and if so, does it have a trace ID? Everything that is not an event or an event that does not have a trace ID is immediately handed to `transmission` to be forwarded on to Honeycomb. If it is an event with a trace ID, the router extracts the trace ID and then uses the `sharder` to decide which member of the Refinery cluster should handle this trace. If it's a peer, the event will be forwarded to that peer. If it's us, the event will be transformed into an internal representation and handed to the `collector` to bundle spans into traces.

`collect/collect.go` the collector is responsible for bundling spans together into traces and deciding when to send them to Honeycomb or if they should be dropped. The first time a trace ID is seen, the collector starts a timer. If the root span (aka a span with a trace ID and no parent ID) arrives before the timer expires, then the trace is considered complete. The trace is sent and the timer is canceled. If the timer expires before the root span arrives, the trace will be sent whether or not it is complete. Just before sending, the collector asks the `sampler` for a sample rate and whether or not to keep the trace. The collector obeys this sampling decision and records it (the record is applied to any spans that may come in as part of the trace after the decision has been made). After making the sampling decision, if the trace is to be kept, it is passed along to the `transmission` for actual sending.

`transmit/transmit.go` is a wrapper around the HTTP interactions with the Honeycomb API. It handles batching events together and sending them upstream.

`logger` and `metrics` are for managing the logs and metrics that Refinery itself produces.

`sampler` contains algorithms to compute sample rates based on the traces provided.

`sharder` determines which peer in a clustered Refinery config is supposed to handle an individual trace.

`types` contains a few type definitions that are used to hand data in between packages.

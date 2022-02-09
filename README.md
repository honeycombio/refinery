# Refinery - the Honeycomb Sampling Proxy

![refinery](https://user-images.githubusercontent.com/6510988/94976958-8cadba80-04cb-11eb-9883-6e8ea554a081.png)

[![OSS Lifecycle](https://img.shields.io/osslifecycle/honeycombio/refinery?color=success)](https://github.com/honeycombio/home/blob/main/honeycomb-oss-lifecycle-and-practices.md)
[![Build Status](https://circleci.com/gh/honeycombio/refinery.svg?style=shield)](https://circleci.com/gh/honeycombio/refinery)

## Purpose

Refinery is a trace-aware sampling proxy. It collects spans emitted by your application, gathers them into traces, and examines them as a whole. This enables Refinery to make an intelligent sampling decision (whether to keep or discard) based on the entire trace. Buffering the spans allows you to use fields that might be present in different spans within the trace to influence the sampling decision. For example, the root span might have HTTP status code, whereas another span might have information on whether the request was served from a cache. Using Refinery, you can choose to keep only traces that had a 500 status code and were also served from a cache.

## Setting up Refinery

Refinery is designed to sit within your infrastructure where all sources of Honeycomb events (aka spans if you're doing tracing) can reach it.
A standard deployment will have a cluster of two or more Refinery processes accessible via a separate load balancer.
Refinery processes must be able to communicate with each other to concentrate traces on single servers.

Within your application (or other Honeycomb event sources) you would configure the `API Host` to be http(s)://load-balancer/. Everything else remains the same (api key, dataset name, etc. - all that lives with the originating client).

### Minimum configuration

The Refinery cluster should have at least 2 servers with 2GB RAM and access to 2 cores each.

Additional RAM and CPU can be used by increasing configuration values to have a larger `CacheCapacity`. The cluster should be monitored for panics caused by running out of memory and scaled up (with either more servers or more RAM per server) when they occur.

### Builds

Refinery is built by [CircleCI](https://circleci.com/gh/honeycombio/refinery). Released versions of Refinery are available via Github under the Releases tab.

## Configuration

Configuration is done in one of two ways, either entirely by the config file or a combination of the config file and a Redis service for managing the list of peers in the cluster.
When using Redis, it only manages peers; all other configuration remains managed by the config file.

There are a few vital configuration options; read through this list and make sure all the variables are set.

### File-based Config

- API Keys: Refinery itself needs to be configured with a list of your API keys. This lets it respond with a 401/Unauthorized if an unexpected API key is used. You can configure Refinery to accept all API keys by setting it to `*` but then you will lose the authentication feedback to your application. Refinery will accept all events even if those events will eventually be rejected by the Honeycomb API due to an API key issue.

- Goal Sample Rate and the list of fields you'd like to use to generate the keys off which sample rate is chosen. This is where the power of the proxy comes in - being able to dynamically choose sample rates based on the contents of the traces as they go by. There is an overall default and dataset-specific sections for this configuration, so that different datasets can have different sets of fields and goal sample rates.

- Trace timeout - it should be set higher (maybe double?) the longest expected trace. If all of your traces complete in under 10 seconds, 30 is a good value here. If you have traces that can last minutes, it should be raised accordingly. Note that the trace doesn't _have_ to complete before this timer expires - but the sampling decision will be made at that time. So any spans that contain fields that you want to use to compute the sample rate should arrive before this timer expires. Additional spans that arrive after the timer has expired will be sent or dropped according to the sampling decision made when the timer expired.

- Peer list: this is a list of all the other servers participating in this Refinery cluster. Traces are evenly distributed across all available servers, and any one trace must be concentrated on one server, regardless of which server handled the incoming spans. The peer list lets the cluster move spans around to the server that is handling the trace. (Not used in the Redis-based config.)

- Buffer size: The `InMemCollector`'s `CacheCapacity` setting determines how many in-flight traces you can have. This should be large enough to avoid overflow. Some multiple (2x, 3x) the total number of in-flight traces you expect is a good place to start. If it's too low you will see the `collect_cache_buffer_overrun` metric increment. If you see that, you should increase the size of the buffer.

There are a few components of Refinery with multiple implementations; the config file lets you choose which you'd like. As an example, there are two logging implementations - one that uses `logrus` and sends logs to STDOUT and a `honeycomb` implementation that sends the log messages to a Honeycomb dataset instead. Components with multiple implementations have one top level config item that lets you choose which implementation to use and then a section further down with additional config options for that choice (for example, the Honeycomb logger requires an API key).

When configuration changes, send Refinery a USR1 signal and it will re-read the configuration.

### Redis-based Peer Management

With peer management in Redis, all config options _except_ peer management are still handled by the config file.
Only coordinating the list of peers in the Refinery cluster is managed with Redis.

To enable the redis-based config:

- set PeerManagement.Type in the config file to "redis"

When launched in redis-config mode, Refinery needs a redis host to use for managing the list of peers in the Refinery cluster. This hostname and port can be specified in one of two ways:

- set the `REFINERY_REDIS_HOST` environment variable (and optionally the `REFINERY_REDIS_USERNAME` and `REFINERY_REDIS_PASSWORD` environment variables)
- set the `RedisHost` field in the config file (and optionally the `RedisUsername` and `RedisPassword` fields in the config file)

The Redis host should be a hostname and a port, for example `redis.mydomain.com:6379`. The example config file has `localhost:6379` which obviously will not work with more than one host. When TLS is required to connect to the Redis instance, set the `UseTLS` config to `true`.

By default, a Refinery process will register itself in Redis using its local hostname as its identifier for peer communications.
In environments where domain name resolution is slow or unreliable, override the reliance on name lookups by specifying the name of the peering network interface with the `IdentifierInterfaceName` configuration option.
See the [Refinery documentation](https://docs.honeycomb.io/manage-data-volume/refinery/) for more details on tuning a cluster.

## How sampling decisions are made

In the configuration file, you can choose from a few sampling methods and specify options for each. The `DynamicSampler` is the most interesting and most commonly used. It uses the `AvgSampleRate` algorithm from the [`dynsampler-go`](https://github.com/honeycombio/dynsampler-go) package. Briefly described, you configure Refinery to examine the trace for a set of fields (for example, `request.status_code` and `request.method`). It collects all the values found in those fields anywhere in the trace (eg "200" and "GET") together into a key it hands to the dynsampler. The dynsampler code will look at the frequency that key appears during the previous 30 seconds (or other value set by the `ClearFrequencySec` setting) and use that to hand back a desired sample rate. More frequent keys are sampled more heavily, so that an even distribution of traffic across the keyspace is represented in Honeycomb.

By selecting fields well, you can drop significant amounts of traffic while still retaining good visibility into the areas of traffic that interest you. For example, if you want to make sure you have a complete list of all URL handlers invoked, you would add the URL (or a normalized form) as one of the fields to include. Be careful in your selection though, because if the combination of fields creates a unique key each time, you won't sample out any traffic. Because of this it is not effective to use fields that have unique values (like a UUID) as one of the sampling fields. Each field included should ideally have values that appear many times within any given 30 second window in order to effectively turn in to a sample rate.

For more detail on how this algorithm works, please refer to the `dynsampler` package itself.

## Dry Run Mode

When getting started with Refinery or when updating sampling rules, it may be helpful to verify that the rules are working as expected before you start dropping traffic. By enabling dry run mode, all spans in each trace will be marked with the sampling decision in a field called `refinery_kept`. All traces will be sent to Honeycomb regardless of the sampling decision. You can then run queries in Honeycomb on this field to check your results and verify that the rules are working as intended. Enable dry run mode by adding `DryRun = true` in your configuration, as noted in `rules_complete.toml`.

When dry run mode is enabled, the metric `trace_send_kept` will increment for each trace, and the metric for `trace_send_dropped` will remain 0, reflecting that we are sending all traces to Honeycomb.

## Scaling Up

Refinery uses bounded queues and circular buffers to manage allocating traces, so even under high volume memory use shouldn't expand dramatically. However, given that traces are stored in a circular buffer, when the throughput of traces exceeds the size of the buffer, things will start to go wrong. If you have statistics configured, a counter named `collect_cache_buffer_overrun` will be incremented each time this happens. The symptoms of this will be that traces will stop getting accumulated together, and instead spans that should be part of the same trace will be treated as two separate traces. All traces will continue to be sent (and sampled) but the sampling decisions will be inconsistent so you'll wind up with partial traces making it through the sampler and it will be very confusing. The size of the circular buffer is a configuration option named `CacheCapacity`. To choose a good value, you should consider the throughput of traces (e.g. traces / second started) and multiply that by the maximum duration of a trace (say, 3 seconds), then multiply that by some large buffer (maybe 10x). This will give you good headroom.

Determining the number of machines necessary in the cluster is not an exact science, and is best influenced by watching for buffer overruns. But for a rough heuristic, count on a single machine using about 2G of memory to handle 5000 incoming events and tracking 500 sub-second traces per second (for each full trace lasting less than a second and an average size of 10 spans per trace).

## Understanding Regular Operation

Refinery emits a number of metrics to give some indication about the health of the process. These metrics can be exposed to Prometheus or sent up to Honeycomb. The interesting ones to watch are:

- Sample rates: how many traces are kept / dropped, and what does the sample rate distribution look like?
- [incoming|peer]_router_\*: how many events (no trace info) vs. spans (have trace info) have been accepted, and how many sent on to peers?
- collect_cache_buffer_overrun: this should remain zero; a positive value indicates the need to grow the size of the collector's circular buffer (via configuration `CacheCapacity`).
- process_uptime_seconds: records the uptime of each process; look for unexpected restarts as a key towards memory constraints.

## Troubleshooting

The default logging level of `warn` is almost entirely silent. The `debug` level emits too much data to be used in production, but contains excellent information in a pre-production environment. Setting the logging level to `debug` during initial configuration will help understand what's working and what's not, but when traffic volumes increase it should be set to `warn`.

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

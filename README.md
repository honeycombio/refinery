
# Samproxy - the Honeycomb Sampling Proxy

![samproxy](https://user-images.githubusercontent.com/1476820/47527709-0e185f00-d858-11e8-8e66-4fd5294d1918.png)

**Alpha Release** This is the initial draft. Please expect and help find bugs! :)  samproxy [![Build Status](https://travis-ci.org/honeycombio/samproxy.svg?branch=master)](https://travis-ci.org/honeycombio/samproxy)

## Purpose

Samproxy is a trace-aware sampling proxy. It collects spans emitted by your applications together in to traces and examines them as a whole in order to use all the information present about how your service handled a given request to make an intelligent decision about how to sample the trace - whether to keep or discard a specific trace. Buffering the spans together let you use fields that might be present in different spans together to influence the sampling decision. It might be that you have the HTTP status code available in the root span, but you want to use other information like whether this request was served from cache to influence sampling.

## Setting up Samproxy

Samproxy is designed to sit within your infrastructure where all sources of Honeycomb events (aka spans if you're doing tracing) can reach it. A standard deployment would include 2 or more servers running Samproxy accessible via a load balancer. Each instance of Samproxy must be configured with a list of all other running instances in order to balance the traces evenly across the cluster.

Within your application (or other Honeycomb event sources) you would configure the `API Host` to be http(s)://load-balancer/. Everything else remains the same (api key, dataset name, etc. - all that lives with the originating client).

### Builds

Samproxy is built by [Travis-CI](https://travis-ci.org/honeycombio/samproxy). Build artifacts are uploaded to S3 and available for download. The download URL is listed at the bottom of the build record.

## Configuration

There are a few vital configuration options:

- API Keys: Samproxy itself needs to be configured with a list of your API keys. This lets it respond with a 401/Unauthorized if an unexpected api key is used. You can configure Samproxy to accept all API keys by setting it to `*` but then you will lose the authentication feedback to your application. Samproxy will accept all events even if those events will eventually be rejected by the Honeycomb API due to an API key issue.

- Goal Sample Rate and a list of fields to use to generate the keys off which sample rate is chosen. This is where the power of the proxy comes in - being able to dynamically choose sample rates based on the contents of the traces as they go by. There is an overall default and dataset-specific sections for this configuration, so that different datasets can have different sets of fields and goal sample rates.

- Trace timeout - it should be set higher (maybe double?) the longest expected trace. If all of your traces complete in under 10 seconds, 30 is a good value here.  If you have traces that can last minutes, it should be raised accordingly. Note that the trace doesn't *have* to complete before this timer expires - but the sampling decision will be made at that time. So any spans that contain fields that you want to use to compute the sample rate should arrive before this timer expires. Additional spans that arrive after the timer has expired will be sent or dropped according to the sampling decision made when the timer expired.

- Peer list: this is a list of all the other servers participating in this Samproxy cluster. Traces are evenly distributed across all available servers, and any one trace must be concentrated on one server, regardless of which server handled the incoming spans. The peer list lets the cluster move spans around to the server that is handling the trace.

- Buffer size: The `InMemCollector`'s `CacheCapacity` setting determines how many in-flight traces you can have. This should be large enough to avoid overflow. Some multiple (2x, 3x) the total number of in-flight traces you expect is a good place to start. If it's too low you will see the `collect_cache_buffer_overrun` metric increment. If you see that, you should increase the size of the buffer.

There are a few components of Samproxy with multiple implementations; the config file lets you choose which you'd like. As an example, there are two logging implementations - one that uses `logrus` and sends logs to STDOUT and a `honeycomb` implementation that sends the log messages to a Honeycomb dataset instead. Components with multiple implementations have one top level config item that lets you choose which implementation to use and then a section further down with additional config options for that choice (for example, the Honeycomb logger requires an API key).

When configuration changes, send Samproxy a USR1 signal and it will re-read the configuration.

## Scaling Up

Samproxy uses bounded queues and circular buffers to manage allocating traces, so even under high volume memory use shouldn't expand dramatically. However, given that traces are stored in a circular buffer, when the throughput of traces exceeds the size of the buffer, things will start to go wrong. If you have stastics configured, a counter named `collect_cache_buffer_overrun` will be incremented each time this happens. The symptoms of this will be that traces will stop getting accumulated together, and instead spans that should be part of the same trace will be treated as two separate traces.  All traces will continue to be sent (and sampled) but the sampling decisions will be inconsistent so you'll wind up with partial traces making it through the sampler and it will be very confusing.  The size of the circular buffer is a configuration option named `CacheCapacity`. To choose a good value, you should consider the throughput of traces (eg traces / second started) and multiply that by the maximum duration of a trace (say, 3 seconds), then multiply that by some large buffer (maybe 10x). This will give you good headroom.

Determining the number of machines necessary in the cluster is not an exact science, and is best influenced by watching for buffer overruns. But for a rough heuristic, count on a single machine using about 2G of memory to handle 5000 incoming events and tracking 500 sub-second traces per second (for each full trace lasting less than a second and an average size of 10 spans per trace).

## Understanding Regular Operation

Samproxy emits a number of metrics to give some indication about the health of the process. These metrics can be exposed to Prometheus or sent up to Honeycomb. The interesting ones to watch are:

- Sample rates: how many traces are kept / dropped, and what does the sample rate distribution look like?
- router_*: how many events (no trace info) vs. spans (have trace info) have been accepted, and how many sent on to peers?
- collect_cache_buffer_overrun: this should remain zero; a positive value indicates the need to grow the size of the collector's circular buffer (via configuration `CacheCapacity`).

## Restarts

Samproxy does not yet buffer traces or sampling decisions to disk. When you restart the process all in-flight traces will be flushed (sent upstream to Honeycomb), but you will lose the record of past trace decisions. When started back up, it will start with a clean slate.

## Architecture of Samproxy itself (for contributors)

Code segmentation

Within each directory, the interface the dependency exports is in the file with the same name as the directory and then (for the most part) each of the other files are alternative implementations of that interface.  For example, in `logger`, `/logger/logger.go` contains the interface definition and `logger/honeycomb.go` contains the implementation of the `logger` interface that will send logs to Honeycomb.

`main.go` sets up the app and makes choices about which versions of dependency implementations to use (eg which logger, which sampler, etc.) It starts up everything and then launches `App`

`app/app.go` is the main control point. When its `Start` function ends, the program shuts down. It launches a `Router` which will be the primary listener for incoming events.

`route/route.go` listens on the network for incoming traffic. It handles traffic depending on two things: is this incoming request an event (or batch of events), and if so, does it have a trace ID? Everything that is not an event or an event that does not have a trace ID is immediately handed to `transmission` to be forwarded on to Honeycomb. If it is an event, the router extracts the trace ID and then uses the `sharder` to decide which member of the Samproxy cluster should handle this trace. If it's a peer, the event will be forwarded to that peer. If it's us, the event will be transformed in to an internal representation and handed to the `collector` to bundle up spans in to traces.

`collect/collect.go` the collector is responsible for bundling spans together in to traces and deciding when to send them to Honeycomb or if they should be dropped. The first time a trace ID is seen, the collector starts a timer. When that timer expires, the trace will be sent, whether or not it is complete. The arrival of the root span (aka a span with a trace ID and no parent ID) indicates the trace is complete. When that happens, the trace is sent and the timer canceled. Just before sending, the collector asks the `sampler` to give it a sample rate and whether to keep the trace. The collector obeys this sampling decision and records it (the record is applied to any spans that may come in as part of the trace after the decision has been made). After making the sampling decision, if the trace is to be kept, it is passed along to the `transmission` for actual sending.

`transmit/transmit.go` is a wrapper around the HTTP interactions with the Honeycomb API. It handles batching events together and sending them upstream.

`logger` and `metrics` are for managing the logs and metrics that Samproxy itself produces.

`sampler` contains algorithms to compute sample rates based on the traces provided.

`sharder` determines which peer in a clustered Samproxy config is supposed to handle and individual trace.

`types` contains a few type definitions that are used to hand data in between packages.

`process` is not yet used, but is a placeholder for when (or if) Samproxy learns to modify the traces that it handles. Examples of processing that might take place: scrubbing fields, adding fields, moving fields from one span to another, and so on.



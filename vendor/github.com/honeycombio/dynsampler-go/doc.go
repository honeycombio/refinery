/*
Package dynsampler contains several sampling algorithms to help you select a representative set of events instead of a full stream.

This package is intended to help sample a stream of tracking events, where events are typically created in response to a stream of traffic (for the purposes of logging or debugging). In general, sampling is used to reduce the total volume of events necessary to represent the stream of traffic in a meaningful way.

For the purposes of these examples, the "traffic" will be a set of HTTP requests being handled by a server, and "event" will be a blob of metadata about a given HTTP request that might be useful to keep track of later. A "sample rate" of 100 means that for every 100 requests, we capture a single event and indicate that it represents 100 similar requests.

Use

Use the `Sampler` interface in your code.  Each different sampling algorithm implements the Sampler interface.

The following guidelines can help you choose a sampler. Depending on the shape of your traffic, one may serve better than another, or you may need to write a new one! Please consider contributing it back to this package if you do.

* If your system has a completely homogeneous stream of requests: use `Static` to use a constant sample rate.

* If your system has a steady stream of requests and a well-known low cardinality partition key (e.g. http status): use `Static` and override sample rates on a per-key basis (e.g. if you know want to sample `HTTP 200/OK` events at a different rate from `HTTP 503/Server Error`).

* If your logging system has a strict cap on the rate it can receive events, use `TotalThroughput`, which will calculate sample rates based on keeping *the entire system's* representative event throughput right around (or under) particular cap.

* If your system has a rough cap on the rate it can receive events and your partitioned keyspace is fairly steady, use `PerKeyThroughput`, which will calculate sample rates based on keeping the event throughput roughly constant *per key/partition* (e.g. per user id)

* The best choice for a system with a large key space and a large disparity between the highest volume and lowest volume keys is `AvgSampleRateWithMin` - it will increase the sample rate of higher volume traffic proportionally to the logarithm of the specific key's volume. If total traffic falls below a configured minimum, it stops sampling to avoid any sampling when the traffic is too low to warrant it.

* `EMASampleRate` works like `AvgSampleRate`, but calculates sample rates based on a moving average (Exponential Moving Average) of many measurement intervals rather than a single isolated interval. In addition, it can detect large bursts in traffic and will trigger a recalculation of sample rates before the regular interval.

Each sampler implementation below has additional configuration parameters and a
detailed description of how it chooses a sample rate.

Some implementations implement `SaveState` and `LoadState` - enabling you to serialize the Sampler's internal state
and load it back. This is useful, for example, if you want to avoid losing calculated sample rates between process
restarts.

*/
package dynsampler

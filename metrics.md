Name: "collect_cache_buffer_overrun"
Type: metrics.Counter
Unit: "count"
Description: "The number of times the trace overwritten in the circular buffer has not yet been sent"

Name: "collect_cache_capacity"
Type: metrics.Gauge
Unit: "count"
Description: "The number of traces that can be stored in the cache"

Name: "collect_cache_entries"
Type: metrics.Histogram
Unit: "count"
Description: "The number of traces currently stored in the cache"

Name: cuckoo_current_capacity
Type: metrics.Gauge
Unit: "trace"
Description: "current capacity of the cuckoo filter"

Name: cuckoo_future_load_factor
Type: metrics.Gauge
Unit: "percentage"
Description: "the fraction of slots occupied in the future cuckoo filter"

Name: cuckoo_current_load_factor
Type: metrics.Gauge
Unit: "percentage"
Description: "the fraction of slots occupied in the current cuckoo filter"

Name: "cache_recent_dropped_traces"
Type: metrics.Gauge
Unit: "traces"
Description: "the current size of the most recent dropped trace cache"

Name: "collect_sent_reasons_cache_entries"
Type: metrics.Histogram
Unit: "count"
Description: "Number of entries in the sent reasons cache"

Name: "is_ready"
Type: metrics.Gauge
Unit: "bool"
Description: "Whether the system is ready to receive traffic"

Name: "is_alive"
Type: metrics.Gauge
Unit: "bool"
Description: "Whether the system is alive and reporting in"

Name: "redis_pubsub_published"
Type: metrics.Counter
Unit: "count"
Description: "Number of messages published to Redis PubSub"

Name: "redis_pubsub_received"
Type: metrics.Counter
Unit: "count"
Description: "Number of messages received from Redis PubSub"

Name: "local_pubsub_published"
Type: metrics.Counter
Unit: "messages"
Description: "The total number of messages sent via the local pubsub implementation"

Name: "local_pubsub_received"
Type: metrics.Counter
Unit: "messages"
Description: "The total number of messages received via the local pubsub implementation"

Name: "num_file_peers"
Type: metrics.Gauge
Unit: "peers"
Description: "Number of peers in the file peer list"

Name: "num_peers"
Type: metrics.Gauge
Unit: "peers"
Description: "the active number of peers in the cluster"

Name: "peer_hash"
Type: metrics.Gauge
Unit: "hash"
Description: "the hash of the current list of peers"

Name: "peer_messages"
Type: metrics.Counter
Unit: "messages"
Description: "the number of messages received by the peers service"

Name: "_num_dropped_by_drop_rule"
Type: metrics.Counter
Unit: "count"
Description: "Number of traces dropped by the drop rule"

Name: "_num_dropped"
Type: metrics.Counter
Unit: "count"
Description: "Number of traces dropped by configured sampler"

Name: "_num_kept"
Type: metrics.Counter
Unit: "count"
Description: "Number of traces kept by configured sampler"

Name: "_sample_rate"
Type: metrics.Histogram
Unit: "count"
Description: "Sample rate for traces"

Name: enqueue_errors
Type: metrics.Counter
Unit: "count"
Description: "The number of errors encountered when enqueueing events"

Name: response_20x
Type: metrics.Counter
Unit: "count"
Description: "The number of successful responses from Honeycomb"

Name: response_errors
Type: metrics.Counter
Unit: "count"
Description: "The number of errors encountered when sending events to Honeycomb"

Name: queued_items
Type: metrics.UpDown
Unit: "count"
Description: "The number of events queued for transmission to Honeycomb"

Name: queue_time
Type: metrics.Histogram
Unit: "microsecond"
Description: "The time spent in the queue before being sent to Honeycomb"

Name: "trace_duration_ms"
Type: metrics.Histogram

Name: "trace_span_count"
Type: metrics.Histogram

Name: "collector_incoming_queue"
Type: metrics.Histogram

Name: "collector_peer_queue_length"
Type: metrics.Gauge

Name: "collector_incoming_queue_length"
Type: metrics.Gauge

Name: "collector_peer_queue"
Type: metrics.Histogram

Name: "collector_cache_size"
Type: metrics.Gauge

Name: "memory_heap_allocation"
Type: metrics.Gauge

Name: "span_received"
Type: metrics.Counter

Name: "span_processed"
Type: metrics.Counter

Name: "spans_waiting"
Type: metrics.UpDown

Name: "trace_sent_cache_hit"
Type: metrics.Counter

Name: "trace_accepted"
Type: metrics.Counter

Name: "trace_send_kept"
Type: metrics.Counter

Name: "trace_send_dropped"
Type: metrics.Counter

Name: "trace_send_has_root"
Type: metrics.Counter

Name: "trace_send_no_root"
Type: metrics.Counter

Name: "trace_forwarded_on_peer_change"
Type: metrics.Gauge

Name: "trace_redistribution_count"
Type: metrics.Gauge

Name: "trace_send_on_shutdown"
Type: metrics.Counter

Name: "trace_forwarded_on_shutdown"
Type: metrics.Counter

Name: trace_send_got_root
Type: metrics.Counter

Name: trace_send_expired
Type: metrics.Counter

Name: trace_send_span_limit
Type: metrics.Counter

Name: trace_send_ejected_full
Type: metrics.Counter

Name: trace_send_ejected_memsize
Type: metrics.Counter

Name: trace_send_late_span
Type: metrics.Counter

Name: "dropped_from_stress"
Type: metrics.Counter

Name: "cluster_stress_level"
Type: metrics.Gauge

Name: "individual_stress_level"
Type: metrics.Gauge

Name: "stress_level"
Type: metrics.Gauge

Name: "stress_relief_activated"
Type: metrics.Gauge

Name: "_router_proxied"
Type: metrics.Counter
Unit: "count"
Description: "the number of events proxied to another refinery"

Name: "_router_event"
Type: metrics.Counter
Unit: "count"
Description: "the number of events received"

Name: "config_hash"
Type: metrics.Gauge
Unit: "hash"
Description: "The hash of the current configuration"

Name: "rule_config_hash"
Type: metrics.Gauge
Unit: "hash"
Description: "The hash of the current rules configuration"

Name: "queue_length"
Type: metrics.Gauge
Unit: "count"
Description: "number of events waiting to be sent to destination"

Name: "queue_overflow"
Type: metrics.Counter
Unit: "count"
Description: "number of events dropped due to queue overflow"

Name: "send_errors"
Type: metrics.Counter
Unit: "count"
Description: "number of errors encountered while sending events to destination"

Name: "send_retries"
Type: metrics.Counter
Unit: "count"
Description: "number of times a batch of events was retried"

Name: "batches_sent"
Type: metrics.Counter
Unit: "count"
Description: "number of batches of events sent to destination"

Name: "messages_sent"
Type: metrics.Counter
Unit: "count"
Description: "number of messages sent to destination"

Name: "response_decode_errors"
Type: metrics.Counter
Unit: "count"
Description: "number of errors encountered while decoding responses from destination"


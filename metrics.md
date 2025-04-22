# Honeycomb Refinery Metrics Documentation

This document contains the description of various metrics used in Refinery.
It was automatically generated on 2025-04-22 at 04:33:15 UTC.

Note: This document does not include metrics defined in the dynsampler-go dependency, as those metrics are generated dynamically at runtime. As a result, certain metrics may be missing or incomplete in this document, but they will still be available during execution with their full names.

## Complete Metrics
This table includes metrics with fully defined names.

| Name | Type | Unit | Description |
|------|------|------|-------------|
| is_ready | Gauge | Dimensionless | Whether the system is ready to receive traffic |
| is_alive | Gauge | Dimensionless | Whether the system is alive and reporting in |
| collect_cache_entries | Histogram | Dimensionless | The number of traces currently stored in the cache |
| cuckoo_current_capacity | Gauge | Dimensionless | current capacity of the cuckoo filter |
| cuckoo_future_load_factor | Gauge | Percent | the fraction of slots occupied in the future cuckoo filter |
| cuckoo_current_load_factor | Gauge | Percent | the fraction of slots occupied in the current cuckoo filter |
| cuckoo_addqueue_full | Counter | Dimensionless | the number of times the add queue was full and a drop decision was dropped |
| cuckoo_addqueue_locktime_uS | Histogram | Microseconds | the time spent holding the add queue lock |
| cache_recent_dropped_traces | Gauge | Dimensionless | the current size of the most recent dropped trace cache |
| collect_sent_reasons_cache_entries | Histogram | Dimensionless | Number of entries in the sent reasons cache |
| redis_pubsub_published | Counter | Dimensionless | Number of messages published to Redis PubSub |
| redis_pubsub_received | Counter | Dimensionless | Number of messages received from Redis PubSub |
| local_pubsub_published | Counter | Dimensionless | The total number of messages sent via the local pubsub implementation |
| local_pubsub_received | Counter | Dimensionless | The total number of messages received via the local pubsub implementation |
| num_file_peers | Gauge | Dimensionless | Number of peers in the file peer list |
| num_peers | Gauge | Dimensionless | the active number of peers in the cluster |
| peer_hash | Gauge | Dimensionless | the hash of the current list of peers |
| peer_messages | Counter | Dimensionless | the number of messages received by the peers service |
| trace_duration_ms | Histogram | Milliseconds | time taken to process a trace from arrival to send |
| trace_span_count | Histogram | Dimensionless | number of spans in a trace |
| collector_incoming_queue | Histogram | Dimensionless | number of spans currently in the incoming queue |
| collector_peer_queue_length | Gauge | Dimensionless | number of spans in the peer queue |
| collector_incoming_queue_length | Gauge | Dimensionless | number of spans in the incoming queue |
| collector_peer_queue | Histogram | Dimensionless | number of spans currently in the peer queue |
| collector_cache_size | Gauge | Dimensionless | number of traces currently stored in the trace cache |
| memory_heap_allocation | Gauge | Bytes | current heap allocation |
| span_received | Counter | Dimensionless | number of spans received by the collector |
| span_processed | Counter | Dimensionless | number of spans processed by the collector |
| spans_waiting | UpDown | Dimensionless | number of spans waiting to be processed by the collector |
| trace_sent_cache_hit | Counter | Dimensionless | number of late spans received for traces that have already been sent |
| trace_accepted | Counter | Dimensionless | number of new traces received by the collector |
| trace_send_kept | Counter | Dimensionless | number of traces that has been kept |
| trace_send_dropped | Counter | Dimensionless | number of traces that has been dropped |
| trace_send_has_root | Counter | Dimensionless | number of kept traces that have a root span |
| trace_send_no_root | Counter | Dimensionless | number of kept traces that do not have a root span |
| trace_forwarded_on_peer_change | Gauge | Dimensionless | number of traces forwarded due to peer membership change |
| trace_redistribution_count | Gauge | Dimensionless | number of traces redistributed due to peer membership change |
| trace_send_on_shutdown | Counter | Dimensionless | number of traces sent during shutdown |
| trace_forwarded_on_shutdown | Counter | Dimensionless | number of traces forwarded during shutdown |
| trace_send_got_root | Counter | Dimensionless | number of traces that are ready for decision due to root span arrival |
| trace_send_expired | Counter | Dimensionless | number of traces that are ready for decision due to TraceTimeout or SendDelay |
| trace_send_span_limit | Counter | Dimensionless | number of traces that are ready for decision due to span limit |
| trace_send_ejected_full | Counter | Dimensionless | number of traces that are ready for decision due to cache capacity overrun |
| trace_send_ejected_memsize | Counter | Dimensionless | number of traces that are ready for decision due to memory overrun |
| trace_send_late_span | Counter | Dimensionless | number of spans that are sent due to late span arrival |
| dropped_from_stress | Counter | Dimensionless | number of spans dropped due to stress relief |
| kept_from_stress | Counter | Dimensionless | number of spans kept due to stress relief |
| trace_kept_sample_rate | Histogram | Dimensionless | sample rate of kept traces |
| trace_aggregate_sample_rate | Histogram | Dimensionless | aggregate sample rate of both kept and dropped traces |
| collector_redistribute_traces_duration_ms | Histogram | Milliseconds | duration of redistributing traces to peers |
| collector_collect_loop_duration_ms | Histogram | Milliseconds | duration of the collect loop, the primary event processing goroutine |
| collector_send_expired_traces_in_cache_dur_ms | Histogram | Milliseconds | duration of sending expired traces in cache |
| collector_outgoing_queue | Histogram | Dimensionless | number of traces waiting to be send to upstream |
| collector_drop_decision_batch_count | Histogram | Dimensionless | number of drop decisions sent in a batch |
| collector_expired_traces_missing_decisions | Gauge | Dimensionless | number of decision spans forwarded for expired traces missing trace decision |
| collector_expired_traces_orphans | Gauge | Dimensionless | number of expired traces missing trace decision when they are sent |
| drop_decision_batches_received | Counter | Dimensionless | number of drop decision batches received |
| kept_decision_batches_received | Counter | Dimensionless | number of kept decision batches received |
| drop_decisions_received | Counter | Dimensionless | total number of drop decisions received |
| kept_decisions_received | Counter | Dimensionless | total number of kept decisions received |
| collector_kept_decisions_queue_full | Counter | Dimensionless | number of times kept trace decision queue is full |
| collector_drop_decisions_queue_full | Counter | Dimensionless | number of times drop trace decision queue is full |
| cluster_stress_level | Gauge | Dimensionless | The overall stress level of the cluster |
| individual_stress_level | Gauge | Dimensionless | The stress level of the individual node |
| stress_level | Gauge | Dimensionless | The stress level that's being used to determine whether to activate stress relief |
| stress_relief_activated | Gauge | Dimensionless | Whether stress relief is currently activated |
| config_hash | Gauge | Dimensionless | The hash of the current configuration |
| rule_config_hash | Gauge | Dimensionless | The hash of the current rules configuration |


## Metrics with Prefix
This table includes metrics with partially defined names.
Metrics in this table don't contain their expected prefixes. This is because the auto-generator is unable to resolve dynamically created metric names during the generation process.

| Name | Type | Unit | Description |
|------|------|------|-------------|
| _num_dropped_by_drop_rule | Counter | Dimensionless | Number of traces dropped by the drop rule |
| _num_dropped | Counter | Dimensionless | Number of traces dropped by configured sampler |
| _num_kept | Counter | Dimensionless | Number of traces kept by configured sampler |
| _sample_rate | Histogram | Dimensionless | Sample rate for traces |
| _sampler_key_cardinality | Histogram | Dimensionless | Number of unique keys being tracked by the sampler |
| enqueue_errors | Counter | Dimensionless | The number of errors encountered when enqueueing events |
| response_20x | Counter | Dimensionless | The number of successful responses from Honeycomb |
| response_errors | Counter | Dimensionless | The number of errors encountered when sending events to Honeycomb |
| queued_items | UpDown | Dimensionless | The number of events queued for transmission to Honeycomb |
| queue_time | Histogram | Microseconds | The time spent in the queue before being sent to Honeycomb |
| _router_proxied | Counter | Dimensionless | the number of events proxied to another refinery |
| _router_event | Counter | Dimensionless | the number of events received |
| _router_event_bytes | Histogram | Bytes | the number of bytes per event received |
| _router_span | Counter | Dimensionless | the number of spans received |
| _router_dropped | Counter | Dimensionless | the number of events dropped because the channel was full |
| _router_nonspan | Counter | Dimensionless | the number of non-span events received |
| _router_peer | Counter | Dimensionless | the number of spans proxied to a peer |
| _router_batch | Counter | Dimensionless | the number of batches of events received |
| _router_otlp | Counter | Dimensionless | the number of batches of otlp requests received |
| bytes_received_traces | Counter | Bytes | the number of bytes received in trace events |
| bytes_received_logs | Counter | Bytes | the number of bytes received in log events |
| queue_length | Gauge | Dimensionless | number of events waiting to be sent to destination |
| queue_overflow | Counter | Dimensionless | number of events dropped due to queue overflow |
| send_errors | Counter | Dimensionless | number of errors encountered while sending events to destination |
| send_retries | Counter | Dimensionless | number of times a batch of events was retried |
| batches_sent | Counter | Dimensionless | number of batches of events sent to destination |
| messages_sent | Counter | Dimensionless | number of messages sent to destination |
| response_decode_errors | Counter | Dimensionless | number of errors encountered while decoding responses from destination |

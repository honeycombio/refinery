# The Flow of Data Through Refinery

```mermaid
flowchart TB
    subgraph Ingestion["📥 Ingestion Layer"]
        HTTP["HTTP Endpoints<br/>/1/event, /1/batch<br/>/v1/traces, /v1/logs"]
        GRPC["gRPC Endpoints<br/>OTLP TraceService"]
        PeerIn["Peer Incoming<br/>(from other Refinery nodes)"]
    end

    subgraph Router["🔀 Router (route.go)"]
        Decompress["Decompress<br/>(gzip/zstd)"]
        Unmarshal["Unmarshal<br/>(JSON/msgpack/protobuf)"]
        Extract["Extract Metadata<br/>(API key, dataset, trace ID)"]
        HasTraceID{"Has TraceID?"}
        StressCheck{"Stressed?"}
        ShardCheck{"My Shard?"}
        RouterType{"Router Type?"}
    end

    subgraph Collector["📦 InMemCollector (collect.go)"]
        WorkerHash["Hash TraceID<br/>to Worker"]

        subgraph Workers["👷 CollectorWorkers (parallel)"]
            subgraph Worker0["Worker 0"]
                W0Incoming["incoming channel"]
                W0Peer["fromPeer channel"]

                subgraph W0Loop["collect() loop"]
                    W0Select{"select"}
                    W0ProcessSpan["processSpan()"]
                    W0Ticker["ticker"]
                    W0SendEarly["sendEarly<br/>(from memory monitor)"]
                    W0ExpiredTraces["sendExpiredTracesInCache()"]
                    W0TracesEarly["sendTracesEarly()"]
                end

                W0Cache["Trace Cache"]
                W0SentCache["Sent Cache"]

                subgraph W0Triggers["Decision Triggers"]
                    TriggerRoot["TraceSendGotRoot"]
                    TriggerLimit["TraceSendSpanLimit"]
                    TriggerExpired["TraceSendExpired"]
                    TriggerMemory["TraceSendEjectedMemsize"]
                end
            end

            subgraph WorkerN["Worker N..."]
                WNIncoming["incoming channel"]
                WNPeer["fromPeer channel"]
                WNLoop["collect() loop"]
                WNCache["Trace Cache"]
                WNSentCache["Sent Cache"]
            end
        end

        TracesToSend["tracesToSend channel"]
        SendTraces["sendTraces()"]
    end

    subgraph Sampling["🎯 Sampling Decision (per worker)"]
        GetSampler["Get Sampler<br/>for dataset/env"]
        Samplers["Samplers:<br/>• Deterministic<br/>• Dynamic/EMA<br/>• Rules-based<br/>• Throughput"]
        MakeDecision["makeDecision()<br/>rate, keep, reason"]
    end

    subgraph Transmission["📤 Transmission Layer"]
        DecorateSpans["Decorate Spans<br/>(sample rate, metadata)"]
        Batch["Batch by Destination<br/>(apiHost + apiKey + dataset)"]
        Serialize["Serialize (msgpack)<br/>Compress (zstd)"]
        UpstreamTX["Upstream<br/>Transmission"]
        PeerTX["Peer<br/>Transmission"]
    end

    subgraph Destinations["🎯 Destinations"]
        Honeycomb["Honeycomb API"]
        PeerNodes["Peer Refinery Nodes"]
    end

    subgraph StressRelief["⚡ Stress Relief"]
        ImmediateDecision["ProcessSpanImmediately()<br/>(deterministic sampling)"]
        ProbeCheck{"My Shard?"}
    end

    %% Ingestion flow - all sources go through same router flow
    HTTP --> Decompress
    GRPC --> Decompress
    PeerIn --> Decompress
    Decompress --> Unmarshal
    Unmarshal --> Extract
    Extract --> HasTraceID

    %% No trace ID - direct upstream
    HasTraceID -->|"No"| UpstreamTX

    %% Has trace ID - check stress
    HasTraceID -->|"Yes"| StressCheck

    %% Stress relief path - bypasses collector entirely
    StressCheck -->|"Yes"| ImmediateDecision
    ImmediateDecision -->|"kept → send upstream"| UpstreamTX
    ImmediateDecision -->|"kept → probe"| ProbeCheck
    ImmediateDecision -->|"dropped"| Drop1["Drop"]
    ProbeCheck -->|"No - notify peer"| PeerTX
    ProbeCheck -->|"Yes - done"| Drop2["Done"]

    %% Normal path - check shard ownership
    StressCheck -->|"No"| ShardCheck
    ShardCheck -->|"No - different peer"| PeerTX
    ShardCheck -->|"Yes - mine"| RouterType

    %% Router type determines method, then hash to worker
    RouterType -->|"IncomingRouter"| WorkerHash
    RouterType -->|"PeerRouter"| WorkerHash

    %% Hash routes to specific worker's channel
    WorkerHash -->|"AddSpan()"| W0Incoming
    WorkerHash -->|"AddSpan()"| WNIncoming
    WorkerHash -->|"AddSpanFromPeer()"| W0Peer
    WorkerHash -->|"AddSpanFromPeer()"| WNPeer

    %% Worker 0 detailed processing
    W0Incoming --> W0Select
    W0Peer --> W0Select
    W0Ticker --> W0Select
    W0SendEarly --> W0Select

    W0Select -->|"span"| W0ProcessSpan
    W0ProcessSpan --> W0Cache

    W0Select -->|"tick"| W0ExpiredTraces
    W0ExpiredTraces --> W0Cache

    W0Select -->|"memory pressure"| W0TracesEarly
    W0TracesEarly --> W0Cache

    %% Decision triggers from cache
    W0Cache -->|"root arrived"| TriggerRoot
    W0Cache -->|"span limit"| TriggerLimit
    W0Cache -->|"timeout"| TriggerExpired
    W0Cache -->|"eviction"| TriggerMemory

    TriggerRoot --> GetSampler
    TriggerLimit --> GetSampler
    TriggerExpired --> GetSampler
    TriggerMemory --> GetSampler

    %% Worker N (simplified)
    WNIncoming --> WNLoop
    WNPeer --> WNLoop
    WNLoop --> WNCache

    %% Sampling (shown once, applies to all workers)
    GetSampler --> Samplers
    Samplers --> MakeDecision
    MakeDecision -->|"record"| W0SentCache

    %% Workers send to shared channel
    MakeDecision -->|"kept"| TracesToSend
    TracesToSend --> SendTraces

    %% Transmission
    SendTraces --> DecorateSpans
    DecorateSpans --> Batch
    Batch --> Serialize
    Serialize --> UpstreamTX

    %% Destinations
    UpstreamTX --> Honeycomb
    PeerTX --> PeerNodes

    %% Late spans
    W0SentCache -.->|"late span<br/>lookup"| W0Loop

```

## Key Data Flow Summary

| Stage | Component | Description |
|-------|-----------|-------------|
| **Ingestion** | `route/route.go` | HTTP/gRPC endpoints receive spans, decompress, and unmarshal |
| **Routing** | `route/route.go:689-803` | Decides: direct send (no trace ID), stress relief, peer forward, or collect |
| **Collection** | `collect/collect.go` | Manager routes spans to workers based on trace ID hash |
| **Workers** | `collect/collector_worker.go` | Parallel workers assemble traces and make sampling decisions |
| **Transmission** | `transmit/direct_transmit.go` | Batches and sends kept spans to Honeycomb |

## Multi-Worker Architecture

The InMemCollector now uses multiple parallel `CollectorWorker` instances:

| Component | Description |
|-----------|-------------|
| **InMemCollector** | Manager that routes spans and coordinates workers |
| **CollectorWorker** | Independent worker with own cache, channels, and collect loop |
| **Worker Assignment** | `hash(traceID) % numWorkers` ensures all spans for a trace go to same worker |

Each worker has:

- **incoming channel** - spans from external clients (via `AddSpan()`)
- **fromPeer channel** - spans from peer Refinery nodes (via `AddSpanFromPeer()`)
- **Trace Cache** - assembles spans into traces
- **Sent Cache** - records sampling decisions for late-arriving spans
- **collect() loop** - processes spans, makes decisions, sends to `tracesToSend`

## Two Router Instances

Refinery runs two separate Router instances that share the same processing logic:

| Router | Listens On | Source | Collector Method |
|--------|------------|--------|------------------|
| **IncomingRouter** | `ListenAddr` | External clients | `AddSpan()` → worker's `incoming` channel |
| **PeerRouter** | `PeerListenAddr` | Other Refinery nodes | `AddSpanFromPeer()` → worker's `fromPeer` channel |

Both routers go through the same flow (decompress → unmarshal → extract → stress check → shard check), then the InMemCollector hashes the trace ID to select a worker.

## Stress Relief

When stressed, `ProcessSpanImmediately()` makes an immediate deterministic decision:

- **Kept spans** are sent directly upstream (bypassing the collector entirely)
- A **probe** is forwarded to the owning peer if it's a different shard (to inform them of the decision)
- **Dropped spans** are discarded immediately

Stress relief completely bypasses the workers to reduce memory pressure.

## Decision Triggers

Within each worker's `collect()` loop, traces are sent for sampling decision based on these triggers:

| Trigger | Metric | Condition |
|---------|--------|-----------|
| **TraceSendGotRoot** | `trace_send_got_root` | Root span arrived, trace ready after SendDelay |
| **TraceSendSpanLimit** | `trace_send_span_limit` | Trace exceeded configured SpanLimit |
| **TraceSendExpired** | `trace_send_expired` | Trace timeout (TraceTimeout) reached without root |
| **TraceSendEjectedMemsize** | `trace_send_ejected_memsize` | Memory pressure triggered coordinated eviction |

The `collect()` loop processes these via:
- **processSpan()** - adds spans to cache, marks trace for sending if root or limit hit
- **sendExpiredTracesInCache()** - called on ticker, finds traces past SendBy time
- **sendTracesEarly()** - called when memory monitor signals eviction needed

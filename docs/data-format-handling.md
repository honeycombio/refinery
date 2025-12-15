# Refinery Data Ingest and Format Handling

## Overview

This document provides visual documentation of how Refinery **ingests** telemetry data in different formats and converts them to an internal representation. This focuses specifically on the data ingest paths - how data arrives, gets parsed, converted, and prepared for internal processing.

Refinery supports two primary protocols:

- **Libhoney Events**: Honeycomb's legacy protocol using JSON or MessagePack serialization
- **OTLP (OpenTelemetry Protocol)**: Industry-standard protocol supporting JSON and Protocol Buffers (protobuf) over HTTP and gRPC

All incoming formats are normalized to an internal representation (`types.Event`) with MessagePack-backed payloads for efficient processing.

## Table of Contents

1. [High-Level Protocol Comparison](#1-high-level-protocol-comparison)
2. [Libhoney Event Processing Flow](#2-libhoney-event-processing-flow)
   - [2a. Single Event Endpoint](#2a-single-event-endpoint-1eventsdataset)
   - [2b. Batch Endpoint](#2b-batch-endpoint-1batchdataset)
3. [OTLP HTTP Processing Flow](#3-otlp-http-processing-flow)
4. [OTLP gRPC Processing Flow](#4-otlp-grpc-processing-flow)
5. [Format Conversion Details](#5-format-conversion-details)
6. [Internal Data Structures](#6-internal-data-structures)

---

## 1. High-Level Protocol Comparison

This diagram shows the different protocols and formats that Refinery accepts, and how they all converge to a common internal representation.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          CLIENT LIBRARIES                                │
├──────────────────────────────┬──────────────────────────────────────────┤
│   libhoney-go / libhoney-js  │      OpenTelemetry SDK                   │
└──────────────┬───────────────┴────────────────┬─────────────────────────┘
               │                                │
               ├─ JSON ────────┐                ├─ JSON ────────┐
               └─ MessagePack ─┤                └─ Protobuf ────┤
                                │                                │
┌───────────────────────────────▼────────────────────────────────▼─────────┐
│                         INPUT FORMATS                                     │
├───────────────────────────────┬───────────────────────────────────────────┤
│  Libhoney JSON                │  OTLP JSON                                │
│  Libhoney MessagePack         │  OTLP Protobuf                            │
└───────────────┬───────────────┴───────────────┬───────────────────────────┘
                │                               │
                │ HTTP                          │ HTTP / gRPC
                │                               │
┌───────────────▼───────────────────────────────▼───────────────────────────┐
│                      REFINERY ENDPOINTS                                    │
├───────────────────────────────┬────────────────────────────────────────────┤
│  /1/events/{dataset}          │  /v1/traces                                │
│  /1/batch/{dataset}           │  (HTTP + gRPC)                             │
│  (HTTP only)                  │                                            │
└───────────────┬───────────────┴────────────────┬───────────────────────────┘
                │                                │
                └────────────┬───────────────────┘
                             │
                             │ Convert to MessagePack
                             │
┌────────────────────────────▼───────────────────────────────────────────────┐
│                   INTERNAL REPRESENTATION                                   │
│                                                                             │
│              types.Event
│              (MessagePack-backed Payload)                                   │
│                                                                             │
│              Ready for internal processing                                  │
│              (sharder, collector, sampling, transmission)                   │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Key Points:**
- **Libhoney**: HTTP-only, supports JSON and MessagePack
- **OTLP Traces**: HTTP and gRPC, supports JSON and Protobuf
- **Common Format**: Trace/span data converges to MessagePack-backed internal types
- **Scope**: This document covers trace/span ingest paths (logs are not converted to MessagePack)

---

## 2. Libhoney Event Processing Flow

Refinery provides **two separate endpoints** for libhoney events, each with different optimization strategies:

- **`/1/events/{dataset}`** - Single event endpoint (simpler processing)
- **`/1/batch/{dataset}`** - Batch endpoint (optimized for high throughput)

### 2a. Single Event Endpoint (`/1/events/{dataset}`)

This endpoint handles individual events with a straightforward unmarshaling path. Both JSON and MessagePack formats go through the same generic `unmarshal()` function into `map[string]interface{}`.

```
                    HTTP Request
                         │
                         │ POST /1/events/{dataset}
                         │
                         ▼
            ┌────────────────────────┐
            │  Extract HTTP Headers  │
            ├────────────────────────┤
            │ • X-Honeycomb-Team     │
            │ • X-Honeycomb-Dataset  │
            │ • Content-Encoding     │
            └────────────┬───────────┘
                         │
                         ▼
            ┌────────────────────────┐
            │ Decompression Needed?  │
            └─┬──────────┬──────────┬┘
              │          │          │
      ┌───────▼──┐  ┌───▼────┐      │
      │   gzip   │  │  zstd  │      none
      │ decomp.  │  │ decomp.│      │
      └─────┬────┘  └────┬───┘      │
            └───────┬────┘          │
                    └────────────── ┘
                         │
                         ▼
            ┌────────────────────────┐
            │  Content-Type Check    │
            └─┬────────────────────┬─┘
              │                    │
      ┌───────▼─────────┐   ┌──────▼──────────┐
      │      JSON       │   │   MessagePack   │
      └───────┬─────────┘   └──────┬──────────┘
              │                    │
              │ Both formats go    │
              │ through generic    │
              ▼ unmarshal()        ▼
      ┌────────────────────────────────────────┐
      │   unmarshal() to map[string]interface{}│
      │        route/route.go:501              │
      └───────────────┬────────────────────────┘
                      │
                      │ Full map allocation
                      │ (less optimized)
                      ▼
      ┌────────────────────────────────────────┐
      │  types.NewPayload(config, data)        │
      │     Creates Payload from map           │
      └───────────────┬────────────────────────┘
                      │
                      ▼
            ┌────────────────────────┐
            │   Create types.Event   │
            │   route/route.go:508   │
            └────────────┬───────────┘
                         │
                         ▼
            ┌────────────────────────┐
            │   ExtractMetadata()    │
            ├────────────────────────┤
            │ • Extract trace ID     │
            │ • Extract parent ID    │
            │ • Extract signal type  │
            └────────────┬───────────┘
                         │
                         ▼
        [Event ready for internal processing]
```

**Key Characteristics:**

- **Simple Path**: Both JSON and MessagePack formats go through `unmarshal()` to `map[string]interface{}`
- **No Optimization**: Full map allocation for every event
- **Use Case**: Single event submission, lower volume scenarios

### 2b. Batch Endpoint (`/1/batch/{dataset}`)

This endpoint uses **custom unmarshalers** with optimizations for high-throughput scenarios. Unlike the single event endpoint, batch requests unmarshal directly to the `batchedEvent` type (NOT to `map[string]interface{}`). The JSON and MessagePack paths are different and use `CoreFieldsUnmarshaler` for efficient field extraction.

#### JSON Processing Path

```
                    HTTP Request
                         │
                         │ POST /1/batch/{dataset}
                         │ Content-Type: application/json
                         │
                         ▼
            ┌────────────────────────┐
            │  Extract HTTP Headers  │
            └────────────┬───────────┘
                         │
                         ▼
            ┌────────────────────────┐
            │    Decompression       │
            └────────────┬───────────┘
                         │
                         ▼
      ┌──────────────────────────────────────┐
      │  batchedEvents.UnmarshalJSON()       │
      │  batched_event.go:144-175            │
      └────────────┬─────────────────────────┘
                   │
                   ▼
      ┌──────────────────────────────────────┐
      │  fastjson.Parse()                    │
      │  (zero-copy JSON parsing)            │
      └────────────┬─────────────────────────┘
                   │
                   │ For each event in array:
                   ▼
      ┌──────────────────────────────────────┐
      │ unmarshalBatchedEventFromFastJSON()  │
      │   batched_event.go:184-235           │
      └────────────┬─────────────────────────┘
                   │
                   ▼
      ┌──────────────────────────────────────┐
      │ Extract top-level fields from JSON:  │
      │  • time (string)                     │
      │  • samplerate (int64)                │
      │  • data (keep as fastjson.Value)     │
      └────────────┬─────────────────────────┘
                   │
                   │ Convert ONLY "data" field
                   ▼
      ┌──────────────────────────────────────┐
      │  types.AppendJSONValue()             │
      │   types/json_to_msgp.go:36           │
      │  (Converts nested "data" to msgpack) │
      └────────────┬─────────────────────────┘
                   │
                   │ MessagePack bytes of "data"
                   ▼
      ┌──────────────────────────────────────┐
      │ CoreFieldsUnmarshaler.               │
      │   UnmarshalPayload()                 │
      │  (Extracts only essential fields)    │
      │  (Sets hasExtractedMetadata = true)  │
      └────────────┬─────────────────────────┘
                   │
                   ▼
            ┌────────────────────────┐
            │   Create types.Event   │
            │  (one per batch entry) │
            └────────────┬───────────┘
                         │
                         ▼
        [Events ready for internal processing]
```

#### MessagePack Processing Path

```
                    HTTP Request
                         │
                         │ POST /1/batch/{dataset}
                         │ Content-Type: application/msgpack
                         │
                         ▼
            ┌────────────────────────┐
            │  Extract HTTP Headers  │
            └────────────┬───────────┘
                         │
                         ▼
            ┌────────────────────────┐
            │    Decompression       │
            └────────────┬───────────┘
                         │
                         ▼
      ┌──────────────────────────────────────┐
      │  batchedEvents.UnmarshalMsg()        │
      │  batched_event.go:119-138            │
      └────────────┬─────────────────────────┘
                   │
                   │ Read msgpack array header
                   ▼
      ┌──────────────────────────────────────┐
      │ For each event in array:             │
      │  batchedEvent.UnmarshalMsg()         │
      │  batched_event.go:48-101             │
      └────────────┬─────────────────────────┘
                   │
                   │ Read msgpack map fields:
                   ▼
      ┌──────────────────────────────────────┐
      │  Switch on field name:               │
      │   case "time":                       │
      │     Read msgpack timestamp           │
      │   case "samplerate":                 │
      │     Read msgpack int64               │
      │   case "data":                       │
      │     Extract msgpack bytes →          │
      └────────────┬─────────────────────────┘
                   │
                   │ MessagePack bytes of "data"
                   ▼
      ┌──────────────────────────────────────┐
      │ CoreFieldsUnmarshaler.               │
      │   UnmarshalPayload()                 │
      │  (Direct msgpack field extraction)   │
      └────────────┬─────────────────────────┘
                   │
                   ▼
            ┌────────────────────────┐
            │   Create types.Event   │
            │  (one per batch entry) │
            └────────────┬───────────┘
                         │
                         ▼
            ┌────────────────────────┐
            │   ExtractMetadata()    │
            └────────────┬───────────┘
                         │
                         ▼
        [Events ready for internal processing]
```

**Key Characteristics:**

- **Custom Unmarshalers**: Uses `batchedEvent` struct with optimized unmarshaling
- **Selective Conversion**: JSON path converts only the nested "data" field to msgpack (not the entire event structure)
- **CoreFieldsUnmarshaler**: Extracts critical fields without full deserialization
- **Optimization Note**: The `ExtractMetadata()` call shown in diagrams is effectively a no-op for batch events. The `CoreFieldsUnmarshaler.UnmarshalPayload()` step already extracted and cached all metadata fields (sets `hasExtractedMetadata = true` at [payload.go:405](../types/payload.go#L405)), so the subsequent `ExtractMetadata()` call immediately returns without doing any work.
- **Use Case**: High throughput batch submissions

### Comparison: Single Event vs Batch Endpoints

| Feature | Single Event (`/1/events`) | Batch (`/1/batch`) |
|---------|---------------------------|-------------------|
| **Unmarshaling Target** | `map[string]interface{}` | `batchedEvent` type |
| **Unmarshaling Method** | Generic `unmarshal()` | Custom unmarshalers |
| **JSON Handling** | Full map allocation | fastjson + partial msgpack |
| **MessagePack Handling** | Full map allocation | Direct msgpack reading |
| **Field Extraction** | Full deserialization | CoreFieldsUnmarshaler |
| **Memory Efficiency** | Lower | Higher |
| **CPU Efficiency** | Lower | Higher |
| **Use Case** | Single event, low volume | Batch, high throughput |

---

## 3. OTLP HTTP Processing Flow

This diagram shows how OTLP trace data is processed when received over HTTP. **Note**: This document only covers trace ingestion; the `/v1/logs` endpoint uses a different processing path that does not convert to messagepack.

```
                    HTTP Request
                         │
                         │ POST /v1/traces
                         │
                         ▼
            ┌────────────────────────────┐
            │ Extract RequestInfo from   │
            │     HTTP Headers           │
            ├────────────────────────────┤
            │ • API key (x-honeycomb-*)  │
            │ • Dataset                  │
            │ • Content-Type             │
            │ • User-Agent               │
            └────────────┬───────────────┘
                         │
                         ▼
            ┌────────────────────────────┐
            │   Validate API Key         │
            │   Apply SendKeyMode        │
            └────────────┬───────────────┘
                         │
                         ▼
            ┌────────────────────────────┐
            │    Content-Type Check      │
            └─┬──────────────────────┬───┘
              │                      │
      ┌───────▼──────────┐   ┌───────▼──────────┐
      │ application/json │   │application/      │
      │                  │   │ x-protobuf       │
      └───────┬──────────┘   └───────┬──────────┘
              │                      │
              │ Increment            │ Increment
              │ JSON metric          │ Protobuf metric
              │                      │
              └──────────┬───────────┘
                         │
                         ▼
            ┌─────────────────────────────────────┐
            │ huskyotlp.Translate                 │
            │  TraceRequestFromReaderSizedWith    │
            │         Msgp()                      │
            │    (Husky Library - 20MB max)       │
            └────────────┬────────────────────────┘
                         │
                         ▼
            ┌─────────────────────────────────────┐
            │    Husky Processing Pipeline:       │
            │                                     │
            │  1. Unmarshal JSON or Protobuf      │
            │         ↓                           │
            │  2. Translate to internal format    │
            │         ↓                           │
            │  3. Optimize to msgpack encoding    │
            └────────────┬────────────────────────┘
                         │
                         ▼
            ┌─────────────────────────────────────┐
            │       Result: BatchMsgp             │
            │   (msgpack-encoded trace events)    │
            └────────────┬────────────────────────┘
                         │
                         ▼
            ┌─────────────────────────────────────┐
            │  processOTLPRequestBatchMsgp()      │
            │   route/otlp_trace.go               │
            └────────────┬────────────────────────┘
                         │
                         ▼
            ┌─────────────────────────────────────┐
            │    CoreFieldsUnmarshaler            │
            │  Extract trace ID, parent ID        │
            │  (without full deserialization)     │
            └────────────┬────────────────────────┘
                         │
                         ▼
            ┌─────────────────────────────────────┐
            │   Create types.Event from msgpack   │
            └────────────┬────────────────────────┘
                         │
                         ▼
            ┌─────────────────────────────────────┐
            │      Send to Collector              │
            │      (for trace sampling)           │
            └────────────┬────────────────────────┘
                         │
                         ▼
            ┌─────────────────────────────────────┐
            │  Return OTLP Success Response       │
            └─────────────────────────────────────┘
```

**Key Processing Steps:**

1. **RequestInfo Extraction**: API key, dataset, content-type, user-agent from headers
2. **Authentication**: Validate API key and apply SendKeyMode
3. **Content-Type Routing**: Both JSON and Protobuf use same translation path
4. **Husky Translation**: Unmarshals input → translates → outputs msgpack (20MB max)
5. **CoreFieldsUnmarshaler**: Extracts only essential fields (trace ID, parent ID)

---

## 4. OTLP gRPC Processing Flow

This diagram shows the custom unmarshaling optimization used for OTLP gRPC trace requests. **Note**: This document only covers trace ingestion; logs use a different gRPC handler.

```
                gRPC Request
                     │
                     │ TraceService/Export
                     │
                     ▼
        ┌────────────────────────────────┐
        │  customTraceExportHandler()    │
        │  Intercepts gRPC call          │
        │  route/otlp_trace.go:195       │
        └────────────┬───────────────────┘
                     │
                     ▼
        ┌────────────────────────────────┐
        │ Extract RequestInfo from       │
        │    gRPC Metadata               │
        ├────────────────────────────────┤
        │ • API key                      │
        │ • Dataset                      │
        │ • User-Agent                   │
        └────────────┬───────────────────┘
                     │
                     ▼
        ┌────────────────────────────────┐
        │   Apply SendKeyMode            │
        │   (API key replacement)        │
        └────────────┬───────────────────┘
                     │
                     ▼
        ┌────────────────────────────────┐
        │  Create                        │
        │  translatedTraceServiceRequest │
        │  wrapper                       │
        ├────────────────────────────────┤
        │ Implements:                    │
        │   protoiface.MessageV1         │
        │   route/otlp_trace.go:156      │
        └────────────┬───────────────────┘
                     │
                     ▼
        ┌────────────────────────────────┐
        │  gRPC calls dec(wrapper)       │
        │  Triggers proto.Unmarshal      │
        └────────────┬───────────────────┘
                     │
                     ▼
        ┌────────────────────────────────┐
        │  wrapper.Unmarshal() called    │
        │  (by gRPC proto unmarshaler)   │
        │  route/otlp_trace.go:180       │
        └────────────┬───────────────────┘
                     │
                     ▼
        ┌────────────────────────────────┐
        │  huskyotlp.                    │
        │  UnmarshalTraceRequestDirect   │
        │         Msgp()                 │
        ├────────────────────────────────┤
        │ *** OPTIMIZATION ***           │
        │ Direct translation:            │
        │   protobuf bytes → msgpack     │
        │                                │
        │ NO intermediate protobuf       │
        │ struct creation!               │
        └────────────┬───────────────────┘
                     │
                     ▼
        ┌────────────────────────────────┐
        │  TranslateOTLPRequestResult    │
        │          Msgp                  │
        │  (msgpack-optimized batch)     │
        └────────────┬───────────────────┘
                     │
                     ▼
        ┌────────────────────────────────┐
        │  TraceServer.                  │
        │    ExportTraceData()           │
        │  route/otlp_trace.go:128       │
        └────────────┬───────────────────┘
                     │
                     ▼
        ┌────────────────────────────────┐
        │   Final Auth Check             │
        └────────────┬───────────────────┘
                     │
                     ▼
        ┌────────────────────────────────┐
        │ processOTLPRequestBatchMsgp()  │
        │   (same as HTTP path)          │
        └────────────┬───────────────────┘
                     │
                     ▼
        ┌────────────────────────────────┐
        │  Return gRPC                   │
        │  ExportTraceServiceResponse    │
        └────────────────────────────────┘
```

**Key Optimization:**

The gRPC path uses a **custom unmarshaling hook** that intercepts the protobuf deserialization process:

1. **translatedTraceServiceRequest**: Implements `protoiface.MessageV1` interface
2. **Unmarshal() Hook**: Called by gRPC's proto.Unmarshal during request processing
3. **Direct Translation**: Converts protobuf bytes directly to msgpack format
4. **Zero-Copy**: Avoids creating intermediate protobuf struct objects
5. **Performance**: Faster than traditional protobuf unmarshaling for high-throughput

**Why This Matters:**
```
Traditional gRPC path:
  protobuf bytes → protobuf struct → internal format → msgpack
  (2 conversions, allocates intermediate struct)

Refinery optimized path:
  protobuf bytes → msgpack (direct)
  (1 conversion, no intermediate allocation)
```

---

## 6. Internal Data Structures

### Core Type Hierarchy

```
┌─────────────────────────────────────────────────────────────┐
│                      types.Payload                          │
├─────────────────────────────────────────────────────────────┤
│  PRIVATE STORAGE:                                           │
│    msgpData          []byte  ← Raw msgpack bytes            │
│    memoizedFields    map[string]any                         │
│                                                             │
│  CACHED METADATA:                                           │
│    MetaTraceID                string                        │
│    MetaSignalType             string                        │
│    MetaAnnotationType         string                        │
│    MetaRefineryProbe          nullableBool                  │
│    MetaRefineryRoot           nullableBool                  │
│    MetaRefineryIncomingUserAgent string                     │
│    MetaRefinerySendBy         int64                         │
│    MetaRefinerySpanDataSize   int64                         │
│    ... (10+ more metadata fields)                           │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Payload Storage Strategy

The `Payload` uses a **hybrid storage approach** for optimal performance:

```
┌─────────────────────────────────────────────────────────────┐
│                    Payload Internal Storage                  │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │          msgpData []byte                              │  │
│  │  [Raw MessagePack bytes - backing storage]            │  │
│  │                                                       │  │
│  │  0xDE 0x00 0x0F 0xA4 0x6E 0x61 0x6D 0x65 ...        │  │
│  │  [Full event data in msgpack binary format]          │  │
│  └──────────────────────────────────────────────────────┘  │
│                         ▲                                    │
│                         │                                    │
│                         │ Lazy deserialization               │
│                         │                                    │
│  ┌──────────────────────┴───────────────────────────────┐  │
│  │     Cached Metadata Fields (struct fields)           │  │
│  │  • MetaTraceID = "abc123"                            │  │
│  │  • MetaSignalType = "span"                           │  │
│  │  • MetaRefineryRoot = true                           │  │
│  │  [Extracted once, zero cost to access]               │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │     memoizedFields map[string]any                     │  │
│  │  • "http.status_code" → 200                          │  │
│  │  • "duration_ms" → 45.2                              │  │
│  │  [Fields accessed via Get(), cached here]            │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### Field Access Methods

| Method | Use Case | Performance | When to Use |
|--------|----------|-------------|-------------|
| **Direct struct field** | Metadata access | **Fastest** (direct memory access) | **Always for known metadata** |
| `Get(key)` | Single field access | Map lookup overhead | Custom/unknown fields only |
| `MemoizeFields(keys...)` | Batch pre-load | Single msgpack iteration | Need multiple custom fields |
| `All()` | Iterate all fields | Slow (full iteration) | Only for transmission |

**Access Pattern Examples:**

```
// BEST - Direct struct field access (fastest)
traceID := payload.MetaTraceID        // ← Direct struct field access (no map lookup)
signalType := payload.MetaSignalType  // ← Direct struct field access (no map lookup)
isRoot := payload.MetaRefineryRoot    // ← Direct struct field access (no map lookup)

// SLOWER - Using Get() for metadata (unnecessary map lookup overhead)
traceID := payload.Get("meta.trace_id")  // ✗ AVOID: Slower than direct struct access

// OK - Custom field (deserializes on demand, cached in memoizedFields)
status := payload.Get("http.status_code")  // ← Iterates msgpack to find field
                                            // ← Cached in memoizedFields map

// Optimized - Batch access for multiple custom fields
payload.MemoizeFields(["field1", "field2", "field3"])  // ← Single iteration
val1 := payload.Get("field1")  // ← From cache (fast)
val2 := payload.Get("field2")  // ← From cache (fast)
val3 := payload.Get("field3")  // ← From cache (fast)
```

**Performance Consideration:**

Direct struct field access (`payload.MetaTraceID`) is significantly faster than map-based access (`payload.Get("meta.trace_id")`) because:
- No map key lookup required
- No string comparison overhead
- Direct memory address access
- Better CPU cache locality

### Metadata Fields (Cached)

These fields are extracted once during initial unmarshaling and cached in the struct for zero-cost access:

- `meta.trace_id` - Trace identifier
- `meta.signal_type` - "log" or "span"
- `meta.annotation_type` - "span_event", "link", etc.
- `meta.refinery.root` - Is root span
- `meta.refinery.probe` - Internal probe marker
- `meta.refinery.incoming_user_agent` - Client user agent
- `meta.refinery.send_by` - When to send trace
- `meta.refinery.span_data_size` - Size of span data
- Plus 10+ additional metadata fields

### CoreFieldsUnmarshaler

Special unmarshaler that extracts **only** core fields without full deserialization:

```
┌─────────────────────────────────────────────────────────────┐
│              CoreFieldsUnmarshaler                           │
├─────────────────────────────────────────────────────────────┤
│  Input:  msgpData []byte                                    │
│                                                              │
│  Extract Only:                                              │
│    • Trace ID (using configurable field names)              │
│    • Parent ID (for root span detection)                    │
│    • Sampling Key fields (for sampling decisions)           │
│                                                              │
│  Method:                                                    │
│    Iterate msgpack, stop when fields found                  │
│    Skip all other fields                                    │
│                                                              │
│  Benefit:                                                   │
│    Fast routing decisions without full deserialization      │
└─────────────────────────────────────────────────────────────┘
```
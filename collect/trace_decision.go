package collect

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"strings"
	"sync"

	"github.com/golang/snappy"
	"github.com/honeycombio/refinery/collect/cache"
	"github.com/honeycombio/refinery/internal/otelutil"
	"go.opentelemetry.io/otel/trace"
)

type decisionType int

// decisionMessageSeparator is the separator used to separate the sender ID from the compressed decisions
// in the decision message.
// The pipe character should not be used in URLs or IP addresses because it's not a valid character in these
// contexts.
const decisionMessageSeparator = "|"

func (d decisionType) String() string {
	switch d {
	case keptDecision:
		return "kept"
	case dropDecision:
		return "drop"
	default:
		return "unknown"
	}
}

var (
	keptDecision decisionType = 1
	dropDecision decisionType = 2
)

type newDecisionMessage func(tds []TraceDecision, senderID string) (string, error)

var _ cache.KeptTrace = &TraceDecision{}

type TraceDecision struct {
	TraceID string
	// if we don'g need to immediately eject traces from the trace cache,
	// we could remove this field. The TraceDecision type could be renamed to
	// keptDecision
	Kept            bool
	Rate            uint
	SamplerKey      string
	SamplerSelector string
	SendReason      string
	HasRoot         bool
	Reason          string
	Count           uint32
	EventCount      uint32
	LinkCount       uint32

	keptReasonIdx uint
}

func (td *TraceDecision) DescendantCount() uint32 {
	return td.Count + td.EventCount + td.LinkCount
}

func (td *TraceDecision) SpanCount() uint32 {
	return td.Count
}

func (td *TraceDecision) SpanEventCount() uint32 {
	return td.EventCount
}

func (td *TraceDecision) SpanLinkCount() uint32 {
	return td.LinkCount
}

func (td *TraceDecision) SampleRate() uint {
	return td.Rate
}

func (td *TraceDecision) ID() string {
	return td.TraceID
}

func (td *TraceDecision) KeptReason() uint {
	return td.keptReasonIdx
}

func (td *TraceDecision) SetKeptReason(reasonIdx uint) {
	td.keptReasonIdx = reasonIdx
}

var bufferPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

var snappyWriterPool = sync.Pool{
	New: func() any { return snappy.NewBufferedWriter(nil) },
}

// Add tracer as a field
type TraceDecisionHandler struct {
	tracer trace.Tracer
}

func (h *TraceDecisionHandler) processDecision(ctx context.Context, msg string, senderID string, isKeptDecision bool) ([]TraceDecision, error) {
	ctx, span := otelutil.StartSpanMulti(ctx, h.tracer, "traceDecision.process", map[string]interface{}{
		"sender.id":      senderID,
		"decision.type":  map[bool]string{true: "kept", false: "dropped"}[isKeptDecision],
		"message.length": len(msg),
	})
	defer span.End()

	separatorIdx := strings.IndexRune(msg, rune(decisionMessageSeparator[0]))
	if separatorIdx == -1 {
		otelutil.AddException(span, fmt.Errorf("invalid decision message format"))
		return nil, fmt.Errorf("invalid decision message format")
	}

	// If the sender ID is the same as the current service, ignore the message
	if msg[:separatorIdx] == senderID {
		span.SetAttributes(otelutil.Attributes(map[string]interface{}{
			"decision.skipped": true,
			"reason":           "self_message",
		})...)
		return nil, nil
	}

	var decisions []TraceDecision
	var err error
	if isKeptDecision {
		decisions, err = h.decompressKeptDecisions(ctx, []byte(msg[separatorIdx+1:]))
	} else {
		var traceIDs string
		traceIDs, err = h.decompressDropDecisions(ctx, []byte(msg[separatorIdx+1:]))
		if err == nil {
			ids := strings.Split(traceIDs, ",")
			decisions = make([]TraceDecision, 0, len(ids))
			for _, id := range ids {
				decisions = append(decisions, TraceDecision{TraceID: id})
			}
		}
	}

	if err != nil {
		otelutil.AddException(span, err)
		return nil, err
	}

	span.SetAttributes(otelutil.Attributes(map[string]interface{}{
		"decisions.count": len(decisions),
	})...)

	return decisions, nil
}

func (h *TraceDecisionHandler) newDroppedDecisionMessage(ctx context.Context, tds []TraceDecision, senderID string) (string, error) {
	ctx, span := otelutil.StartSpanMulti(ctx, h.tracer, "traceDecision.newDroppedMessage", map[string]interface{}{
		"sender.id":       senderID,
		"decisions.count": len(tds),
	})
	defer span.End()

	if len(tds) == 0 {
		otelutil.AddException(span, fmt.Errorf("no dropped trace decisions provided"))
		return "", fmt.Errorf("no dropped trace decisions provided")
	}
	if senderID == "" {
		otelutil.AddException(span, fmt.Errorf("no sender ID provided"))
		return "", fmt.Errorf("no sender ID provided")
	}

	payload := make([]string, 0, len(tds))
	for _, td := range tds {
		if td.TraceID != "" {
			payload = append(payload, td.TraceID)
		}
	}

	compressed, err := h.compress(ctx, strings.Join(payload, ","))
	if err != nil {
		otelutil.AddException(span, err)
		return "", err
	}

	span.SetAttributes(otelutil.Attributes(map[string]interface{}{
		"compressed.size_bytes": len(compressed),
	})...)

	return senderID + decisionMessageSeparator + string(compressed), nil
}

func (h *TraceDecisionHandler) compress(ctx context.Context, data any) ([]byte, error) {
	ctx, span := otelutil.StartSpan(ctx, h.tracer, "traceDecision.compress")
	defer span.End()

	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	compr := snappyWriterPool.Get().(*snappy.Writer)
	compr.Reset(buf)
	defer snappyWriterPool.Put(compr)

	enc := gob.NewEncoder(compr)
	if err := enc.Encode(data); err != nil {
		otelutil.AddException(span, err)
		return nil, err
	}

	if err := compr.Close(); err != nil {
		otelutil.AddException(span, err)
		return nil, err
	}

	result := bytes.Clone(buf.Bytes())
	span.SetAttributes(otelutil.Attributes(map[string]interface{}{
		"compression.input_size":  buf.Cap(),
		"compression.output_size": len(result),
	})...)

	return result, nil
}

func (h *TraceDecisionHandler) decompressKeptDecisions(ctx context.Context, data []byte) ([]TraceDecision, error) {
	ctx, span := otelutil.StartSpanMulti(ctx, h.tracer, "traceDecision.decompressKept", map[string]interface{}{
		"compressed.size_bytes": len(data),
	})
	defer span.End()

	buf := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(buf)
	buf.Reset()
	buf.Write(data)

	reader := snappy.NewReader(buf)
	dec := gob.NewDecoder(reader)
	var tds []TraceDecision
	if err := dec.Decode(&tds); err != nil {
		otelutil.AddException(span, err)
		return nil, err
	}

	span.SetAttributes(otelutil.Attributes(map[string]interface{}{
		"decisions.count": len(tds),
	})...)

	return tds, nil
}

func (h *TraceDecisionHandler) decompressDropDecisions(ctx context.Context, data []byte) (string, error) {
	ctx, span := otelutil.StartSpanMulti(ctx, h.tracer, "traceDecision.decompressDrop", map[string]interface{}{
		"compressed.size_bytes": len(data),
	})
	defer span.End()

	buf := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(buf)
	buf.Reset()
	buf.Write(data)

	reader := snappy.NewReader(buf)
	dec := gob.NewDecoder(reader)
	var traceIDs string
	if err := dec.Decode(&traceIDs); err != nil {
		otelutil.AddException(span, err)
		return "", err
	}

	span.SetAttributes(otelutil.Attributes(map[string]interface{}{
		"decompressed.size_bytes": len(traceIDs),
	})...)

	return traceIDs, nil
}

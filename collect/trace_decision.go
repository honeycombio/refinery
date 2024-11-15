package collect

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"strings"
	"sync"

	"github.com/golang/snappy"
	"github.com/honeycombio/refinery/collect/cache"
)

type decisionType int

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

type newDecisionMessage func([]TraceDecision) (string, error)

func newDroppedDecisionMessage(tds []TraceDecision) (string, error) {
	if len(tds) == 0 {
		return "", fmt.Errorf("no dropped trace decisions provided")
	}

	traceIDs := make([]string, 0, len(tds))
	for _, td := range tds {
		if td.TraceID != "" {
			traceIDs = append(traceIDs, td.TraceID)
		}
	}

	if len(traceIDs) == 0 {
		return "", fmt.Errorf("no valid trace IDs provided")
	}

	return strings.Join(traceIDs, ","), nil
}
func newDroppedTraceDecision(msg string) ([]TraceDecision, error) {
	if msg == "" {
		return nil, fmt.Errorf("empty drop message")
	}
	var decisions []TraceDecision
	for _, traceID := range strings.Split(msg, ",") {
		decisions = append(decisions, TraceDecision{
			TraceID: traceID,
		})
	}

	return decisions, nil
}

func newKeptDecisionMessage(tds []TraceDecision) (string, error) {
	if len(tds) == 0 {
		return "", fmt.Errorf("no kept trace decisions provided")
	}
	compressed, err := compress(tds)
	if err != nil {
		return "", err
	}
	return string(compressed), nil
}

func newKeptTraceDecision(msg string) ([]TraceDecision, error) {
	compressed, err := decompress([]byte(msg))
	if err != nil {
		return nil, err
	}
	return compressed, nil
}

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

func compress(tds []TraceDecision) ([]byte, error) {
	// Get a buffer from the pool and reset it
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	// Get a snappy writer from the pool, set it to write to the buffer, and reset it
	compr := snappyWriterPool.Get().(*snappy.Writer)
	compr.Reset(buf)
	defer snappyWriterPool.Put(compr)

	enc := gob.NewEncoder(compr)
	if err := enc.Encode(tds); err != nil {
		return nil, err
	}

	// Flush snappy writer
	if err := compr.Close(); err != nil {
		return nil, err
	}

	// Copy the buffer’s bytes to avoid reuse issues when returning
	return bytes.Clone(buf.Bytes()), nil
}

func decompress(data []byte) ([]TraceDecision, error) {
	// Get a buffer from the pool and set it up with data
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	buf.Write(data)
	defer bufferPool.Put(buf)

	// Snappy reader to decompress data in buffer
	compr := snappy.NewReader(buf)
	dec := gob.NewDecoder(compr)

	var tds []TraceDecision
	if err := dec.Decode(&tds); err != nil {
		return nil, err
	}
	return tds, nil
}

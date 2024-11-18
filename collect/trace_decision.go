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

func newDroppedDecisionMessage(tds []TraceDecision, senderID string) (string, error) {
	if len(tds) == 0 {
		return "", fmt.Errorf("no dropped trace decisions provided")
	}
	if senderID == "" {
		return "", fmt.Errorf("no sender ID provided")
	}

	payload := make([]string, 0, len(tds))
	for _, td := range tds {
		if td.TraceID != "" {
			payload = append(payload, td.TraceID)
		}
	}

	compressed, err := compress(strings.Join(payload, ","))
	if err != nil {
		return "", err
	}
	return senderID + decisionMessageSeparator + string(compressed), nil
}

func newDroppedTraceDecision(msg string, senderID string) ([]TraceDecision, error) {
	// Use IndexRune here since it's faster than SplitN and requires less allocation
	separatorIdx := strings.IndexRune(msg, rune(decisionMessageSeparator[0]))
	if separatorIdx == -1 {
		return nil, fmt.Errorf("invalid dropped decision message")
	}

	if msg[:separatorIdx] != senderID {
		return nil, nil
	}

	ids, err := decompressDropDecisions([]byte(msg[separatorIdx+1:]))
	if err != nil {
		return nil, err
	}

	traceIDs := strings.Split(ids, ",")
	decisions := make([]TraceDecision, 0, len(traceIDs))
	for _, traceID := range traceIDs {
		decisions = append(decisions, TraceDecision{
			TraceID: traceID,
		})
	}
	return decisions, nil
}

func newKeptDecisionMessage(tds []TraceDecision, senderID string) (string, error) {
	if len(tds) == 0 {
		return "", fmt.Errorf("no kept trace decisions provided")
	}

	if senderID == "" {
		return "", fmt.Errorf("no sender ID provided")
	}

	compressed, err := compress(tds)
	if err != nil {
		return "", err
	}
	return senderID + decisionMessageSeparator + string(compressed), nil
}

func newKeptTraceDecision(msg string, senderID string) ([]TraceDecision, error) {
	// Use IndexRune here since it's faster than SplitN and requires less allocation
	separatorIdx := strings.IndexRune(msg, rune(decisionMessageSeparator[0]))
	if separatorIdx == -1 {
		return nil, fmt.Errorf("invalid dropped decision message")
	}

	if msg[:separatorIdx] != senderID {
		return nil, nil
	}

	compressed, err := decompressKeptDecisions([]byte(msg[separatorIdx+1:]))
	if err != nil {
		return nil, err
	}
	return compressed, nil
}

func isMyDecision(msg string, senderID string) bool {
	return strings.HasPrefix(msg, senderID+decisionMessageSeparator)
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

func compress(data any) ([]byte, error) {
	// Get a buffer from the pool and reset it
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	// Get a snappy writer from the pool, set it to write to the buffer, and reset it
	compr := snappyWriterPool.Get().(*snappy.Writer)
	compr.Reset(buf)
	defer snappyWriterPool.Put(compr)

	enc := gob.NewEncoder(compr)
	if err := enc.Encode(data); err != nil {
		return nil, err
	}

	// Flush snappy writer
	if err := compr.Close(); err != nil {
		return nil, err
	}

	// Copy the buffer’s bytes to avoid reuse issues when returning
	return bytes.Clone(buf.Bytes()), nil
}

func decompressKeptDecisions(data []byte) ([]TraceDecision, error) {
	// Get a buffer from the pool and set it up with data
	buf := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(buf)
	buf.Reset()
	buf.Write(data)

	// Snappy reader to decompress data in buffer
	reader := snappy.NewReader(buf)
	dec := gob.NewDecoder(reader)

	var tds []TraceDecision
	if err := dec.Decode(&tds); err != nil {
		return nil, err
	}
	return tds, nil
}

func decompressDropDecisions(data []byte) (string, error) {
	// Get a buffer from the pool and set it up with data
	buf := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(buf)
	buf.Reset()
	buf.Write(data)

	// Snappy reader to decompress data in buffer
	reader := snappy.NewReader(buf)
	dec := gob.NewDecoder(reader)

	var traceIDs string
	if err := dec.Decode(&traceIDs); err != nil {
		return "", err
	}
	return traceIDs, nil
}

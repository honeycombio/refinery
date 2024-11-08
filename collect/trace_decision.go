package collect

import (
	"encoding/json"
	"fmt"
	"strings"
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
func newKeptDecisionMessage(tds []TraceDecision) (string, error) {
	if len(tds) == 0 {
		return "", fmt.Errorf("no kept trace decisions provided")
	}

	data, err := json.Marshal(tds)
	if err != nil {
		return "", err
	}

	return string(data), nil
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

func newKeptTraceDecision(msg string) ([]TraceDecision, error) {
	keptDecisions := make([]TraceDecision, 0)
	err := json.Unmarshal([]byte(msg), &keptDecisions)
	if err != nil {
		return nil, err
	}

	return keptDecisions, nil
}

type TraceDecision struct {
	TraceID string
	// if we don'g need to immediately eject traces from the trace cache,
	// we could remove this field. The TraceDecision type could be renamed to
	// keptDecision
	Kept            bool
	SampleRate      uint
	SamplerKey      string `json:",omitempty"`
	SamplerSelector string `json:",omitempty"`
	SendReason      string
	HasRoot         bool
	KeptReason      string
	Count           uint32 `json:",omitempty"` // number of spans in the trace
	EventCount      uint32 `json:",omitempty"` // number of span events in the trace
	LinkCount       uint32 `json:",omitempty"` // number of span links in the trace
}

func (td *TraceDecision) DescendantCount() uint32 {
	return td.Count + td.EventCount + td.LinkCount
}

package collect

import (
	"encoding/json"
	"fmt"
	"strings"
)

func newDroppedDecisionMessage(traceIDs ...string) (string, error) {
	if len(traceIDs) == 0 {
		return "", fmt.Errorf("no traceIDs provided")
	}
	data := strings.Join(traceIDs, ",")
	return string(data), nil
}
func newKeptDecisionMessage(td TraceDecision) (string, error) {
	if td.TraceID == "" {
		return "", fmt.Errorf("no traceID provided")
	}
	data, err := json.Marshal(td)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

func newDroppedTraceDecision(msg string) []string {
	var traceIDs []string
	for _, traceID := range strings.Split(msg, ",") {
		traceIDs = append(traceIDs, traceID)
	}

	return traceIDs
}

func newKeptTraceDecision(msg string) (*TraceDecision, error) {
	keptDecision := &TraceDecision{}
	err := json.Unmarshal([]byte(msg), keptDecision)
	if err != nil {
		return nil, err
	}

	return keptDecision, nil
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

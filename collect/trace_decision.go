package collect

import (
	"encoding/json"
)

func newTraceDecisionMessage(td TraceDecision) (string, error) {
	data, err := json.Marshal(td)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

func unmarshalTraceDecisionMessage(msg string) (td TraceDecision, err error) {
	err = json.Unmarshal([]byte(msg), &td)
	if err != nil {
		return td, err
	}

	return td, nil
}

type TraceDecision struct {
	ID         string
	Kept       bool
	Rate       uint
	SamplerKey string
	Selector   string
	Reason     string
	SendReason string
	Count      uint32 // number of spans in the trace
	EventCount uint32 // number of span events in the trace
	LinkCount  uint32 // number of span links in the trace
	HasRoot    bool
	Metadata   map[string]interface{}
}

func (td *TraceDecision) DescendantCount() uint32 {
	return td.Count + td.EventCount + td.LinkCount
}

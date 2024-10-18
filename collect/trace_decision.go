package collect

import (
	"encoding/json"
	"fmt"
	"strings"
)

const (
	droppedPrefix         = "drop"
	keptPrefix            = "kept"
	traceMessageSeparator = ":"
)

func newDroppedDecisionMessage(traceIDs ...string) (string, error) {
	if len(traceIDs) == 0 {
		return "", fmt.Errorf("no traceIDs provided")
	}
	data := strings.Join(traceIDs, ",")
	return droppedPrefix + traceMessageSeparator + string(data), nil
}
func newKeptDecisionMessage(td TraceDecision) (string, error) {
	if td.TraceID == "" {
		return "", fmt.Errorf("no traceID provided")
	}
	data, err := json.Marshal(td)
	if err != nil {
		return "", err
	}

	return keptPrefix + traceMessageSeparator + string(data), nil
}

func unmarshalTraceDecisionMessage(msg string) (td []TraceDecision, err error) {
	data := strings.SplitN(msg, traceMessageSeparator, 2)
	if len(data) != 2 {
		return nil, fmt.Errorf("invalid message format for trace decision")
	}

	switch data[0] {
	case droppedPrefix:
		for _, traceID := range strings.Split(data[1], ",") {
			td = append(td, TraceDecision{TraceID: traceID})
		}
		return td, nil
	case keptPrefix:
		keptDecision := TraceDecision{}
		err = json.Unmarshal([]byte(data[1]), &keptDecision)
		if err != nil {
			return nil, err
		}
		td = append(td, keptDecision)

		return td, nil
	default:
		return nil, fmt.Errorf("unexpected message prefix for trace decision")
	}
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

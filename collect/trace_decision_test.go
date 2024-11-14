package collect

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewDroppedTraceDecision(t *testing.T) {
	tests := []struct {
		name string
		msg  string
		want []TraceDecision
	}{
		{
			name: "multiple dropped decisions",
			msg:  "1,2,3",
			want: []TraceDecision{{TraceID: "1"}, {TraceID: "2"}, {TraceID: "3"}},
		},
		{
			name: "single dropped decision",
			msg:  "1",
			want: []TraceDecision{{TraceID: "1"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newDroppedTraceDecision(tt.msg)
			require.NoError(t, err)
			assert.EqualValues(t, tt.want, got)
		})
	}
}

func TestNewDroppedDecisionMessage(t *testing.T) {
	tests := []struct {
		name      string
		decisions []TraceDecision
		want      string
		wantErr   bool
	}{
		{
			name:      "no traceIDs provided",
			decisions: nil,
			want:      "",
			wantErr:   true,
		},
		{
			name:      "one traceID",
			decisions: []TraceDecision{{TraceID: "1"}},
			want:      "1",
			wantErr:   false,
		},
		{
			name: "multiple traceIDs",
			decisions: []TraceDecision{{TraceID: "1"},
				{TraceID: "2"}, {TraceID: "3"}},
			want:    "1,2,3",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newDroppedDecisionMessage(tt.decisions)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			assert.Equal(t, tt.want, got)
		})
	}
}
func TestKeptDecisionRoundTrip(t *testing.T) {
	// Test data for kept decisions covering all fields
	tds := []TraceDecision{
		{
			TraceID:         "trace1",
			Kept:            true,
			Rate:            1,
			SamplerKey:      "sampler1",
			SamplerSelector: "selector1",
			SendReason:      "reason1",
			HasRoot:         true,
			Reason:          "valid reason 1",
			Count:           5,
			EventCount:      10,
			LinkCount:       15,
		},
		{
			TraceID:         "trace2",
			Kept:            true,
			Rate:            2,
			SamplerKey:      "sampler2",
			SamplerSelector: "selector2",
			SendReason:      "reason2",
			HasRoot:         false,
			Reason:          "valid reason 2",
			Count:           3,
			EventCount:      6,
			LinkCount:       9,
		},
	}

	// Step 1: Create a kept decision message
	msg, err := newKeptDecisionMessage(tds)
	assert.NoError(t, err, "expected no error for valid kept decision message")
	assert.NotEmpty(t, msg, "expected non-empty message")

	// Step 3: Decompress the message back to TraceDecision using newKeptTraceDecision
	decompressedTds, err := newKeptTraceDecision(msg)
	assert.NoError(t, err, "expected no error during decompression of the kept decision message")
	assert.Len(t, decompressedTds, len(tds), "expected decompressed TraceDecision length to match original")

	// Step 4: Verify that the decompressed data matches the original TraceDecision data
	for i, td := range decompressedTds {
		assert.Equal(t, td.TraceID, tds[i].TraceID, "expected TraceID to match")
		assert.Equal(t, td.Kept, tds[i].Kept, "expected Kept status to match")
		assert.Equal(t, td.Rate, tds[i].Rate, "expected Rate to match")
		assert.Equal(t, td.SamplerKey, tds[i].SamplerKey, "expected SamplerKey to match")
		assert.Equal(t, td.SamplerSelector, tds[i].SamplerSelector, "expected SamplerSelector to match")
		assert.Equal(t, td.SendReason, tds[i].SendReason, "expected SendReason to match")
		assert.Equal(t, td.HasRoot, tds[i].HasRoot, "expected HasRoot to match")
		assert.Equal(t, td.Reason, tds[i].Reason, "expected Reason to match")
		assert.Equal(t, td.Count, tds[i].Count, "expected Count to match")
		assert.Equal(t, td.EventCount, tds[i].EventCount, "expected EventCount to match")
		assert.Equal(t, td.LinkCount, tds[i].LinkCount, "expected LinkCount to match")
	}
}

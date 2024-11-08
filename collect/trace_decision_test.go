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

func TestNewKeptTraceDecision(t *testing.T) {
	tests := []struct {
		name    string
		msg     string
		want    []TraceDecision
		wantErr bool
	}{
		{
			name: "kept decision",
			msg:  `[{"TraceID":"1", "Kept": true, "SampleRate": 100, "SendReason":"` + TraceSendGotRoot + `"}]`,
			want: []TraceDecision{
				{TraceID: "1", Kept: true, SampleRate: 100, SendReason: TraceSendGotRoot}},
			wantErr: false,
		},
		{
			name:    "invalid message format",
			msg:     "invalid",
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newKeptTraceDecision(tt.msg)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

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

func TestNewKeptDecisionMessage(t *testing.T) {
	tests := []struct {
		name    string
		td      []TraceDecision
		want    string
		wantErr bool
	}{
		{
			name: "kept decision",
			td: []TraceDecision{
				{
					TraceID:    "1",
					Kept:       true,
					SampleRate: 100,
					SendReason: TraceSendGotRoot,
					KeptReason: "deterministic",
				},
			},
			want:    `[{"TraceID":"1","Kept":true,"SampleRate":100,"SendReason":"trace_send_got_root","HasRoot":false,"KeptReason":"deterministic"}]`,
			wantErr: false,
		},
		{
			name:    "invalid",
			td:      nil,
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newKeptDecisionMessage(tt.td)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

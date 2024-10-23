package collect

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnmarshalTraceDecisionMessage(t *testing.T) {
	tests := []struct {
		name    string
		msg     string
		want    []TraceDecision
		wantErr bool
	}{
		{
			name: "dropped decision",
			msg:  "drop:1,2,3",
			want: []TraceDecision{
				{TraceID: "1"},
				{TraceID: "2"},
				{TraceID: "3"},
			},
			wantErr: false,
		},
		{
			name: "kept decision",
			msg:  `kept:{"TraceID":"1", "Kept": true, "SampleRate": 100, "SendReason":"` + TraceSendGotRoot + `"}`,
			want: []TraceDecision{
				{TraceID: "1", Kept: true, SampleRate: 100, SendReason: TraceSendGotRoot},
			},
			wantErr: false,
		},
		{
			name:    "invalid message format",
			msg:     "invalid",
			want:    nil,
			wantErr: true,
		},
		{
			name:    "unexpected message prefix",
			msg:     "unexpected:1",
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := unmarshalTraceDecisionMessage(tt.msg)
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
		name     string
		traceIDs []string
		want     string
		wantErr  bool
	}{
		{
			name:     "no traceIDs provided",
			traceIDs: nil,
			want:     "",
			wantErr:  true,
		},
		{
			name:     "one traceID",
			traceIDs: []string{"1"},
			want:     "drop:1",
			wantErr:  false,
		},
		{
			name:     "multiple traceIDs",
			traceIDs: []string{"1", "2", "3"},
			want:     "drop:1,2,3",
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newDroppedDecisionMessage(tt.traceIDs...)
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
		td      TraceDecision
		want    string
		wantErr bool
	}{
		{
			name: "kept decision",
			td: TraceDecision{
				TraceID:    "1",
				Kept:       true,
				SampleRate: 100,
				SendReason: TraceSendGotRoot,
				KeptReason: "deterministic",
			},
			want:    `kept:{"TraceID":"1","Kept":true,"SampleRate":100,"SendReason":"trace_send_got_root","HasRoot":false,"KeptReason":"deterministic"}`,
			wantErr: false,
		},
		{
			name:    "invalid",
			td:      TraceDecision{},
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

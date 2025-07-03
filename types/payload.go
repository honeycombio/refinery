package types

import (
	"bytes"
	"encoding/json"
	"fmt"
	"iter"
	"maps"
	"slices"
	"strings"

	"github.com/honeycombio/refinery/config"
	jsoniter "github.com/json-iterator/go"
	"github.com/tinylib/msgp/msgp"
)

type FieldType int

const (
	FieldTypeUnknown FieldType = iota
	FieldTypeInt64
	FieldTypeFloat64
	FieldTypeString
	FieldTypeBool

	// Arrays, maps, other stuff supported by the wire protocols but not
	// expected to be very common.
	FieldTypeOther
)

// Metadata field constants
const (
	MetaSignalType                = "meta.signal_type"
	MetaTraceID                   = "meta.trace_id"
	MetaAnnotationType            = "meta.annotation_type"
	MetaRefineryProbe             = "meta.refinery.probe"
	MetaRefineryRoot              = "meta.refinery.root"
	MetaRefineryIncomingUserAgent = "meta.refinery.incoming_user_agent"
	MetaRefinerySendBy            = "meta.refinery.send_by"
	MetaRefinerySpanDataSize      = "meta.refinery.span_data_size"
	MetaRefineryMinSpan           = "meta.refinery.min_span"
	MetaRefineryForwarded         = "meta.refinery.forwarded"
	MetaRefineryExpiredTrace      = "meta.refinery.expired_trace"

	//	These fields are not used by the refinery itself for sampling decisions.
	// They are used to pass information from refinery to honeycomb.
	MetaRefineryLocalHostname      = "meta.refinery.local_hostname"
	MetaStressed                   = "meta.stressed"
	MetaRefineryReason             = "meta.refinery.reason"
	MetaRefinerySendReason         = "meta.refinery.send_reason"
	MetaSpanEventCount             = "meta.span_event_count"
	MetaSpanLinkCount              = "meta.span_link_count"
	MetaSpanCount                  = "meta.span_count"
	MetaEventCount                 = "meta.event_count"
	MetaRefineryOriginalSampleRate = "meta.refinery.original_sample_rate"
	MetaRefineryShutdownSend       = "meta.refinery.shutdown_send"
	MetaRefinerySampleKey          = "meta.refinery.sample_key"
)

// Contains an entry for each of the specialized metadata fields.
// All metadata field keys MUST start with "meta." prefix.
// To add a new field, just add it to the Payload struct, and add a new entry
// to metadataFields. Yes, this could all be done with reflect but that would
// be terribly slow. Yes, this could be done with generated code but maintaining
// a code generator won't make anyone's life easier. Yes, the *Msgp functions
// could be implemented in terms of get and set but this would transit the
// concrete values through type any, which is inefficient. This is the compromise.
type metadataField struct {
	key           string
	expectedType  FieldType
	get           func(p *Payload) (value any, ok bool)               // Payload.Get, Payload.All
	set           func(p *Payload, value any)                         // Payload.Set
	appendMsgp    func(p *Payload, in []byte) (out []byte, ok bool)   // Payload.MarshalMsg
	unmarshalMsgp func(p *Payload, in []byte) (out []byte, err error) // Payload.extractMetadataFromBytes
}

var metadataFields = []metadataField{
	{
		key:          MetaSignalType,
		expectedType: FieldTypeString,
		get: func(p *Payload) (value any, ok bool) {
			if p.MetaSignalType != "" {
				return p.MetaSignalType, true
			}
			return nil, false
		},
		set: func(p *Payload, value any) {
			if v, ok := value.(string); ok {
				p.MetaSignalType = v
			}
		},
		appendMsgp: func(p *Payload, in []byte) (out []byte, ok bool) {
			if p.MetaSignalType != "" {
				out = msgp.AppendString(in, MetaSignalType)
				out = msgp.AppendString(out, p.MetaSignalType)
				return out, true
			}
			return in, false
		},
		unmarshalMsgp: func(p *Payload, in []byte) (out []byte, err error) {
			p.MetaSignalType, out, err = msgp.ReadStringBytes(in)
			return out, err
		},
	},
	{
		key:          MetaTraceID,
		expectedType: FieldTypeString,
		get: func(p *Payload) (value any, ok bool) {
			if p.MetaTraceID != "" {
				return p.MetaTraceID, true
			}
			return nil, false
		},
		set: func(p *Payload, value any) {
			if v, ok := value.(string); ok {
				p.MetaTraceID = v
			}
		},
		appendMsgp: func(p *Payload, in []byte) (out []byte, ok bool) {
			return in, false
		},
		unmarshalMsgp: func(p *Payload, in []byte) (out []byte, err error) {
			p.MetaTraceID, out, err = msgp.ReadStringBytes(in)
			return out, err
		},
	},
	{
		key:          MetaAnnotationType,
		expectedType: FieldTypeString,
		get: func(p *Payload) (value any, ok bool) {
			if p.MetaAnnotationType != "" {
				return p.MetaAnnotationType, true
			}
			return nil, false
		},
		set: func(p *Payload, value any) {
			if v, ok := value.(string); ok {
				p.MetaAnnotationType = v
			}
		},
		appendMsgp: func(p *Payload, in []byte) (out []byte, ok bool) {
			if p.MetaAnnotationType != "" {
				out = msgp.AppendString(in, MetaAnnotationType)
				out = msgp.AppendString(out, p.MetaAnnotationType)
				return out, true
			}
			return in, false
		},
		unmarshalMsgp: func(p *Payload, in []byte) (out []byte, err error) {
			p.MetaAnnotationType, out, err = msgp.ReadStringBytes(in)
			return out, err
		},
	},
	{
		key:          MetaRefineryProbe,
		expectedType: FieldTypeBool,
		get: func(p *Payload) (value any, ok bool) {
			if p.MetaRefineryProbe.HasValue {
				return p.MetaRefineryProbe.Value, true
			}
			return nil, false
		},
		set: func(p *Payload, value any) {
			if v, ok := value.(bool); ok {
				p.MetaRefineryProbe.Set(v)
			}
		},
		appendMsgp: func(p *Payload, in []byte) (out []byte, ok bool) {
			if p.MetaRefineryProbe.HasValue {
				out = msgp.AppendString(in, MetaRefineryProbe)
				out = msgp.AppendBool(out, p.MetaRefineryProbe.Value)
				return out, true
			}
			return in, false
		},
		unmarshalMsgp: func(p *Payload, in []byte) (out []byte, err error) {
			var val bool
			val, out, err = msgp.ReadBoolBytes(in)
			if err == nil {
				p.MetaRefineryProbe.Set(val)
			}
			return out, err
		},
	},
	{
		key:          MetaRefineryRoot,
		expectedType: FieldTypeBool,
		get: func(p *Payload) (value any, ok bool) {
			if p.MetaRefineryRoot.HasValue {
				return p.MetaRefineryRoot.Value, true
			}
			return nil, false
		},
		set: func(p *Payload, value any) {
			if v, ok := value.(bool); ok {
				p.MetaRefineryRoot.Set(v)
			}
		},
		appendMsgp: func(p *Payload, in []byte) (out []byte, ok bool) {
			if p.MetaRefineryRoot.HasValue && p.MetaRefineryRoot.Value {
				out = msgp.AppendString(in, MetaRefineryRoot)
				out = msgp.AppendBool(out, p.MetaRefineryRoot.Value)
				return out, true
			}
			return in, false
		},
		unmarshalMsgp: func(p *Payload, in []byte) (out []byte, err error) {
			var val bool
			val, out, err = msgp.ReadBoolBytes(in)
			if err == nil {
				p.MetaRefineryRoot.Set(val)
			}
			return out, err
		},
	},
	{
		key:          MetaRefineryIncomingUserAgent,
		expectedType: FieldTypeString,
		get: func(p *Payload) (value any, ok bool) {
			if p.MetaRefineryIncomingUserAgent != "" {
				return p.MetaRefineryIncomingUserAgent, true
			}
			return nil, false
		},
		set: func(p *Payload, value any) {
			if v, ok := value.(string); ok {
				p.MetaRefineryIncomingUserAgent = v
			}
		},
		appendMsgp: func(p *Payload, in []byte) (out []byte, ok bool) {
			if p.MetaRefineryIncomingUserAgent != "" {
				out = msgp.AppendString(in, MetaRefineryIncomingUserAgent)
				out = msgp.AppendString(out, p.MetaRefineryIncomingUserAgent)
				return out, true
			}
			return in, false
		},
		unmarshalMsgp: func(p *Payload, in []byte) (out []byte, err error) {
			p.MetaRefineryIncomingUserAgent, out, err = msgp.ReadStringBytes(in)
			return out, err
		},
	},
	{
		key:          MetaRefinerySendBy,
		expectedType: FieldTypeInt64,
		get: func(p *Payload) (value any, ok bool) {
			if p.MetaRefinerySendBy != 0 {
				return p.MetaRefinerySendBy, true
			}
			return nil, false
		},
		set: func(p *Payload, value any) {
			if v, ok := value.(int64); ok {
				p.MetaRefinerySendBy = v
			}
		},
		appendMsgp: func(p *Payload, in []byte) (out []byte, ok bool) {
			if p.MetaRefinerySendBy != 0 {
				out = msgp.AppendString(in, MetaRefinerySendBy)
				out = msgp.AppendInt64(out, p.MetaRefinerySendBy)
				return out, true
			}
			return in, false
		},
		unmarshalMsgp: func(p *Payload, in []byte) (out []byte, err error) {
			p.MetaRefinerySendBy, out, err = msgp.ReadInt64Bytes(in)
			return out, err
		},
	},
	{
		key:          MetaRefinerySpanDataSize,
		expectedType: FieldTypeInt64,
		get: func(p *Payload) (value any, ok bool) {
			if p.MetaRefinerySpanDataSize != 0 {
				return p.MetaRefinerySpanDataSize, true
			}
			return nil, false
		},
		set: func(p *Payload, value any) {
			if v, ok := value.(int64); ok {
				p.MetaRefinerySpanDataSize = v
			}
		},
		appendMsgp: func(p *Payload, in []byte) (out []byte, ok bool) {
			if p.MetaRefinerySpanDataSize != 0 {
				out = msgp.AppendString(in, MetaRefinerySpanDataSize)
				out = msgp.AppendInt64(out, p.MetaRefinerySpanDataSize)
				return out, true
			}
			return in, false
		},
		unmarshalMsgp: func(p *Payload, in []byte) (out []byte, err error) {
			p.MetaRefinerySpanDataSize, out, err = msgp.ReadInt64Bytes(in)
			return out, err
		},
	},
	{
		key:          MetaRefineryMinSpan,
		expectedType: FieldTypeBool,
		get: func(p *Payload) (value any, ok bool) {
			if p.MetaRefineryMinSpan.HasValue {
				return p.MetaRefineryMinSpan.Value, true
			}
			return nil, false
		},
		set: func(p *Payload, value any) {
			if v, ok := value.(bool); ok {
				p.MetaRefineryMinSpan.Set(v)
			}
		},
		appendMsgp: func(p *Payload, in []byte) (out []byte, ok bool) {
			if p.MetaRefineryMinSpan.HasValue {
				out = msgp.AppendString(in, MetaRefineryMinSpan)
				out = msgp.AppendBool(out, p.MetaRefineryMinSpan.Value)
				return out, true
			}
			return in, false
		},
		unmarshalMsgp: func(p *Payload, in []byte) (out []byte, err error) {
			var val bool
			val, out, err = msgp.ReadBoolBytes(in)
			if err == nil {
				p.MetaRefineryMinSpan.Set(val)
			}
			return out, err
		},
	},
	{
		key:          MetaRefineryForwarded,
		expectedType: FieldTypeString,
		get: func(p *Payload) (value any, ok bool) {
			if p.MetaRefineryForwarded != "" {
				return p.MetaRefineryForwarded, true
			}
			return nil, false
		},
		set: func(p *Payload, value any) {
			if v, ok := value.(string); ok {
				p.MetaRefineryForwarded = v
			}
		},
		appendMsgp: func(p *Payload, in []byte) (out []byte, ok bool) {
			if p.MetaRefineryForwarded != "" {
				out = msgp.AppendString(in, MetaRefineryForwarded)
				out = msgp.AppendString(out, p.MetaRefineryForwarded)
				return out, true
			}
			return in, false
		},
		unmarshalMsgp: func(p *Payload, in []byte) (out []byte, err error) {
			p.MetaRefineryForwarded, out, err = msgp.ReadStringBytes(in)
			return out, err
		},
	},
	{
		key:          MetaRefineryExpiredTrace,
		expectedType: FieldTypeBool,
		get: func(p *Payload) (value any, ok bool) {
			if p.MetaRefineryExpiredTrace.HasValue {
				return p.MetaRefineryExpiredTrace.Value, true
			}
			return nil, false
		},
		set: func(p *Payload, value any) {
			if v, ok := value.(bool); ok {
				p.MetaRefineryExpiredTrace.Set(v)
			}
		},
		appendMsgp: func(p *Payload, in []byte) (out []byte, ok bool) {
			if p.MetaRefineryExpiredTrace.HasValue {
				out = msgp.AppendString(in, MetaRefineryExpiredTrace)
				out = msgp.AppendBool(out, p.MetaRefineryExpiredTrace.Value)
				return out, true
			}
			return in, false
		},
		unmarshalMsgp: func(p *Payload, in []byte) (out []byte, err error) {
			var val bool
			val, out, err = msgp.ReadBoolBytes(in)
			if err == nil {
				p.MetaRefineryExpiredTrace.Set(val)
			}
			return out, err
		},
	},
	{
		key:          MetaRefineryLocalHostname,
		expectedType: FieldTypeString,
		get: func(p *Payload) (value any, ok bool) {
			if p.MetaRefineryLocalHostname != "" {
				return p.MetaRefineryLocalHostname, true
			}
			return nil, false
		},
		set: func(p *Payload, value any) {
			if v, ok := value.(string); ok {
				p.MetaRefineryLocalHostname = v
			}
		},
		appendMsgp: func(p *Payload, in []byte) (out []byte, ok bool) {
			if p.MetaRefineryLocalHostname != "" {
				out = msgp.AppendString(in, MetaRefineryLocalHostname)
				out = msgp.AppendString(out, p.MetaRefineryLocalHostname)
				return out, true
			}
			return in, false
		},
		unmarshalMsgp: func(p *Payload, in []byte) (out []byte, err error) {
			p.MetaRefineryLocalHostname, out, err = msgp.ReadStringBytes(in)
			return out, err
		},
	},
	{
		key:          MetaStressed,
		expectedType: FieldTypeBool,
		get: func(p *Payload) (value any, ok bool) {
			if p.MetaStressed.HasValue {
				return p.MetaStressed.Value, true
			}
			return nil, false
		},
		set: func(p *Payload, value any) {
			if v, ok := value.(bool); ok {
				p.MetaStressed.Set(v)
			}
		},
		appendMsgp: func(p *Payload, in []byte) (out []byte, ok bool) {
			if p.MetaStressed.HasValue {
				out = msgp.AppendString(in, MetaStressed)
				out = msgp.AppendBool(out, p.MetaStressed.Value)
				return out, true
			}
			return in, false
		},
		unmarshalMsgp: func(p *Payload, in []byte) (out []byte, err error) {
			var val bool
			val, out, err = msgp.ReadBoolBytes(in)
			if err == nil {
				p.MetaStressed.Set(val)
			}
			return out, err
		},
	},
	{
		key:          MetaRefineryReason,
		expectedType: FieldTypeString,
		get: func(p *Payload) (value any, ok bool) {
			if p.MetaRefineryReason != "" {
				return p.MetaRefineryReason, true
			}
			return nil, false
		},
		set: func(p *Payload, value any) {
			if v, ok := value.(string); ok {
				p.MetaRefineryReason = v
			}
		},
		appendMsgp: func(p *Payload, in []byte) (out []byte, ok bool) {
			if p.MetaRefineryReason != "" {
				out = msgp.AppendString(in, MetaRefineryReason)
				out = msgp.AppendString(out, p.MetaRefineryReason)
				return out, true
			}
			return in, false
		},
		unmarshalMsgp: func(p *Payload, in []byte) (out []byte, err error) {
			p.MetaRefineryReason, out, err = msgp.ReadStringBytes(in)
			return out, err
		},
	},
	{
		key:          MetaRefinerySendReason,
		expectedType: FieldTypeString,
		get: func(p *Payload) (value any, ok bool) {
			if p.MetaRefinerySendReason != "" {
				return p.MetaRefinerySendReason, true
			}
			return nil, false
		},
		set: func(p *Payload, value any) {
			if v, ok := value.(string); ok {
				p.MetaRefinerySendReason = v
			}
		},
		appendMsgp: func(p *Payload, in []byte) (out []byte, ok bool) {
			if p.MetaRefinerySendReason != "" {
				out = msgp.AppendString(in, MetaRefinerySendReason)
				out = msgp.AppendString(out, p.MetaRefinerySendReason)
				return out, true
			}
			return in, false
		},
		unmarshalMsgp: func(p *Payload, in []byte) (out []byte, err error) {
			p.MetaRefinerySendReason, out, err = msgp.ReadStringBytes(in)
			return out, err
		},
	},
	{
		key:          MetaSpanEventCount,
		expectedType: FieldTypeInt64,
		get: func(p *Payload) (value any, ok bool) {
			if p.MetaSpanEventCount != 0 {
				return p.MetaSpanEventCount, true
			}
			return nil, false
		},
		set: func(p *Payload, value any) {
			if v, ok := value.(int64); ok {
				p.MetaSpanEventCount = v
			}
		},
		appendMsgp: func(p *Payload, in []byte) (out []byte, ok bool) {
			if p.MetaSpanEventCount != 0 {
				out = msgp.AppendString(in, MetaSpanEventCount)
				out = msgp.AppendInt64(out, p.MetaSpanEventCount)
				return out, true
			}
			return in, false
		},
		unmarshalMsgp: func(p *Payload, in []byte) (out []byte, err error) {
			p.MetaSpanEventCount, out, err = msgp.ReadInt64Bytes(in)
			return out, err
		},
	},
	{
		key:          MetaSpanLinkCount,
		expectedType: FieldTypeInt64,
		get: func(p *Payload) (value any, ok bool) {
			if p.MetaSpanLinkCount != 0 {
				return p.MetaSpanLinkCount, true
			}
			return nil, false
		},
		set: func(p *Payload, value any) {
			if v, ok := value.(int64); ok {
				p.MetaSpanLinkCount = v
			}
		},
		appendMsgp: func(p *Payload, in []byte) (out []byte, ok bool) {
			if p.MetaSpanLinkCount != 0 {
				out = msgp.AppendString(in, MetaSpanLinkCount)
				out = msgp.AppendInt64(out, p.MetaSpanLinkCount)
				return out, true
			}
			return in, false
		},
		unmarshalMsgp: func(p *Payload, in []byte) (out []byte, err error) {
			p.MetaSpanLinkCount, out, err = msgp.ReadInt64Bytes(in)
			return out, err
		},
	},
	{
		key:          MetaSpanCount,
		expectedType: FieldTypeInt64,
		get: func(p *Payload) (value any, ok bool) {
			if p.MetaSpanCount != 0 {
				return p.MetaSpanCount, true
			}
			return nil, false
		},
		set: func(p *Payload, value any) {
			if v, ok := value.(int64); ok {
				p.MetaSpanCount = v
			}
		},
		appendMsgp: func(p *Payload, in []byte) (out []byte, ok bool) {
			if p.MetaSpanCount != 0 {
				out = msgp.AppendString(in, MetaSpanCount)
				out = msgp.AppendInt64(out, p.MetaSpanCount)
				return out, true
			}
			return in, false
		},
		unmarshalMsgp: func(p *Payload, in []byte) (out []byte, err error) {
			p.MetaSpanCount, out, err = msgp.ReadInt64Bytes(in)
			return out, err
		},
	},
	{
		key:          MetaEventCount,
		expectedType: FieldTypeInt64,
		get: func(p *Payload) (value any, ok bool) {
			if p.MetaEventCount != 0 {
				return p.MetaEventCount, true
			}
			return nil, false
		},
		set: func(p *Payload, value any) {
			if v, ok := value.(int64); ok {
				p.MetaEventCount = v
			}
		},
		appendMsgp: func(p *Payload, in []byte) (out []byte, ok bool) {
			if p.MetaEventCount != 0 {
				out = msgp.AppendString(in, MetaEventCount)
				out = msgp.AppendInt64(out, p.MetaEventCount)
				return out, true
			}
			return in, false
		},
		unmarshalMsgp: func(p *Payload, in []byte) (out []byte, err error) {
			p.MetaEventCount, out, err = msgp.ReadInt64Bytes(in)
			return out, err
		},
	},
	{
		key:          MetaRefineryOriginalSampleRate,
		expectedType: FieldTypeInt64,
		get: func(p *Payload) (value any, ok bool) {
			if p.MetaRefineryOriginalSampleRate != 0 {
				return p.MetaRefineryOriginalSampleRate, true
			}
			return nil, false
		},
		set: func(p *Payload, value any) {
			if v, ok := value.(int64); ok {
				p.MetaRefineryOriginalSampleRate = v
			}
		},
		appendMsgp: func(p *Payload, in []byte) (out []byte, ok bool) {
			if p.MetaRefineryOriginalSampleRate != 0 {
				out = msgp.AppendString(in, MetaRefineryOriginalSampleRate)
				out = msgp.AppendInt64(out, p.MetaRefineryOriginalSampleRate)
				return out, true
			}
			return in, false
		},
		unmarshalMsgp: func(p *Payload, in []byte) (out []byte, err error) {
			p.MetaRefineryOriginalSampleRate, out, err = msgp.ReadInt64Bytes(in)
			return out, err
		},
	},
	{
		key:          MetaRefineryShutdownSend,
		expectedType: FieldTypeBool,
		get: func(p *Payload) (value any, ok bool) {
			if p.MetaRefineryShutdownSend.HasValue {
				return p.MetaRefineryShutdownSend.Value, true
			}
			return nil, false
		},
		set: func(p *Payload, value any) {
			if v, ok := value.(bool); ok {
				p.MetaRefineryShutdownSend.Set(v)
			}
		},
		appendMsgp: func(p *Payload, in []byte) (out []byte, ok bool) {
			if p.MetaRefineryShutdownSend.HasValue {
				out = msgp.AppendString(in, MetaRefineryShutdownSend)
				out = msgp.AppendBool(out, p.MetaRefineryShutdownSend.Value)
				return out, true
			}
			return in, false
		},
		unmarshalMsgp: func(p *Payload, in []byte) (out []byte, err error) {
			var val bool
			val, out, err = msgp.ReadBoolBytes(in)
			if err == nil {
				p.MetaRefineryShutdownSend.Set(val)
			}
			return out, err
		},
	},
	{
		key:          MetaRefinerySampleKey,
		expectedType: FieldTypeString,
		get: func(p *Payload) (value any, ok bool) {
			if p.MetaRefinerySampleKey != "" {
				return p.MetaRefinerySampleKey, true
			}
			return nil, false
		},
		set: func(p *Payload, value any) {
			if v, ok := value.(string); ok {
				p.MetaRefinerySampleKey = v
			}
		},
		appendMsgp: func(p *Payload, in []byte) (out []byte, ok bool) {
			if p.MetaRefinerySampleKey != "" {
				out = msgp.AppendString(in, MetaRefinerySampleKey)
				out = msgp.AppendString(out, p.MetaRefinerySampleKey)
				return out, true
			}
			return in, false
		},
		unmarshalMsgp: func(p *Payload, in []byte) (out []byte, err error) {
			p.MetaRefinerySampleKey, out, err = msgp.ReadStringBytes(in)
			return out, err
		},
	},
}

// isMetadataField checks if a given key is a metadata field
func isMetadataField(key string) bool {
	for _, field := range metadataFields {
		if field.key == key {
			return true
		}
	}
	return false
}

type nullableBool struct {
	HasValue bool
	Value    bool
}

func (nb *nullableBool) Set(value bool) {
	nb.HasValue = true
	nb.Value = value
}

func (nb *nullableBool) Unset() {
	nb.HasValue = false
	nb.Value = false
}

// Holds a conceptual map[string]any, but with key metadata fields exposed
// directly for efficiency. When deserialized from MessagePack data, the serial
// data is kept internally rather than being fully unmarshaled into an expensive
// map. Since refinery must shuttle the entire payload through the API to
// honeycomb, but never accessess most fields, this is a big speedup.
type Payload struct {
	// A serialized messagepack map used to source fields.
	msgpMap MsgpPayloadMap

	// Deserialized fields, either from the internal msgpMap, or set externally.
	memoizedFields map[string]any
	// missingFields is a set of fields that were not found in the payload.
	// this is used to avoid repeatedly deserializing fields that are not present.
	missingFields map[string]struct{}

	hasExtractedMetadata bool

	config config.Config

	// Cached metadata fields for efficient access
	MetaSignalType                string       // meta.signal_type
	MetaTraceID                   string       // meta.trace_id
	MetaAnnotationType            string       // meta.annotation_type
	MetaRefineryProbe             nullableBool // meta.refinery.probe
	MetaRefineryRoot              nullableBool // meta.refinery.root
	MetaRefineryIncomingUserAgent string       // meta.refinery.incoming_user_agent
	MetaRefinerySendBy            int64        // meta.refinery.send_by (Unix timestamp)
	MetaRefinerySpanDataSize      int64        // meta.refinery.span_data_size
	MetaRefineryMinSpan           nullableBool // meta.refinery.min_span
	MetaRefineryForwarded         string       // meta.refinery.forwarded
	MetaRefineryExpiredTrace      nullableBool // meta.refinery.expired_trace

	MetaRefineryLocalHostname      string       // meta.refinery.local_hostname
	MetaStressed                   nullableBool // meta.stressed
	MetaRefineryReason             string       // meta.refinery.reason
	MetaRefinerySendReason         string       // meta.refinery.send_reason
	MetaSpanEventCount             int64        // meta.span_event_count
	MetaSpanLinkCount              int64        // meta.span_link_count
	MetaSpanCount                  int64        // meta.span_count
	MetaEventCount                 int64        // meta.event_count
	MetaRefineryOriginalSampleRate int64        // meta.refinery.original_sample_rate
	MetaRefineryShutdownSend       nullableBool // meta.refinery.shutdown_send
	MetaRefinerySampleKey          string       // meta.refinery.sample_key
}

// extractMetadataFromBytes extracts metadata from msgpack data.
// If consumed is non-nil, it will be set to the number of bytes consumed from the data.
func (p *Payload) extractMetadataFromBytes(data []byte) (int, error) {
	if !p.MetaRefineryRoot.HasValue {
		p.MetaRefineryRoot.Set(true)
	}

	traceIdFieldNames := p.config.GetTraceIdFieldNames()
	parentIdFieldNames := p.config.GetParentIdFieldNames()

	// Read the map header
	mapSize, remaining, err := msgp.ReadMapHeaderBytes(data)
	if err != nil {
		return len(data) - len(remaining), fmt.Errorf("failed to read msgpack map header: %w", err)
	}

	// Process all map entries
	for i := uint32(0); i < mapSize; i++ {
		// Read the key
		var keyBytes []byte
		keyBytes, remaining, err = msgp.ReadMapKeyZC(remaining)
		if err != nil {
			return len(data) - len(remaining), fmt.Errorf("failed to read msgpack key: %w", err)
		}

		valueType := msgp.NextType(remaining)

		// Check if this is a metadata field we care about
		handled := false

		// Optimization: only check metadata fields if key starts with "meta."
		if bytes.HasPrefix(keyBytes, []byte("meta.")) {
			// Try to handle as a metadata field
			for _, field := range metadataFields {
				// Skip fields that don't match the expected type
				switch field.expectedType {
				case FieldTypeString:
					if valueType != msgp.StrType {
						continue
					}
				case FieldTypeBool:
					if valueType != msgp.BoolType {
						continue
					}
				case FieldTypeInt64:
					if valueType != msgp.IntType && valueType != msgp.UintType {
						continue
					}
				}

				if bytes.Equal(keyBytes, []byte(field.key)) {
					newRemaining, err := field.unmarshalMsgp(p, remaining)
					if err != nil {
						return len(data) - len(remaining), fmt.Errorf("failed to read value for key %s: %w", string(keyBytes), err)
					}
					remaining = newRemaining
					handled = true
					break
				}
			}
		}

		// Handle special trace ID and parent ID fields
		if !handled && valueType == msgp.StrType {
			if p.MetaTraceID == "" && sliceContains(traceIdFieldNames, keyBytes) {
				p.MetaTraceID, remaining, err = msgp.ReadStringBytes(remaining)
				handled = true
			} else if sliceContains(parentIdFieldNames, keyBytes) {
				var parentId string
				parentId, remaining, err = msgp.ReadStringBytes(remaining)
				if err == nil && parentId != "" {
					p.MetaRefineryRoot.Set(false)
				}
				handled = true
			}
		}

		if err != nil {
			return len(data) - len(remaining), fmt.Errorf("failed to read value for key %s: %w", string(keyBytes), err)
		}

		// If we didn't handle this field as metadata, skip it
		if !handled {
			remaining, err = msgp.Skip(remaining)
			if err != nil {
				return len(data) - len(remaining), fmt.Errorf("failed to skip value: %w", err)
			}
		}
	}

	// A log message cannot be a root span.
	if p.MetaSignalType == "log" {
		p.MetaRefineryRoot.Unset()
	}

	p.hasExtractedMetadata = true
	return len(data) - len(remaining), nil
}

// ExtractMetadata populates the cached metadata fields from the payload data.
// This MUST be called manually after creating a non-empty Payload
// to populate the metadata fields.
func (p *Payload) ExtractMetadata() error {
	if p.hasExtractedMetadata {
		return nil
	}

	if !p.MetaRefineryRoot.HasValue {
		p.MetaRefineryRoot.Set(true)
	}

	var traceIdFieldNames, parentIdFieldNames []string
	if p.config != nil {
		traceIdFieldNames = p.config.GetTraceIdFieldNames()
		parentIdFieldNames = p.config.GetParentIdFieldNames()
	}

	// For memoized fields, directly access the map
	if p.memoizedFields != nil {
		for key, value := range p.memoizedFields {
			// Try metadata fields first
			handled := false
			for _, field := range metadataFields {
				if field.key == key {
					if field.expectedType == FieldTypeInt64 {
						switch t := value.(type) {
						case float64:
							// JSON unmarshal will generally turn ints into floats.
							field.set(p, int64(t))
						case int:
							field.set(p, int64(t))
						default:
							field.set(p, t)
						}
					} else {
						field.set(p, value)
					}
					handled = true
					break
				}
			}

			// If not handled as metadata, check for trace/parent ID fields
			if !handled {
				// Check if this is a trace ID field
				if p.MetaTraceID == "" && slices.Contains(traceIdFieldNames, key) {
					if v, ok := value.(string); ok && v != "" {
						p.MetaTraceID = v
					}
				} else if slices.Contains(parentIdFieldNames, key) {
					// Check if this is a parent ID field
					if v, ok := value.(string); ok && v != "" {
						p.MetaRefineryRoot.Set(false)
					}
				}
			}
		}
	}

	// For msgpMap fields, extract from the raw bytes
	if p.msgpMap.Size() > 0 {
		_, err := p.extractMetadataFromBytes(p.msgpMap.rawData)
		if err != nil {
			return err
		}
	}

	// A log message cannot be a root span.
	if p.MetaSignalType == "log" {
		p.MetaRefineryRoot.Unset()
	}

	p.hasExtractedMetadata = true
	return nil
}

// NewPayload creates a new Payload from a map of fields. This is not populate
// metadata fields
func NewPayload(config config.Config, data map[string]any) Payload {
	p := Payload{
		memoizedFields: data,
	}

	p.config = config
	return p
}

// UnmarshalMsgpack implements msgpack.Unmarshaler, but doesn't unmarshal. Instead it
// keep a reference to serialized data.
func (p *Payload) UnmarshalMsgpack(data []byte) error {
	p.msgpMap = MsgpPayloadMap{rawData: data}
	p.ExtractMetadata()
	return nil
}

// UnmarshalMsg implements msgp.Unmarshaler, similar to above but expects to be
// part of a larger message.
func (p *Payload) UnmarshalMsg(bts []byte) (o []byte, err error) {
	// Extract metadata and get consumed bytes
	consumed, err := p.extractMetadataFromBytes(bts)
	if err != nil {
		return nil, err
	}

	// Store the raw data
	ourData := bts[:consumed]
	p.msgpMap = MsgpPayloadMap{rawData: ourData}

	// Return remainder
	return bts[consumed:], nil
}

func (p *Payload) UnmarshalJSON(data []byte) error {
	var fields map[string]any
	if err := jsoniter.Unmarshal(data, &fields); err != nil {
		return err
	}
	p.memoizedFields = fields
	p.ExtractMetadata()
	return nil
}

func (p *Payload) MemoizeFields(keys ...string) {
	if p.memoizedFields == nil {
		p.memoizedFields = make(map[string]any, len(keys))
	}
	if p.missingFields == nil {
		p.missingFields = make(map[string]struct{}, len(keys))
	}

	keysToFind := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if _, ok := p.missingFields[key]; ok {
			continue
		}
		if _, ok := p.memoizedFields[key]; !ok {
			keysToFind[key] = struct{}{}
		}
	}
	if len(keysToFind) == 0 {
		return
	}

	iter, err := p.msgpMap.Iterate()
	if err != nil {
		return
	}

	var keysFound int
	for keysFound < len(keysToFind) {
		keyBytes, _, err := iter.NextKey()
		if err != nil {
			break
		}

		// Note we deliberately don't put string(keyBytes) in a variable here,
		// because doing so will move it to the heap on every iteration.
		// Keeping the string cast inline like this allows us to avoid the heap
		// unless we're actually going to memoize the field.
		if _, ok := keysToFind[string(keyBytes)]; ok {
			value, err := iter.ValueAny()
			if err != nil {
				break
			}
			key := string(keyBytes)
			p.memoizedFields[key] = value
			keysFound++
		}
	}

	for key := range keysToFind {
		if _, ok := p.memoizedFields[key]; !ok {
			p.missingFields[key] = struct{}{}
		}
	}
}

func (p *Payload) Exists(key string) bool {
	// if the key is a metadata field, check the dedicated field
	switch key {
	case MetaSignalType:
		return p.MetaSignalType != ""
	case MetaTraceID:
		return p.MetaTraceID != ""
	case MetaAnnotationType:
		return p.MetaAnnotationType != ""
	case MetaRefineryProbe:
		return p.MetaRefineryProbe.HasValue
	case MetaRefineryRoot:
		return p.MetaRefineryRoot.HasValue
	case MetaRefineryIncomingUserAgent:
		return p.MetaRefineryIncomingUserAgent != ""
	case MetaRefinerySendBy:
		return p.MetaRefinerySendBy != 0
	case MetaRefinerySpanDataSize:
		return p.MetaRefinerySpanDataSize != 0
	case MetaRefineryMinSpan:
		return p.MetaRefineryMinSpan.HasValue
	case MetaRefineryForwarded:
		return p.MetaRefineryForwarded != ""
	case MetaRefineryExpiredTrace:
		return p.MetaRefineryExpiredTrace.HasValue
	case MetaRefineryLocalHostname:
		return p.MetaRefineryLocalHostname != ""
	case MetaStressed:
		return p.MetaStressed.HasValue
	case MetaRefineryReason:
		return p.MetaRefineryReason != ""
	case MetaRefinerySendReason:
		return p.MetaRefinerySendReason != ""
	case MetaSpanEventCount:
		return p.MetaSpanEventCount != 0
	case MetaSpanLinkCount:
		return p.MetaSpanLinkCount != 0
	case MetaSpanCount:
		return p.MetaSpanCount != 0
	case MetaEventCount:
		return p.MetaEventCount != 0
	case MetaRefineryOriginalSampleRate:
		return p.MetaRefineryOriginalSampleRate != 0
	case MetaRefineryShutdownSend:
		return p.MetaRefineryShutdownSend.HasValue
	case MetaRefinerySampleKey:
		return p.MetaRefinerySampleKey != ""
	}
	if p.memoizedFields != nil {
		if _, ok := p.memoizedFields[key]; ok {
			return true
		}
	}

	if p.missingFields != nil {
		if _, ok := p.missingFields[key]; ok {
			return false
		}
	}

	iter, err := p.msgpMap.Iterate()
	if err != nil {
		return false
	}

	for {
		keyBytes, _, err := iter.NextKey()
		if err != nil {
			break
		}

		if string(keyBytes) == key {
			return true
		}
	}

	return false
}

// Get retrieves a value from the Payload by key.
// Use Get if the field is expected to only be accessed once.
// If the field is expected to be accessed multiple times, use MemoizeFields
func (p *Payload) Get(key string) any {
	// Check if this is a metadata field and return from dedicated field
	if strings.HasPrefix(key, "meta.") {
		for _, field := range metadataFields {
			if field.key == key {
				if value, ok := field.get(p); ok {
					return value
				}
				break
			}
		}
	}

	if p.memoizedFields != nil {
		if value, ok := p.memoizedFields[key]; ok {
			return value
		}
	}

	if p.missingFields != nil {
		if _, ok := p.missingFields[key]; ok {
			return nil
		}
	}

	iter, err := p.msgpMap.Iterate()
	if err != nil {
		return nil
	}

	for {
		keyBytes, _, err := iter.NextKey()
		if err != nil {
			break
		}

		if string(keyBytes) == key {
			value, err := iter.ValueAny()
			if err == nil {
				return value
			}
			break
		}
	}

	return nil
}

func (p *Payload) Set(key string, value any) {
	// Check if this is a metadata field and update dedicated field
	for _, field := range metadataFields {
		if field.key == key {
			field.set(p, value)
			break
		}
	}

	if p.memoizedFields == nil {
		p.memoizedFields = make(map[string]any)
	}
	p.memoizedFields[key] = value
}

func (p *Payload) IsEmpty() bool {
	return len(p.memoizedFields) == 0 && p.msgpMap.Size() == 0
}

// All() allows easily iterating all values in the Payload, but this is very
// NOT EFFICIENT relative to getting a subset of values using Get. Don't use
// this in non-test code unless you have to other choice.
// We only expect this to happen when transmitting sampled events using Libhoney.
func (p *Payload) All() iter.Seq2[string, any] {
	return func(yield func(string, any) bool) {
		// First yield metadata fields with non-default values
		for _, field := range metadataFields {
			if field.key == MetaTraceID {
				continue
			}
			if field.key == MetaRefineryRoot && !p.MetaRefineryRoot.Value {
				// Skip the root field if it's false, as it doesn't need to be yielded
				continue
			}
			if value, ok := field.get(p); ok {
				if !yield(field.key, value) {
					return
				}
			}
		}

		// Then yield memoized fields
		for key, value := range p.memoizedFields {
			if !yield(key, value) {
				return
			}
		}

		// Then iterate through msgpMap for any remaining fields
		iter, err := p.msgpMap.Iterate()
		if err != nil {
			return
		}

		for {
			keyBytes, _, err := iter.NextKey()
			if err != nil {
				break
			}

			key := string(keyBytes)
			if p.memoizedFields != nil {
				if _, ok := p.memoizedFields[key]; ok {
					// Don't yield the same key twice. Memoized values take
					// precedence over serialized values, which we can't
					// update.
					continue
				}
			}

			// Skip metadata fields as they're already yielded
			if isMetadataField(key) {
				continue
			}

			value, err := iter.ValueAny()
			if err != nil {
				return
			}
			if !yield(key, value) {
				return
			}
		}
	}
}

// Estimates data size, not very accurately, but it's fast.
func (p *Payload) GetDataSize() int {
	total := p.msgpMap.Size()
	for k, v := range p.memoizedFields {
		total += len(k) + getByteSize(v)
	}
	return total
}

// getByteSize returns the size of the given value in bytes.
// This is a rough estimate, but it's good enough for our purposes.
// Maps and slices are the most complex, so we'll just add up the sizes of their entries.
func getByteSize(val any) int {
	switch value := val.(type) {
	case bool:
		return 1
	case float64, int64, int:
		return 8
	case string:
		return len(value)
	case []byte: // also catch []uint8
		return len(value)
	case []any:
		total := 0
		for _, v := range value {
			total += getByteSize(v)
		}
		return total
	case map[string]any:
		total := 0
		for k, v := range value {
			total += len(k) + getByteSize(v)
		}
		return total
	default:
		return 8 // catchall
	}
}

// MarshalJSON implements json.Marshaler to serialize the Payload as a single JSON object
// containing all fields from both memoizedFields and msgpMap. This is incredibly
// inefficient and is only here (for now) to support our legacy nested field implementation.
func (p Payload) MarshalJSON() ([]byte, error) {
	data := maps.Collect(p.All())
	maps.DeleteFunc(data, func(k string, v any) bool {
		return k == MetaTraceID
	})

	return json.Marshal(data)
}

// Implements msgpack.Marshaler.
// Inefficient, only here for test cases where we serialize a Payload field.
func (p Payload) MarshalMsgpack() ([]byte, error) {
	return p.MarshalMsg(nil)
}

// Implements msgp.Marshaler.
// Appends marshaled payload to supplied buffer.
func (p Payload) MarshalMsg(buf []byte) ([]byte, error) {
	// Save the starting length of the buffer
	startLen := len(buf)

	// Reserve space for map16 header (always 3 bytes: 0xde + 2 byte count)
	// Note that for <16 elements, a single-byte header could be used instead,
	// but to take advantage of that we'd have to do an expensive copy further
	// down. So, we'll just use the 3-byte header in all cases.
	buf = append(buf, 0xde, 0, 0)

	var actualCount uint32

	// Serialize metadata fields with non-default values
	for _, field := range metadataFields {
		if newBuf, ok := field.appendMsgp(&p, buf); ok {
			buf = newBuf
			actualCount++
		}
	}

	// Serialize regular memoized fields
	for key, value := range p.memoizedFields {
		buf = msgp.AppendString(buf, key)
		var err error
		buf, err = msgp.AppendIntf(buf, value)
		if err != nil {
			return buf, err
		}
		actualCount++
	}

	// Serialize msgpMap fields, skipping duplicates
	iter, err := p.msgpMap.Iterate()
	if err == nil {
		for {
			keyBytes, _, err := iter.NextKey()
			if err != nil {
				break
			}

			keyStr := string(keyBytes)
			// Skip if already serialized from memoizedFields
			if _, ok := p.memoizedFields[keyStr]; ok {
				continue
			}

			// Skip metadata fields as they're serialized separately
			if isMetadataField(keyStr) {
				continue
			}

			raw, err := iter.valueSerializedBytesZC()
			if err != nil {
				return buf, err
			}

			// Why AppendStringFromBytes? Because maps keys _can_ be a binary
			// type, but msgp expects them to be a string type. The fallback to
			// the binary read in ReadMapKeyZC which we use to read these
			// allocates garbage memory.
			buf = msgp.AppendStringFromBytes(buf, keyBytes)
			buf = append(buf, raw...)
			actualCount++
		}
	}

	// Check that we don't exceed the map16 limit
	if actualCount > 65535 {
		return buf, fmt.Errorf("payload has %d fields, exceeds msgpack map16 limit of 65535", actualCount)
	}

	// Write the actual count into the reserved bytes (big-endian)
	buf[startLen+1] = byte(actualCount >> 8)
	buf[startLen+2] = byte(actualCount)

	return buf, nil
}

// TODO implement Sizer so buffer can be correctly presized

// For debugging purposes
func (p Payload) String() string {
	buf, _ := p.MarshalJSON()
	return string(buf)
}

// When trying to find a particular []byte in a slice of strings, we could use
// slices.Contains, but this involves casting the []byte to a string which does
// a heap allocation. This is cheaper.
func sliceContains(in []string, find []byte) bool {
	for i := range in {
		if bytes.Equal([]byte(in[i]), find) {
			return true
		}
	}
	return false
}

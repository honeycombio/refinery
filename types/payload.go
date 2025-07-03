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

	// These fields are not used by the refinery itself for sampling decisions.
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
	expectedType  FieldType
	get           func(p *Payload) (value any, ok bool)               // Payload.Get, Payload.All
	set           func(p *Payload, value any)                         // Payload.Set
	exist         func(p *Payload) bool                               // Payload.Exists
	appendMsgp    func(p *Payload, in []byte) (out []byte, ok bool)   // Payload.MarshalMsg
	unmarshalMsgp func(p *Payload, in []byte) (out []byte, err error) // Payload.extractMetadataFromBytes
}

var metadataFields = map[string]metadataField{
	MetaSignalType:                stringField(MetaSignalType, func(p *Payload) *string { return &p.MetaSignalType }),
	MetaTraceID:                   stringField(MetaTraceID, func(p *Payload) *string { return &p.MetaTraceID }),
	MetaAnnotationType:            stringField(MetaAnnotationType, func(p *Payload) *string { return &p.MetaAnnotationType }),
	MetaRefineryProbe:             boolField(MetaRefineryProbe, func(p *Payload) *nullableBool { return &p.MetaRefineryProbe }),
	MetaRefineryRoot:              boolField(MetaRefineryRoot, func(p *Payload) *nullableBool { return &p.MetaRefineryRoot }),
	MetaRefineryIncomingUserAgent: stringField(MetaRefineryIncomingUserAgent, func(p *Payload) *string { return &p.MetaRefineryIncomingUserAgent }),
	MetaRefinerySendBy:            int64Field(MetaRefinerySendBy, func(p *Payload) *int64 { return &p.MetaRefinerySendBy }),
	MetaRefinerySpanDataSize:      int64Field(MetaRefinerySpanDataSize, func(p *Payload) *int64 { return &p.MetaRefinerySpanDataSize }),
	MetaRefineryMinSpan:           boolField(MetaRefineryMinSpan, func(p *Payload) *nullableBool { return &p.MetaRefineryMinSpan }),
	MetaRefineryForwarded:         stringField(MetaRefineryForwarded, func(p *Payload) *string { return &p.MetaRefineryForwarded }),
	MetaRefineryExpiredTrace:      boolField(MetaRefineryExpiredTrace, func(p *Payload) *nullableBool { return &p.MetaRefineryExpiredTrace }),

	MetaRefineryLocalHostname:      stringField(MetaRefineryLocalHostname, func(p *Payload) *string { return &p.MetaRefineryLocalHostname }),
	MetaStressed:                   boolField(MetaStressed, func(p *Payload) *nullableBool { return &p.MetaStressed }),
	MetaRefineryReason:             stringField(MetaRefineryReason, func(p *Payload) *string { return &p.MetaRefineryReason }),
	MetaRefinerySendReason:         stringField(MetaRefinerySendReason, func(p *Payload) *string { return &p.MetaRefinerySendReason }),
	MetaSpanEventCount:             int64Field(MetaSpanEventCount, func(p *Payload) *int64 { return &p.MetaSpanEventCount }),
	MetaSpanLinkCount:              int64Field(MetaSpanLinkCount, func(p *Payload) *int64 { return &p.MetaSpanLinkCount }),
	MetaSpanCount:                  int64Field(MetaSpanCount, func(p *Payload) *int64 { return &p.MetaSpanCount }),
	MetaEventCount:                 int64Field(MetaEventCount, func(p *Payload) *int64 { return &p.MetaEventCount }),
	MetaRefineryOriginalSampleRate: int64Field(MetaRefineryOriginalSampleRate, func(p *Payload) *int64 { return &p.MetaRefineryOriginalSampleRate }),
	MetaRefineryShutdownSend:       boolField(MetaRefineryShutdownSend, func(p *Payload) *nullableBool { return &p.MetaRefineryShutdownSend }),
	MetaRefinerySampleKey:          stringField(MetaRefinerySampleKey, func(p *Payload) *string { return &p.MetaRefinerySampleKey }),
}

// Helpers to set up metadataField entries based on the supplied key and
// ptr function to get access to the correct field.
func stringField(key string, ptr func(*Payload) *string) metadataField {
	return metadataField{
		expectedType: FieldTypeString,
		get: func(p *Payload) (value any, ok bool) {
			strPtr := ptr(p)
			if *strPtr != "" {
				return *strPtr, true
			}
			return nil, false
		},
		set: func(p *Payload, value any) {
			if v, ok := value.(string); ok {
				strPtr := ptr(p)
				*strPtr = v
			}
		},
		exist: func(p *Payload) bool {
			strPtr := ptr(p)
			return *strPtr != ""
		},
		appendMsgp: func(p *Payload, in []byte) (out []byte, ok bool) {
			strPtr := ptr(p)
			if *strPtr != "" {
				out = msgp.AppendString(in, key)
				out = msgp.AppendString(out, *strPtr)
				return out, true
			}
			return in, false
		},
		unmarshalMsgp: func(p *Payload, in []byte) (out []byte, err error) {
			strPtr := ptr(p)
			*strPtr, out, err = msgp.ReadStringBytes(in)
			return out, err
		},
	}
}

func boolField(key string, ptr func(*Payload) *nullableBool) metadataField {
	return metadataField{
		expectedType: FieldTypeBool,
		get: func(p *Payload) (value any, ok bool) {
			boolPtr := ptr(p)
			if boolPtr.HasValue {
				return boolPtr.Value, true
			}
			return nil, false
		},
		set: func(p *Payload, value any) {
			if v, ok := value.(bool); ok {
				boolPtr := ptr(p)
				boolPtr.Set(v)
			}
		},
		exist: func(p *Payload) bool {
			boolPtr := ptr(p)
			return boolPtr.HasValue
		},
		appendMsgp: func(p *Payload, in []byte) (out []byte, ok bool) {
			boolPtr := ptr(p)
			if boolPtr.HasValue {
				out = msgp.AppendString(in, key)
				out = msgp.AppendBool(out, boolPtr.Value)
				return out, true
			}
			return in, false
		},
		unmarshalMsgp: func(p *Payload, in []byte) (out []byte, err error) {
			var val bool
			val, out, err = msgp.ReadBoolBytes(in)
			if err == nil {
				boolPtr := ptr(p)
				boolPtr.Set(val)
			}
			return out, err
		},
	}
}

func int64Field(key string, ptr func(*Payload) *int64) metadataField {
	return metadataField{
		expectedType: FieldTypeInt64,
		get: func(p *Payload) (value any, ok bool) {
			intPtr := ptr(p)
			if *intPtr != 0 {
				return *intPtr, true
			}
			return nil, false
		},
		set: func(p *Payload, value any) {
			if v, ok := value.(int64); ok {
				intPtr := ptr(p)
				*intPtr = v
			}
		},
		exist: func(p *Payload) bool {
			intPtr := ptr(p)
			return *intPtr != 0
		},
		appendMsgp: func(p *Payload, in []byte) (out []byte, ok bool) {
			intPtr := ptr(p)
			if *intPtr != 0 {
				out = msgp.AppendString(in, key)
				out = msgp.AppendInt64(out, *intPtr)
				return out, true
			}
			return in, false
		},
		unmarshalMsgp: func(p *Payload, in []byte) (out []byte, err error) {
			intPtr := ptr(p)
			*intPtr, out, err = msgp.ReadInt64Bytes(in)
			return out, err
		},
	}
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
			if field, ok := metadataFields[string(keyBytes)]; ok {
				// Check if field matches the expected type
				var typeIsCorrect bool
				switch field.expectedType {
				case FieldTypeString:
					typeIsCorrect = valueType == msgp.StrType || valueType == msgp.BinType
				case FieldTypeBool:
					typeIsCorrect = valueType == msgp.BoolType
				case FieldTypeInt64:
					typeIsCorrect = valueType == msgp.IntType || valueType == msgp.UintType
				}
				if typeIsCorrect {
					remaining, err = field.unmarshalMsgp(p, remaining)
					if err != nil {
						return len(data) - len(remaining), fmt.Errorf("failed to read value for key %s: %w", string(keyBytes), err)
					}
					handled = true
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
			if field, ok := metadataFields[key]; ok {
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

// UnmarshalMsgpack implements msgpack.Unmarshaler, but doesn't unmarshal.
// Instead it keeps a copy of the serialized data.
func (p *Payload) UnmarshalMsgpack(data []byte) error {
	p.msgpMap = MsgpPayloadMap{rawData: slices.Clone(data)}
	p.ExtractMetadata()
	return nil
}

// UnmarshalMsg implements msgp.Unmarshaler, similar to above but expects to be
// part of a larger message. Makes a local copy of the bytes it's hanging onto.
func (p *Payload) UnmarshalMsg(bts []byte) (o []byte, err error) {
	// Extract metadata and get consumed bytes
	consumed, err := p.extractMetadataFromBytes(bts)
	if err != nil {
		return nil, err
	}

	// Store the raw data
	ourData := slices.Clone(bts[:consumed])
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

			// Use Set here so we'll prefer metadata fields where appropriate.
			p.Set(key, value)
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
	if strings.HasPrefix(key, "meta.") {
		if field, ok := metadataFields[key]; ok {
			return field.exist(p)
		}
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
		if field, ok := metadataFields[key]; ok {
			value, _ := field.get(p)
			return value
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
	if field, ok := metadataFields[key]; ok {
		field.set(p, value)
		return
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
		for key, field := range metadataFields {
			if key == MetaTraceID {
				continue
			}
			if key == MetaRefineryRoot && !p.MetaRefineryRoot.Value {
				// Skip the root field if it's false, as it doesn't need to be yielded
				continue
			}

			if value, ok := field.get(p); ok {
				if !yield(key, value) {
					return
				}
			}
		}

		// Then yield memoized fields
		for key, value := range p.memoizedFields {
			// Skip metadata fields.
			if _, ok := metadataFields[key]; ok {
				continue
			}
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
			if _, ok := metadataFields[key]; ok {
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
		// Skip metadata fields as they're serialized separately
		if _, ok := metadataFields[key]; ok {
			continue
		}

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
			if _, ok := metadataFields[keyStr]; ok {
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
